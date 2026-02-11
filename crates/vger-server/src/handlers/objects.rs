use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::AppState;

#[derive(serde::Deserialize, Default)]
pub struct ObjectQuery {
    pub list: Option<String>,
    pub mkdir: Option<String>,
}

/// GET /{repo}/{*path} — if ?list present, list keys; otherwise read object.
/// Supports Range header for partial reads.
pub async fn get_or_list(
    State(state): State<AppState>,
    Path((repo, key)): Path<(String, String)>,
    Query(query): Query<ObjectQuery>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    if query.list.is_some() {
        return list_keys(state, &repo, &key).await;
    }

    let file_path = state
        .file_path(&repo, &key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    if !file_path.exists() {
        return Err(ServerError::NotFound(key));
    }

    // Check for Range header
    if let Some(range_header) = headers.get("Range").and_then(|v| v.to_str().ok()) {
        return handle_range_read(&file_path, range_header).await;
    }

    let data = tokio::task::spawn_blocking(move || std::fs::read(file_path))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::from)?;

    Ok((StatusCode::OK, data).into_response())
}

/// HEAD /{repo}/{*path} — check existence, return Content-Length.
pub async fn head_object(
    State(state): State<AppState>,
    Path((repo, key)): Path<(String, String)>,
) -> Result<Response, ServerError> {
    let file_path = state
        .file_path(&repo, &key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let meta = match tokio::fs::metadata(&file_path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
        Err(e) => return Err(ServerError::from(e)),
    };

    Ok((
        StatusCode::OK,
        [("Content-Length", meta.len().to_string())],
        Body::empty(),
    )
        .into_response())
}

/// PUT /{repo}/{*path} — write object. Enforces append-only and quota.
pub async fn put_object(
    State(state): State<AppState>,
    Path((repo, key)): Path<(String, String)>,
    _headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    let file_path = state
        .file_path(&repo, &key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    // Append-only: reject overwrites of pack files
    if state.inner.config.append_only && key.starts_with("packs/") && file_path.exists() {
        return Err(ServerError::Forbidden(
            "append-only: cannot overwrite pack files".into(),
        ));
    }

    // Quota enforcement
    let quota = state.inner.config.quota_bytes;
    if quota > 0 {
        let used = state.quota_used(&repo);
        if used + body.len() as u64 > quota {
            return Err(ServerError::PayloadTooLarge(format!(
                "quota exceeded: used {used}, limit {quota}, request {}",
                body.len()
            )));
        }
    }

    // Track old file size for quota accounting
    let old_size = file_path.metadata().map(|m| m.len()).unwrap_or(0);

    // Ensure parent directory exists
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(ServerError::from)?;
    }

    let data_len = body.len() as u64;
    tokio::task::spawn_blocking(move || std::fs::write(file_path, &body))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::from)?;

    // Update quota
    if data_len > old_size {
        state.add_quota_usage(&repo, data_len - old_size);
    } else {
        state.sub_quota_usage(&repo, old_size - data_len);
    }

    // Detect manifest write → record backup timestamp
    if key == "manifest" {
        state.record_backup(&repo);
    }

    let status = if old_size > 0 {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::CREATED
    };
    Ok(status.into_response())
}

/// DELETE /{repo}/{*path} — delete object. Rejected in append-only mode.
pub async fn delete_object(
    State(state): State<AppState>,
    Path((repo, key)): Path<(String, String)>,
) -> Result<Response, ServerError> {
    if state.inner.config.append_only {
        return Err(ServerError::Forbidden(
            "append-only: delete not allowed".into(),
        ));
    }

    let file_path = state
        .file_path(&repo, &key)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let old_size = file_path.metadata().map(|m| m.len()).unwrap_or(0);

    match tokio::fs::remove_file(&file_path).await {
        Ok(()) => {
            state.sub_quota_usage(&repo, old_size);
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(StatusCode::NOT_FOUND.into_response())
        }
        Err(e) => Err(ServerError::from(e)),
    }
}

/// POST /{repo}/{*path}?mkdir — create directory.
pub async fn post_object(
    State(state): State<AppState>,
    Path((repo, key)): Path<(String, String)>,
    Query(query): Query<ObjectQuery>,
) -> Result<Response, ServerError> {
    if query.mkdir.is_some() {
        let dir_path = state
            .file_path(&repo, &key)
            .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
        tokio::fs::create_dir_all(&dir_path)
            .await
            .map_err(ServerError::from)?;
        return Ok(StatusCode::CREATED.into_response());
    }

    Ok(StatusCode::BAD_REQUEST.into_response())
}

async fn list_keys(
    state: AppState,
    repo: &str,
    prefix: &str,
) -> Result<Response, ServerError> {
    let dir_path = state
        .file_path(repo, prefix)
        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

    let prefix_owned = prefix.to_string();
    let keys = tokio::task::spawn_blocking(move || list_files_recursive(&dir_path, &prefix_owned))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(keys).into_response())
}

fn list_files_recursive(dir: &std::path::Path, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let full_key = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix.trim_end_matches('/'), name)
            };
            if path.is_dir() {
                keys.extend(list_files_recursive(&path, &full_key));
            } else {
                keys.push(full_key);
            }
        }
    }
    keys
}

async fn handle_range_read(
    file_path: &std::path::Path,
    range_header: &str,
) -> Result<Response, ServerError> {
    // Parse "bytes=<start>-<end>"
    let range_str = range_header
        .strip_prefix("bytes=")
        .ok_or_else(|| ServerError::BadRequest("invalid Range header".into()))?;

    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return Err(ServerError::BadRequest("invalid Range header".into()));
    }

    let start: u64 = parts[0]
        .parse()
        .map_err(|_| ServerError::BadRequest("invalid range start".into()))?;
    let end: u64 = parts[1]
        .parse()
        .map_err(|_| ServerError::BadRequest("invalid range end".into()))?;

    let length = end - start + 1;
    let file_path = file_path.to_path_buf();

    let data = tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(&file_path)?;
        let file_len = f.metadata()?.len();
        if start >= file_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "range start beyond file size",
            ));
        }
        f.seek(SeekFrom::Start(start))?;
        let to_read = length.min(file_len - start) as usize;
        let mut buf = vec![0u8; to_read];
        f.read_exact(&mut buf)?;
        Ok::<_, std::io::Error>((buf, file_len))
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    let (buf, file_len) = data;
    let actual_end = start + buf.len() as u64 - 1;

    Ok((
        StatusCode::PARTIAL_CONTENT,
        [
            (
                "Content-Range",
                format!("bytes {start}-{actual_end}/{file_len}"),
            ),
            ("Content-Length", buf.len().to_string()),
        ],
        buf,
    )
        .into_response())
}
