use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use crate::error::ServerError;
use crate::state::AppState;

const PACK_MAGIC: &[u8; 8] = b"VGERPACK";
const MAX_REPACK_OPS: usize = 10_000;
const MAX_KEEP_BLOBS_PER_OP: usize = 200_000;

#[derive(serde::Deserialize, Default)]
pub struct RepoQuery {
    pub stats: Option<String>,
    #[serde(rename = "verify-structure")]
    pub verify_structure: Option<String>,
    pub init: Option<String>,
    #[serde(rename = "batch-delete")]
    pub batch_delete: Option<String>,
    pub repack: Option<String>,
}

/// GET /{repo} — dispatches based on query parameter.
pub async fn repo_dispatch(
    State(state): State<AppState>,
    Path(repo): Path<String>,
    Query(query): Query<RepoQuery>,
) -> Result<Response, ServerError> {
    if query.stats.is_some() {
        return repo_stats(state, &repo).await;
    }
    if query.verify_structure.is_some() {
        return verify_structure(state, &repo).await;
    }
    Err(ServerError::BadRequest(
        "missing query parameter (stats, verify-structure)".into(),
    ))
}

/// POST /{repo} — dispatches based on query parameter.
pub async fn repo_action_dispatch(
    State(state): State<AppState>,
    Path(repo): Path<String>,
    Query(query): Query<RepoQuery>,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    if query.init.is_some() {
        return repo_init(state, &repo).await;
    }
    if query.batch_delete.is_some() {
        return batch_delete(state, &repo, body).await;
    }
    if query.repack.is_some() {
        return repack(state, &repo, body).await;
    }
    Err(ServerError::BadRequest(
        "missing query parameter (init, batch-delete, repack)".into(),
    ))
}

/// GET / — list all repos.
pub async fn list_repos(State(state): State<AppState>) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();
    let repos = tokio::task::spawn_blocking(move || {
        let mut names = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        names.push(name.to_string());
                    }
                }
            }
        }
        names.sort();
        names
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(repos).into_response())
}

/// GET /health — unauthenticated health check.
pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let uptime = state.inner.start_time.elapsed().as_secs();

    // Count repos
    let data_dir = state.inner.data_dir.clone();
    let repo_count = tokio::task::spawn_blocking(move || {
        std::fs::read_dir(&data_dir)
            .map(|entries| entries.flatten().filter(|e| e.path().is_dir()).count())
            .unwrap_or(0)
    })
    .await
    .unwrap_or(0);

    // Get disk free space
    let disk_free = get_disk_free(&state.inner.data_dir);

    axum::Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": uptime,
        "disk_free_bytes": disk_free,
        "repos": repo_count,
    }))
}

async fn repo_stats(state: AppState, repo: &str) -> Result<Response, ServerError> {
    let repo_dir = state
        .repo_path(repo)
        .ok_or_else(|| ServerError::BadRequest("invalid repo".into()))?;

    match tokio::fs::metadata(&repo_dir).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ServerError::NotFound(format!("repo '{repo}' not found")));
        }
        Err(e) => return Err(ServerError::from(e)),
    }

    let repo_name = repo.to_string();
    let repo_dir_clone = repo_dir.clone();

    let (total_bytes, total_objects, total_packs) =
        tokio::task::spawn_blocking(move || count_repo_stats(&repo_dir_clone))
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?;

    let last_backup = state
        .inner
        .last_backup_at
        .read()
        .unwrap()
        .get(&repo_name)
        .cloned();

    let quota_bytes = state.inner.config.quota_bytes;
    let quota_used = state.quota_used(&repo_name);

    Ok(axum::Json(serde_json::json!({
        "total_bytes": total_bytes,
        "total_objects": total_objects,
        "total_packs": total_packs,
        "last_backup_at": last_backup,
        "quota_bytes": quota_bytes,
        "quota_used_bytes": quota_used,
    }))
    .into_response())
}

async fn repo_init(state: AppState, repo: &str) -> Result<Response, ServerError> {
    let repo_dir = state
        .repo_path(repo)
        .ok_or_else(|| ServerError::BadRequest("invalid repo".into()))?;

    match tokio::fs::metadata(&repo_dir).await {
        Ok(_) => {
            return Err(ServerError::Conflict(format!(
                "repo '{repo}' already exists"
            )));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(ServerError::from(e)),
    }

    let repo_dir_clone = repo_dir.clone();
    tokio::task::spawn_blocking(move || {
        // Create base dirs
        std::fs::create_dir_all(&repo_dir_clone)?;
        std::fs::create_dir_all(repo_dir_clone.join("keys"))?;
        std::fs::create_dir_all(repo_dir_clone.join("snapshots"))?;
        std::fs::create_dir_all(repo_dir_clone.join("locks"))?;
        // Create 256 pack shard directories
        for i in 0..=255u8 {
            std::fs::create_dir_all(repo_dir_clone.join("packs").join(format!("{i:02x}")))?;
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    Ok(StatusCode::CREATED.into_response())
}

async fn batch_delete(
    state: AppState,
    repo: &str,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    if state.inner.config.append_only {
        return Err(ServerError::Forbidden(
            "append-only: batch-delete not allowed".into(),
        ));
    }

    let keys: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid JSON: {e}")))?;

    let repo_name = repo.to_string();
    let state_clone = state.clone();

    for key in keys {
        let file_path = state_clone
            .file_path(&repo_name, &key)
            .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;

        let old_size = match tokio::fs::metadata(&file_path).await {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(ServerError::from(e)),
        };
        if let Err(e) = tokio::fs::remove_file(&file_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(ServerError::from(e));
            }
        } else {
            state_clone.sub_quota_usage(&repo_name, old_size);
        }
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn repack(
    state: AppState,
    repo: &str,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    let plan: RepackPlan = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid repack plan: {e}")))?;
    validate_repack_plan(&plan)?;

    if state.inner.config.append_only && plan.operations.iter().any(|op| op.delete_after) {
        return Err(ServerError::Forbidden(
            "append-only: repack with delete not allowed".into(),
        ));
    }

    let repo_name = repo.to_string();
    let state_clone = state.clone();

    let results =
        tokio::task::spawn_blocking(move || execute_repack(&state_clone, &repo_name, &plan))
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?
            .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(results).into_response())
}

async fn verify_structure(state: AppState, repo: &str) -> Result<Response, ServerError> {
    let repo_dir = state
        .repo_path(repo)
        .ok_or_else(|| ServerError::BadRequest("invalid repo".into()))?;

    match tokio::fs::metadata(&repo_dir).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ServerError::NotFound(format!("repo '{repo}' not found")));
        }
        Err(e) => return Err(ServerError::from(e)),
    }

    let repo_dir_clone = repo_dir.clone();
    let result = tokio::task::spawn_blocking(move || check_structure(&repo_dir_clone))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(result).into_response())
}

// --- Repack types and logic ---

#[derive(serde::Deserialize)]
struct RepackPlan {
    operations: Vec<RepackOperation>,
}

#[derive(serde::Deserialize)]
struct RepackOperation {
    source_pack: String,
    keep_blobs: Vec<BlobRef>,
    delete_after: bool,
}

#[derive(serde::Deserialize)]
struct BlobRef {
    offset: u64,
    length: u64,
}

#[derive(serde::Serialize)]
struct RepackResult {
    completed: Vec<RepackOpResult>,
}

#[derive(serde::Serialize)]
struct RepackOpResult {
    source_pack: String,
    new_pack: Option<String>,
    /// Offset of each kept blob in the new pack (in order of keep_blobs).
    new_offsets: Vec<u64>,
    deleted: bool,
}

fn execute_repack(state: &AppState, repo: &str, plan: &RepackPlan) -> Result<RepackResult, String> {
    use std::io::{Read, Seek, SeekFrom};

    let mut completed = Vec::new();

    for op in &plan.operations {
        let source_path = state
            .file_path(repo, &op.source_pack)
            .ok_or_else(|| "invalid pack path".to_string())?;

        if op.keep_blobs.is_empty() {
            // Just delete the pack
            if op.delete_after {
                let old_size = source_path.metadata().map(|m| m.len()).unwrap_or(0);
                let _ = std::fs::remove_file(&source_path);
                state.sub_quota_usage(repo, old_size);
            }
            completed.push(RepackOpResult {
                source_pack: op.source_pack.clone(),
                new_pack: None,
                new_offsets: vec![],
                deleted: op.delete_after,
            });
            continue;
        }

        // Read live blobs from source pack
        let mut source = std::fs::File::open(&source_path)
            .map_err(|e| format!("open {}: {e}", op.source_pack))?;
        let source_len = source
            .metadata()
            .map_err(|e| format!("stat {}: {e}", op.source_pack))?
            .len();

        let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(op.keep_blobs.len());
        for blob_ref in &op.keep_blobs {
            if blob_ref.length == 0 {
                return Err("repack blob length must be > 0".to_string());
            }
            let end = blob_ref
                .offset
                .checked_add(blob_ref.length)
                .ok_or_else(|| "repack blob range overflow".to_string())?;
            if end > source_len {
                return Err(format!(
                    "repack blob range out of bounds: offset={} length={} file_size={source_len}",
                    blob_ref.offset, blob_ref.length
                ));
            }
            source
                .seek(SeekFrom::Start(blob_ref.offset))
                .map_err(|e| format!("seek: {e}"))?;
            let mut buf = vec![0u8; blob_ref.length as usize];
            source
                .read_exact(&mut buf)
                .map_err(|e| format!("read: {e}"))?;
            blobs.push(buf);
        }

        // Write new pack (magic + version + length-prefixed blobs, no trailing header)
        let mut new_pack_data = Vec::new();
        new_pack_data.extend_from_slice(PACK_MAGIC);
        new_pack_data.push(1u8); // version

        let mut new_offsets = Vec::with_capacity(blobs.len());
        for blob in &blobs {
            let blob_len = blob.len() as u32;
            let offset = new_pack_data.len() as u64 + 4; // past the length prefix
            new_pack_data.extend_from_slice(&blob_len.to_le_bytes());
            new_pack_data.extend_from_slice(blob);
            new_offsets.push(offset);
        }

        // Compute pack ID from pack contents
        let pack_id_hex = blake2b_256_hex(&new_pack_data);
        let shard = &pack_id_hex[..2];
        let new_pack_key = format!("packs/{shard}/{pack_id_hex}");

        let new_pack_path = state
            .file_path(repo, &new_pack_key)
            .ok_or_else(|| "invalid new pack path".to_string())?;

        if let Some(parent) = new_pack_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
        }

        std::fs::write(&new_pack_path, &new_pack_data)
            .map_err(|e| format!("write new pack: {e}"))?;

        // Update quota
        state.add_quota_usage(repo, new_pack_data.len() as u64);

        // Delete source if requested
        if op.delete_after {
            let old_size = source_path.metadata().map(|m| m.len()).unwrap_or(0);
            let _ = std::fs::remove_file(&source_path);
            state.sub_quota_usage(repo, old_size);
        }

        completed.push(RepackOpResult {
            source_pack: op.source_pack.clone(),
            new_pack: Some(new_pack_key),
            new_offsets,
            deleted: op.delete_after,
        });
    }

    Ok(RepackResult { completed })
}

fn blake2b_256_hex(data: &[u8]) -> String {
    let mut hasher = Blake2bVar::new(32).expect("valid output size");
    hasher.update(data);
    let mut out = [0u8; 32];
    hasher
        .finalize_variable(&mut out)
        .expect("valid output buffer length");
    hex::encode(out)
}

fn validate_repack_plan(plan: &RepackPlan) -> Result<(), ServerError> {
    if plan.operations.len() > MAX_REPACK_OPS {
        return Err(ServerError::BadRequest(format!(
            "too many repack operations: {} (max {MAX_REPACK_OPS})",
            plan.operations.len()
        )));
    }
    for (idx, op) in plan.operations.iter().enumerate() {
        if !is_valid_pack_key(&op.source_pack) {
            return Err(ServerError::BadRequest(format!(
                "invalid source_pack at operation {idx}: {}",
                op.source_pack
            )));
        }
        if op.keep_blobs.len() > MAX_KEEP_BLOBS_PER_OP {
            return Err(ServerError::BadRequest(format!(
                "too many keep_blobs at operation {idx}: {} (max {MAX_KEEP_BLOBS_PER_OP})",
                op.keep_blobs.len()
            )));
        }
        for (blob_idx, blob) in op.keep_blobs.iter().enumerate() {
            if blob.length == 0 {
                return Err(ServerError::BadRequest(format!(
                    "blob length must be > 0 at operation {idx} blob {blob_idx}"
                )));
            }
            if blob.length > u32::MAX as u64 {
                return Err(ServerError::BadRequest(format!(
                    "blob length exceeds pack format max at operation {idx} blob {blob_idx}"
                )));
            }
            if blob.offset.checked_add(blob.length).is_none() {
                return Err(ServerError::BadRequest(format!(
                    "blob range overflow at operation {idx} blob {blob_idx}"
                )));
            }
        }
    }
    Ok(())
}

fn is_valid_pack_key(key: &str) -> bool {
    let parts: Vec<&str> = key.trim_matches('/').split('/').collect();
    if parts.len() != 3 || parts[0] != "packs" {
        return false;
    }
    parts[1].len() == 2
        && parts[1].chars().all(|c| c.is_ascii_hexdigit())
        && parts[2].len() == 64
        && parts[2].chars().all(|c| c.is_ascii_hexdigit())
}

fn check_structure(repo_dir: &std::path::Path) -> serde_json::Value {
    let mut errors: Vec<String> = Vec::new();
    let mut pack_count = 0u64;
    let mut total_size = 0u64;

    // Check required files
    for required in &["config", "manifest", "index", "keys/repokey"] {
        let path = repo_dir.join(required);
        if !path.exists() {
            errors.push(format!("missing required file: {required}"));
        }
    }

    // Check pack shard structure
    let packs_dir = repo_dir.join("packs");
    if packs_dir.exists() {
        if let Ok(shards) = std::fs::read_dir(&packs_dir) {
            for shard_entry in shards.flatten() {
                let shard_name = shard_entry.file_name().to_string_lossy().to_string();

                // Verify shard is 2-char hex
                if shard_name.len() != 2 || !shard_name.chars().all(|c| c.is_ascii_hexdigit()) {
                    errors.push(format!("invalid shard directory: packs/{shard_name}"));
                    continue;
                }

                if let Ok(packs) = std::fs::read_dir(shard_entry.path()) {
                    for pack_entry in packs.flatten() {
                        let pack_name = pack_entry.file_name().to_string_lossy().to_string();
                        pack_count += 1;

                        // Verify pack name is 64-char hex
                        if pack_name.len() != 64
                            || !pack_name.chars().all(|c| c.is_ascii_hexdigit())
                        {
                            errors
                                .push(format!("invalid pack name: packs/{shard_name}/{pack_name}"));
                        }

                        let meta = pack_entry.metadata();
                        if let Ok(meta) = meta {
                            let size = meta.len();
                            total_size += size;

                            // Check minimum size (magic 8 + version 1 + header_len 4 = 13)
                            if size < 13 {
                                errors.push(format!(
                                    "pack too small ({size} bytes): packs/{shard_name}/{pack_name}"
                                ));
                            } else {
                                // Check magic bytes
                                if let Ok(data) = std::fs::read(pack_entry.path()) {
                                    if &data[..8] != PACK_MAGIC {
                                        errors.push(format!(
                                            "invalid pack magic: packs/{shard_name}/{pack_name}"
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Check for stale locks
    let locks_dir = repo_dir.join("locks");
    let stale_locks = if locks_dir.exists() {
        std::fs::read_dir(&locks_dir)
            .map(|entries| entries.flatten().count())
            .unwrap_or(0)
    } else {
        0
    };

    serde_json::json!({
        "ok": errors.is_empty(),
        "errors": errors,
        "pack_count": pack_count,
        "total_size": total_size,
        "stale_locks": stale_locks,
    })
}

fn count_repo_stats(repo_dir: &std::path::Path) -> (u64, u64, u64) {
    let mut total_bytes = 0u64;
    let mut total_objects = 0u64;
    let mut total_packs = 0u64;

    fn walk(dir: &std::path::Path, bytes: &mut u64, objects: &mut u64) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, bytes, objects);
                } else if let Ok(meta) = path.metadata() {
                    *bytes += meta.len();
                    *objects += 1;
                }
            }
        }
    }

    walk(repo_dir, &mut total_bytes, &mut total_objects);

    // Count packs specifically
    let packs_dir = repo_dir.join("packs");
    if packs_dir.exists() {
        fn count_packs(dir: &std::path::Path, count: &mut u64) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        count_packs(&entry.path(), count);
                    } else {
                        *count += 1;
                    }
                }
            }
        }
        count_packs(&packs_dir, &mut total_packs);
    }

    (total_bytes, total_objects, total_packs)
}

fn get_disk_free(path: &std::path::Path) -> u64 {
    use sysinfo::Disks;
    let disks = Disks::new_with_refreshed_list();
    for disk in disks.list() {
        if path.starts_with(disk.mount_point()) {
            return disk.available_space();
        }
    }
    0
}
