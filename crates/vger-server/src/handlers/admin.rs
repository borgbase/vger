use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use crate::error::ServerError;
use crate::state::{read_unpoisoned, AppState};

static REPACK_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    pub list: Option<String>,
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
    if query.list.is_some() {
        return repo_list_all(state, &repo).await;
    }
    Err(ServerError::BadRequest(
        "missing query parameter (stats, verify-structure, list)".into(),
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

    let last_backup = read_unpoisoned(&state.inner.last_backup_at, "last_backup_at")
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

/// GET /{repo}?list — list all keys in the repository.
async fn repo_list_all(state: AppState, repo: &str) -> Result<Response, ServerError> {
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
    let keys = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<String>> {
        let mut keys = Vec::new();
        list_all_recursive(&repo_dir_clone, &repo_dir_clone, &mut keys)?;
        keys.sort();
        Ok(keys)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    Ok(axum::Json(keys).into_response())
}

/// Recursively list all files under `dir`, producing keys relative to `root`.
fn list_all_recursive(
    dir: &std::path::Path,
    root: &std::path::Path,
    out: &mut Vec<String>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            list_all_recursive(&path, root, out)?;
        } else if let Ok(rel) = path.strip_prefix(root) {
            // Use forward slashes for storage keys regardless of platform.
            let key: String = rel.iter().map(|c| c.to_string_lossy()).collect::<Vec<_>>().join("/");
            out.push(key);
        }
    }
    Ok(())
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
    use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

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

        // Open source pack
        let mut source = std::fs::File::open(&source_path)
            .map_err(|e| format!("open {}: {e}", op.source_pack))?;
        let source_len = source
            .metadata()
            .map_err(|e| format!("stat {}: {e}", op.source_pack))?
            .len();

        // Create temp file for streaming write
        let temp_id = REPACK_TEMP_COUNTER.fetch_add(1, Relaxed);
        let temp_path = source_path.with_file_name(format!(".repack_tmp.{temp_id}"));

        // Write new pack to temp file. Collect write errors so we can
        // drop the file handle before cleanup (required on Windows).
        let temp_file =
            std::fs::File::create(&temp_path).map_err(|e| format!("create temp: {e}"))?;
        let mut writer = BufWriter::new(temp_file);
        let mut hasher = Blake2bVar::new(32).expect("valid output size");

        let mut pack_offset: u64 = 9; // PACK_MAGIC(8) + version(1)
        let mut new_offsets = Vec::with_capacity(op.keep_blobs.len());
        let mut scratch = Vec::new();

        let write_result: Result<(), String> = (|| {
            // Write pack header: magic + version
            write_and_hash(&mut writer, &mut hasher, PACK_MAGIC)
                .map_err(|e| format!("write header: {e}"))?;
            write_and_hash(&mut writer, &mut hasher, &[1u8])
                .map_err(|e| format!("write version: {e}"))?;

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

                // Read blob from source into reusable scratch buffer
                scratch.resize(blob_ref.length as usize, 0);
                source
                    .seek(SeekFrom::Start(blob_ref.offset))
                    .map_err(|e| format!("seek: {e}"))?;
                source
                    .read_exact(&mut scratch)
                    .map_err(|e| format!("read: {e}"))?;

                // Write length prefix
                let blob_len = blob_ref.length as u32;
                write_and_hash(&mut writer, &mut hasher, &blob_len.to_le_bytes())
                    .map_err(|e| format!("write len: {e}"))?;

                // Record offset past the length prefix
                new_offsets.push(pack_offset + 4);

                // Write blob data
                write_and_hash(&mut writer, &mut hasher, &scratch)
                    .map_err(|e| format!("write blob: {e}"))?;

                pack_offset += 4 + blob_ref.length;
            }

            writer.flush().map_err(|e| format!("flush: {e}"))?;
            Ok(())
        })();

        // Drop writer (closes file handle) before any cleanup or rename
        drop(writer);

        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(e);
        }

        // Finalize hash → pack ID
        let pack_id_hex = finalize_blake2b_256_hex(hasher);
        let shard = &pack_id_hex[..2];
        let new_pack_key = format!("packs/{shard}/{pack_id_hex}");

        let new_pack_path = state.file_path(repo, &new_pack_key).ok_or_else(|| {
            let _ = std::fs::remove_file(&temp_path);
            "invalid new pack path".to_string()
        })?;

        if let Some(parent) = new_pack_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                format!("mkdir: {e}")
            })?;
        }

        std::fs::rename(&temp_path, &new_pack_path).map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            format!("rename new pack: {e}")
        })?;

        // Update quota
        state.add_quota_usage(repo, pack_offset);

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

/// Write data to writer and feed to hasher in one step.
fn write_and_hash(
    writer: &mut impl std::io::Write,
    hasher: &mut Blake2bVar,
    data: &[u8],
) -> std::io::Result<()> {
    writer.write_all(data)?;
    hasher.update(data);
    Ok(())
}

fn finalize_blake2b_256_hex(hasher: Blake2bVar) -> String {
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

                            // Check minimum size: magic(8) + version(1) = 9
                            if size < 9 {
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

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use blake2::digest::{Update, VariableOutput};
    use blake2::Blake2bVar;

    use super::super::test_helpers::*;
    use super::PACK_MAGIC;

    /// Build a minimal pack file: PACK_MAGIC + version(1) + [u32_le_len | blob]...
    /// Returns (pack bytes, vec of (offset, length) for each blob — offset points
    /// past the length prefix, matching what repack's keep_blobs expects).
    fn build_pack(blobs: &[&[u8]]) -> (Vec<u8>, Vec<(u64, u64)>) {
        let mut buf = Vec::new();
        buf.extend_from_slice(PACK_MAGIC);
        buf.push(0x01); // version

        let mut refs = Vec::new();
        for blob in blobs {
            let offset = buf.len() as u64;
            let len = blob.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(blob);
            // The repack code reads from the raw file offsets, so the BlobRef
            // offset should include the length prefix. We'll store the offset
            // of the length prefix and the total length (prefix + data) for
            // keep_blobs. Actually, looking at the repack code, keep_blobs
            // offset/length refer to arbitrary byte ranges in the source file.
            // The repack reads [offset..offset+length] verbatim and writes it
            // to the new pack with a new u32_le length prefix.
            //
            // For our tests, we want to keep just the blob data (without the
            // source length prefix), so offset = past the length prefix,
            // length = blob data length.
            refs.push((offset + 4, blob.len() as u64));
        }
        (buf, refs)
    }

    /// Write a pack file to disk and return its storage key (packs/<shard>/<hex>).
    fn write_pack(tmp: &std::path::Path, pack_bytes: &[u8]) -> String {
        // Hash the content to get the pack name (matches how repack hashes)
        let pack_id = blake2b_256_hex(pack_bytes);
        let shard = &pack_id[..2];
        let key = format!("packs/{shard}/{pack_id}");
        let path = tmp.join(TEST_REPO).join("packs").join(shard).join(&pack_id);
        std::fs::write(&path, pack_bytes).expect("write pack file");
        key
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

    fn repack_body(ops: &[serde_json::Value]) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "operations": ops })).unwrap()
    }

    fn repack_op(
        source_pack: &str,
        keep_blobs: &[(u64, u64)],
        delete_after: bool,
    ) -> serde_json::Value {
        let blobs: Vec<serde_json::Value> = keep_blobs
            .iter()
            .map(|(offset, length)| serde_json::json!({ "offset": offset, "length": length }))
            .collect();
        serde_json::json!({
            "source_pack": source_pack,
            "keep_blobs": blobs,
            "delete_after": delete_after,
        })
    }

    #[tokio::test]
    async fn repack_single_blob() {
        let (router, _state, tmp) = setup_app(0);

        let blob = b"hello world repack";
        let (pack_bytes, refs) = build_pack(&[blob]);
        let source_key = write_pack(tmp.path(), &pack_bytes);

        let body = repack_body(&[repack_op(&source_key, &refs, false)]);
        let resp = authed_post(router.clone(), &format!("/{TEST_REPO}?repack"), body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let completed = result["completed"].as_array().unwrap();
        assert_eq!(completed.len(), 1);

        let op = &completed[0];
        let new_pack_key = op["new_pack"].as_str().unwrap();
        assert!(new_pack_key.starts_with("packs/"));

        // Read the new pack file and verify magic + version
        let new_pack_path = tmp.path().join(TEST_REPO).join(new_pack_key);
        let new_pack_data = std::fs::read(&new_pack_path).expect("read new pack");
        assert_eq!(&new_pack_data[..8], PACK_MAGIC);
        assert_eq!(new_pack_data[8], 0x01);

        // Verify the returned offset points to the correct blob data
        let new_offset = op["new_offsets"][0].as_u64().unwrap();
        let blob_data = &new_pack_data[new_offset as usize..new_offset as usize + blob.len()];
        assert_eq!(blob_data, blob);
    }

    #[tokio::test]
    async fn repack_multiple_blobs_offsets() {
        let (router, _state, tmp) = setup_app(0);

        let blobs: Vec<&[u8]> = vec![b"first", b"second-blob", b"third!!"];
        let (pack_bytes, refs) = build_pack(&blobs);
        let source_key = write_pack(tmp.path(), &pack_bytes);

        let body = repack_body(&[repack_op(&source_key, &refs, false)]);
        let resp = authed_post(router, &format!("/{TEST_REPO}?repack"), body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        let new_pack_key = op["new_pack"].as_str().unwrap();
        let new_offsets: Vec<u64> = op["new_offsets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect();
        assert_eq!(new_offsets.len(), 3);

        let new_pack_path = tmp.path().join(TEST_REPO).join(new_pack_key);
        let new_pack_data = std::fs::read(&new_pack_path).expect("read new pack");

        // Each returned offset should read back the original blob
        for (i, blob) in blobs.iter().enumerate() {
            let off = new_offsets[i] as usize;
            let actual = &new_pack_data[off..off + blob.len()];
            assert_eq!(actual, *blob, "blob {i} mismatch");
        }
    }

    #[tokio::test]
    async fn repack_hash_matches_content() {
        let (router, _state, tmp) = setup_app(0);

        let (pack_bytes, refs) = build_pack(&[b"hash-check-data"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);

        let body = repack_body(&[repack_op(&source_key, &refs, false)]);
        let resp = authed_post(router, &format!("/{TEST_REPO}?repack"), body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let new_pack_key = result["completed"][0]["new_pack"].as_str().unwrap();

        // The pack key is packs/<shard>/<hex>. The hex should be blake2b-256 of contents.
        let pack_hex = new_pack_key.split('/').next_back().unwrap();
        let new_pack_path = tmp.path().join(TEST_REPO).join(new_pack_key);
        let new_pack_data = std::fs::read(&new_pack_path).expect("read new pack");
        let actual_hash = blake2b_256_hex(&new_pack_data);
        assert_eq!(actual_hash, pack_hex);
    }

    #[tokio::test]
    async fn repack_delete_source() {
        let (router, state, tmp) = setup_app(0);

        let (pack_bytes, refs) = build_pack(&[b"delete-me"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(TEST_REPO).join(&source_key);
        assert!(source_path.exists());

        // Seed quota with the source pack size
        state.add_quota_usage(TEST_REPO, pack_bytes.len() as u64);

        let body = repack_body(&[repack_op(&source_key, &refs, true)]);
        let resp = authed_post(router, &format!("/{TEST_REPO}?repack"), body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert!(op["deleted"].as_bool().unwrap());
        assert!(op["new_pack"].as_str().is_some());

        // Source should be gone
        assert!(!source_path.exists(), "source pack not deleted");
    }

    #[tokio::test]
    async fn repack_empty_keeps_deletes_only() {
        let (router, state, tmp) = setup_app(0);

        let (pack_bytes, _refs) = build_pack(&[b"going-away"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(TEST_REPO).join(&source_key);

        state.add_quota_usage(TEST_REPO, pack_bytes.len() as u64);
        let used_before = state.quota_used(TEST_REPO);

        // Repack with empty keep_blobs + delete_after
        let body = repack_body(&[repack_op(&source_key, &[], true)]);
        let resp = authed_post(router, &format!("/{TEST_REPO}?repack"), body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert!(op["new_pack"].is_null());
        assert!(op["deleted"].as_bool().unwrap());
        assert!(op["new_offsets"].as_array().unwrap().is_empty());

        assert!(!source_path.exists(), "source pack not deleted");
        assert!(
            state.quota_used(TEST_REPO) < used_before,
            "quota should have decreased"
        );
    }

    #[tokio::test]
    async fn list_all_keys_in_repo() {
        let (router, _state, _tmp) = setup_app(0);

        // PUT two known keys
        let resp = authed_put(
            router.clone(),
            &format!("/{TEST_REPO}/config"),
            b"cfg".to_vec(),
        )
        .await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = authed_put(
            router.clone(),
            &format!("/{TEST_REPO}/manifest"),
            b"man".to_vec(),
        )
        .await;
        assert_status(&resp, StatusCode::CREATED);

        // GET /{repo}?list should return both keys
        let resp = authed_get(router.clone(), &format!("/{TEST_REPO}?list")).await;
        assert_status(&resp, StatusCode::OK);

        let keys: Vec<String> =
            serde_json::from_slice(&body_bytes(resp).await).expect("parse JSON");
        assert!(
            keys.contains(&"config".to_string()),
            "expected 'config' in {keys:?}"
        );
        assert!(
            keys.contains(&"manifest".to_string()),
            "expected 'manifest' in {keys:?}"
        );
    }
}
