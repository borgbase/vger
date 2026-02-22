use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use vger_protocol::{
    check_protocol_version, is_valid_pack_key, validate_blob_ref, RepackOperationResult,
    RepackPlanRequest, RepackResultResponse, VerifyBlobRef as ProtoVerifyBlobRef,
    VerifyPackRequest, VerifyPackResult, VerifyPacksPlanRequest, VerifyPacksResponse,
    PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_CURRENT, PACK_VERSION_MAX, PACK_VERSION_MIN,
};

use crate::error::ServerError;
use crate::state::{read_unpoisoned, AppState};

static REPACK_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    #[serde(rename = "verify-packs")]
    pub verify_packs: Option<String>,
    pub list: Option<String>,
}

/// GET / — dispatches based on query parameter.
pub async fn repo_dispatch(
    State(state): State<AppState>,
    Query(query): Query<RepoQuery>,
) -> Result<Response, ServerError> {
    if query.stats.is_some() {
        return repo_stats(state).await;
    }
    if query.verify_structure.is_some() {
        return verify_structure(state).await;
    }
    if query.list.is_some() {
        return repo_list_all(state).await;
    }
    Err(ServerError::BadRequest(
        "missing query parameter (stats, verify-structure, list)".into(),
    ))
}

/// POST / — dispatches based on query parameter.
pub async fn repo_action_dispatch(
    State(state): State<AppState>,
    Query(query): Query<RepoQuery>,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    if query.init.is_some() {
        return repo_init(state).await;
    }
    if query.batch_delete.is_some() {
        return batch_delete(state, body).await;
    }
    if query.repack.is_some() {
        return repack(state, body).await;
    }
    if query.verify_packs.is_some() {
        return verify_packs(state, body).await;
    }
    Err(ServerError::BadRequest(
        "missing query parameter (init, batch-delete, repack, verify-packs)".into(),
    ))
}

/// GET /health — unauthenticated health check.
pub async fn health() -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn repo_stats(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    let (total_bytes, total_objects, total_packs) =
        tokio::task::spawn_blocking(move || count_repo_stats(&data_dir))
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?;

    let last_backup = *read_unpoisoned(&state.inner.last_backup_at, "last_backup_at");

    let quota_bytes = state.inner.config.quota_bytes;
    let quota_used = state.quota_used();

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

async fn repo_init(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    // Reject if data_dir contains unexpected entries
    let bad = crate::state::unexpected_entries(&data_dir);
    if !bad.is_empty() {
        return Err(ServerError::Conflict(format!(
            "data directory contains unexpected entries: {}",
            bad.join(", ")
        )));
    }

    // Check if already initialized (config file exists)
    let config_path = data_dir.join("config");
    match tokio::fs::metadata(&config_path).await {
        Ok(_) => {
            return Err(ServerError::Conflict("repo already initialized".into()));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(ServerError::from(e)),
    }

    let data_dir_clone = data_dir.clone();
    tokio::task::spawn_blocking(move || {
        // Create base dirs
        std::fs::create_dir_all(&data_dir_clone)?;
        std::fs::create_dir_all(data_dir_clone.join("keys"))?;
        std::fs::create_dir_all(data_dir_clone.join("snapshots"))?;
        std::fs::create_dir_all(data_dir_clone.join("locks"))?;
        // Create 256 pack shard directories
        for i in 0..=255u8 {
            std::fs::create_dir_all(data_dir_clone.join("packs").join(format!("{i:02x}")))?;
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    Ok(StatusCode::CREATED.into_response())
}

async fn batch_delete(state: AppState, body: axum::body::Bytes) -> Result<Response, ServerError> {
    if state.inner.config.append_only {
        return Err(ServerError::Forbidden(
            "append-only: batch-delete not allowed".into(),
        ));
    }

    let keys: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid JSON: {e}")))?;

    let state_clone = state.clone();

    for key in keys {
        let file_path = state_clone
            .file_path(&key)
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
            state_clone.sub_quota_usage(old_size);
        }
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn repack(state: AppState, body: axum::body::Bytes) -> Result<Response, ServerError> {
    let plan: RepackPlanRequest = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid repack plan: {e}")))?;
    validate_repack_plan(&plan)?;

    if state.inner.config.append_only && plan.operations.iter().any(|op| op.delete_after) {
        return Err(ServerError::Forbidden(
            "append-only: repack with delete not allowed".into(),
        ));
    }

    let state_clone = state.clone();

    let results = tokio::task::spawn_blocking(move || execute_repack(&state_clone, &plan))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(results).into_response())
}

async fn verify_packs(state: AppState, body: axum::body::Bytes) -> Result<Response, ServerError> {
    let plan: VerifyPacksPlanRequest = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid verify-packs plan: {e}")))?;
    validate_verify_packs_plan(&plan)?;

    let state_clone = state.clone();

    let results = tokio::task::spawn_blocking(move || execute_verify_packs(&state_clone, &plan))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(results).into_response())
}

async fn verify_structure(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    let result = tokio::task::spawn_blocking(move || check_structure(&data_dir))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(result).into_response())
}

/// GET /?list — list all keys in the repository.
async fn repo_list_all(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    let keys = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<String>> {
        let mut keys = Vec::new();
        list_all_recursive(&data_dir, &data_dir, &mut keys)?;
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
            let key: String = rel
                .iter()
                .map(|c| c.to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.push(key);
        }
    }
    Ok(())
}

// --- Repack logic ---

fn execute_repack(
    state: &AppState,
    plan: &RepackPlanRequest,
) -> Result<RepackResultResponse, String> {
    use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

    let mut completed = Vec::new();

    for op in &plan.operations {
        let source_path = state
            .file_path(&op.source_pack)
            .ok_or_else(|| "invalid pack path".to_string())?;

        if op.keep_blobs.is_empty() {
            // Just delete the pack
            if op.delete_after {
                let old_size = source_path.metadata().map(|m| m.len()).unwrap_or(0);
                let _ = std::fs::remove_file(&source_path);
                state.sub_quota_usage(old_size);
            }
            completed.push(RepackOperationResult {
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

        let mut pack_offset: u64 = PACK_HEADER_SIZE as u64;
        let mut new_offsets = Vec::with_capacity(op.keep_blobs.len());
        let mut scratch = Vec::new();

        let write_result: Result<(), String> = (|| {
            // Write pack header: magic + version
            write_and_hash(&mut writer, &mut hasher, PACK_MAGIC)
                .map_err(|e| format!("write header: {e}"))?;
            write_and_hash(&mut writer, &mut hasher, &[PACK_VERSION_CURRENT])
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

                // Cross-check the on-disk 4-byte length prefix against the
                // requested blob length. The prefix lives at offset - 4; if
                // it doesn't match, the client's index metadata is stale.
                let prefix_offset = blob_ref.offset.checked_sub(4).ok_or_else(|| {
                    format!(
                        "repack blob at offset {} has no room for length prefix",
                        blob_ref.offset
                    )
                })?;
                let mut prefix_buf = [0u8; 4];
                source
                    .seek(SeekFrom::Start(prefix_offset))
                    .map_err(|e| format!("seek prefix: {e}"))?;
                source
                    .read_exact(&mut prefix_buf)
                    .map_err(|e| format!("read prefix: {e}"))?;
                let on_disk_len = u32::from_le_bytes(prefix_buf);
                if on_disk_len as u64 != blob_ref.length {
                    return Err(format!(
                        "repack blob at offset {}: on-disk length prefix ({}) \
                         does not match requested length ({})",
                        blob_ref.offset, on_disk_len, blob_ref.length
                    ));
                }

                // Read blob from source into reusable scratch buffer
                // (cursor is already at blob_ref.offset after reading the prefix)
                scratch.resize(blob_ref.length as usize, 0);
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

        let new_pack_path = state.file_path(&new_pack_key).ok_or_else(|| {
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
        state.add_quota_usage(pack_offset);

        // Delete source if requested
        if op.delete_after {
            let old_size = source_path.metadata().map(|m| m.len()).unwrap_or(0);
            let _ = std::fs::remove_file(&source_path);
            state.sub_quota_usage(old_size);
        }

        completed.push(RepackOperationResult {
            source_pack: op.source_pack.clone(),
            new_pack: Some(new_pack_key),
            new_offsets,
            deleted: op.delete_after,
        });
    }

    Ok(RepackResultResponse { completed })
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

fn validate_repack_plan(plan: &RepackPlanRequest) -> Result<(), ServerError> {
    check_protocol_version(plan.protocol_version).map_err(ServerError::BadRequest)?;
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
            validate_blob_ref(
                blob.offset,
                blob.length,
                &format!("operation {idx} blob {blob_idx}"),
            )
            .map_err(ServerError::BadRequest)?;
        }
    }
    Ok(())
}

// --- Verify-packs logic ---

/// Must match or slightly exceed client-side SERVER_VERIFY_BATCH_SIZE (100).
const MAX_VERIFY_PACKS: usize = 100;
/// Maximum estimated bytes of pack I/O per verify request (matches client-side cap).
const MAX_VERIFY_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

fn validate_verify_packs_plan(plan: &VerifyPacksPlanRequest) -> Result<(), ServerError> {
    check_protocol_version(plan.protocol_version).map_err(ServerError::BadRequest)?;
    if plan.packs.len() > MAX_VERIFY_PACKS {
        return Err(ServerError::BadRequest(format!(
            "too many packs to verify: {} (max {MAX_VERIFY_PACKS})",
            plan.packs.len()
        )));
    }

    // Byte-volume gate: the server reads each full pack from disk, so bound
    // total I/O using the client-declared expected_size per pack.
    let mut total_bytes: u64 = 0;

    for (idx, entry) in plan.packs.iter().enumerate() {
        if !is_valid_pack_key(&entry.pack_key) {
            return Err(ServerError::BadRequest(format!(
                "invalid pack_key at pack {idx}: {}",
                entry.pack_key
            )));
        }
        if entry.expected_size == 0 {
            return Err(ServerError::BadRequest(format!(
                "expected_size must be > 0 at pack {idx}"
            )));
        }

        for (blob_idx, blob) in entry.expected_blobs.iter().enumerate() {
            validate_blob_ref(
                blob.offset,
                blob.length,
                &format!("pack {idx} blob {blob_idx}"),
            )
            .map_err(ServerError::BadRequest)?;
        }
        total_bytes = total_bytes.saturating_add(entry.expected_size);
    }

    if total_bytes > MAX_VERIFY_BYTES {
        return Err(ServerError::BadRequest(format!(
            "estimated verify I/O too large: {total_bytes} bytes (max {MAX_VERIFY_BYTES})"
        )));
    }

    Ok(())
}

fn execute_verify_packs(state: &AppState, plan: &VerifyPacksPlanRequest) -> VerifyPacksResponse {
    let mut results = Vec::with_capacity(plan.packs.len());
    let mut bytes_read: u64 = 0;
    let mut truncated = false;

    for entry in &plan.packs {
        // Stat before reading to enforce byte cap with actual file sizes.
        let file_path = match state.file_path(&entry.pack_key) {
            Some(p) => p,
            None => {
                results.push(VerifyPackResult {
                    pack_key: entry.pack_key.clone(),
                    hash_valid: false,
                    header_valid: false,
                    blobs_valid: false,
                    error: Some("invalid pack path".into()),
                });
                continue;
            }
        };
        let file_size = match std::fs::metadata(&file_path) {
            Ok(m) => m.len(),
            Err(e) => {
                results.push(VerifyPackResult {
                    pack_key: entry.pack_key.clone(),
                    hash_valid: false,
                    header_valid: false,
                    blobs_valid: false,
                    error: Some(format!("stat failed: {e}")),
                });
                continue;
            }
        };
        if bytes_read.saturating_add(file_size) > MAX_VERIFY_BYTES {
            truncated = true;
            break;
        }
        bytes_read += file_size;
        results.push(verify_single_pack(&file_path, entry));
    }

    VerifyPacksResponse { results, truncated }
}

fn verify_single_pack(file_path: &std::path::Path, entry: &VerifyPackRequest) -> VerifyPackResult {
    let pack_data = match std::fs::read(file_path) {
        Ok(d) => d,
        Err(e) => {
            return VerifyPackResult {
                pack_key: entry.pack_key.clone(),
                hash_valid: false,
                header_valid: false,
                blobs_valid: false,
                error: Some(format!("read failed: {e}")),
            };
        }
    };

    // 1. Recompute BLAKE2b-256 hash and compare to pack ID from storage key
    let actual_hash = finalize_blake2b_256_hex({
        let mut hasher = Blake2bVar::new(32).expect("valid output size");
        hasher.update(&pack_data);
        hasher
    });
    let expected_hash = entry.pack_key.split('/').next_back().unwrap_or("");
    let hash_valid = actual_hash == expected_hash;

    // 2. Validate pack header (magic + version)
    let header_valid = pack_data.len() >= PACK_HEADER_SIZE
        && &pack_data[..8] == PACK_MAGIC
        && (PACK_VERSION_MIN..=PACK_VERSION_MAX).contains(&pack_data[8]);

    // 3. Forward-scan blobs and cross-reference against expected entries
    let blobs_valid = if header_valid {
        verify_blob_boundaries(&pack_data, &entry.expected_blobs)
    } else {
        false
    };

    let error = if !hash_valid {
        Some(format!(
            "hash mismatch: expected {expected_hash}, got {actual_hash}"
        ))
    } else if !header_valid {
        Some("invalid pack header".into())
    } else if !blobs_valid {
        Some("blob boundary mismatch".into())
    } else {
        None
    };

    VerifyPackResult {
        pack_key: entry.pack_key.clone(),
        hash_valid,
        header_valid,
        blobs_valid,
        error,
    }
}

/// Forward-scan the pack's blob boundaries and verify that every expected blob
/// exists at the declared (offset, length).
fn verify_blob_boundaries(pack_data: &[u8], expected_blobs: &[ProtoVerifyBlobRef]) -> bool {
    // Scan actual blob boundaries from the pack
    let mut pos = PACK_HEADER_SIZE;
    let mut actual_blobs: Vec<(u64, u64)> = Vec::new();
    let blobs_end = pack_data.len();

    while pos + 4 <= blobs_end {
        let blob_len = u32::from_le_bytes(match pack_data[pos..pos + 4].try_into() {
            Ok(b) => b,
            Err(_) => return false,
        });
        if pos + 4 + blob_len as usize > blobs_end {
            break;
        }
        actual_blobs.push(((pos + 4) as u64, blob_len as u64));
        pos += 4 + blob_len as usize;
    }

    // Trailing bytes = corrupt
    if pos != blobs_end {
        return false;
    }

    // Every expected blob must match an actual blob (O(n) lookup via HashSet)
    let actual_set: std::collections::HashSet<(u64, u64)> = actual_blobs.into_iter().collect();
    for expected in expected_blobs {
        if !actual_set.contains(&(expected.offset, expected.length)) {
            return false;
        }
    }

    true
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

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use blake2::digest::{Update, VariableOutput};
    use blake2::Blake2bVar;

    use super::super::test_helpers::*;
    use vger_protocol::PACK_MAGIC;

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
        let path = tmp.join("packs").join(shard).join(&pack_id);
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
        let resp = authed_post(router.clone(), "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let completed = result["completed"].as_array().unwrap();
        assert_eq!(completed.len(), 1);

        let op = &completed[0];
        let new_pack_key = op["new_pack"].as_str().unwrap();
        assert!(new_pack_key.starts_with("packs/"));

        // Read the new pack file and verify magic + version
        let new_pack_path = tmp.path().join(new_pack_key);
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
        let resp = authed_post(router, "/?repack", body).await;
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

        let new_pack_path = tmp.path().join(new_pack_key);
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
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let new_pack_key = result["completed"][0]["new_pack"].as_str().unwrap();

        // The pack key is packs/<shard>/<hex>. The hex should be blake2b-256 of contents.
        let pack_hex = new_pack_key.split('/').next_back().unwrap();
        let new_pack_path = tmp.path().join(new_pack_key);
        let new_pack_data = std::fs::read(&new_pack_path).expect("read new pack");
        let actual_hash = blake2b_256_hex(&new_pack_data);
        assert_eq!(actual_hash, pack_hex);
    }

    #[tokio::test]
    async fn repack_delete_source() {
        let (router, state, tmp) = setup_app(0);

        let (pack_bytes, refs) = build_pack(&[b"delete-me"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(&source_key);
        assert!(source_path.exists());

        // Seed quota with the source pack size
        state.add_quota_usage(pack_bytes.len() as u64);

        let body = repack_body(&[repack_op(&source_key, &refs, true)]);
        let resp = authed_post(router, "/?repack", body).await;
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
        let source_path = tmp.path().join(&source_key);

        state.add_quota_usage(pack_bytes.len() as u64);
        let used_before = state.quota_used();

        // Repack with empty keep_blobs + delete_after
        let body = repack_body(&[repack_op(&source_key, &[], true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert!(op["new_pack"].is_null());
        assert!(op["deleted"].as_bool().unwrap());
        assert!(op["new_offsets"].as_array().unwrap().is_empty());

        assert!(!source_path.exists(), "source pack not deleted");
        assert!(
            state.quota_used() < used_before,
            "quota should have decreased"
        );
    }

    #[tokio::test]
    async fn list_all_keys_in_repo() {
        let (router, _state, _tmp) = setup_app(0);

        // PUT two known keys
        let resp = authed_put(router.clone(), "/config", b"cfg".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = authed_put(router.clone(), "/manifest", b"man".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);

        // GET /?list should return both keys
        let resp = authed_get(router.clone(), "/?list").await;
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
