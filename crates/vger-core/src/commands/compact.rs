use std::collections::{HashMap, HashSet};

use tracing::{info, warn};

use super::util::{open_repo, with_repo_lock};
use crate::config::VgerConfig;
use crate::crypto::pack_id::PackId;
use crate::error::{Result, VgerError};
use crate::repo::pack::{
    read_blob_from_pack, read_pack_header, PackHeaderEntry, PackType, PackWriter,
    DEFAULT_MAX_BLOB_OVERHEAD,
};
use crate::repo::Repository;
use crate::storage::{RepackBlobRef, RepackOperationRequest, RepackPlanRequest};

/// Statistics returned by the compact command.
#[derive(Debug, Default)]
pub struct CompactStats {
    pub packs_total: u64,
    pub packs_repacked: u64,
    pub packs_deleted_empty: u64,
    pub blobs_live: u64,
    pub blobs_dead: u64,
    pub space_freed: u64,
    pub packs_corrupt: u64,
    pub packs_orphan: u64,
}

/// Per-pack analysis of live vs dead blobs.
struct PackAnalysis {
    pack_id: PackId,
    storage_key: String,
    entries: Vec<PackHeaderEntry>,
    live_entries: Vec<PackHeaderEntry>,
    total_bytes: u64,
    dead_bytes: u64,
}

pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
) -> Result<CompactStats> {
    let mut repo = open_repo(config, passphrase)?;
    with_repo_lock(&mut repo, |repo| {
        compact_repo(repo, threshold, max_repack_size, dry_run)
    })
}

/// Core compact logic operating on an already-opened repository.
pub fn compact_repo(
    repo: &mut Repository,
    threshold: f64,
    max_repack_size: Option<u64>,
    dry_run: bool,
) -> Result<CompactStats> {
    let mut stats = CompactStats::default();

    // Phase 1: Enumerate all packs and analyze live/dead blobs
    let mut analyses: Vec<PackAnalysis> = Vec::new();

    // Build a set of (pack_id, offset) pairs from the chunk index for fast lookup
    let live_set: HashSet<(PackId, u64)> = repo
        .chunk_index()
        .iter()
        .map(|(_, entry)| (entry.pack_id, entry.pack_offset))
        .collect();

    let mut discovered_pack_ids: HashSet<PackId> = HashSet::new();

    for shard in 0u16..256 {
        let prefix = format!("packs/{:02x}/", shard);
        let keys = repo.storage.list(&prefix)?;

        for key in &keys {
            if key.ends_with('/') {
                continue;
            }

            let pack_id = match PackId::from_storage_key(key) {
                Ok(id) => id,
                Err(e) => {
                    warn!("Skipping invalid pack key '{}': {}", key, e);
                    continue;
                }
            };

            stats.packs_total += 1;
            discovered_pack_ids.insert(pack_id);

            let entries =
                match read_pack_header(repo.storage.as_ref(), &pack_id, repo.crypto.as_ref()) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!("Skipping corrupt pack {}: {}", pack_id, e);
                        stats.packs_corrupt += 1;
                        continue;
                    }
                };

            let mut live_entries = Vec::new();
            let mut total_bytes: u64 = 0;
            let mut dead_bytes: u64 = 0;

            for entry in &entries {
                let blob_size = 4 + entry.length as u64;
                total_bytes += blob_size;

                if live_set.contains(&(pack_id, entry.offset)) {
                    live_entries.push(entry.clone());
                    stats.blobs_live += 1;
                } else {
                    dead_bytes += blob_size;
                    stats.blobs_dead += 1;
                }
            }

            if dead_bytes == 0 {
                continue;
            }

            let unused_ratio = if total_bytes > 0 {
                (dead_bytes as f64 / total_bytes as f64) * 100.0
            } else {
                0.0
            };

            if unused_ratio >= threshold {
                analyses.push(PackAnalysis {
                    pack_id,
                    storage_key: key.clone(),
                    entries,
                    live_entries,
                    total_bytes,
                    dead_bytes,
                });
            }
        }
    }

    // Detect orphan packs: packs on disk but not referenced by the chunk index
    let indexed_packs: HashSet<PackId> = live_set.iter().map(|(pack_id, _)| *pack_id).collect();
    for pack_id in &discovered_pack_ids {
        if !indexed_packs.contains(pack_id) {
            stats.packs_orphan += 1;
        }
    }

    if stats.packs_corrupt > 0 {
        warn!(
            "{} corrupt pack(s) found; run `vger check --verify-data` for details",
            stats.packs_corrupt
        );
    }
    if stats.packs_orphan > 0 {
        info!(
            "{} orphan pack(s) found (present on disk but not referenced by index)",
            stats.packs_orphan
        );
    }

    // Sort by most wasteful first (highest dead bytes)
    analyses.sort_by(|a, b| b.dead_bytes.cmp(&a.dead_bytes));
    let selected = select_analyses_by_cap(&analyses, max_repack_size);

    if dry_run {
        for a in &selected {
            let pct = (a.dead_bytes as f64 / a.total_bytes as f64) * 100.0;
            if a.live_entries.is_empty() {
                info!(
                    "Would delete empty pack {} ({:.1}% unused, {} dead blobs)",
                    a.pack_id,
                    pct,
                    a.entries.len(),
                );
                stats.packs_deleted_empty += 1;
            } else {
                info!(
                    "Would repack {} ({:.1}% unused, {} live / {} dead blobs)",
                    a.pack_id,
                    pct,
                    a.live_entries.len(),
                    a.entries.len() - a.live_entries.len(),
                );
                stats.packs_repacked += 1;
            }
            stats.space_freed += a.dead_bytes;
        }
        return Ok(stats);
    }

    if try_server_side_repack(repo, &selected, &mut stats)? {
        return Ok(stats);
    }

    // Phase 2: Repack
    let mut total_repacked_bytes: u64 = 0;
    let pack_target = repo.config.min_pack_size as usize;

    for analysis in &selected {
        if let Some(cap) = max_repack_size {
            if total_repacked_bytes >= cap {
                info!("Reached max-repack-size limit, stopping");
                break;
            }
        }

        if analysis.live_entries.is_empty() {
            info!("Deleting empty pack {}", analysis.pack_id);
            repo.storage.delete(&analysis.storage_key)?;
            stats.packs_deleted_empty += 1;
            stats.space_freed += analysis.total_bytes;
            continue;
        }

        info!(
            "Repacking {} ({} live blobs)",
            analysis.pack_id,
            analysis.live_entries.len(),
        );

        let pack_type = if analysis.live_entries[0].obj_type == 5 {
            PackType::Tree
        } else {
            PackType::Data
        };

        // Pre-scan for max blob overhead: legacy repos may contain blobs larger
        // than the current 16 MiB chunk cap, so the compact path sizes dynamically.
        let max_blob_overhead = analysis
            .live_entries
            .iter()
            .map(|e| e.length as usize + 4) // 4-byte length prefix + blob data
            .max()
            .unwrap_or(DEFAULT_MAX_BLOB_OVERHEAD);

        let mut writer = PackWriter::new(pack_type, pack_target, max_blob_overhead);

        for entry in &analysis.live_entries {
            let blob_data = read_blob_from_pack(
                repo.storage.as_ref(),
                &analysis.pack_id,
                entry.offset,
                entry.length,
            )?;

            writer.add_blob(
                entry.obj_type,
                entry.chunk_id,
                blob_data,
                entry.uncompressed_size,
            )?;
        }

        let (new_pack_id, new_entries) =
            writer.flush(repo.storage.as_ref(), repo.crypto.as_ref())?;

        for (chunk_id, stored_size, offset, _refcount) in &new_entries {
            repo.chunk_index_mut()
                .update_location(chunk_id, new_pack_id, *offset, *stored_size);
        }

        // Save state BEFORE deleting old pack (crash safety)
        repo.save_state()?;

        repo.storage.delete(&analysis.storage_key)?;

        stats.packs_repacked += 1;
        stats.space_freed += analysis.dead_bytes;
        total_repacked_bytes += analysis.total_bytes;
    }

    if stats.packs_repacked > 0 || stats.packs_deleted_empty > 0 {
        repo.save_state()?;
    }

    Ok(stats)
}

fn select_analyses_by_cap(
    analyses: &[PackAnalysis],
    max_repack_size: Option<u64>,
) -> Vec<&PackAnalysis> {
    let mut selected = Vec::new();
    let mut total = 0u64;

    for analysis in analyses {
        if let Some(cap) = max_repack_size {
            if total >= cap {
                break;
            }
        }
        selected.push(analysis);
        total = total.saturating_add(analysis.total_bytes);
    }

    selected
}

fn try_server_side_repack(
    repo: &mut Repository,
    analyses: &[&PackAnalysis],
    stats: &mut CompactStats,
) -> Result<bool> {
    if analyses.is_empty() {
        return Ok(true);
    }

    let mut operations = Vec::with_capacity(analyses.len());
    for analysis in analyses {
        operations.push(RepackOperationRequest {
            source_pack: analysis.storage_key.clone(),
            keep_blobs: analysis
                .live_entries
                .iter()
                .map(|entry| RepackBlobRef {
                    offset: entry.offset,
                    length: entry.length as u64,
                })
                .collect(),
            delete_after: true,
        });
    }
    let plan = RepackPlanRequest { operations };

    let response = match repo.storage.server_repack(&plan) {
        Ok(resp) => resp,
        Err(VgerError::UnsupportedBackend(_)) => return Ok(false),
        Err(err) => return Err(err),
    };

    let mut completed_by_source: HashMap<String, crate::storage::RepackOperationResult> = response
        .completed
        .into_iter()
        .map(|op| (op.source_pack.clone(), op))
        .collect();

    for analysis in analyses {
        let result = completed_by_source
            .remove(&analysis.storage_key)
            .ok_or_else(|| {
                VgerError::Other(format!(
                    "server repack response missing operation for {}",
                    analysis.storage_key
                ))
            })?;

        if analysis.live_entries.is_empty() {
            if result.deleted {
                stats.packs_deleted_empty += 1;
                stats.space_freed += analysis.total_bytes;
            }
            continue;
        }

        let new_pack_key = result.new_pack.as_ref().ok_or_else(|| {
            VgerError::Other(format!(
                "server repack did not return new pack for {}",
                analysis.storage_key
            ))
        })?;
        let new_pack_id = PackId::from_storage_key(new_pack_key).map_err(|e| {
            VgerError::Other(format!(
                "server repack returned invalid pack key '{new_pack_key}': {e}"
            ))
        })?;

        if result.new_offsets.len() != analysis.live_entries.len() {
            return Err(VgerError::Other(format!(
                "server repack offsets mismatch for {}: expected {}, got {}",
                analysis.storage_key,
                analysis.live_entries.len(),
                result.new_offsets.len()
            )));
        }

        for (entry, new_offset) in analysis.live_entries.iter().zip(result.new_offsets.iter()) {
            repo.chunk_index_mut().update_location(
                &entry.chunk_id,
                new_pack_id,
                *new_offset,
                entry.length,
            );
        }

        stats.packs_repacked += 1;
        if result.deleted {
            stats.space_freed += analysis.dead_bytes;
        }
    }

    repo.save_state()?;
    Ok(true)
}
