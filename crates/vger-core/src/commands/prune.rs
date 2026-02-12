use std::collections::HashMap;

use chrono::Utc;

use crate::config::{RetentionConfig, SourceEntry, VgerConfig};
use crate::error::{Result, VgerError};
use crate::prune::{apply_policy, apply_policy_by_label, PruneDecision};
use crate::repo::Repository;
use crate::storage;

use super::list::{load_snapshot_items, load_snapshot_meta};
use super::util::with_repo_lock;

pub struct PruneStats {
    pub kept: usize,
    pub pruned: usize,
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

/// Formatted list entry for --list output.
pub struct PruneListEntry {
    pub action: String,
    pub snapshot_name: String,
    pub reasons: Vec<String>,
}

pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
) -> Result<(PruneStats, Vec<PruneListEntry>)> {
    let backend = storage::backend_from_config(&config.repository)?;
    let mut repo = Repository::open(backend, passphrase)?;
    with_repo_lock(&mut repo, |repo| {
        let now = Utc::now();

        // When --source is given, restrict to matching snapshots only
        let target_snapshots = if source_filter.is_empty() {
            repo.manifest.snapshots.clone()
        } else {
            repo.manifest
                .snapshots
                .iter()
                .filter(|e| source_filter.contains(&e.source_label))
                .cloned()
                .collect()
        };

        // Build per-source retention map
        let source_retentions: HashMap<String, RetentionConfig> = sources
            .iter()
            .filter_map(|s| s.retention.as_ref().map(|r| (s.label.clone(), r.clone())))
            .collect();

        let has_sources = !sources.is_empty();

        let decisions = if has_sources {
            // Label-aware: group by source_label and apply per-source retention
            if !config.retention.has_any_rule()
                && source_retentions.values().all(|r| !r.has_any_rule())
            {
                return Err(VgerError::Config(
                    "no retention rules configured — set at least one keep_* option in the retention section or per-source".into(),
                ));
            }
            apply_policy_by_label(
                &target_snapshots,
                &config.retention,
                &source_retentions,
                now,
            )?
        } else {
            // No sources — fall back to flat policy
            if !config.retention.has_any_rule() {
                return Err(VgerError::Config(
                    "no retention rules configured — set at least one keep_* option in the retention section".into(),
                ));
            }
            apply_policy(&target_snapshots, &config.retention, now)?
        };

        // Build list output
        let mut list_entries = Vec::new();
        let mut kept = 0usize;
        let mut to_prune: Vec<String> = Vec::new();

        for entry in &decisions {
            match &entry.decision {
                PruneDecision::Keep { reasons } => {
                    kept += 1;
                    if list || dry_run {
                        list_entries.push(PruneListEntry {
                            action: "keep".into(),
                            snapshot_name: entry.snapshot_name.clone(),
                            reasons: reasons.clone(),
                        });
                    }
                }
                PruneDecision::Prune => {
                    to_prune.push(entry.snapshot_name.clone());
                    if list || dry_run {
                        list_entries.push(PruneListEntry {
                            action: "prune".into(),
                            snapshot_name: entry.snapshot_name.clone(),
                            reasons: Vec::new(),
                        });
                    }
                }
            }
        }

        if dry_run {
            return Ok((
                PruneStats {
                    kept,
                    pruned: to_prune.len(),
                    chunks_deleted: 0,
                    space_freed: 0,
                },
                list_entries,
            ));
        }

        // Delete pruned snapshots (process oldest first)
        to_prune.reverse();
        let mut total_chunks_deleted = 0u64;
        let mut total_space_freed = 0u64;

        for snapshot_name in &to_prune {
            // Get snapshot ID before we modify manifest
            let snapshot_id_hex = repo
                .manifest
                .find_snapshot(snapshot_name)
                .map(|e| hex::encode(&e.id))
                .ok_or_else(|| VgerError::SnapshotNotFound(snapshot_name.clone()))?;

            let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
            let items = load_snapshot_items(repo, snapshot_name)?;

            // Decrement data chunk refcounts
            // Orphaned blobs remain in pack files until a future `compact` command.
            for item in &items {
                for chunk_ref in &item.chunks {
                    if let Some((rc, size)) = repo.chunk_index.decrement(&chunk_ref.id) {
                        if rc == 0 {
                            total_chunks_deleted += 1;
                            total_space_freed += size as u64;
                        }
                    }
                }
            }

            // Decrement item-stream chunk refcounts
            for chunk_id in &snapshot_meta.item_ptrs {
                if let Some((rc, size)) = repo.chunk_index.decrement(chunk_id) {
                    if rc == 0 {
                        total_chunks_deleted += 1;
                        total_space_freed += size as u64;
                    }
                }
            }

            // Delete snapshot metadata
            repo.storage
                .delete(&format!("snapshots/{snapshot_id_hex}"))?;

            // Remove from manifest
            repo.manifest.remove_snapshot(snapshot_name);
        }

        // Single atomic save after all deletions
        repo.save_state()?;

        Ok((
            PruneStats {
                kept,
                pruned: to_prune.len(),
                chunks_deleted: total_chunks_deleted,
                space_freed: total_space_freed,
            },
            list_entries,
        ))
    })
}
