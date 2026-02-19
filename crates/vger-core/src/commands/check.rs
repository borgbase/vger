use std::collections::HashMap;

use crate::compress;
use crate::config::VgerConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::crypto::pack_id::PackId;
use crate::error::Result;
use crate::repo::format::{unpack_object_expect_with_context, ObjectType};
use crate::repo::pack::read_blob_from_pack;
use crate::snapshot::item::ItemType;

use super::list::{for_each_decoded_item, load_snapshot_item_stream, load_snapshot_meta};
use super::util::open_repo;

/// A single integrity issue found during check.
#[derive(Debug)]
pub struct CheckError {
    pub context: String,
    pub message: String,
}

/// Summary of a check run.
pub struct CheckResult {
    pub snapshots_checked: usize,
    pub items_checked: usize,
    pub chunks_existence_checked: usize,
    pub packs_existence_checked: usize,
    pub chunks_data_verified: usize,
    pub errors: Vec<CheckError>,
}

#[derive(Debug, Clone)]
pub enum CheckProgressEvent {
    SnapshotStarted {
        current: usize,
        total: usize,
        name: String,
    },
    PacksExistencePhaseStarted {
        total_packs: usize,
    },
    PacksExistenceProgress {
        checked: usize,
        total_packs: usize,
    },
    ChunksDataPhaseStarted {
        total_chunks: usize,
    },
    ChunksDataProgress {
        verified: usize,
        total_chunks: usize,
    },
}

fn emit_progress(
    progress: &mut Option<&mut dyn FnMut(CheckProgressEvent)>,
    event: CheckProgressEvent,
) {
    if let Some(callback) = progress.as_deref_mut() {
        callback(event);
    }
}

/// Run `vger check`.
pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    verify_data: bool,
) -> Result<CheckResult> {
    run_with_progress(config, passphrase, verify_data, None)
}

pub fn run_with_progress(
    config: &VgerConfig,
    passphrase: Option<&str>,
    verify_data: bool,
    mut progress: Option<&mut dyn FnMut(CheckProgressEvent)>,
) -> Result<CheckResult> {
    let mut repo = open_repo(config, passphrase)?;

    let mut errors: Vec<CheckError> = Vec::new();
    let mut snapshots_checked: usize = 0;
    let mut items_checked: usize = 0;

    // Step 1: Check each snapshot in manifest
    let snapshot_entries = repo.manifest().snapshots.clone();
    let snapshot_count = snapshot_entries.len();
    for (i, entry) in snapshot_entries.iter().enumerate() {
        emit_progress(
            &mut progress,
            CheckProgressEvent::SnapshotStarted {
                current: i + 1,
                total: snapshot_count,
                name: entry.name.clone(),
            },
        );

        // Load snapshot metadata
        let meta = match load_snapshot_meta(&repo, &entry.name) {
            Ok(m) => m,
            Err(e) => {
                errors.push(CheckError {
                    context: format!("snapshot '{}'", entry.name),
                    message: format!("failed to load metadata: {e}"),
                });
                continue;
            }
        };

        // Verify item_ptrs exist in chunk index
        for chunk_id in &meta.item_ptrs {
            if !repo.chunk_index().contains(chunk_id) {
                errors.push(CheckError {
                    context: format!("snapshot '{}' item_ptrs", entry.name),
                    message: format!("chunk {chunk_id} not in index"),
                });
            }
        }

        // Load item stream (needs &mut repo for blob cache), then check items
        let items_stream = match load_snapshot_item_stream(&mut repo, &entry.name) {
            Ok(s) => s,
            Err(e) => {
                errors.push(CheckError {
                    context: format!("snapshot '{}'", entry.name),
                    message: format!("failed to load items: {e}"),
                });
                continue;
            }
        };

        let mut per_snapshot_items = 0usize;
        let entry_name = entry.name.clone();
        if let Err(e) = for_each_decoded_item(&items_stream, |item| {
            per_snapshot_items += 1;
            if item.entry_type == ItemType::RegularFile {
                for chunk_ref in &item.chunks {
                    if !repo.chunk_index().contains(&chunk_ref.id) {
                        errors.push(CheckError {
                            context: format!("snapshot '{}' file '{}'", entry_name, item.path),
                            message: format!("chunk {} not in index", chunk_ref.id),
                        });
                    }
                }
            }
            Ok(())
        }) {
            errors.push(CheckError {
                context: format!("snapshot '{}'", entry.name),
                message: format!("failed to load items: {e}"),
            });
            continue;
        }

        items_checked += per_snapshot_items;
        snapshots_checked += 1;
    }

    // Step 2: Verify all unique pack files exist in storage (deduplicated)
    let chunks_existence_checked = repo.chunk_index().len();
    let mut pack_chunk_counts: HashMap<PackId, usize> = HashMap::new();
    for (_chunk_id, entry) in repo.chunk_index().iter() {
        *pack_chunk_counts.entry(entry.pack_id).or_insert(0) += 1;
    }
    let total_packs = pack_chunk_counts.len();
    emit_progress(
        &mut progress,
        CheckProgressEvent::PacksExistencePhaseStarted { total_packs },
    );
    let mut packs_existence_checked: usize = 0;
    for (pack_id, chunk_count) in &pack_chunk_counts {
        let pack_key = pack_id.storage_key();
        if !repo.storage.exists(&pack_key)? {
            errors.push(CheckError {
                context: "chunk index".into(),
                message: format!(
                    "pack {pack_id} missing from storage (referenced by {chunk_count} chunks)"
                ),
            });
        }
        packs_existence_checked += 1;
        if packs_existence_checked.is_multiple_of(100) {
            emit_progress(
                &mut progress,
                CheckProgressEvent::PacksExistenceProgress {
                    checked: packs_existence_checked,
                    total_packs,
                },
            );
        }
    }

    // Step 3: If --verify-data, read + decrypt + decompress + recompute ID
    let mut chunks_data_verified: usize = 0;
    if verify_data {
        emit_progress(
            &mut progress,
            CheckProgressEvent::ChunksDataPhaseStarted {
                total_chunks: chunks_existence_checked,
            },
        );
        let chunk_id_key = repo.crypto.chunk_id_key();
        for (chunk_id, entry) in repo.chunk_index().iter() {
            let raw = match read_blob_from_pack(
                repo.storage.as_ref(),
                &entry.pack_id,
                entry.pack_offset,
                entry.stored_size,
            ) {
                Ok(data) => data,
                Err(e) => {
                    errors.push(CheckError {
                        context: "verify-data".into(),
                        message: format!("chunk {chunk_id}: read failed: {e}"),
                    });
                    continue;
                }
            };

            // Decrypt
            let compressed = match unpack_object_expect_with_context(
                &raw,
                ObjectType::ChunkData,
                &chunk_id.0,
                repo.crypto.as_ref(),
            ) {
                Ok(bytes) => bytes,
                Err(e) => {
                    errors.push(CheckError {
                        context: "verify-data".into(),
                        message: format!("chunk {chunk_id}: decrypt failed: {e}"),
                    });
                    continue;
                }
            };

            // Decompress
            let plaintext = match compress::decompress(&compressed) {
                Ok(data) => data,
                Err(e) => {
                    errors.push(CheckError {
                        context: "verify-data".into(),
                        message: format!("chunk {chunk_id}: decompress failed: {e}"),
                    });
                    continue;
                }
            };

            // Recompute chunk ID and compare
            let recomputed = ChunkId::compute(chunk_id_key, &plaintext);
            if &recomputed != chunk_id {
                errors.push(CheckError {
                    context: "verify-data".into(),
                    message: format!("chunk {chunk_id}: ID mismatch (recomputed {recomputed})",),
                });
            }

            chunks_data_verified += 1;
            if chunks_data_verified.is_multiple_of(1000) {
                emit_progress(
                    &mut progress,
                    CheckProgressEvent::ChunksDataProgress {
                        verified: chunks_data_verified,
                        total_chunks: chunks_existence_checked,
                    },
                );
            }
        }
    }

    Ok(CheckResult {
        snapshots_checked,
        items_checked,
        chunks_existence_checked,
        packs_existence_checked,
        chunks_data_verified,
        errors,
    })
}
