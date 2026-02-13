use crate::compress;
use crate::config::VgerConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::error::Result;
use crate::repo::format::unpack_object;
use crate::repo::pack::read_blob_from_pack;
use crate::repo::Repository;
use crate::snapshot::item::ItemType;
use crate::storage;

use super::list::{load_snapshot_items, load_snapshot_meta};

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
    ChunksExistencePhaseStarted {
        total_chunks: usize,
    },
    ChunksExistenceProgress {
        checked: usize,
        total_chunks: usize,
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
    let backend = storage::backend_from_config(&config.repository, None)?;
    let repo = Repository::open(backend, passphrase)?;

    let mut errors: Vec<CheckError> = Vec::new();
    let mut snapshots_checked: usize = 0;
    let mut items_checked: usize = 0;

    // Step 1: Check each snapshot in manifest
    let snapshot_count = repo.manifest.snapshots.len();
    for (i, entry) in repo.manifest.snapshots.iter().enumerate() {
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
            if !repo.chunk_index.contains(chunk_id) {
                errors.push(CheckError {
                    context: format!("snapshot '{}' item_ptrs", entry.name),
                    message: format!("chunk {chunk_id} not in index"),
                });
            }
        }

        // Load and check items
        let items = match load_snapshot_items(&repo, &entry.name) {
            Ok(items) => items,
            Err(e) => {
                errors.push(CheckError {
                    context: format!("snapshot '{}'", entry.name),
                    message: format!("failed to load items: {e}"),
                });
                continue;
            }
        };

        for item in &items {
            if item.entry_type == ItemType::RegularFile {
                for chunk_ref in &item.chunks {
                    if !repo.chunk_index.contains(&chunk_ref.id) {
                        errors.push(CheckError {
                            context: format!("snapshot '{}' file '{}'", entry.name, item.path),
                            message: format!("chunk {} not in index", chunk_ref.id),
                        });
                    }
                }
            }
        }

        items_checked += items.len();
        snapshots_checked += 1;
    }

    // Step 2: Verify all chunks' pack files exist in storage
    let total_chunks = repo.chunk_index.len();
    emit_progress(
        &mut progress,
        CheckProgressEvent::ChunksExistencePhaseStarted { total_chunks },
    );
    let mut chunks_existence_checked: usize = 0;
    for (_chunk_id, entry) in repo.chunk_index.iter() {
        let pack_key = entry.pack_id.storage_key();
        if !repo.storage.exists(&pack_key)? {
            errors.push(CheckError {
                context: "chunk index".into(),
                message: format!("pack {} missing from storage", entry.pack_id),
            });
        }
        chunks_existence_checked += 1;
        if chunks_existence_checked.is_multiple_of(1000) {
            emit_progress(
                &mut progress,
                CheckProgressEvent::ChunksExistenceProgress {
                    checked: chunks_existence_checked,
                    total_chunks,
                },
            );
        }
    }

    // Step 3: If --verify-data, read + decrypt + decompress + recompute ID
    let mut chunks_data_verified: usize = 0;
    if verify_data {
        emit_progress(
            &mut progress,
            CheckProgressEvent::ChunksDataPhaseStarted { total_chunks },
        );
        let chunk_id_key = repo.crypto.chunk_id_key();
        for (chunk_id, entry) in repo.chunk_index.iter() {
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
            let compressed = match unpack_object(&raw, repo.crypto.as_ref()) {
                Ok((_obj_type, bytes)) => bytes,
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
                        total_chunks,
                    },
                );
            }
        }
    }

    Ok(CheckResult {
        snapshots_checked,
        items_checked,
        chunks_existence_checked,
        chunks_data_verified,
        errors,
    })
}
