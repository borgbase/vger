use crate::archive::item::ItemType;
use crate::compress;
use crate::config::BorgConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::error::Result;
use crate::repo::format::unpack_object;
use crate::repo::Repository;
use crate::storage;

use super::list::{load_archive_items, load_archive_meta};

/// A single integrity issue found during check.
#[derive(Debug)]
pub struct CheckError {
    pub context: String,
    pub message: String,
}

/// Summary of a check run.
pub struct CheckResult {
    pub archives_checked: usize,
    pub items_checked: usize,
    pub chunks_existence_checked: usize,
    pub chunks_data_verified: usize,
    pub errors: Vec<CheckError>,
}

/// Run `borg-rs check`.
pub fn run(config: &BorgConfig, passphrase: Option<&str>, verify_data: bool) -> Result<CheckResult> {
    let backend = storage::backend_from_config(&config.repository)?;
    let repo = Repository::open(backend, passphrase)?;

    let mut errors: Vec<CheckError> = Vec::new();
    let mut archives_checked: usize = 0;
    let mut items_checked: usize = 0;

    // Step 1: Check each archive in manifest
    let archive_count = repo.manifest.archives.len();
    for (i, entry) in repo.manifest.archives.iter().enumerate() {
        eprintln!(
            "[{}/{}] Checking archive '{}'...",
            i + 1,
            archive_count,
            entry.name
        );

        // Load archive metadata
        let meta = match load_archive_meta(&repo, &entry.name) {
            Ok(m) => m,
            Err(e) => {
                errors.push(CheckError {
                    context: format!("archive '{}'", entry.name),
                    message: format!("failed to load metadata: {e}"),
                });
                continue;
            }
        };

        // Verify item_ptrs exist in chunk index
        for chunk_id in &meta.item_ptrs {
            if !repo.chunk_index.contains(chunk_id) {
                errors.push(CheckError {
                    context: format!("archive '{}' item_ptrs", entry.name),
                    message: format!("chunk {chunk_id} not in index"),
                });
            }
        }

        // Load and check items
        let items = match load_archive_items(&repo, &entry.name) {
            Ok(items) => items,
            Err(e) => {
                errors.push(CheckError {
                    context: format!("archive '{}'", entry.name),
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
                            context: format!("archive '{}' file '{}'", entry.name, item.path),
                            message: format!("chunk {} not in index", chunk_ref.id),
                        });
                    }
                }
            }
        }

        items_checked += items.len();
        archives_checked += 1;
    }

    // Step 2: Verify all chunks in index exist in storage
    let total_chunks = repo.chunk_index.len();
    eprintln!("Verifying existence of {total_chunks} chunks...");
    let mut chunks_existence_checked: usize = 0;
    for (chunk_id, _entry) in repo.chunk_index.iter() {
        if !repo.storage.exists(&chunk_id.storage_key())? {
            errors.push(CheckError {
                context: "chunk index".into(),
                message: format!("chunk {chunk_id} missing from storage"),
            });
        }
        chunks_existence_checked += 1;
        if chunks_existence_checked % 1000 == 0 {
            eprintln!(
                "  existence: {chunks_existence_checked}/{total_chunks}",
            );
        }
    }

    // Step 3: If --verify-data, read + decrypt + decompress + recompute ID
    let mut chunks_data_verified: usize = 0;
    if verify_data {
        eprintln!("Verifying data integrity of {total_chunks} chunks...");
        let chunk_id_key = repo.crypto.chunk_id_key();
        for (chunk_id, _entry) in repo.chunk_index.iter() {
            let raw = match repo.storage.get(&chunk_id.storage_key())? {
                Some(data) => data,
                None => {
                    // Already reported in existence check
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
                    message: format!(
                        "chunk {chunk_id}: ID mismatch (recomputed {recomputed})",
                    ),
                });
            }

            chunks_data_verified += 1;
            if chunks_data_verified % 1000 == 0 {
                eprintln!(
                    "  verify-data: {chunks_data_verified}/{total_chunks}",
                );
            }
        }
    }

    Ok(CheckResult {
        archives_checked,
        items_checked,
        chunks_existence_checked,
        chunks_data_verified,
        errors,
    })
}
