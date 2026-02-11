use std::os::unix::fs::MetadataExt;
use std::path::Path;

use chrono::Utc;
use rand::RngCore;
use tracing::info;
use walkdir::WalkDir;

use crate::archive::item::{ChunkRef, Item, ItemType};
use crate::archive::{ArchiveMeta, ArchiveStats};
use crate::chunker;
use crate::compress::Compression;
use crate::config::{BorgConfig, ChunkerConfig};
use crate::crypto::chunk_id::ChunkId;
use crate::error::{BorgError, Result};
use crate::repo::format::{pack_object, ObjectType};
use crate::repo::manifest::ArchiveEntry;
use crate::repo::{lock, Repository};
use crate::storage;

/// Items chunker config — finer granularity for the item metadata stream.
fn items_chunker_config() -> ChunkerConfig {
    ChunkerConfig {
        min_size: 32 * 1024,    // 32 KiB
        avg_size: 128 * 1024,   // 128 KiB
        max_size: 512 * 1024,   // 512 KiB
    }
}

/// Run `borg-rs create`.
pub fn run(
    config: &BorgConfig,
    archive_name: &str,
    passphrase: Option<&str>,
    source_dirs: &[String],
    compression: Compression,
) -> Result<ArchiveStats> {
    let backend = storage::backend_from_config(&config.repository)?;
    let mut repo = Repository::open(backend, passphrase)?;

    // Check archive name is unique
    if repo.manifest.find_archive(archive_name).is_some() {
        return Err(BorgError::ArchiveAlreadyExists(archive_name.into()));
    }

    // Acquire lock
    let lock_guard = lock::acquire_lock(repo.storage.as_ref())?;

    let time_start = Utc::now();
    let mut stats = ArchiveStats::default();
    let mut all_items: Vec<Item> = Vec::new();

    // Build exclude patterns
    let mut glob_builder = globset::GlobSetBuilder::new();
    for pat in &config.exclude_patterns {
        glob_builder.add(
            globset::Glob::new(pat)
                .map_err(|e| BorgError::Config(format!("invalid exclude pattern '{pat}': {e}")))?,
        );
    }
    let excludes = glob_builder
        .build()
        .map_err(|e| BorgError::Config(format!("glob build: {e}")))?;

    // Walk source directories
    let dirs = if source_dirs.is_empty() {
        &config.source_directories
    } else {
        source_dirs
    };

    for source_dir in dirs {
        let source_path = Path::new(source_dir);
        if !source_path.exists() {
            return Err(BorgError::Other(format!(
                "source directory does not exist: {source_dir}"
            )));
        }

        for entry in WalkDir::new(source_path).follow_links(false).sort_by_file_name() {
            let entry =
                entry.map_err(|e| BorgError::Other(format!("walkdir error: {e}")))?;

            let rel_path = entry
                .path()
                .strip_prefix(source_path)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .to_string();

            // Skip root directory itself
            if rel_path.is_empty() {
                continue;
            }

            // Apply exclude patterns
            if excludes.is_match(&rel_path) {
                continue;
            }

            let metadata = entry.metadata()
                .map_err(|e| BorgError::Other(format!("stat error for {}: {e}", entry.path().display())))?;

            let file_type = metadata.file_type();

            let (entry_type, link_target) = if file_type.is_dir() {
                (ItemType::Directory, None)
            } else if file_type.is_symlink() {
                let target = std::fs::read_link(entry.path())
                    .map_err(|e| BorgError::Other(format!("readlink: {e}")))?;
                (ItemType::Symlink, Some(target.to_string_lossy().to_string()))
            } else if file_type.is_file() {
                (ItemType::RegularFile, None)
            } else {
                // Skip special files (block devices, FIFOs, etc.)
                continue;
            };

            let mtime_ns = metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec();

            let mut item = Item {
                path: rel_path,
                entry_type,
                mode: metadata.mode(),
                uid: metadata.uid(),
                gid: metadata.gid(),
                user: None,
                group: None,
                mtime: mtime_ns,
                atime: None,
                ctime: None,
                size: metadata.len(),
                chunks: Vec::new(),
                link_target,
                xattrs: None,
            };

            // For regular files, chunk and store the content
            if entry_type == ItemType::RegularFile && metadata.len() > 0 {
                let file_data = std::fs::read(entry.path())
                    .map_err(|e| BorgError::Io(e))?;

                let chunk_ranges = chunker::chunk_data(&file_data, &repo.config.chunker_params);

                for (offset, length) in chunk_ranges {
                    let chunk_data = &file_data[offset..offset + length];
                    let (chunk_id, csize, is_new) =
                        repo.store_chunk(chunk_data, compression)?;

                    stats.original_size += length as u64;
                    stats.compressed_size += csize as u64;
                    if is_new {
                        stats.deduplicated_size += csize as u64;
                    }

                    item.chunks.push(ChunkRef {
                        id: chunk_id,
                        size: length as u32,
                        csize,
                    });
                }

                stats.nfiles += 1;
            }

            all_items.push(item);
        }
    }

    // Serialize all items into a msgpack byte stream
    let items_bytes = rmp_serde::to_vec(&all_items)?;

    // Chunk the item stream and store each chunk
    let items_config = items_chunker_config();
    let item_chunk_ranges = chunker::chunk_data(&items_bytes, &items_config);

    let mut item_ptrs: Vec<ChunkId> = Vec::new();
    for (offset, length) in item_chunk_ranges {
        let chunk_data = &items_bytes[offset..offset + length];
        let (chunk_id, _csize, _is_new) = repo.store_chunk(chunk_data, compression)?;
        item_ptrs.push(chunk_id);
    }

    let time_end = Utc::now();

    // Build archive metadata
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".into());
    let username = whoami::username();

    let archive_meta = ArchiveMeta {
        name: archive_name.to_string(),
        hostname,
        username,
        time: time_start,
        time_end,
        chunker_params: repo.config.chunker_params.clone(),
        comment: String::new(),
        item_ptrs,
        stats: stats.clone(),
    };

    // Generate archive ID and store
    let mut archive_id = vec![0u8; 32];
    rand::thread_rng().fill_bytes(&mut archive_id);

    let meta_bytes = rmp_serde::to_vec(&archive_meta)?;
    let meta_packed = pack_object(ObjectType::ArchiveMeta, &meta_bytes, repo.crypto.as_ref())?;
    let archive_id_hex = hex::encode(&archive_id);
    repo.storage
        .put(&format!("archives/{archive_id_hex}"), &meta_packed)?;

    // Update manifest
    repo.manifest.timestamp = Utc::now();
    repo.manifest.archives.push(ArchiveEntry {
        name: archive_name.to_string(),
        id: archive_id,
        time: time_start,
    });

    // Save manifest and index
    repo.save_state()?;

    // Release lock
    lock::release_lock(repo.storage.as_ref(), lock_guard)?;

    info!(
        "Archive '{}' created: {} files, {} original, {} compressed, {} deduplicated",
        archive_name, stats.nfiles, stats.original_size, stats.compressed_size, stats.deduplicated_size
    );

    Ok(stats)
}
