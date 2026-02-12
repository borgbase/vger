use std::os::unix::fs::MetadataExt;
use std::path::Path;

use chrono::Utc;
use rand::RngCore;
use tracing::info;
use walkdir::WalkDir;

use super::util::with_repo_lock;
use crate::chunker;
use crate::compress::Compression;
use crate::config::{ChunkerConfig, VgerConfig};
use crate::crypto::chunk_id::ChunkId;
use crate::error::{Result, VgerError};
use crate::repo::format::{pack_object, ObjectType};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use crate::snapshot::{SnapshotMeta, SnapshotStats};
use crate::storage;

/// Items chunker config — finer granularity for the item metadata stream.
fn items_chunker_config() -> ChunkerConfig {
    ChunkerConfig {
        min_size: 32 * 1024,  // 32 KiB
        avg_size: 128 * 1024, // 128 KiB
        max_size: 512 * 1024, // 512 KiB
    }
}

fn flush_item_stream_chunk(
    repo: &mut Repository,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    compression: Compression,
) -> Result<()> {
    if item_stream.is_empty() {
        return Ok(());
    }
    let chunk_data = std::mem::take(item_stream);
    let (chunk_id, _csize, _is_new) = repo.store_chunk(&chunk_data, compression, PackType::Tree)?;
    item_ptrs.push(chunk_id);
    Ok(())
}

/// Run `vger backup` for one or more source directories.
pub struct BackupRequest<'a> {
    pub snapshot_name: &'a str,
    pub passphrase: Option<&'a str>,
    pub source_paths: &'a [String],
    pub source_label: &'a str,
    pub exclude_patterns: &'a [String],
    pub compression: Compression,
    pub label: &'a str,
}

pub fn run(config: &VgerConfig, req: BackupRequest<'_>) -> Result<SnapshotStats> {
    let snapshot_name = req.snapshot_name;
    let passphrase = req.passphrase;
    let source_paths = req.source_paths;
    let source_label = req.source_label;
    let exclude_patterns = req.exclude_patterns;
    let compression = req.compression;
    let label = req.label;

    if source_paths.is_empty() {
        return Err(VgerError::Other("no source paths specified".into()));
    }

    let multi_path = source_paths.len() > 1;

    let backend = storage::backend_from_config(&config.repository)?;
    let mut repo = Repository::open(backend, passphrase)?;

    with_repo_lock(&mut repo, |repo| {
        // Check snapshot name is unique while holding the lock.
        if repo.manifest.find_snapshot(snapshot_name).is_some() {
            return Err(VgerError::SnapshotAlreadyExists(snapshot_name.into()));
        }

        let time_start = Utc::now();
        let mut stats = SnapshotStats::default();
        let mut item_stream = Vec::new();
        let mut item_ptrs: Vec<ChunkId> = Vec::new();
        let items_config = items_chunker_config();

        // Build exclude patterns
        let mut glob_builder = globset::GlobSetBuilder::new();
        for pat in exclude_patterns {
            glob_builder.add(
                globset::Glob::new(pat).map_err(|e| {
                    VgerError::Config(format!("invalid exclude pattern '{pat}': {e}"))
                })?,
            );
        }
        let excludes = glob_builder
            .build()
            .map_err(|e| VgerError::Config(format!("glob build: {e}")))?;

        // Walk each source directory
        for source_path in source_paths {
            let source = Path::new(source_path.as_str());
            if !source.exists() {
                return Err(VgerError::Other(format!(
                    "source directory does not exist: {source_path}"
                )));
            }

            // For multi-path mode, derive basename prefix
            let prefix = if multi_path {
                let base = source
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| source_path.clone());
                // Emit a directory item for the prefix
                let dir_item = Item {
                    path: base.clone(),
                    entry_type: ItemType::Directory,
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                    user: None,
                    group: None,
                    mtime: 0,
                    atime: None,
                    ctime: None,
                    size: 0,
                    chunks: Vec::new(),
                    link_target: None,
                    xattrs: None,
                };
                let item_bytes = rmp_serde::to_vec(&dir_item)?;
                item_stream.extend_from_slice(&item_bytes);
                if item_stream.len() >= items_config.avg_size as usize {
                    flush_item_stream_chunk(repo, &mut item_stream, &mut item_ptrs, compression)?;
                }
                Some(base)
            } else {
                None
            };

            for entry in WalkDir::new(source).follow_links(false).sort_by_file_name() {
                let entry = entry.map_err(|e| VgerError::Other(format!("walkdir error: {e}")))?;

                let rel_path = entry
                    .path()
                    .strip_prefix(source)
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

                let metadata = entry.metadata().map_err(|e| {
                    VgerError::Other(format!("stat error for {}: {e}", entry.path().display()))
                })?;

                let file_type = metadata.file_type();

                let (entry_type, link_target) = if file_type.is_dir() {
                    (ItemType::Directory, None)
                } else if file_type.is_symlink() {
                    let target = std::fs::read_link(entry.path())
                        .map_err(|e| VgerError::Other(format!("readlink: {e}")))?;
                    (
                        ItemType::Symlink,
                        Some(target.to_string_lossy().to_string()),
                    )
                } else if file_type.is_file() {
                    (ItemType::RegularFile, None)
                } else {
                    // Skip special files (block devices, FIFOs, etc.)
                    continue;
                };

                let mtime_ns = metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec();

                // In multi-path mode, prefix each item path with the source basename
                let item_path = match &prefix {
                    Some(pfx) => format!("{pfx}/{rel_path}"),
                    None => rel_path,
                };

                let mut item = Item {
                    path: item_path,
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
                    let file_data = std::fs::read(entry.path()).map_err(VgerError::Io)?;

                    let chunk_ranges = chunker::chunk_data(&file_data, &repo.config.chunker_params);

                    for (offset, length) in chunk_ranges {
                        let chunk_data = &file_data[offset..offset + length];
                        let (chunk_id, csize, is_new) =
                            repo.store_chunk(chunk_data, compression, PackType::Data)?;

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

                // Stream item metadata to avoid materializing a full Vec<Item>.
                let item_bytes = rmp_serde::to_vec(&item)?;
                item_stream.extend_from_slice(&item_bytes);
                if item_stream.len() >= items_config.avg_size as usize {
                    flush_item_stream_chunk(repo, &mut item_stream, &mut item_ptrs, compression)?;
                }
            }
        }
        flush_item_stream_chunk(repo, &mut item_stream, &mut item_ptrs, compression)?;

        let time_end = Utc::now();

        // Build snapshot metadata
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".into());
        let username = whoami::username();

        let snapshot_meta = SnapshotMeta {
            name: snapshot_name.to_string(),
            hostname,
            username,
            time: time_start,
            time_end,
            chunker_params: repo.config.chunker_params.clone(),
            comment: String::new(),
            item_ptrs,
            stats: stats.clone(),
            source_label: source_label.to_string(),
            source_paths: source_paths.to_vec(),
            label: label.to_string(),
        };

        // Generate snapshot ID and store
        let mut snapshot_id = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut snapshot_id);

        let meta_bytes = rmp_serde::to_vec(&snapshot_meta)?;
        let meta_packed = pack_object(ObjectType::SnapshotMeta, &meta_bytes, repo.crypto.as_ref())?;
        let snapshot_id_hex = hex::encode(&snapshot_id);
        repo.storage
            .put(&format!("snapshots/{snapshot_id_hex}"), &meta_packed)?;

        // Update manifest
        repo.manifest.timestamp = Utc::now();
        repo.manifest.snapshots.push(SnapshotEntry {
            name: snapshot_name.to_string(),
            id: snapshot_id,
            time: time_start,
            source_label: source_label.to_string(),
            label: label.to_string(),
            source_paths: source_paths.to_vec(),
        });

        // Save manifest and index
        repo.save_state()?;

        info!(
            "Snapshot '{}' created: {} files, {} original, {} compressed, {} deduplicated",
            snapshot_name,
            stats.nfiles,
            stats.original_size,
            stats.compressed_size,
            stats.deduplicated_size
        );

        Ok(stats)
    })
}
