use std::fs::File;
use std::path::Path;

use chrono::Utc;
use rayon::prelude::*;
use tracing::{debug, info, warn};

use crate::chunker;
use crate::compress::Compression;
use crate::config::{ChunkerConfig, CommandDump};
use crate::crypto::chunk_id::ChunkId;
use crate::error::{Result, VgerError};
use crate::limits::{self, ByteRateLimiter};
use crate::platform::{fs, shell};
use crate::repo::file_cache::FileCache;
use crate::repo::format::{pack_object, ObjectType};
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use crate::snapshot::SnapshotStats;

use super::walk::{build_configured_walker, read_item_xattrs};
use super::{append_item_to_stream, emit_progress, emit_stats_progress};
use super::{BackupProgressEvent, PreparedChunk};

/// Batch result from the parallel phase: either fully transformed or hash-only.
enum BatchResult {
    Transformed(PreparedChunk),
    HashOnly { chunk_id: ChunkId, size: u32 },
}

#[allow(clippy::too_many_arguments)]
pub(super) fn flush_regular_file_batch(
    repo: &mut Repository,
    compression: Compression,
    chunk_id_key: &[u8; 32],
    transform_pool: Option<&rayon::ThreadPool>,
    raw_chunks: &mut Vec<Vec<u8>>,
    item: &mut Item,
    stats: &mut SnapshotStats,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    if raw_chunks.is_empty() {
        return Ok(());
    }

    // Parallel phase: hash → filter check → transform or hash-only.
    let batch_results: Vec<Result<BatchResult>> = {
        let crypto = repo.crypto.as_ref();
        let do_work = |data: &Vec<u8>| -> Result<BatchResult> {
            let chunk_id = ChunkId::compute(chunk_id_key, data);

            // If the xor filter says "probably exists", skip compress+encrypt.
            if let Some(filter) = dedup_filter {
                use xorf::Filter;
                let key = crate::index::dedup_cache::chunk_id_to_u64(&chunk_id);
                if filter.contains(&key) {
                    return Ok(BatchResult::HashOnly {
                        chunk_id,
                        size: data.len() as u32,
                    });
                }
            }

            let compressed = crate::compress::compress(compression, data)?;
            let packed = pack_object(ObjectType::ChunkData, &compressed, crypto)?;
            Ok(BatchResult::Transformed(PreparedChunk {
                chunk_id,
                uncompressed_size: data.len() as u32,
                packed,
            }))
        };
        if let Some(pool) = transform_pool {
            pool.install(|| raw_chunks.par_iter().map(do_work).collect())
        } else {
            raw_chunks.iter().map(do_work).collect()
        }
    };

    // Sequential phase: dedup check and commit.
    // Don't clear raw_chunks yet — we may need raw data for false positives.
    for (i, result) in batch_results.into_iter().enumerate() {
        match result? {
            BatchResult::Transformed(prepared) => {
                let size = prepared.uncompressed_size;
                let existing = if dedup_filter.is_some() {
                    repo.bump_ref_prefilter_miss(&prepared.chunk_id)
                } else {
                    repo.bump_ref_if_exists(&prepared.chunk_id)
                };
                if let Some(csize) = existing {
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: prepared.chunk_id,
                        size,
                        csize,
                    });
                } else {
                    let csize = repo.commit_prepacked_chunk(
                        prepared.chunk_id,
                        prepared.packed,
                        prepared.uncompressed_size,
                        PackType::Data,
                    )?;
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    stats.deduplicated_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: prepared.chunk_id,
                        size,
                        csize,
                    });
                }
            }
            BatchResult::HashOnly { chunk_id, size } => {
                if let Some(csize) = repo.bump_ref_prefilter_hit(&chunk_id) {
                    // True dedup hit — skip transform.
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: chunk_id,
                        size,
                        csize,
                    });
                } else {
                    // False positive — inline compress+encrypt.
                    let csize = repo.commit_chunk_inline(
                        chunk_id,
                        &raw_chunks[i],
                        compression,
                        PackType::Data,
                    )?;
                    stats.original_size += size as u64;
                    stats.compressed_size += csize as u64;
                    stats.deduplicated_size += csize as u64;
                    item.chunks.push(ChunkRef {
                        id: chunk_id,
                        size,
                        csize,
                    });
                }
            }
        }
    }

    raw_chunks.clear();

    Ok(())
}

/// Tracks a small file pending in the cross-file batch.
struct PendingBatchFile {
    item: Item,
    metadata_summary: fs::MetadataSummary,
    abs_path: String,
    chunk_start: usize,
    chunk_count: usize,
}

/// Accumulates chunks from many small files for a single rayon dispatch.
struct CrossFileBatch {
    files: Vec<PendingBatchFile>,
    raw_chunks: Vec<Vec<u8>>,
    pending_bytes: usize,
}

/// Flush threshold: 32 MiB or 8192 chunks.
pub(super) const CROSS_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;
pub(super) const CROSS_BATCH_MAX_CHUNKS: usize = 8192;

impl CrossFileBatch {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            raw_chunks: Vec::new(),
            pending_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    fn should_flush(&self) -> bool {
        self.pending_bytes >= CROSS_BATCH_MAX_BYTES
            || self.raw_chunks.len() >= CROSS_BATCH_MAX_CHUNKS
    }

    fn add_file(
        &mut self,
        item: Item,
        data: Vec<u8>,
        metadata_summary: fs::MetadataSummary,
        abs_path: String,
    ) {
        let chunk_start = self.raw_chunks.len();
        self.pending_bytes += data.len();
        self.raw_chunks.push(data);
        self.files.push(PendingBatchFile {
            item,
            metadata_summary,
            abs_path,
            chunk_start,
            chunk_count: 1,
        });
    }
}

/// Flush all accumulated small-file chunks in a single rayon dispatch, then
/// distribute results back to their owning files in walk order.
#[allow(clippy::too_many_arguments)]
fn flush_cross_file_batch(
    batch: &mut CrossFileBatch,
    repo: &mut Repository,
    compression: Compression,
    chunk_id_key: &[u8; 32],
    transform_pool: Option<&rayon::ThreadPool>,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    // Parallel phase: hash → filter check → transform or hash-only.
    let batch_results: Vec<Result<BatchResult>> = {
        let crypto = repo.crypto.as_ref();
        let do_work = |data: &Vec<u8>| -> Result<BatchResult> {
            let chunk_id = ChunkId::compute(chunk_id_key, data);

            if let Some(filter) = dedup_filter {
                use xorf::Filter;
                let key = crate::index::dedup_cache::chunk_id_to_u64(&chunk_id);
                if filter.contains(&key) {
                    return Ok(BatchResult::HashOnly {
                        chunk_id,
                        size: data.len() as u32,
                    });
                }
            }

            let compressed = crate::compress::compress(compression, data)?;
            let packed = pack_object(ObjectType::ChunkData, &compressed, crypto)?;
            Ok(BatchResult::Transformed(PreparedChunk {
                chunk_id,
                uncompressed_size: data.len() as u32,
                packed,
            }))
        };
        if let Some(pool) = transform_pool {
            pool.install(|| batch.raw_chunks.par_iter().map(do_work).collect())
        } else {
            batch.raw_chunks.iter().map(do_work).collect()
        }
    };

    // Convert to Option so we can .take() without cloning packed data.
    let mut results: Vec<Option<BatchResult>> = batch_results
        .into_iter()
        .map(|r| r.map(Some))
        .collect::<Result<Vec<_>>>()?;

    // Sequential phase: iterate files in walk order, commit chunks, update caches.
    for file in batch.files.drain(..) {
        if let Some(cb) = progress.as_deref_mut() {
            cb(BackupProgressEvent::FileStarted {
                path: file.item.path.clone(),
            });
        }

        let mut item = file.item;

        let chunk_slice = file.chunk_start..(file.chunk_start + file.chunk_count);
        for (slot, raw) in results[chunk_slice.clone()]
            .iter_mut()
            .zip(&batch.raw_chunks[chunk_slice])
        {
            let br = slot.take().expect("chunk already consumed");
            match br {
                BatchResult::Transformed(p) => {
                    let size = p.uncompressed_size;
                    let existing = if dedup_filter.is_some() {
                        repo.bump_ref_prefilter_miss(&p.chunk_id)
                    } else {
                        repo.bump_ref_if_exists(&p.chunk_id)
                    };
                    if let Some(csize) = existing {
                        stats.original_size += size as u64;
                        stats.compressed_size += csize as u64;
                        item.chunks.push(ChunkRef {
                            id: p.chunk_id,
                            size,
                            csize,
                        });
                    } else {
                        let csize = repo.commit_prepacked_chunk(
                            p.chunk_id,
                            p.packed,
                            p.uncompressed_size,
                            PackType::Data,
                        )?;
                        stats.original_size += size as u64;
                        stats.compressed_size += csize as u64;
                        stats.deduplicated_size += csize as u64;
                        item.chunks.push(ChunkRef {
                            id: p.chunk_id,
                            size,
                            csize,
                        });
                    }
                }
                BatchResult::HashOnly { chunk_id, size } => {
                    if let Some(csize) = repo.bump_ref_prefilter_hit(&chunk_id) {
                        stats.original_size += size as u64;
                        stats.compressed_size += csize as u64;
                        item.chunks.push(ChunkRef {
                            id: chunk_id,
                            size,
                            csize,
                        });
                    } else {
                        // False positive — inline compress+encrypt.
                        let csize =
                            repo.commit_chunk_inline(chunk_id, raw, compression, PackType::Data)?;
                        stats.original_size += size as u64;
                        stats.compressed_size += csize as u64;
                        stats.deduplicated_size += csize as u64;
                        item.chunks.push(ChunkRef {
                            id: chunk_id,
                            size,
                            csize,
                        });
                    }
                }
            }
        }

        stats.nfiles += 1;

        append_item_to_stream(
            repo,
            item_stream,
            item_ptrs,
            &item,
            items_config,
            compression,
        )?;

        new_file_cache.insert(
            file.abs_path,
            file.metadata_summary.device,
            file.metadata_summary.inode,
            file.metadata_summary.mtime_ns,
            file.metadata_summary.ctime_ns,
            file.metadata_summary.size,
            std::mem::take(&mut item.chunks),
        );

        emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
    }

    batch.raw_chunks.clear();
    batch.pending_bytes = 0;

    Ok(())
}

pub(super) fn build_transform_pool(max_threads: usize) -> Result<Option<rayon::ThreadPool>> {
    // max_threads == 1 means explicitly sequential (no pool).
    if max_threads == 1 {
        return Ok(None);
    }

    // max_threads == 0 means use all available cores (rayon default).
    let mut builder = rayon::ThreadPoolBuilder::new();
    if max_threads > 1 {
        builder = builder.num_threads(max_threads);
    }

    builder
        .build()
        .map(Some)
        .map_err(|e| VgerError::Other(format!("failed to create rayon thread pool: {e}")))
}

/// Default timeout for command_dump execution (1 hour).
pub(super) const COMMAND_DUMP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

/// Execute a shell command and capture its stdout.
pub(super) fn execute_dump_command(dump: &CommandDump) -> Result<Vec<u8>> {
    let output =
        shell::run_script_with_timeout(&dump.command, COMMAND_DUMP_TIMEOUT).map_err(|e| {
            VgerError::Other(format!(
                "failed to execute command_dump '{}': {}",
                dump.name, e
            ))
        })?;

    if !output.status.success() {
        let code = output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(VgerError::Other(format!(
            "command_dump '{}' failed (exit code {code}): {stderr}",
            dump.name
        )));
    }

    if output.stdout.is_empty() {
        warn!(name = %dump.name, "command_dump produced empty output");
    }

    Ok(output.stdout)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_command_dumps(
    repo: &mut Repository,
    command_dumps: &[CommandDump],
    compression: Compression,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    time_start: chrono::DateTime<Utc>,
) -> Result<()> {
    if command_dumps.is_empty() {
        return Ok(());
    }

    let dumps_dir_item = Item {
        path: ".vger-dumps".to_string(),
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
    append_item_to_stream(
        repo,
        item_stream,
        item_ptrs,
        &dumps_dir_item,
        items_config,
        compression,
    )?;

    for dump in command_dumps {
        info!(
            name = %dump.name,
            command = %dump.command,
            "executing command dump"
        );
        let data = execute_dump_command(dump)?;
        let data_len = data.len() as u64;

        let chunk_ranges = chunker::chunk_data(&data, &repo.config.chunker_params);
        let chunk_id_key = *repo.crypto.chunk_id_key();

        let mut chunk_refs = Vec::new();
        for (offset, length) in chunk_ranges {
            let chunk_data = &data[offset..offset + length];
            let chunk_id = ChunkId::compute(&chunk_id_key, chunk_data);
            let size = length as u32;

            if let Some(csize) = repo.bump_ref_if_exists(&chunk_id) {
                stats.original_size += size as u64;
                stats.compressed_size += csize as u64;
                chunk_refs.push(ChunkRef {
                    id: chunk_id,
                    size,
                    csize,
                });
            } else {
                let (chunk_id, csize, _is_new) =
                    repo.store_chunk(chunk_data, compression, PackType::Data)?;
                stats.original_size += size as u64;
                stats.compressed_size += csize as u64;
                stats.deduplicated_size += csize as u64;
                chunk_refs.push(ChunkRef {
                    id: chunk_id,
                    size,
                    csize,
                });
            }
        }

        stats.nfiles += 1;

        let dump_item = Item {
            path: format!(".vger-dumps/{}", dump.name),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: time_start.timestamp_nanos_opt().unwrap_or(0),
            atime: None,
            ctime: None,
            size: data_len,
            chunks: chunk_refs,
            link_target: None,
            xattrs: None,
        };
        append_item_to_stream(
            repo,
            item_stream,
            item_ptrs,
            &dump_item,
            items_config,
            compression,
        )?;

        emit_stats_progress(progress, stats, Some(format!(".vger-dumps/{}", dump.name)));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_regular_file_item(
    repo: &mut Repository,
    entry_path: &Path,
    metadata_summary: fs::MetadataSummary,
    compression: Compression,
    transform_pool: Option<&rayon::ThreadPool>,
    read_limiter: Option<&ByteRateLimiter>,
    item: &mut Item,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    max_pending_transform_bytes: usize,
    max_pending_file_actions: usize,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    if let Some(cb) = progress.as_deref_mut() {
        cb(BackupProgressEvent::FileStarted {
            path: item.path.clone(),
        });
    }

    // File-level cache: skip read/chunk/compress/encrypt for unchanged files.
    let abs_path = entry_path.to_string_lossy().to_string();
    let file_size = metadata_summary.size;

    let cache_hit = repo.file_cache().lookup(
        &abs_path,
        metadata_summary.device,
        metadata_summary.inode,
        metadata_summary.mtime_ns,
        metadata_summary.ctime_ns,
        file_size,
    );

    if let Some(cached_refs) = cache_hit {
        // Clone to release the borrow on file_cache so we can mutate repo below.
        let cached_refs = cached_refs.to_vec();
        super::commit::commit_cache_hit(repo, item, cached_refs, stats);

        new_file_cache.insert(
            abs_path,
            metadata_summary.device,
            metadata_summary.inode,
            metadata_summary.mtime_ns,
            metadata_summary.ctime_ns,
            file_size,
            item.chunks.clone(),
        );

        debug!(path = %item.path, "file cache hit");
        emit_stats_progress(progress, stats, Some(item.path.clone()));
        return Ok(());
    }

    let chunk_id_key = *repo.crypto.chunk_id_key();
    let file = File::open(entry_path).map_err(VgerError::Io)?;
    let chunk_stream = chunker::chunk_stream(
        limits::LimitedReader::new(file, read_limiter),
        &repo.config.chunker_params,
    );

    let mut raw_chunks: Vec<Vec<u8>> = Vec::new();
    let mut pending_bytes: usize = 0;

    for chunk_result in chunk_stream {
        let chunk = chunk_result.map_err(|e| {
            VgerError::Other(format!("chunking failed for {}: {e}", entry_path.display()))
        })?;

        let data_len = chunk.data.len();
        pending_bytes = pending_bytes.saturating_add(data_len);
        raw_chunks.push(chunk.data);

        if pending_bytes >= max_pending_transform_bytes
            || raw_chunks.len() >= max_pending_file_actions
        {
            flush_regular_file_batch(
                repo,
                compression,
                &chunk_id_key,
                transform_pool,
                &mut raw_chunks,
                item,
                stats,
                dedup_filter,
            )?;
            pending_bytes = 0;
        }
    }

    flush_regular_file_batch(
        repo,
        compression,
        &chunk_id_key,
        transform_pool,
        &mut raw_chunks,
        item,
        stats,
        dedup_filter,
    )?;

    stats.nfiles += 1;

    // Update file cache with the chunks we just stored.
    new_file_cache.insert(
        abs_path,
        metadata_summary.device,
        metadata_summary.inode,
        metadata_summary.mtime_ns,
        metadata_summary.ctime_ns,
        file_size,
        item.chunks.clone(),
    );

    emit_stats_progress(progress, stats, Some(item.path.clone()));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_source_path(
    repo: &mut Repository,
    source_path: &str,
    multi_path: bool,
    exclude_patterns: &[String],
    exclude_if_present: &[String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    compression: Compression,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    max_pending_transform_bytes: usize,
    max_pending_file_actions: usize,
    read_limiter: Option<&ByteRateLimiter>,
    transform_pool: Option<&rayon::ThreadPool>,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    emit_progress(
        progress,
        BackupProgressEvent::SourceStarted {
            source_path: source_path.to_string(),
        },
    );

    let source = Path::new(source_path);
    let walk_builder = build_configured_walker(
        source,
        exclude_patterns,
        exclude_if_present,
        one_file_system,
        git_ignore,
    )?;

    // For multi-path mode, derive basename prefix.
    let prefix = if multi_path {
        let base = source
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| source_path.to_string());
        // Emit a directory item for the prefix.
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
        append_item_to_stream(
            repo,
            item_stream,
            item_ptrs,
            &dir_item,
            items_config,
            compression,
        )?;
        Some(base)
    } else {
        None
    };

    let chunk_id_key = *repo.crypto.chunk_id_key();
    let min_chunk_size = repo.config.chunker_params.min_size as u64;
    let mut cross_batch = CrossFileBatch::new();

    for entry in walk_builder.build() {
        let entry = entry.map_err(|e| VgerError::Other(format!("walk error: {e}")))?;

        let rel_path = entry
            .path()
            .strip_prefix(source)
            .unwrap_or(entry.path())
            .to_string_lossy()
            .to_string();

        // Skip root directory itself.
        if rel_path.is_empty() {
            continue;
        }

        let metadata = std::fs::symlink_metadata(entry.path()).map_err(|e| {
            VgerError::Other(format!("stat error for {}: {e}", entry.path().display()))
        })?;

        let file_type = metadata.file_type();
        let metadata_summary = fs::summarize_metadata(&metadata, &file_type);

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

        // In multi-path mode, prefix each item path with the source basename.
        let item_path = match &prefix {
            Some(pfx) => format!("{pfx}/{rel_path}"),
            None => rel_path,
        };

        let mut item = Item {
            path: item_path,
            entry_type,
            mode: metadata_summary.mode,
            uid: metadata_summary.uid,
            gid: metadata_summary.gid,
            user: None,
            group: None,
            mtime: metadata_summary.mtime_ns,
            atime: None,
            ctime: None,
            size: metadata_summary.size,
            chunks: Vec::new(),
            link_target,
            xattrs: None,
        };

        if xattrs_enabled {
            item.xattrs = read_item_xattrs(entry.path());
        }

        // For regular files, chunk and store the content.
        if entry_type == ItemType::RegularFile && metadata_summary.size > 0 {
            // Small-file fast path: read directly, accumulate in cross-file batch.
            if metadata_summary.size < min_chunk_size {
                // Flush batch before cache-hit check since it may need walk-order items.
                let abs_path = entry.path().to_string_lossy().to_string();

                let cache_hit = repo.file_cache().lookup(
                    &abs_path,
                    metadata_summary.device,
                    metadata_summary.inode,
                    metadata_summary.mtime_ns,
                    metadata_summary.ctime_ns,
                    metadata_summary.size,
                );

                if let Some(cached_refs) = cache_hit {
                    let cached_refs = cached_refs.to_vec();
                    // Flush batch to preserve walk order before the cache-hit item.
                    flush_cross_file_batch(
                        &mut cross_batch,
                        repo,
                        compression,
                        &chunk_id_key,
                        transform_pool,
                        items_config,
                        item_stream,
                        item_ptrs,
                        stats,
                        new_file_cache,
                        progress,
                        dedup_filter,
                    )?;

                    if let Some(cb) = progress.as_deref_mut() {
                        cb(BackupProgressEvent::FileStarted {
                            path: item.path.clone(),
                        });
                    }

                    super::commit::commit_cache_hit(repo, &mut item, cached_refs, stats);

                    new_file_cache.insert(
                        abs_path,
                        metadata_summary.device,
                        metadata_summary.inode,
                        metadata_summary.mtime_ns,
                        metadata_summary.ctime_ns,
                        metadata_summary.size,
                        item.chunks.clone(),
                    );

                    debug!(path = %item.path, "file cache hit (sequential small)");
                    emit_stats_progress(progress, stats, Some(item.path.clone()));
                } else {
                    // Cache miss — read and add to batch.
                    let data = std::fs::read(entry.path()).map_err(VgerError::Io)?;
                    cross_batch.add_file(item, data, metadata_summary, abs_path);

                    if cross_batch.should_flush() {
                        flush_cross_file_batch(
                            &mut cross_batch,
                            repo,
                            compression,
                            &chunk_id_key,
                            transform_pool,
                            items_config,
                            item_stream,
                            item_ptrs,
                            stats,
                            new_file_cache,
                            progress,
                            dedup_filter,
                        )?;
                    }
                    continue; // item will be appended by flush
                }
            } else {
                // Large file — flush batch first to maintain walk order.
                flush_cross_file_batch(
                    &mut cross_batch,
                    repo,
                    compression,
                    &chunk_id_key,
                    transform_pool,
                    items_config,
                    item_stream,
                    item_ptrs,
                    stats,
                    new_file_cache,
                    progress,
                    dedup_filter,
                )?;

                process_regular_file_item(
                    repo,
                    entry.path(),
                    metadata_summary,
                    compression,
                    transform_pool,
                    read_limiter,
                    &mut item,
                    stats,
                    new_file_cache,
                    progress,
                    max_pending_transform_bytes,
                    max_pending_file_actions,
                    dedup_filter,
                )?;
            }
        } else {
            // Non-regular-file — flush batch first to maintain walk order.
            flush_cross_file_batch(
                &mut cross_batch,
                repo,
                compression,
                &chunk_id_key,
                transform_pool,
                items_config,
                item_stream,
                item_ptrs,
                stats,
                new_file_cache,
                progress,
                dedup_filter,
            )?;
        }

        // Stream item metadata to avoid materializing a full Vec<Item>.
        append_item_to_stream(
            repo,
            item_stream,
            item_ptrs,
            &item,
            items_config,
            compression,
        )?;
    }

    // Flush any remaining small files in the cross-file batch.
    flush_cross_file_batch(
        &mut cross_batch,
        repo,
        compression,
        &chunk_id_key,
        transform_pool,
        items_config,
        item_stream,
        item_ptrs,
        stats,
        new_file_cache,
        progress,
        dedup_filter,
    )?;

    emit_progress(
        progress,
        BackupProgressEvent::SourceFinished {
            source_path: source_path.to_string(),
        },
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CommandDump;

    #[cfg(windows)]
    fn shell_echo_hello() -> &'static str {
        "Write-Output hello"
    }

    #[cfg(not(windows))]
    fn shell_echo_hello() -> &'static str {
        "echo hello"
    }

    #[cfg(windows)]
    fn shell_fail() -> &'static str {
        "exit 1"
    }

    #[cfg(not(windows))]
    fn shell_fail() -> &'static str {
        "false"
    }

    #[cfg(windows)]
    fn shell_success_no_output() -> &'static str {
        "$null = 1"
    }

    #[cfg(not(windows))]
    fn shell_success_no_output() -> &'static str {
        "true"
    }

    #[test]
    fn execute_dump_command_captures_stdout() {
        let dump = CommandDump {
            name: "test.txt".to_string(),
            command: shell_echo_hello().to_string(),
        };
        let result = execute_dump_command(&dump).unwrap();
        let text = String::from_utf8(result).unwrap();
        assert_eq!(text.trim_end(), "hello");
    }

    #[test]
    fn execute_dump_command_fails_on_nonzero_exit() {
        let dump = CommandDump {
            name: "fail.txt".to_string(),
            command: shell_fail().to_string(),
        };
        let result = execute_dump_command(&dump);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("command_dump 'fail.txt' failed"));
    }

    #[test]
    fn execute_dump_command_empty_stdout_succeeds() {
        let dump = CommandDump {
            name: "empty.txt".to_string(),
            command: shell_success_no_output().to_string(),
        };
        let result = execute_dump_command(&dump).unwrap();
        assert!(result.is_empty());
    }
}
