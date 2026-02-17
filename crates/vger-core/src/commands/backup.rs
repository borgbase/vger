use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use chrono::Utc;
use ignore::{gitignore::Gitignore, WalkBuilder};
use rand::RngCore;
use rayon::prelude::*;
use tracing::{debug, info, warn};

use super::util::with_repo_lock;
use crate::chunker;
use crate::compress::Compression;
use crate::config::{ChunkerConfig, CommandDump, VgerConfig};
use crate::crypto::chunk_id::ChunkId;
use crate::error::{Result, VgerError};
use crate::limits::{self, ByteRateLimiter};
use crate::platform::{fs, shell};
use crate::repo::file_cache::FileCache;
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

pub(crate) fn flush_item_stream_chunk(
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

pub(crate) fn append_item_to_stream(
    repo: &mut Repository,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    item: &Item,
    items_config: &ChunkerConfig,
    compression: Compression,
) -> Result<()> {
    let item_bytes = rmp_serde::to_vec(item)?;
    item_stream.extend_from_slice(&item_bytes);
    if item_stream.len() >= items_config.avg_size as usize {
        flush_item_stream_chunk(repo, item_stream, item_ptrs, compression)?;
    }
    Ok(())
}

pub(crate) fn build_explicit_excludes(source: &Path, patterns: &[String]) -> Result<Gitignore> {
    let mut builder = ignore::gitignore::GitignoreBuilder::new(source);
    for pat in patterns {
        builder
            .add_line(None, pat)
            .map_err(|e| VgerError::Config(format!("invalid exclude pattern '{pat}': {e}")))?;
    }
    builder
        .build()
        .map_err(|e| VgerError::Config(format!("exclude matcher build failed: {e}")))
}

pub(crate) fn should_skip_for_device(
    one_file_system: bool,
    source_dev: u64,
    entry_dev: u64,
) -> bool {
    one_file_system && source_dev != entry_dev
}

#[cfg(unix)]
fn read_item_xattrs(path: &Path) -> Option<HashMap<String, Vec<u8>>> {
    let names = match xattr::list(path) {
        Ok(names) => names,
        Err(e) => {
            warn!(
                path = %path.display(),
                error = %e,
                "failed to list extended attributes"
            );
            return None;
        }
    };

    let mut attrs = HashMap::new();
    for name in names {
        let key = match name.to_str() {
            Some(name) => name.to_string(),
            None => {
                warn!(
                    path = %path.display(),
                    attr = ?name,
                    "skipping extended attribute with non-UTF8 name"
                );
                continue;
            }
        };

        match xattr::get(path, &name) {
            Ok(Some(value)) => {
                attrs.insert(key, value);
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    path = %path.display(),
                    attr = %key,
                    error = %e,
                    "failed to read extended attribute"
                );
            }
        }
    }

    if attrs.is_empty() {
        None
    } else {
        Some(attrs)
    }
}

#[cfg(not(unix))]
fn read_item_xattrs(_path: &Path) -> Option<HashMap<String, Vec<u8>>> {
    None
}

pub(crate) struct PreparedChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) uncompressed_size: u32,
    pub(crate) packed: Vec<u8>,
}

pub(crate) fn flush_regular_file_batch(
    repo: &mut Repository,
    compression: Compression,
    chunk_id_key: &[u8; 32],
    transform_pool: Option<&rayon::ThreadPool>,
    raw_chunks: &mut Vec<Vec<u8>>,
    item: &mut Item,
    stats: &mut SnapshotStats,
) -> Result<()> {
    if raw_chunks.is_empty() {
        return Ok(());
    }

    // Parallel phase: compute ChunkId + compress + encrypt for every chunk.
    let prepared_results: Vec<Result<PreparedChunk>> = {
        let crypto = repo.crypto.as_ref();
        let do_work = |data: &Vec<u8>| -> Result<PreparedChunk> {
            let chunk_id = ChunkId::compute(chunk_id_key, data);
            let compressed = crate::compress::compress(compression, data)?;
            let packed = pack_object(ObjectType::ChunkData, &compressed, crypto)?;
            Ok(PreparedChunk {
                chunk_id,
                uncompressed_size: data.len() as u32,
                packed,
            })
        };
        if let Some(pool) = transform_pool {
            pool.install(|| raw_chunks.par_iter().map(do_work).collect())
        } else {
            raw_chunks.iter().map(do_work).collect()
        }
    };

    raw_chunks.clear();

    // Sequential phase: dedup check and commit.
    for result in prepared_results {
        let prepared = result?;
        let size = prepared.uncompressed_size;

        if let Some(csize) = repo.bump_ref_if_exists(&prepared.chunk_id) {
            // Duplicate — already in index (or committed earlier in this batch).
            stats.original_size += size as u64;
            stats.compressed_size += csize as u64;
            item.chunks.push(ChunkRef {
                id: prepared.chunk_id,
                size,
                csize,
            });
        } else {
            // New chunk — commit packed data into pack writer.
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
const CROSS_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;
const CROSS_BATCH_MAX_CHUNKS: usize = 8192;

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
/// distribute PreparedChunks back to their owning files in walk order.
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
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    // Parallel phase: compute ChunkId + compress + encrypt for ALL accumulated chunks at once.
    let prepared_results: Vec<Result<PreparedChunk>> = {
        let crypto = repo.crypto.as_ref();
        let do_work = |data: &Vec<u8>| -> Result<PreparedChunk> {
            let chunk_id = ChunkId::compute(chunk_id_key, data);
            let compressed = crate::compress::compress(compression, data)?;
            let packed = pack_object(ObjectType::ChunkData, &compressed, crypto)?;
            Ok(PreparedChunk {
                chunk_id,
                uncompressed_size: data.len() as u32,
                packed,
            })
        };
        if let Some(pool) = transform_pool {
            pool.install(|| batch.raw_chunks.par_iter().map(do_work).collect())
        } else {
            batch.raw_chunks.iter().map(do_work).collect()
        }
    };

    // Convert to Option<PreparedChunk> so we can .take() without cloning packed data.
    let mut prepared: Vec<Option<PreparedChunk>> = prepared_results
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

        for slot in prepared
            .iter_mut()
            .skip(file.chunk_start)
            .take(file.chunk_count)
        {
            let p = slot.take().expect("chunk already consumed");
            let size = p.uncompressed_size;

            if let Some(csize) = repo.bump_ref_if_exists(&p.chunk_id) {
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

#[derive(Debug, Clone)]
pub enum BackupProgressEvent {
    SourceStarted {
        source_path: String,
    },
    SourceFinished {
        source_path: String,
    },
    FileStarted {
        path: String,
    },
    StatsUpdated {
        nfiles: u64,
        original_size: u64,
        compressed_size: u64,
        deduplicated_size: u64,
        current_file: Option<String>,
    },
}

pub(crate) fn emit_progress(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    event: BackupProgressEvent,
) {
    if let Some(callback) = progress.as_deref_mut() {
        callback(event);
    }
}

pub(crate) fn emit_stats_progress(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    stats: &SnapshotStats,
    current_file: Option<String>,
) {
    emit_progress(
        progress,
        BackupProgressEvent::StatsUpdated {
            nfiles: stats.nfiles,
            original_size: stats.original_size,
            compressed_size: stats.compressed_size,
            deduplicated_size: stats.deduplicated_size,
            current_file,
        },
    );
}

/// Run `vger backup` for one or more source directories.
pub struct BackupRequest<'a> {
    pub snapshot_name: &'a str,
    pub passphrase: Option<&'a str>,
    pub source_paths: &'a [String],
    pub source_label: &'a str,
    pub exclude_patterns: &'a [String],
    pub exclude_if_present: &'a [String],
    pub one_file_system: bool,
    pub git_ignore: bool,
    pub xattrs_enabled: bool,
    pub compression: Compression,
    pub command_dumps: &'a [CommandDump],
}

pub fn run(config: &VgerConfig, req: BackupRequest<'_>) -> Result<SnapshotStats> {
    run_with_progress(config, req, None)
}

fn build_transform_pool(max_threads: usize) -> Result<Option<rayon::ThreadPool>> {
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

fn compute_large_file_threshold(pipeline_buffer_bytes: usize, num_workers: usize) -> u64 {
    // Avoid potential overflow in `num_workers * 2` for pathological configs.
    let denom = num_workers.max(1).saturating_mul(2);
    (pipeline_buffer_bytes / denom.max(1)) as u64
}

/// Default timeout for command_dump execution (1 hour).
const COMMAND_DUMP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

/// Execute a shell command and capture its stdout.
fn execute_dump_command(dump: &CommandDump) -> Result<Vec<u8>> {
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
fn process_command_dumps(
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
fn process_regular_file_item(
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
        // Cache entries were pre-sanitized against the index before backup.
        let mut file_original: u64 = 0;
        let mut file_compressed: u64 = 0;
        for cr in &cached_refs {
            repo.increment_chunk_ref(&cr.id);
            file_original += cr.size as u64;
            file_compressed += cr.csize as u64;
        }

        stats.nfiles += 1;
        stats.original_size += file_original;
        stats.compressed_size += file_compressed;
        // No deduplicated_size contribution — all chunks already existed.

        item.chunks = cached_refs;

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
fn process_source_path(
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
) -> Result<()> {
    emit_progress(
        progress,
        BackupProgressEvent::SourceStarted {
            source_path: source_path.to_string(),
        },
    );

    let source = Path::new(source_path);
    if !source.exists() {
        return Err(VgerError::Other(format!(
            "source directory does not exist: {source_path}"
        )));
    }
    let source_dev = std::fs::symlink_metadata(source)
        .map(|m| fs::summarize_metadata(&m, &m.file_type()).device)
        .map_err(|e| VgerError::Other(format!("stat error for {}: {e}", source.display())))?;
    let explicit_excludes = build_explicit_excludes(source, exclude_patterns)?;

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

    let mut walk_builder = WalkBuilder::new(source);
    walk_builder.follow_links(false);
    walk_builder.hidden(false);
    walk_builder.ignore(false); // Only honor .gitignore (optional), not .ignore.
    walk_builder.git_global(false); // Ignore user-global excludes.
    walk_builder.git_exclude(false); // Ignore .git/info/exclude.
    walk_builder.parents(git_ignore); // Read parent .gitignore only when enabled.
    walk_builder.git_ignore(git_ignore);
    walk_builder.require_git(false);
    walk_builder.sort_by_file_name(std::ffi::OsStr::cmp);

    let markers = exclude_if_present.to_vec();
    let source_path_buf = source.to_path_buf();
    walk_builder.filter_entry(move |entry| {
        let path = entry.path();
        if path == source_path_buf {
            return true;
        }

        let rel = path.strip_prefix(&source_path_buf).unwrap_or(path);
        let is_dir = entry.file_type().is_some_and(|ft| ft.is_dir());

        // Apply explicit exclude patterns from config (gitignore syntax).
        if explicit_excludes
            .matched_path_or_any_parents(rel, is_dir)
            .is_ignore()
        {
            return false;
        }

        // Stay on the source filesystem when enabled.
        if one_file_system {
            if let Ok(metadata) = std::fs::symlink_metadata(path) {
                let entry_dev = fs::summarize_metadata(&metadata, &metadata.file_type()).device;
                if should_skip_for_device(one_file_system, source_dev, entry_dev) {
                    return false;
                }
            }
        }

        // Skip directories containing any configured marker file.
        if is_dir && !markers.is_empty() {
            for marker in &markers {
                if path.join(marker).exists() {
                    return false;
                }
            }
        }

        true
    });

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
                    )?;

                    if let Some(cb) = progress.as_deref_mut() {
                        cb(BackupProgressEvent::FileStarted {
                            path: item.path.clone(),
                        });
                    }

                    // Cache entries were pre-sanitized against the index before backup.
                    let mut file_original: u64 = 0;
                    let mut file_compressed: u64 = 0;
                    for cr in &cached_refs {
                        repo.increment_chunk_ref(&cr.id);
                        file_original += cr.size as u64;
                        file_compressed += cr.csize as u64;
                    }
                    stats.nfiles += 1;
                    stats.original_size += file_original;
                    stats.compressed_size += file_compressed;
                    item.chunks = cached_refs;

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
    )?;

    emit_progress(
        progress,
        BackupProgressEvent::SourceFinished {
            source_path: source_path.to_string(),
        },
    );

    Ok(())
}

pub fn run_with_progress(
    config: &VgerConfig,
    req: BackupRequest<'_>,
    mut progress: Option<&mut dyn FnMut(BackupProgressEvent)>,
) -> Result<SnapshotStats> {
    let snapshot_name = req.snapshot_name;
    let passphrase = req.passphrase;
    let source_paths = req.source_paths;
    let source_label = req.source_label;
    let exclude_patterns = req.exclude_patterns;
    let exclude_if_present = req.exclude_if_present;
    let one_file_system = req.one_file_system;
    let git_ignore = req.git_ignore;
    let xattrs_enabled = if req.xattrs_enabled && !fs::xattrs_supported() {
        warn!("xattrs requested but not supported on this platform; continuing without xattrs");
        false
    } else {
        req.xattrs_enabled
    };
    let compression = req.compression;
    let command_dumps = req.command_dumps;

    if source_paths.is_empty() && command_dumps.is_empty() {
        return Err(VgerError::Other(
            "no source paths or command dumps specified".into(),
        ));
    }
    if one_file_system && !cfg!(unix) {
        warn!("one_file_system filtering has limited support on this platform");
    }

    let multi_path = source_paths.len() > 1;

    let _nice_guard = match limits::NiceGuard::apply(config.limits.cpu.nice) {
        Ok(guard) => guard,
        Err(e) => {
            warn!(
                "could not apply limits.cpu.nice={}: {e}",
                config.limits.cpu.nice
            );
            None
        }
    };
    let read_limiter = ByteRateLimiter::from_mib_per_sec(config.limits.io.read_mib_per_sec);
    let max_pending_transform_bytes = config.limits.cpu.transform_batch_bytes();
    let max_pending_file_actions = config.limits.cpu.max_pending_actions();
    let upload_concurrency = config.limits.cpu.upload_concurrency();
    let pipeline_depth = config.limits.cpu.effective_pipeline_depth();

    // Resolve effective worker count before building the rayon pool so we
    // can right-size it in pipeline mode (avoids 2× thread oversubscription).
    let num_workers = if config.limits.cpu.max_threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
    } else {
        config.limits.cpu.max_threads
    };

    let transform_pool = if pipeline_depth > 0 {
        // Pipeline workers handle most parallelism. Size rayon for
        // large-file inline flushes: half the budget.
        // build_transform_pool returns None when threads==1,
        // preserving max_threads=1 → sequential semantics.
        let rayon_threads = num_workers.div_ceil(2);
        build_transform_pool(rayon_threads)?
    } else {
        build_transform_pool(config.limits.cpu.max_threads)?
    };

    let throttle_bps = limits::network_write_throttle_bps(&config.limits);
    let backend = storage::backend_from_config(&config.repository, throttle_bps)?;
    let backend =
        limits::wrap_backup_storage_backend(backend, &config.repository.url, &config.limits)?;
    let mut repo = Repository::open(backend, passphrase)?;

    with_repo_lock(&mut repo, |repo| {
        // Check snapshot name is unique while holding the lock.
        if repo.manifest().find_snapshot(snapshot_name).is_some() {
            return Err(VgerError::SnapshotAlreadyExists(snapshot_name.into()));
        }

        // Pre-sanitize stale file-cache entries whose chunks were pruned by
        // delete+compact. Must happen before enable_dedup_mode() which drops
        // the full chunk index. We temporarily take the cache to avoid
        // simultaneous mutable + immutable borrows of `repo`.
        //
        // INVARIANT: After this step, all remaining cache entries reference only
        // chunks present in the index. Cache-hit paths rely on this and skip
        // per-file existence checks for throughput.
        {
            let mut cache = repo.take_file_cache();
            let pruned = cache.prune_stale_entries(&|id| repo.chunk_exists(id));
            if pruned > 0 {
                info!(
                    pruned_entries = pruned,
                    "removed stale file cache entries referencing pruned chunks"
                );
            }
            repo.restore_file_cache(cache);
        }

        // Switch to tiered dedup mode to minimize memory during backup.
        // Uses mmap'd cache + xor filter when available, falls back to
        // DedupIndex HashMap on first backup or after index changes.
        // The full index is reloaded and updated at save_state time.
        repo.enable_tiered_dedup_mode();

        let time_start = Utc::now();
        let mut stats = SnapshotStats::default();
        let mut item_stream = Vec::new();
        let mut item_ptrs: Vec<ChunkId> = Vec::new();
        let items_config = items_chunker_config();
        let mut new_file_cache = FileCache::with_capacity(repo.file_cache().len());

        // Execute command dumps before walking filesystem
        process_command_dumps(
            repo,
            command_dumps,
            compression,
            &items_config,
            &mut item_stream,
            &mut item_ptrs,
            &mut stats,
            &mut progress,
            time_start,
        )?;

        // Apply configurable upload concurrency.
        repo.set_max_in_flight_uploads(upload_concurrency);
        repo.activate_pack_buffer_pool();

        let pipeline_buffer_bytes = config.limits.cpu.pipeline_buffer_bytes();

        if pipeline_depth > 0 && !source_paths.is_empty() {
            // Parallel pipeline: walk → parallel_map (read+chunk+hash+compress+encrypt)
            // → readahead → sequential consumer (dedup + pack commit).
            let file_cache_snapshot = repo.take_file_cache();
            let crypto = std::sync::Arc::clone(&repo.crypto);
            let large_file_threshold =
                compute_large_file_threshold(pipeline_buffer_bytes, num_workers);

            super::pipeline::run_parallel_pipeline(
                repo,
                source_paths,
                multi_path,
                exclude_patterns,
                exclude_if_present,
                one_file_system,
                git_ignore,
                xattrs_enabled,
                &file_cache_snapshot,
                &crypto,
                compression,
                read_limiter.as_deref(),
                num_workers,
                pipeline_depth,
                large_file_threshold,
                &items_config,
                &mut item_stream,
                &mut item_ptrs,
                &mut stats,
                &mut new_file_cache,
                &mut progress,
                transform_pool.as_ref(),
                max_pending_transform_bytes,
                max_pending_file_actions,
                pipeline_buffer_bytes,
            )?;
        } else {
            // Sequential fallback (pipeline_depth == 0 or no source paths).
            for source_path in source_paths {
                process_source_path(
                    repo,
                    source_path,
                    multi_path,
                    exclude_patterns,
                    exclude_if_present,
                    one_file_system,
                    git_ignore,
                    xattrs_enabled,
                    compression,
                    &items_config,
                    &mut item_stream,
                    &mut item_ptrs,
                    &mut stats,
                    &mut new_file_cache,
                    max_pending_transform_bytes,
                    max_pending_file_actions,
                    read_limiter.as_deref(),
                    transform_pool.as_ref(),
                    &mut progress,
                )?;
            }
        }
        flush_item_stream_chunk(repo, &mut item_stream, &mut item_ptrs, compression)?;

        let time_end = Utc::now();

        // Build snapshot metadata
        let hostname = crate::platform::hostname();
        let username = std::env::var("USER").unwrap_or_else(|_| "unknown".into());

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
            label: String::new(),
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
        repo.manifest_mut().timestamp = Utc::now();
        repo.manifest_mut().snapshots.push(SnapshotEntry {
            name: snapshot_name.to_string(),
            id: snapshot_id,
            time: time_start,
            source_label: source_label.to_string(),
            label: String::new(),
            source_paths: source_paths.to_vec(),
        });

        // Replace file cache with the freshly-built one (drops stale entries).
        repo.set_file_cache(new_file_cache);

        // Save manifest, index, and file cache
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn one_file_system_device_filter_logic() {
        assert!(should_skip_for_device(true, 42, 43));
        assert!(!should_skip_for_device(true, 42, 42));
        assert!(!should_skip_for_device(false, 42, 43));
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

    #[test]
    fn compute_large_file_threshold_scales_with_workers() {
        let pipeline_buffer_bytes = 32 * 1024 * 1024;
        assert_eq!(
            compute_large_file_threshold(pipeline_buffer_bytes, 1),
            16 * 1024 * 1024
        );
        assert_eq!(
            compute_large_file_threshold(pipeline_buffer_bytes, 2),
            8 * 1024 * 1024
        );
        // 0 workers is treated as "at least one" defensively.
        assert_eq!(
            compute_large_file_threshold(pipeline_buffer_bytes, 0),
            16 * 1024 * 1024
        );
    }

    #[test]
    fn compute_large_file_threshold_handles_overflowing_worker_math() {
        // Pathological config should not overflow/panic.
        assert_eq!(compute_large_file_threshold(1024, usize::MAX), 0);
    }
}
