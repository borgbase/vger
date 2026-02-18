use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::mem;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use ignore::WalkBuilder;
use pariter::IteratorExt;
use tracing::{debug, warn};

use crate::chunker;
use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::crypto::CryptoEngine;
use crate::error::{Result, VgerError};
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use crate::repo::file_cache::FileCache;
use crate::repo::format::{pack_object, ObjectType};
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use crate::snapshot::SnapshotStats;

use super::backup::{build_explicit_excludes, should_skip_for_device, BackupProgressEvent};

// ---------------------------------------------------------------------------
// ByteBudget — semaphore-style memory cap for in-flight pipeline data
// ---------------------------------------------------------------------------

/// Tracks available bytes for in-flight pipeline data.
///
/// Workers call [`acquire`] before buffering file data and the consumer calls
/// [`release`] after committing chunks. This caps the total materialized
/// `ProcessedFile` data to approximately `capacity` bytes.
///
/// **Approximation**: Workers acquire by walk-time `file_size` (from stat
/// metadata). If a file grows between stat and read, actual buffered bytes may
/// slightly exceed the cap. This is a pragmatic trade-off — re-statting at open
/// time is still racy, and chunk-level accounting adds significant complexity
/// for an edge case.
struct ByteBudget {
    state: Mutex<BudgetState>,
    freed: Condvar,
    peak_acquired: AtomicUsize,
}

struct BudgetState {
    available: usize,
    capacity: usize,
    poisoned: bool,
}

impl ByteBudget {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                available: capacity,
                capacity,
                poisoned: false,
            }),
            freed: Condvar::new(),
            peak_acquired: AtomicUsize::new(0),
        }
    }

    /// Block until `n` bytes are available, then subtract them.
    ///
    /// If `n > capacity`, it is clamped to `capacity` so a single file larger
    /// than the budget can still proceed (it just acquires the entire budget
    /// and runs alone). Returns `Err` if the budget has been poisoned.
    fn acquire(&self, n: usize) -> Result<usize> {
        let mut st = self.state.lock().unwrap();
        let n = n.min(st.capacity);
        loop {
            if st.poisoned {
                return Err(VgerError::Other("pipeline budget poisoned".into()));
            }
            if st.available >= n {
                st.available -= n;
                let acquired = st.capacity - st.available;
                self.peak_acquired.fetch_max(acquired, Ordering::Relaxed);
                return Ok(n);
            }
            st = self.freed.wait(st).unwrap();
        }
    }

    /// Return `n` bytes to the budget and wake any blocked workers.
    fn release(&self, n: usize) {
        let mut st = self.state.lock().unwrap();
        st.available = (st.available + n).min(st.capacity);
        self.freed.notify_all();
    }

    /// Poison the budget so all current and future `acquire` calls return `Err`.
    fn poison(&self) {
        let mut st = self.state.lock().unwrap();
        st.poisoned = true;
        self.freed.notify_all();
    }

    /// Return the peak number of acquired (in-flight) bytes observed.
    fn peak_acquired(&self) -> usize {
        self.peak_acquired.load(Ordering::Relaxed)
    }
}

/// RAII guard that releases budget bytes on drop (worker failure safety).
///
/// Call [`defuse`] to transfer ownership of the acquired bytes to the
/// `ProcessedEntry` — the consumer will then call `release` explicitly.
struct BudgetGuard<'a> {
    budget: &'a ByteBudget,
    bytes: usize,
}

impl<'a> BudgetGuard<'a> {
    /// Acquire `n` bytes from the budget, returning a guard that will release
    /// them on drop if not defused.
    fn new(budget: &'a ByteBudget, n: usize) -> Result<Self> {
        let acquired = budget.acquire(n)?;
        Ok(Self {
            budget,
            bytes: acquired,
        })
    }

    /// Consume the guard without releasing the bytes. Returns the byte count
    /// so the caller can pass it to `ProcessedEntry::ProcessedFile.acquired_bytes`.
    fn defuse(self) -> usize {
        let bytes = self.bytes;
        mem::forget(self);
        bytes
    }
}

impl Drop for BudgetGuard<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.release(self.bytes);
        }
    }
}

/// Read xattrs for a path (unix only).
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

// ---------------------------------------------------------------------------
// Parallel file processing pipeline (pariter-based)
// ---------------------------------------------------------------------------

/// Walk entry produced by the sequential walk phase.
enum WalkEntry {
    File {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        file_size: u64,
    },
    CacheHit {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        cached_refs: Vec<ChunkRef>,
    },
    NonFile {
        item: Item,
    },
    SourceStarted {
        path: String,
    },
    SourceFinished {
        path: String,
    },
}

/// Result from a parallel worker.
pub(crate) enum ProcessedEntry {
    /// Small/medium file: chunks classified by xor filter (hash-only or fully transformed).
    ProcessedFile {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        chunks: Vec<super::backup::WorkerChunk>,
        /// Bytes acquired from ByteBudget; consumer must release after committing.
        acquired_bytes: usize,
    },
    /// Large file (>= threshold): worker did NO I/O, consumer streams it inline.
    LargeFile {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
    },
    /// File cache hit — consumer just bumps refcounts.
    CacheHit {
        item: Item,
        abs_path: String,
        metadata: fs::MetadataSummary,
        cached_refs: Vec<ChunkRef>,
    },
    /// Non-file item (directory, symlink, zero-size file).
    NonFile {
        item: Item,
    },
    SourceStarted {
        path: String,
    },
    SourceFinished {
        path: String,
    },
}

/// Classify a single chunk: hash → xor filter check → transform or hash-only.
fn classify_chunk(
    chunk_id: ChunkId,
    data: Vec<u8>,
    dedup_filter: Option<&xorf::Xor8>,
    compression: Compression,
    crypto: &dyn CryptoEngine,
) -> Result<super::backup::WorkerChunk> {
    if let Some(filter) = dedup_filter {
        use xorf::Filter;
        let key = crate::index::dedup_cache::chunk_id_to_u64(&chunk_id);
        if filter.contains(&key) {
            return Ok(super::backup::WorkerChunk::Hashed(
                super::backup::HashedChunk { chunk_id, data },
            ));
        }
    }
    let compressed = crate::compress::compress(compression, &data)?;
    let packed = pack_object(ObjectType::ChunkData, &compressed, crypto)?;
    Ok(super::backup::WorkerChunk::Prepared(
        super::backup::PreparedChunk {
            chunk_id,
            uncompressed_size: data.len() as u32,
            packed,
        },
    ))
}

/// Process a single walk entry in a parallel worker thread.
///
/// For small/medium files: read, chunk, hash → classify via xor filter.
/// For large files (>= threshold): return immediately so consumer streams them inline.
#[allow(clippy::too_many_arguments)]
fn process_file_worker(
    entry: WalkEntry,
    chunk_id_key: &[u8; 32],
    crypto: &dyn CryptoEngine,
    compression: Compression,
    chunker_config: &ChunkerConfig,
    read_limiter: Option<&ByteRateLimiter>,
    large_file_threshold: u64,
    budget: &ByteBudget,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<ProcessedEntry> {
    match entry {
        WalkEntry::File {
            item,
            abs_path,
            metadata,
            file_size,
        } => {
            // Large files bypass parallel processing — consumer streams them.
            if file_size >= large_file_threshold {
                return Ok(ProcessedEntry::LargeFile {
                    item,
                    abs_path,
                    metadata,
                });
            }

            // Acquire budget before any I/O. The guard auto-releases on `?` bail.
            let requested_bytes = usize::try_from(file_size).unwrap_or(usize::MAX);
            let guard = BudgetGuard::new(budget, requested_bytes)?;

            // Small file (< min_chunk_size): read whole, single chunk.
            if file_size < chunker_config.min_size as u64 {
                let mut file = File::open(Path::new(&abs_path)).map_err(VgerError::Io)?;
                let mut data = Vec::with_capacity(file_size as usize);
                if let Some(limiter) = read_limiter {
                    limits::LimitedReader::new(&mut file, Some(limiter))
                        .read_to_end(&mut data)
                        .map_err(VgerError::Io)?;
                } else {
                    file.read_to_end(&mut data).map_err(VgerError::Io)?;
                }

                let chunk_id = ChunkId::compute(chunk_id_key, &data);
                let worker_chunk =
                    classify_chunk(chunk_id, data, dedup_filter, compression, crypto)?;

                let acquired_bytes = guard.defuse();
                return Ok(ProcessedEntry::ProcessedFile {
                    item,
                    abs_path,
                    metadata,
                    chunks: vec![worker_chunk],
                    acquired_bytes,
                });
            }

            // Medium file: read, chunk via FastCDC, then hash → classify each chunk.
            let file = File::open(Path::new(&abs_path)).map_err(VgerError::Io)?;
            let chunk_stream = chunker::chunk_stream(
                limits::LimitedReader::new(file, read_limiter),
                chunker_config,
            );

            let mut worker_chunks = Vec::new();
            for chunk_result in chunk_stream {
                let chunk = chunk_result.map_err(|e| {
                    VgerError::Other(format!("chunking failed for {abs_path}: {e}"))
                })?;

                let chunk_id = ChunkId::compute(chunk_id_key, &chunk.data);
                worker_chunks.push(classify_chunk(
                    chunk_id,
                    chunk.data,
                    dedup_filter,
                    compression,
                    crypto,
                )?);
            }

            let acquired_bytes = guard.defuse();
            Ok(ProcessedEntry::ProcessedFile {
                item,
                abs_path,
                metadata,
                chunks: worker_chunks,
                acquired_bytes,
            })
        }

        WalkEntry::CacheHit {
            item,
            abs_path,
            metadata,
            cached_refs,
        } => Ok(ProcessedEntry::CacheHit {
            item,
            abs_path,
            metadata,
            cached_refs,
        }),

        WalkEntry::NonFile { item } => Ok(ProcessedEntry::NonFile { item }),

        WalkEntry::SourceStarted { path } => Ok(ProcessedEntry::SourceStarted { path }),

        WalkEntry::SourceFinished { path } => Ok(ProcessedEntry::SourceFinished { path }),
    }
}

/// Build a walk iterator that yields `WalkEntry` items for all source paths.
#[allow(clippy::too_many_arguments)]
fn build_walk_iter<'a>(
    source_paths: &'a [String],
    multi_path: bool,
    exclude_patterns: &'a [String],
    exclude_if_present: &'a [String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &'a FileCache,
) -> Box<dyn Iterator<Item = Result<WalkEntry>> + Send + 'a> {
    let iter = source_paths.iter().flat_map(move |source_path| {
        let source_started = std::iter::once(Ok(WalkEntry::SourceStarted {
            path: source_path.clone(),
        }));

        let entries = walk_source(
            source_path,
            multi_path,
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            xattrs_enabled,
            file_cache,
        );

        let source_finished = std::iter::once(Ok(WalkEntry::SourceFinished {
            path: source_path.clone(),
        }));

        source_started.chain(entries).chain(source_finished)
    });

    Box::new(iter)
}

/// Walk a single source path and yield WalkEntry items.
#[allow(clippy::too_many_arguments)]
fn walk_source<'a>(
    source_path: &'a str,
    multi_path: bool,
    exclude_patterns: &'a [String],
    exclude_if_present: &'a [String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &'a FileCache,
) -> Box<dyn Iterator<Item = Result<WalkEntry>> + Send + 'a> {
    // Validate source exists and get device id.
    let source = Path::new(source_path);
    let source_dev = match std::fs::symlink_metadata(source) {
        Ok(m) => fs::summarize_metadata(&m, &m.file_type()).device,
        Err(e) => {
            return Box::new(std::iter::once(Err(VgerError::Other(format!(
                "source directory does not exist: {source_path}: {e}"
            )))));
        }
    };

    let explicit_excludes = match build_explicit_excludes(source, exclude_patterns) {
        Ok(e) => e,
        Err(e) => return Box::new(std::iter::once(Err(e))),
    };

    // Multi-path prefix item.
    let prefix = if multi_path {
        let base = source
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| source_path.to_string());
        Some(base)
    } else {
        None
    };

    let prefix_item: Box<dyn Iterator<Item = Result<WalkEntry>> + Send> =
        if let Some(ref pfx) = prefix {
            let dir_item = Item {
                path: pfx.clone(),
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
            Box::new(std::iter::once(Ok(WalkEntry::NonFile { item: dir_item })))
        } else {
            Box::new(std::iter::empty())
        };

    let mut walk_builder = WalkBuilder::new(source);
    walk_builder.follow_links(false);
    walk_builder.hidden(false);
    walk_builder.ignore(false);
    walk_builder.git_global(false);
    walk_builder.git_exclude(false);
    walk_builder.parents(git_ignore);
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

        if explicit_excludes
            .matched_path_or_any_parents(rel, is_dir)
            .is_ignore()
        {
            return false;
        }

        if one_file_system {
            if let Ok(metadata) = std::fs::symlink_metadata(path) {
                let entry_dev = fs::summarize_metadata(&metadata, &metadata.file_type()).device;
                if should_skip_for_device(one_file_system, source_dev, entry_dev) {
                    return false;
                }
            }
        }

        if is_dir && !markers.is_empty() {
            for marker in &markers {
                if path.join(marker).exists() {
                    return false;
                }
            }
        }

        true
    });

    let source_owned = source.to_path_buf();
    let prefix_clone = prefix.clone();
    let walk_entries = walk_builder.build().filter_map(move |entry_result| {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                return Some(Err(VgerError::Other(format!("walk error: {e}"))));
            }
        };

        let rel_path = entry
            .path()
            .strip_prefix(&source_owned)
            .unwrap_or(entry.path())
            .to_string_lossy()
            .to_string();

        if rel_path.is_empty() {
            return None;
        }

        let metadata = match std::fs::symlink_metadata(entry.path()) {
            Ok(m) => m,
            Err(e) => {
                return Some(Err(VgerError::Other(format!(
                    "stat error for {}: {e}",
                    entry.path().display()
                ))));
            }
        };

        let file_type = metadata.file_type();
        let metadata_summary = fs::summarize_metadata(&metadata, &file_type);

        let (entry_type, link_target) = if file_type.is_dir() {
            (ItemType::Directory, None)
        } else if file_type.is_symlink() {
            match std::fs::read_link(entry.path()) {
                Ok(target) => (
                    ItemType::Symlink,
                    Some(target.to_string_lossy().to_string()),
                ),
                Err(e) => {
                    return Some(Err(VgerError::Other(format!("readlink: {e}"))));
                }
            }
        } else if file_type.is_file() {
            (ItemType::RegularFile, None)
        } else {
            return None; // skip special files
        };

        let item_path = match &prefix_clone {
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

        if entry_type == ItemType::RegularFile && metadata_summary.size > 0 {
            let abs_path = entry.path().to_string_lossy().to_string();

            // Check file cache (read-only).
            let cache_hit = file_cache.lookup(
                &abs_path,
                metadata_summary.device,
                metadata_summary.inode,
                metadata_summary.mtime_ns,
                metadata_summary.ctime_ns,
                metadata_summary.size,
            );

            if let Some(cached_refs) = cache_hit {
                return Some(Ok(WalkEntry::CacheHit {
                    item,
                    abs_path,
                    metadata: metadata_summary,
                    cached_refs: cached_refs.to_vec(),
                }));
            }

            Some(Ok(WalkEntry::File {
                file_size: metadata_summary.size,
                item,
                abs_path,
                metadata: metadata_summary,
            }))
        } else {
            Some(Ok(WalkEntry::NonFile { item }))
        }
    });

    Box::new(prefix_item.chain(walk_entries))
}

/// Consume a single processed entry: dedup check, pack commit, item stream, file cache.
#[allow(clippy::too_many_arguments)]
fn consume_processed_entry(
    entry: ProcessedEntry,
    repo: &mut Repository,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    compression: Compression,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    // For LargeFile inline processing:
    transform_pool: Option<&rayon::ThreadPool>,
    read_limiter: Option<&ByteRateLimiter>,
    max_pending_transform_bytes: usize,
    max_pending_file_actions: usize,
    budget: &ByteBudget,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    use super::backup::{append_item_to_stream, emit_stats_progress, flush_regular_file_batch};

    match entry {
        ProcessedEntry::ProcessedFile {
            mut item,
            abs_path,
            metadata,
            chunks,
            acquired_bytes,
        } => {
            if let Some(cb) = progress.as_deref_mut() {
                cb(BackupProgressEvent::FileStarted {
                    path: item.path.clone(),
                });
            }

            for worker_chunk in chunks {
                match worker_chunk {
                    super::backup::WorkerChunk::Prepared(prepared) => {
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
                    super::backup::WorkerChunk::Hashed(hashed) => {
                        let size = hashed.data.len() as u32;
                        if let Some(csize) = repo.bump_ref_prefilter_hit(&hashed.chunk_id) {
                            // True dedup hit — skip transform.
                            stats.original_size += size as u64;
                            stats.compressed_size += csize as u64;
                            item.chunks.push(ChunkRef {
                                id: hashed.chunk_id,
                                size,
                                csize,
                            });
                        } else {
                            // False positive — inline compress+encrypt.
                            let csize = repo.commit_chunk_inline(
                                hashed.chunk_id,
                                &hashed.data,
                                compression,
                                PackType::Data,
                            )?;
                            stats.original_size += size as u64;
                            stats.compressed_size += csize as u64;
                            stats.deduplicated_size += csize as u64;
                            item.chunks.push(ChunkRef {
                                id: hashed.chunk_id,
                                size,
                                csize,
                            });
                        }
                    }
                }
            }

            // Release budget bytes now that chunks are committed.
            budget.release(acquired_bytes);

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
                abs_path,
                metadata.device,
                metadata.inode,
                metadata.mtime_ns,
                metadata.ctime_ns,
                metadata.size,
                std::mem::take(&mut item.chunks),
            );

            emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
        }

        ProcessedEntry::LargeFile {
            mut item,
            abs_path,
            metadata,
        } => {
            if let Some(cb) = progress.as_deref_mut() {
                cb(BackupProgressEvent::FileStarted {
                    path: item.path.clone(),
                });
            }

            let chunk_id_key = *repo.crypto.chunk_id_key();
            let file = File::open(Path::new(&abs_path)).map_err(VgerError::Io)?;
            let chunk_stream = chunker::chunk_stream(
                limits::LimitedReader::new(file, read_limiter),
                &repo.config.chunker_params,
            );

            let mut raw_chunks: Vec<Vec<u8>> = Vec::new();
            let mut pending_bytes: usize = 0;

            for chunk_result in chunk_stream {
                let chunk = chunk_result.map_err(|e| {
                    VgerError::Other(format!("chunking failed for {abs_path}: {e}"))
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
                        &mut item,
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
                &mut item,
                stats,
                dedup_filter,
            )?;

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
                abs_path,
                metadata.device,
                metadata.inode,
                metadata.mtime_ns,
                metadata.ctime_ns,
                metadata.size,
                std::mem::take(&mut item.chunks),
            );

            emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
        }

        ProcessedEntry::CacheHit {
            mut item,
            abs_path,
            metadata,
            cached_refs,
        } => {
            if let Some(cb) = progress.as_deref_mut() {
                cb(BackupProgressEvent::FileStarted {
                    path: item.path.clone(),
                });
            }

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

            append_item_to_stream(
                repo,
                item_stream,
                item_ptrs,
                &item,
                items_config,
                compression,
            )?;

            new_file_cache.insert(
                abs_path,
                metadata.device,
                metadata.inode,
                metadata.mtime_ns,
                metadata.ctime_ns,
                metadata.size,
                std::mem::take(&mut item.chunks),
            );

            debug!(path = %item.path, "file cache hit (parallel)");
            emit_stats_progress(progress, stats, Some(std::mem::take(&mut item.path)));
        }

        ProcessedEntry::NonFile { item } => {
            append_item_to_stream(
                repo,
                item_stream,
                item_ptrs,
                &item,
                items_config,
                compression,
            )?;
        }

        ProcessedEntry::SourceStarted { path } => {
            super::backup::emit_progress(
                progress,
                BackupProgressEvent::SourceStarted { source_path: path },
            );
        }

        ProcessedEntry::SourceFinished { path } => {
            super::backup::emit_progress(
                progress,
                BackupProgressEvent::SourceFinished { source_path: path },
            );
        }
    }

    Ok(())
}

/// Run the parallel file processing pipeline using pariter.
///
/// Walk → parallel_map (read+chunk+hash+compress+encrypt) → readahead → sequential consumer.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_parallel_pipeline(
    repo: &mut Repository,
    source_paths: &[String],
    multi_path: bool,
    exclude_patterns: &[String],
    exclude_if_present: &[String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    file_cache: &FileCache,
    crypto: &Arc<dyn CryptoEngine>,
    compression: Compression,
    read_limiter: Option<&ByteRateLimiter>,
    num_workers: usize,
    readahead_depth: usize,
    large_file_threshold: u64,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    transform_pool: Option<&rayon::ThreadPool>,
    max_pending_transform_bytes: usize,
    max_pending_file_actions: usize,
    pipeline_buffer_bytes: usize,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    let chunk_id_key = *crypto.chunk_id_key();
    let chunker_config = repo.config.chunker_params.clone();
    let budget = ByteBudget::new(pipeline_buffer_bytes);

    std::thread::scope(|s| {
        let walk_iter = build_walk_iter(
            source_paths,
            multi_path,
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            xattrs_enabled,
            file_cache,
        );

        // Take a reference so the `move` closure captures `&ByteBudget` (which
        // is Copy+Clone) rather than trying to move `ByteBudget` itself.
        let budget_ref = &budget;

        // Flatten Result<WalkEntry> — errors become ProcessedEntry errors through the pipeline.
        let mut parallel_iter = walk_iter
            .parallel_map_scoped_custom(
                s,
                |o| o.threads(num_workers),
                move |entry_result: Result<WalkEntry>| -> Result<ProcessedEntry> {
                    let entry = entry_result?;
                    process_file_worker(
                        entry,
                        &chunk_id_key,
                        &**crypto,
                        compression,
                        &chunker_config,
                        read_limiter,
                        large_file_threshold,
                        budget_ref,
                        dedup_filter,
                    )
                },
            )
            .readahead_scoped_custom(s, |o| o.buffer_size(readahead_depth));

        // Use explicit `while let` so `parallel_iter` is only borrowed via
        // `next()` — keeping it owned in this scope. On error we poison the
        // budget first (unblocking workers stuck in `acquire`), then drop the
        // iterator (which joins workers). This prevents deadlock.
        // A `for` loop would move the iterator, making the explicit
        // `drop(parallel_iter)` below impossible.
        let mut consume_err: Option<VgerError> = None;
        #[allow(clippy::while_let_on_iterator)]
        while let Some(result) = parallel_iter.next() {
            match result.and_then(|entry| {
                consume_processed_entry(
                    entry,
                    repo,
                    stats,
                    new_file_cache,
                    items_config,
                    item_stream,
                    item_ptrs,
                    compression,
                    progress,
                    transform_pool,
                    read_limiter,
                    max_pending_transform_bytes,
                    max_pending_file_actions,
                    &budget,
                    dedup_filter,
                )
            }) {
                Ok(()) => {}
                Err(e) => {
                    budget.poison();
                    consume_err = Some(e);
                    break;
                }
            }
        }
        drop(parallel_iter);

        debug_assert!(
            budget.peak_acquired() <= pipeline_buffer_bytes,
            "pipeline exceeded memory budget: peak {} > cap {}",
            budget.peak_acquired(),
            pipeline_buffer_bytes,
        );

        match consume_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_budget_acquire_release() {
        let budget = ByteBudget::new(1024);
        budget.acquire(512).unwrap();
        budget.acquire(512).unwrap();
        // Budget is exhausted — release and re-acquire.
        budget.release(1024);
        budget.acquire(1024).unwrap();
        budget.release(1024);
    }

    #[test]
    fn byte_budget_blocks_and_unblocks() {
        use std::sync::atomic::AtomicBool;
        let budget = Arc::new(ByteBudget::new(100));
        budget.acquire(100).unwrap();

        let acquired = Arc::new(AtomicBool::new(false));
        let acquired2 = Arc::clone(&acquired);
        let budget2 = Arc::clone(&budget);

        let handle = std::thread::spawn(move || {
            budget2.acquire(50).unwrap();
            acquired2.store(true, Ordering::SeqCst);
            budget2.release(50);
        });

        // Give the thread time to block.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!acquired.load(Ordering::SeqCst), "should be blocked");

        // Release budget — thread should unblock.
        budget.release(100);
        handle.join().unwrap();
        assert!(acquired.load(Ordering::SeqCst), "should have acquired");
    }

    #[test]
    fn byte_budget_oversized_clamps() {
        // Request larger than capacity doesn't deadlock — clamped to capacity.
        let budget = ByteBudget::new(64);
        budget.acquire(128).unwrap();
        budget.release(64); // clamped to 64
    }

    #[test]
    fn byte_budget_poison_unblocks() {
        let budget = Arc::new(ByteBudget::new(100));
        budget.acquire(100).unwrap();

        let budget2 = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            let result = budget2.acquire(50);
            assert!(result.is_err(), "should fail after poison");
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        budget.poison();
        handle.join().unwrap();

        // Subsequent acquire also fails.
        assert!(budget.acquire(1).is_err());
    }

    #[test]
    fn byte_budget_concurrent_stress() {
        let budget = Arc::new(ByteBudget::new(1000));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let b = Arc::clone(&budget);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    b.acquire(100).unwrap();
                    // Simulate some work.
                    std::thread::yield_now();
                    b.release(100);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All released — full budget should be available.
        budget.acquire(1000).unwrap();
        budget.release(1000);
    }

    #[test]
    fn byte_budget_enforces_cap() {
        use std::sync::atomic::AtomicUsize;

        let cap = 500usize;
        let budget = Arc::new(ByteBudget::new(cap));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let b = Arc::clone(&budget);
            let inf = Arc::clone(&in_flight);
            let pk = Arc::clone(&peak);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    let chunk = 100;
                    b.acquire(chunk).unwrap();
                    let current = inf.fetch_add(chunk, Ordering::SeqCst) + chunk;
                    pk.fetch_max(current, Ordering::Relaxed);
                    std::thread::yield_now();
                    inf.fetch_sub(chunk, Ordering::SeqCst);
                    b.release(chunk);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(
            peak.load(Ordering::SeqCst) <= cap,
            "peak {} exceeded cap {}",
            peak.load(Ordering::SeqCst),
            cap
        );
    }

    #[test]
    fn budget_guard_releases_on_drop() {
        let budget = ByteBudget::new(100);
        {
            let _guard = BudgetGuard::new(&budget, 100).unwrap();
            // Guard drops here — should release.
        }
        // Should succeed because guard released.
        budget.acquire(100).unwrap();
        budget.release(100);
    }

    #[test]
    fn budget_guard_defuse_transfers() {
        let budget = ByteBudget::new(100);
        let bytes = {
            let guard = BudgetGuard::new(&budget, 80).unwrap();
            guard.defuse()
        };
        assert_eq!(bytes, 80);
        // Budget should still be held — only 20 available.
        // Acquire the remaining 20 to prove exactly 80 is still held.
        budget.acquire(20).unwrap();
        // Now manually release both the defused amount and our 20.
        budget.release(80);
        budget.release(20);
        // Full budget available again.
        budget.acquire(100).unwrap();
        budget.release(100);
    }

    #[test]
    fn budget_guard_oversized_request_clamps_to_capacity() {
        let budget = ByteBudget::new(64);
        let bytes = {
            let guard = BudgetGuard::new(&budget, 128).unwrap();
            guard.defuse()
        };
        assert_eq!(bytes, 64, "defused bytes should match acquired budget");

        // Budget is fully held by the defused guard, so no bytes remain.
        budget.release(bytes);
        budget.acquire(64).unwrap();
        budget.release(64);
    }
}
