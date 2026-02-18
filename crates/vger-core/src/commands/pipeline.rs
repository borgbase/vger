use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek};
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
    #[allow(dead_code)] // Used in tests; production code uses from_pre_acquired.
    fn new(budget: &'a ByteBudget, n: usize) -> Result<Self> {
        let acquired = budget.acquire(n)?;
        Ok(Self {
            budget,
            bytes: acquired,
        })
    }

    /// Wrap already-acquired bytes in an RAII guard (no acquire call).
    ///
    /// Used when budget was acquired by the walk thread before dispatch to
    /// workers. The guard ensures bytes are released if the worker `?`-bails.
    fn from_pre_acquired(budget: &'a ByteBudget, bytes: usize) -> Self {
        Self { budget, bytes }
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

/// Acquire budget for a walk entry before dispatching to a worker.
///
/// Called by the dedicated walk thread to reserve memory in walk order,
/// preventing deadlock between pariter's ordered output and worker-side
/// budget acquisition.
fn reserve_budget(entry: &WalkEntry, budget: &ByteBudget) -> Result<usize> {
    match entry {
        WalkEntry::File { file_size, .. } => {
            budget.acquire(usize::try_from(*file_size).unwrap_or(usize::MAX))
        }
        WalkEntry::FileSegment { len, .. } => budget.acquire(*len as usize),
        _ => Ok(0),
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
    FileSegment {
        /// Only present for segment 0; `None` for continuations.
        item: Option<Item>,
        abs_path: Arc<str>,
        metadata: fs::MetadataSummary,
        segment_index: usize,
        num_segments: usize,
        offset: u64,
        len: u64,
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
    /// Segment of a large file: worker processed one fixed-size slice.
    FileSegment {
        /// Only present for segment 0; `None` for continuations.
        item: Option<Item>,
        abs_path: Arc<str>,
        metadata: fs::MetadataSummary,
        chunks: Vec<super::backup::WorkerChunk>,
        acquired_bytes: usize,
        segment_index: usize,
        num_segments: usize,
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
/// Budget bytes are pre-acquired by the walk thread; `pre_acquired_bytes`
/// is wrapped in a [`BudgetGuard`] for error safety (auto-release on `?` bail).
#[allow(clippy::too_many_arguments)]
fn process_file_worker(
    entry: WalkEntry,
    chunk_id_key: &[u8; 32],
    crypto: &dyn CryptoEngine,
    compression: Compression,
    chunker_config: &ChunkerConfig,
    read_limiter: Option<&ByteRateLimiter>,
    budget: &ByteBudget,
    pre_acquired_bytes: usize,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<ProcessedEntry> {
    match entry {
        WalkEntry::File {
            item,
            abs_path,
            metadata,
            file_size,
        } => {
            // Budget was pre-acquired by the walk thread. Wrap in a guard for
            // error safety — if we `?`-bail, the guard drops and releases bytes.
            let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

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

        WalkEntry::FileSegment {
            item,
            abs_path,
            metadata,
            segment_index,
            num_segments,
            offset,
            len,
        } => {
            let guard = BudgetGuard::from_pre_acquired(budget, pre_acquired_bytes);

            let mut file = File::open(Path::new(&*abs_path)).map_err(VgerError::Io)?;
            file.seek(std::io::SeekFrom::Start(offset))
                .map_err(VgerError::Io)?;
            let reader = file.take(len);

            let chunk_stream = chunker::chunk_stream(
                limits::LimitedReader::new(reader, read_limiter),
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
            Ok(ProcessedEntry::FileSegment {
                item,
                abs_path,
                metadata,
                chunks: worker_chunks,
                acquired_bytes,
                segment_index,
                num_segments,
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
    segment_size: u64,
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
            segment_size,
        );

        let source_finished = std::iter::once(Ok(WalkEntry::SourceFinished {
            path: source_path.clone(),
        }));

        source_started.chain(entries).chain(source_finished)
    });

    Box::new(iter)
}

/// Lazy iterator over walk entries for a single filesystem entry.
///
/// Avoids heap allocation for the common zero/single-entry cases.
/// The `Segments` variant lazily yields `FileSegment` entries for large files.
enum WalkItems {
    /// No entries (e.g. root entry, special files).
    Empty,
    /// Exactly one entry (regular file, directory, symlink, error, cache hit).
    One(Option<Result<WalkEntry>>),
    /// Large file split into N segments, yielded lazily.
    Segments {
        /// Moved into segment 0; `None` for continuations.
        item: Option<Item>,
        abs_path: Arc<str>,
        metadata: fs::MetadataSummary,
        segment_size: u64,
        file_size: u64,
        num_segments: usize,
        next: usize,
    },
}

impl Iterator for WalkItems {
    type Item = Result<WalkEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            WalkItems::Empty => None,
            WalkItems::One(val) => val.take(),
            WalkItems::Segments {
                item,
                abs_path,
                metadata,
                segment_size,
                file_size,
                num_segments,
                next,
            } => {
                let i = *next;
                if i >= *num_segments {
                    return None;
                }
                *next = i + 1;
                let offset = i as u64 * *segment_size;
                let len = (*segment_size).min(*file_size - offset);
                // Segment 0 moves the item; continuations pass None.
                let seg_item = if i == 0 { item.take() } else { None };
                Some(Ok(WalkEntry::FileSegment {
                    item: seg_item,
                    abs_path: abs_path.clone(),
                    metadata: *metadata,
                    segment_index: i,
                    num_segments: *num_segments,
                    offset,
                    len,
                }))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            WalkItems::Empty => (0, Some(0)),
            WalkItems::One(val) => {
                let n = usize::from(val.is_some());
                (n, Some(n))
            }
            WalkItems::Segments {
                num_segments, next, ..
            } => {
                let remaining = num_segments.saturating_sub(*next);
                (remaining, Some(remaining))
            }
        }
    }
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
    segment_size: u64,
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
    let walk_entries = walk_builder
        .build()
        .flat_map(move |entry_result| -> WalkItems {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    return WalkItems::One(Some(Err(VgerError::Other(format!("walk error: {e}")))));
                }
            };

            let rel_path = entry
                .path()
                .strip_prefix(&source_owned)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .to_string();

            if rel_path.is_empty() {
                return WalkItems::Empty;
            }

            let metadata = match std::fs::symlink_metadata(entry.path()) {
                Ok(m) => m,
                Err(e) => {
                    return WalkItems::One(Some(Err(VgerError::Other(format!(
                        "stat error for {}: {e}",
                        entry.path().display()
                    )))));
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
                        return WalkItems::One(Some(Err(VgerError::Other(format!(
                            "readlink: {e}"
                        )))));
                    }
                }
            } else if file_type.is_file() {
                (ItemType::RegularFile, None)
            } else {
                return WalkItems::Empty; // skip special files
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
                    return WalkItems::One(Some(Ok(WalkEntry::CacheHit {
                        item,
                        abs_path,
                        metadata: metadata_summary,
                        cached_refs: cached_refs.to_vec(),
                    })));
                }

                let file_size = metadata_summary.size;
                if file_size > segment_size {
                    // Split large file into segments for lazy parallel processing.
                    let num_segments = file_size.div_ceil(segment_size) as usize;
                    let abs_path: Arc<str> = abs_path.into();
                    WalkItems::Segments {
                        item: Some(item),
                        abs_path,
                        metadata: metadata_summary,
                        segment_size,
                        file_size,
                        num_segments,
                        next: 0,
                    }
                } else {
                    WalkItems::One(Some(Ok(WalkEntry::File {
                        file_size,
                        item,
                        abs_path,
                        metadata: metadata_summary,
                    })))
                }
            } else {
                WalkItems::One(Some(Ok(WalkEntry::NonFile { item })))
            }
        });

    Box::new(prefix_item.chain(walk_entries))
}

/// Tracks in-progress accumulation of a segmented large file.
struct LargeFileAccum {
    item: Item,
    abs_path: Arc<str>,
    metadata: fs::MetadataSummary,
    next_expected_index: usize,
    num_segments: usize,
}

/// Validate and update the segment accumulator state machine.
///
/// For segment 0: initializes the accumulator (errors if one already exists).
/// For continuations: validates ordering, file identity, and segment count.
/// Returns `Ok(())` on success.
fn validate_segment_accum(
    large_file_accum: &mut Option<LargeFileAccum>,
    item: Option<Item>,
    abs_path: Arc<str>,
    metadata: fs::MetadataSummary,
    segment_index: usize,
    num_segments: usize,
) -> Result<()> {
    if segment_index == 0 {
        if large_file_accum.is_some() {
            return Err(VgerError::Other("nested large file segmentation".into()));
        }
        let item = item.ok_or_else(|| VgerError::Other("BUG: segment 0 must carry item".into()))?;
        *large_file_accum = Some(LargeFileAccum {
            item,
            abs_path,
            metadata,
            next_expected_index: 1,
            num_segments,
        });
    } else {
        let accum = large_file_accum
            .as_mut()
            .ok_or_else(|| VgerError::Other("FileSegment without preceding segment 0".into()))?;
        if segment_index != accum.next_expected_index {
            return Err(VgerError::Other(format!(
                "segment index mismatch: expected {}, got {segment_index}",
                accum.next_expected_index
            )));
        }
        if abs_path != accum.abs_path {
            return Err(VgerError::Other("segment file identity mismatch".into()));
        }
        if num_segments != accum.num_segments {
            return Err(VgerError::Other(format!(
                "segment count mismatch: expected {}, got {num_segments}",
                accum.num_segments
            )));
        }
        accum.next_expected_index += 1;
    }
    Ok(())
}

/// Commit worker chunks into a repository, updating item and stats.
///
/// Shared by `ProcessedFile` and `FileSegment` consumer arms.
fn process_worker_chunks(
    repo: &mut Repository,
    item: &mut Item,
    chunks: Vec<super::backup::WorkerChunk>,
    stats: &mut SnapshotStats,
    compression: Compression,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
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
    Ok(())
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
    budget: &ByteBudget,
    dedup_filter: Option<&xorf::Xor8>,
    large_file_accum: &mut Option<LargeFileAccum>,
) -> Result<()> {
    use super::backup::{append_item_to_stream, emit_stats_progress};

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

            process_worker_chunks(repo, &mut item, chunks, stats, compression, dedup_filter)?;

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

        ProcessedEntry::FileSegment {
            item,
            abs_path,
            metadata,
            chunks,
            acquired_bytes,
            segment_index,
            num_segments,
        } => {
            // For segment 0, fire progress event before validation consumes `item`.
            if segment_index == 0 {
                if let (Some(cb), Some(it)) = (progress.as_deref_mut(), item.as_ref()) {
                    cb(BackupProgressEvent::FileStarted {
                        path: it.path.clone(),
                    });
                }
            }

            validate_segment_accum(
                large_file_accum,
                item,
                abs_path,
                metadata,
                segment_index,
                num_segments,
            )?;

            // Process chunks via shared helper.
            let accum = large_file_accum.as_mut().ok_or_else(|| {
                VgerError::Other("BUG: large_file_accum missing after segment validation".into())
            })?;
            process_worker_chunks(
                repo,
                &mut accum.item,
                chunks,
                stats,
                compression,
                dedup_filter,
            )?;
            budget.release(acquired_bytes);

            if segment_index == num_segments - 1 {
                // Last segment: finalize.
                let mut accum = large_file_accum.take().ok_or_else(|| {
                    VgerError::Other("BUG: large_file_accum missing at segment finalization".into())
                })?;
                stats.nfiles += 1;

                append_item_to_stream(
                    repo,
                    item_stream,
                    item_ptrs,
                    &accum.item,
                    items_config,
                    compression,
                )?;

                new_file_cache.insert(
                    accum.abs_path.to_string(),
                    accum.metadata.device,
                    accum.metadata.inode,
                    accum.metadata.mtime_ns,
                    accum.metadata.ctime_ns,
                    accum.metadata.size,
                    std::mem::take(&mut accum.item.chunks),
                );

                emit_stats_progress(progress, stats, Some(std::mem::take(&mut accum.item.path)));
            }
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
    segment_size: u64,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    new_file_cache: &mut FileCache,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    pipeline_buffer_bytes: usize,
    dedup_filter: Option<&xorf::Xor8>,
) -> Result<()> {
    debug_assert!(segment_size > 0, "segment_size must be non-zero");
    let chunk_id_key = *crypto.chunk_id_key();
    let chunker_config = repo.config.chunker_params.clone();
    let budget = ByteBudget::new(pipeline_buffer_bytes);

    std::thread::scope(|s| {
        // Bounded channel: walk thread sends pre-budgeted (entry, bytes) pairs.
        // Capacity provides scheduling slack only — ByteBudget enforces the
        // byte-level memory cap. Sizing by worker count keeps the channel small
        // regardless of segment_size or mixed file sizes.
        let chan_cap = num_workers * 2;
        let (walk_tx, walk_rx) =
            std::sync::mpsc::sync_channel::<Result<(WalkEntry, usize)>>(chan_cap);

        // Reference so `move` closures capture `&ByteBudget`.
        let budget_ref = &budget;

        // --- Walk thread: iterate + acquire budget in walk order -------------
        s.spawn(move || {
            let walk_iter = build_walk_iter(
                source_paths,
                multi_path,
                exclude_patterns,
                exclude_if_present,
                one_file_system,
                git_ignore,
                xattrs_enabled,
                file_cache,
                segment_size,
            );

            for entry_result in walk_iter {
                match entry_result {
                    Ok(entry) => {
                        let acquired = match reserve_budget(&entry, budget_ref) {
                            Ok(n) => n,
                            Err(e) => {
                                // Budget poisoned — propagate and stop.
                                let _ = walk_tx.send(Err(e));
                                return;
                            }
                        };
                        if walk_tx.send(Ok((entry, acquired))).is_err() {
                            // Consumer dropped the receiver (error path) —
                            // release the bytes we just acquired and stop.
                            budget_ref.release(acquired);
                            return;
                        }
                    }
                    Err(e) => {
                        // Consumer aborts on first error — stop walking.
                        let _ = walk_tx.send(Err(e));
                        return;
                    }
                }
            }
            // walk_tx drops here → channel closes → walk_rx iterator ends.
        });

        // --- Parallel workers: receive pre-budgeted entries from channel -----
        let mut parallel_iter = walk_rx
            .into_iter()
            .parallel_map_scoped_custom(
                s,
                |o| o.threads(num_workers).buffer_size(num_workers),
                move |item: Result<(WalkEntry, usize)>| -> Result<ProcessedEntry> {
                    let (entry, pre_acquired) = item?;
                    process_file_worker(
                        entry,
                        &chunk_id_key,
                        &**crypto,
                        compression,
                        &chunker_config,
                        read_limiter,
                        budget_ref,
                        pre_acquired,
                        dedup_filter,
                    )
                },
            )
            .readahead_scoped_custom(s, |o| o.buffer_size(readahead_depth));

        // --- Consumer: sequential commit of ordered results -------------------
        // Explicit `while let` keeps `parallel_iter` owned so we can drop it
        // on the error path.  Shutdown sequence on error:
        //   1. budget.poison()  — wakes walk thread blocked in acquire()
        //   2. break            — stops consuming
        //   3. drop(parallel_iter) — drops walk_rx, joins workers; walk thread
        //      exits from either acquire() wakeup or send() disconnect
        let mut consume_err: Option<VgerError> = None;
        let mut large_file_accum: Option<LargeFileAccum> = None;
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
                    &budget,
                    dedup_filter,
                    &mut large_file_accum,
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

        // Verify no in-progress segmented file was left incomplete.
        if consume_err.is_none() {
            if let Some(accum) = &large_file_accum {
                consume_err = Some(VgerError::Other(format!(
                    "incomplete segmented file '{}': received {}/{} segments",
                    accum.abs_path, accum.next_expected_index, accum.num_segments,
                )));
            }
        }

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

    // -----------------------------------------------------------------------
    // Segment accumulator validation tests
    // -----------------------------------------------------------------------

    fn test_item(path: &str) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 1024,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        }
    }

    fn test_metadata() -> fs::MetadataSummary {
        fs::MetadataSummary {
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            device: 0,
            inode: 0,
            size: 1024,
        }
    }

    #[test]
    fn segment_out_of_order() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
        )
        .unwrap();

        // Skip segment 1, feed segment 2 → error.
        let err =
            validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 2, 3).unwrap_err();
        assert!(
            err.to_string().contains("segment index mismatch"),
            "expected 'segment index mismatch', got: {err}"
        );
    }

    #[test]
    fn segment_file_identity_mismatch() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
        )
        .unwrap();

        // Feed segment 1 with different abs_path → error.
        let err =
            validate_segment_accum(&mut accum, None, "/tmp/file_b".into(), meta, 1, 3).unwrap_err();
        assert!(
            err.to_string().contains("segment file identity mismatch"),
            "expected 'segment file identity mismatch', got: {err}"
        );
    }

    #[test]
    fn segment_nested_start() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A (3 segments).
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
        )
        .unwrap();

        // Feed segment 0 for file B before file A completes → error.
        let err = validate_segment_accum(
            &mut accum,
            Some(test_item("file_b")),
            "/tmp/file_b".into(),
            meta,
            0,
            2,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("nested large file segmentation"),
            "expected 'nested large file segmentation', got: {err}"
        );
    }

    #[test]
    fn segment_without_start() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 1 with no prior segment 0 → error.
        let err =
            validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 1, 3).unwrap_err();
        assert!(
            err.to_string()
                .contains("FileSegment without preceding segment 0"),
            "expected 'FileSegment without preceding segment 0', got: {err}"
        );
    }

    #[test]
    fn incomplete_accumulator_check() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 and 1 of a 3-segment file, but not segment 2.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
        )
        .unwrap();
        validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 1, 3).unwrap();

        // Simulate the post-loop check from run_parallel_pipeline.
        assert!(accum.is_some(), "accum should still be active");
        let a = accum.as_ref().unwrap();
        assert_eq!(a.next_expected_index, 2);
        assert_eq!(a.num_segments, 3);
        // The real pipeline generates this error:
        let err_msg = format!(
            "incomplete segmented file '{}': received {}/{} segments",
            a.abs_path, a.next_expected_index, a.num_segments,
        );
        assert!(
            err_msg.contains("incomplete segmented file"),
            "expected incomplete message, got: {err_msg}"
        );
    }

    #[test]
    fn segment_count_mismatch() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 with num_segments=3.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            meta,
            0,
            3,
        )
        .unwrap();

        // Feed segment 1 with different num_segments → error.
        let err =
            validate_segment_accum(&mut accum, None, "/tmp/file_a".into(), meta, 1, 5).unwrap_err();
        assert!(
            err.to_string().contains("segment count mismatch"),
            "expected 'segment count mismatch', got: {err}"
        );
    }

    #[test]
    fn segment_happy_path() {
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed all 3 segments in order.
        for i in 0..3 {
            let item = if i == 0 {
                Some(test_item("file_a"))
            } else {
                None
            };
            validate_segment_accum(&mut accum, item, "/tmp/file_a".into(), meta, i, 3).unwrap();
        }

        // Accumulator should be present with next_expected_index == 3.
        let a = accum.as_ref().unwrap();
        assert_eq!(a.next_expected_index, 3);
        assert_eq!(a.num_segments, 3);
        assert_eq!(a.item.path, "file_a");
        assert_eq!(&*a.abs_path, "/tmp/file_a");
    }

    // -----------------------------------------------------------------------
    // WalkItems iterator tests
    // -----------------------------------------------------------------------

    #[test]
    fn walk_items_empty() {
        let mut it = WalkItems::Empty;
        assert_eq!(it.size_hint(), (0, Some(0)));
        assert!(it.next().is_none());
    }

    #[test]
    fn walk_items_one() {
        let entry = Ok(WalkEntry::NonFile {
            item: test_item("x"),
        });
        let mut it = WalkItems::One(Some(entry));
        assert_eq!(it.size_hint(), (1, Some(1)));
        assert!(it.next().is_some());
        assert_eq!(it.size_hint(), (0, Some(0)));
        assert!(it.next().is_none());
    }

    #[test]
    fn walk_items_segments_lazy() {
        let meta = test_metadata();
        let mut it = WalkItems::Segments {
            item: Some(test_item("big")),
            abs_path: "/tmp/big".into(),
            metadata: meta,
            segment_size: 100,
            file_size: 250,
            num_segments: 3,
            next: 0,
        };
        assert_eq!(it.size_hint(), (3, Some(3)));

        // Segment 0 should carry Some(item).
        let seg0 = it.next().unwrap().unwrap();
        if let WalkEntry::FileSegment {
            item,
            segment_index,
            offset,
            len,
            ..
        } = seg0
        {
            assert!(item.is_some(), "segment 0 must carry item");
            assert_eq!(segment_index, 0);
            assert_eq!(offset, 0);
            assert_eq!(len, 100);
        } else {
            panic!("expected FileSegment");
        }

        // Segment 1 should carry None item.
        let seg1 = it.next().unwrap().unwrap();
        if let WalkEntry::FileSegment {
            item,
            segment_index,
            offset,
            len,
            ..
        } = seg1
        {
            assert!(item.is_none(), "continuation must carry None item");
            assert_eq!(segment_index, 1);
            assert_eq!(offset, 100);
            assert_eq!(len, 100);
        } else {
            panic!("expected FileSegment");
        }

        // Segment 2 (last): len should be remainder.
        let seg2 = it.next().unwrap().unwrap();
        if let WalkEntry::FileSegment {
            segment_index,
            offset,
            len,
            ..
        } = seg2
        {
            assert_eq!(segment_index, 2);
            assert_eq!(offset, 200);
            assert_eq!(len, 50);
        } else {
            panic!("expected FileSegment");
        }

        assert_eq!(it.size_hint(), (0, Some(0)));
        assert!(it.next().is_none());
    }
}
