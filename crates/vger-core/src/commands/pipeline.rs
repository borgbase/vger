use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::Sender;
use ignore::WalkBuilder;
use tracing::warn;

use crate::chunker;
use crate::config::ChunkerConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::error::VgerError;
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use crate::repo::file_cache::FileCache;
use crate::snapshot::item::{ChunkRef, Item, ItemType};

use super::backup::{build_explicit_excludes, should_skip_for_device};

/// Byte-level backpressure for the pipeline channel.
///
/// Tracks total in-flight chunk bytes. The producer blocks when the budget
/// is exceeded; the consumer releases bytes after processing each chunk.
pub struct ByteBudget {
    in_flight: AtomicUsize,
    max_bytes: usize,
}

impl ByteBudget {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            max_bytes,
        }
    }

    /// Acquire `n` bytes of budget. Spins with parking when over budget.
    pub fn acquire(&self, n: usize) {
        loop {
            let current = self.in_flight.load(Ordering::Acquire);
            // Allow the send if we're under budget OR if nothing is in flight
            // (ensures progress even for chunks larger than the budget).
            if current == 0 || current + n <= self.max_bytes {
                // Try to claim the bytes atomically.
                if self
                    .in_flight
                    .compare_exchange_weak(
                        current,
                        current + n,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return;
                }
                // CAS failed, retry immediately.
                continue;
            }
            // Over budget — yield then retry.
            std::thread::park_timeout(std::time::Duration::from_micros(100));
        }
    }

    /// Release `n` bytes of budget after the consumer has processed a chunk.
    pub fn release(&self, n: usize) {
        self.in_flight.fetch_sub(n, Ordering::Release);
    }
}

/// Messages sent from the I/O producer to the consumer.
pub enum FileMessage {
    /// A file whose cached chunk refs are still valid (all chunks present).
    CacheHit {
        item: Item,
        cached_refs: Vec<ChunkRef>,
        metadata_summary: fs::MetadataSummary,
        abs_path: String,
    },
    /// Start of a new file being streamed chunk-by-chunk.
    FileStart {
        item: Item,
        metadata_summary: fs::MetadataSummary,
        abs_path: String,
    },
    /// A single chunk from the file currently being streamed.
    FileChunk {
        chunk_id: ChunkId,
        data: Vec<u8>,
    },
    /// End of the file currently being streamed.
    FileEnd,
    /// A non-file item (directory, symlink) — just needs to be appended to the item stream.
    NonFileItem { item: Item },
    /// Signals the start of processing a source path.
    SourceStarted { source_path: String },
    /// Signals the end of processing a source path.
    SourceFinished { source_path: String },
    /// All sources have been processed.
    Done,
    /// An error occurred during production.
    Error(VgerError),
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

/// Producer function: walks the filesystem, reads and chunks files, sends messages.
///
/// Runs on a dedicated I/O thread. The `file_cache` is borrowed read-only for
/// cache-hit detection. The consumer is responsible for all mutable repo operations.
///
/// Chunks are streamed one at a time via `FileStart`/`FileChunk`/`FileEnd` messages.
/// The `byte_budget` provides backpressure to cap total in-flight chunk data.
#[allow(clippy::too_many_arguments)]
pub fn run_producer(
    tx: &Sender<FileMessage>,
    source_paths: &[String],
    multi_path: bool,
    exclude_patterns: &[String],
    exclude_if_present: &[String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    chunker_config: &ChunkerConfig,
    chunk_id_key: &[u8; 32],
    file_cache: &FileCache,
    read_limiter: Option<&ByteRateLimiter>,
    byte_budget: &Arc<ByteBudget>,
) {
    for source_path in source_paths {
        if tx
            .send(FileMessage::SourceStarted {
                source_path: source_path.clone(),
            })
            .is_err()
        {
            return; // consumer dropped
        }

        if let Err(e) = produce_source(
            tx,
            source_path,
            multi_path,
            exclude_patterns,
            exclude_if_present,
            one_file_system,
            git_ignore,
            xattrs_enabled,
            chunker_config,
            chunk_id_key,
            file_cache,
            read_limiter,
            byte_budget,
        ) {
            let _ = tx.send(FileMessage::Error(e));
            return;
        }

        if tx
            .send(FileMessage::SourceFinished {
                source_path: source_path.clone(),
            })
            .is_err()
        {
            return;
        }
    }

    let _ = tx.send(FileMessage::Done);
}

#[allow(clippy::too_many_arguments)]
fn produce_source(
    tx: &Sender<FileMessage>,
    source_path: &str,
    multi_path: bool,
    exclude_patterns: &[String],
    exclude_if_present: &[String],
    one_file_system: bool,
    git_ignore: bool,
    xattrs_enabled: bool,
    chunker_config: &ChunkerConfig,
    chunk_id_key: &[u8; 32],
    file_cache: &FileCache,
    read_limiter: Option<&ByteRateLimiter>,
    byte_budget: &Arc<ByteBudget>,
) -> crate::error::Result<()> {
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

    let prefix = if multi_path {
        let base = source
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| source_path.to_string());
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
        if tx
            .send(FileMessage::NonFileItem { item: dir_item })
            .is_err()
        {
            return Ok(());
        }
        Some(base)
    } else {
        None
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

    for entry in walk_builder.build() {
        let entry = entry.map_err(|e| VgerError::Other(format!("walk error: {e}")))?;

        let rel_path = entry
            .path()
            .strip_prefix(source)
            .unwrap_or(entry.path())
            .to_string_lossy()
            .to_string();

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
            continue;
        };

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
                let cached_refs = cached_refs.to_vec();
                if tx
                    .send(FileMessage::CacheHit {
                        item,
                        cached_refs,
                        metadata_summary,
                        abs_path,
                    })
                    .is_err()
                {
                    return Ok(());
                }
                continue;
            }

            // Cache miss: stream chunks one at a time.
            if tx
                .send(FileMessage::FileStart {
                    item,
                    metadata_summary,
                    abs_path,
                })
                .is_err()
            {
                return Ok(());
            }

            let file = File::open(entry.path()).map_err(VgerError::Io)?;
            let chunk_stream = chunker::chunk_stream(
                limits::LimitedReader::new(file, read_limiter),
                chunker_config,
            );

            for chunk_result in chunk_stream {
                let chunk = chunk_result.map_err(|e| {
                    VgerError::Other(format!(
                        "chunking failed for {}: {e}",
                        entry.path().display()
                    ))
                })?;
                let chunk_id = ChunkId::compute(chunk_id_key, &chunk.data);
                let data_len = chunk.data.len();

                // Block until we have budget for this chunk's bytes.
                byte_budget.acquire(data_len);

                if tx
                    .send(FileMessage::FileChunk {
                        chunk_id,
                        data: chunk.data,
                    })
                    .is_err()
                {
                    return Ok(());
                }
            }

            if tx.send(FileMessage::FileEnd).is_err() {
                return Ok(());
            }
        } else {
            // Non-regular-file or zero-size regular file — send as-is.
            if tx.send(FileMessage::NonFileItem { item }).is_err() {
                return Ok(());
            }
        }
    }

    Ok(())
}
