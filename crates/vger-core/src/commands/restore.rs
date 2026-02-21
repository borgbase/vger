use std::collections::{HashMap, HashSet};
#[cfg(not(unix))]
use std::io::{Seek, Write as IoWrite};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use smallvec::SmallVec;
use tracing::{debug, info};

use crate::compress;
use crate::config::VgerConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::crypto::pack_id::PackId;
use crate::crypto::CryptoEngine;
use crate::error::{Result, VgerError};
use crate::platform::fs;
use crate::repo::format::{unpack_object_expect_with_context_into, ObjectType};
#[cfg(test)]
use crate::snapshot::item::Item;
use crate::snapshot::item::ItemType;
use crate::storage::StorageBackend;

use super::util::open_repo_without_index_or_cache;

// ---------------------------------------------------------------------------
// Constants for coalesced parallel restore
// ---------------------------------------------------------------------------

/// Maximum gap (in bytes) between blobs in the same pack that will be coalesced
/// into a single range read rather than issuing separate requests.
const MAX_COALESCE_GAP: u64 = 256 * 1024; // 256 KiB

/// Maximum size of a single coalesced range read.
const MAX_READ_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB

/// Number of parallel reader threads for downloading pack data.
/// Kept high for throughput while reducing peak memory versus 8 workers.
const MAX_READER_THREADS: usize = 6;

/// Maximum number of simultaneously open output files per restore worker.
/// Caps fd usage while still avoiding per-chunk open/close churn.
const MAX_OPEN_FILES_PER_GROUP: usize = 16;

// ---------------------------------------------------------------------------
// Data structures for the coalesced parallel restore
// ---------------------------------------------------------------------------

/// Where to write a chunk's decompressed data.
struct WriteTarget {
    file_idx: usize,
    file_offset: u64,
}

/// A chunk within a coalesced read group.
struct PlannedBlob {
    chunk_id: ChunkId,
    pack_offset: u64,
    stored_size: u32,
    expected_size: u32,
    /// Most chunks are referenced by exactly one file, so SmallVec stores
    /// the single target inline without a heap allocation.
    targets: SmallVec<[WriteTarget; 1]>,
}

/// A coalesced read — maps to a single storage range GET.
struct ReadGroup {
    pack_id: PackId,
    read_start: u64,
    read_end: u64, // exclusive
    blobs: Vec<PlannedBlob>,
}

/// Output file metadata for post-restore attribute application.
struct PlannedFile {
    target_path: PathBuf,
    total_size: u64,
    mode: u32,
    mtime: i64,
    xattrs: Option<HashMap<String, Vec<u8>>>,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Run `vger restore`.
pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    pattern: Option<&str>,
    xattrs_enabled: bool,
) -> Result<RestoreStats> {
    let filter = pattern
        .map(|p| {
            globset::GlobBuilder::new(p)
                .literal_separator(false)
                .build()
                .map(|g| g.compile_matcher())
        })
        .transpose()
        .map_err(|e| VgerError::Config(format!("invalid pattern: {e}")))?;

    restore_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        move |path| {
            filter
                .as_ref()
                .map(|matcher| matcher.is_match(path))
                .unwrap_or(true)
        },
    )
}

/// Run `vger restore` for a selected set of paths.
///
/// An item is included if its path exactly matches an entry in `selected_paths`,
/// or if any prefix of its path matches (i.e. a parent directory was selected).
pub fn run_selected(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    selected_paths: &HashSet<String>,
    xattrs_enabled: bool,
) -> Result<RestoreStats> {
    restore_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        |path| path_matches_selection(path, selected_paths),
    )
}

// ---------------------------------------------------------------------------
// Core restore logic — phased approach
// ---------------------------------------------------------------------------

fn restore_with_filter<F>(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    xattrs_enabled: bool,
    mut include_path: F,
) -> Result<RestoreStats>
where
    F: FnMut(&str) -> bool,
{
    let mut repo = open_repo_without_index_or_cache(config, passphrase)?;
    // Shrink blob cache for restore — the parallel pipeline reads pack data
    // directly via storage.get_range(), so the cache only serves the small
    // item-stream tree-pack chunks. 2 MiB is plenty.
    repo.set_blob_cache_max_bytes(2 * 1024 * 1024);
    let xattrs_enabled = if xattrs_enabled && !fs::xattrs_supported() {
        tracing::warn!(
            "xattrs requested but not supported on this platform; continuing without xattrs"
        );
        false
    } else {
        xattrs_enabled
    };

    // Try to open the mmap restore cache before loading the index.
    let restore_cache = repo.open_restore_cache();

    // Resolve "latest" or exact snapshot name
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();
    if resolved_name != snapshot_name {
        info!("Resolved '{}' to snapshot {}", snapshot_name, resolved_name);
    }

    // Load raw item stream bytes (not decoded Items).  When the restore cache
    // is available, read tree-pack chunks via the cache to avoid loading the
    // full chunk index.
    let items_stream = if let Some(ref cache) = restore_cache {
        match super::list::load_snapshot_item_stream_via_lookup(&mut repo, &resolved_name, |id| {
            cache.lookup(id)
        }) {
            Ok(stream) => stream,
            Err(VgerError::ChunkNotInIndex(_)) => {
                info!("restore cache missing tree-pack chunk, falling back to full index");
                repo.load_chunk_index()?;
                super::list::load_snapshot_item_stream(&mut repo, &resolved_name)?
            }
            Err(e) => return Err(e),
        }
    } else {
        repo.load_chunk_index()?;
        super::list::load_snapshot_item_stream(&mut repo, &resolved_name)?
    };

    let dest_path = Path::new(dest);
    std::fs::create_dir_all(dest_path)?;
    let dest_root = dest_path
        .canonicalize()
        .map_err(|e| VgerError::Other(format!("invalid destination '{}': {e}", dest)))?;

    // Stream items: create dirs/symlinks immediately, build file plan + chunk targets.
    let (planned_files, chunk_targets, mut stats, verified_dirs) =
        stream_and_plan(&items_stream, &dest_root, &mut include_path, xattrs_enabled)?;
    drop(items_stream); // free raw bytes before read group building

    if !planned_files.is_empty() {
        // Build read groups from chunk targets.
        let mut groups = if !chunk_targets.is_empty() {
            if let Some(ref cache) = restore_cache {
                info!("using mmap restore cache ({} entries)", cache.entry_count());
                repo.clear_chunk_index();

                // Pre-scan: check if all chunks exist in cache before consuming chunk_targets.
                let all_in_cache = chunk_targets.keys().all(|id| cache.lookup(id).is_some());

                if all_in_cache {
                    build_read_groups(chunk_targets, |id| cache.lookup(id))?
                } else {
                    info!("restore cache lookup failed, falling back to filtered index");
                    if repo.chunk_index().is_empty() {
                        repo.load_chunk_index()?;
                    }
                    let needed_chunks: HashSet<ChunkId> = chunk_targets.keys().copied().collect();
                    repo.retain_chunk_index(&needed_chunks);
                    drop(needed_chunks);
                    info!(
                        "loaded filtered chunk index ({} entries)",
                        repo.chunk_index().len()
                    );
                    let index = repo.chunk_index();
                    build_read_groups(chunk_targets, |id| {
                        index
                            .get(id)
                            .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
                    })?
                }
            } else {
                info!("restore cache unavailable, using filtered index");
                if repo.chunk_index().is_empty() {
                    repo.load_chunk_index()?;
                }
                let needed_chunks: HashSet<ChunkId> = chunk_targets.keys().copied().collect();
                repo.retain_chunk_index(&needed_chunks);
                drop(needed_chunks);
                info!(
                    "loaded filtered chunk index ({} entries)",
                    repo.chunk_index().len()
                );
                let index = repo.chunk_index();
                build_read_groups(chunk_targets, |id| {
                    index
                        .get(id)
                        .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
                })?
            }
        } else {
            Vec::new()
        };

        // Free chunk index memory — all pack locations are now in PlannedBlob structs.
        // After this point repo is only used for .storage and .crypto.
        repo.clear_chunk_index();

        // Sort groups by pack ID (shard-aligned) then offset for sequential I/O.
        groups.sort_by(|a, b| {
            a.pack_id
                .0
                .cmp(&b.pack_id.0)
                .then(a.read_start.cmp(&b.read_start))
        });

        debug!(
            "planned {} coalesced read groups for {} files",
            groups.len(),
            planned_files.len(),
        );

        // Phase 3: Pre-create files with the correct size.
        // Safety: parents verified in pass 1 are trusted — this is a
        // single-process operation so no concurrent destination tampering
        // can occur between passes. Unverified parents still get the full
        // canonicalize check.
        for pf in &planned_files {
            if pf
                .target_path
                .parent()
                .is_none_or(|p| !verified_dirs.contains(p))
            {
                ensure_parent_exists_within_root(&pf.target_path, &dest_root)?;
            }
            let file = std::fs::File::create(&pf.target_path)?;
            if pf.total_size > 0 {
                file.set_len(pf.total_size)?;
            }
        }

        // Phase 4: Parallel restore — download ranges, decrypt, decompress, write.
        let bytes_written =
            execute_parallel_restore(&planned_files, groups, &repo.storage, repo.crypto.as_ref())?;

        // Phase 5: Apply file metadata (mode, mtime, xattrs).
        for pf in &planned_files {
            let _ = fs::apply_mode(&pf.target_path, pf.mode);
            if xattrs_enabled {
                apply_item_xattrs(&pf.target_path, pf.xattrs.as_ref());
            }
            let (mtime_secs, mtime_nanos) = split_unix_nanos(pf.mtime);
            let _ = fs::set_file_mtime(&pf.target_path, mtime_secs, mtime_nanos);
        }

        stats.files = planned_files.len() as u64;
        stats.total_bytes = bytes_written;
    }

    info!(
        "Restored {} files, {} dirs, {} symlinks ({} bytes)",
        stats.files, stats.dirs, stats.symlinks, stats.total_bytes
    );

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Streaming item processing — two-pass over raw msgpack bytes
// ---------------------------------------------------------------------------

/// Stream items from raw bytes: create dirs/symlinks immediately, build file
/// plan + chunk targets.  Two passes ensure directories exist before
/// symlinks/files are processed.
///
/// Note: `include_path` may be called more than once per path (once per pass).
/// Callers must not rely on single-invocation semantics for stateful filters.
///
/// Because directories are created during decoding (pass 1), a malformed item
/// stream that fails to decode partway through may leave partial directories on
/// disk.  This is acceptable — directory creation is idempotent and restore
/// is not transactional.
#[allow(clippy::type_complexity)]
fn stream_and_plan<F>(
    items_stream: &[u8],
    dest_root: &Path,
    include_path: &mut F,
    xattrs_enabled: bool,
) -> Result<(
    Vec<PlannedFile>,
    HashMap<ChunkId, ChunkTargets>,
    RestoreStats,
    HashSet<PathBuf>,
)>
where
    F: FnMut(&str) -> bool,
{
    let mut stats = RestoreStats::default();
    let mut verified_dirs: HashSet<PathBuf> = HashSet::new();
    verified_dirs.insert(dest_root.to_path_buf());

    // Pass 1: Create all matching directories.
    super::list::for_each_decoded_item(items_stream, |item| {
        if item.entry_type != ItemType::Directory || !include_path(&item.path) {
            return Ok(());
        }
        let rel = sanitize_item_path(&item.path)?;
        let target = dest_root.join(&rel);
        ensure_path_within_root(&target, dest_root)?;
        std::fs::create_dir_all(&target)?;
        ensure_path_within_root(&target, dest_root)?;
        let _ = fs::apply_mode(&target, item.mode);
        if xattrs_enabled {
            apply_item_xattrs(&target, item.xattrs.as_ref());
        }
        verified_dirs.insert(target);
        stats.dirs += 1;
        Ok(())
    })?;

    // Pass 2: Process symlinks (create immediately) and files (build plan).
    let mut planned_files = Vec::new();
    let mut chunk_targets: HashMap<ChunkId, ChunkTargets> = HashMap::new();

    super::list::for_each_decoded_item(items_stream, |item| {
        if !include_path(&item.path) {
            return Ok(());
        }
        match item.entry_type {
            ItemType::Symlink => {
                if let Some(ref link_target) = item.link_target {
                    let rel = sanitize_item_path(&item.path)?;
                    let target = dest_root.join(&rel);
                    // Safety: see Phase 3 comment — single-process, no
                    // concurrent tampering between pass 1 and pass 2.
                    if target.parent().is_none_or(|p| !verified_dirs.contains(p)) {
                        ensure_parent_exists_within_root(&target, dest_root)?;
                    }
                    let _ = std::fs::remove_file(&target);
                    fs::create_symlink(Path::new(link_target), &target)?;
                    if xattrs_enabled {
                        apply_item_xattrs(&target, item.xattrs.as_ref());
                    }
                    stats.symlinks += 1;
                }
            }
            ItemType::RegularFile => {
                let rel = sanitize_item_path(&item.path)?;
                let target = dest_root.join(&rel);
                let file_idx = planned_files.len();
                let mut file_offset: u64 = 0;
                for chunk_ref in &item.chunks {
                    let entry = chunk_targets
                        .entry(chunk_ref.id)
                        .or_insert_with(|| ChunkTargets {
                            expected_size: chunk_ref.size,
                            targets: SmallVec::new(),
                        });
                    if entry.expected_size != chunk_ref.size {
                        return Err(VgerError::InvalidFormat(format!(
                            "chunk {} has inconsistent logical sizes in snapshot metadata: {} vs {}",
                            chunk_ref.id, entry.expected_size, chunk_ref.size
                        )));
                    }
                    entry.targets.push(WriteTarget {
                        file_idx,
                        file_offset,
                    });
                    file_offset += chunk_ref.size as u64;
                }
                planned_files.push(PlannedFile {
                    target_path: target,
                    total_size: file_offset,
                    mode: item.mode,
                    mtime: item.mtime,
                    xattrs: item.xattrs.clone(),
                });
            }
            ItemType::Directory => {} // handled in pass 1
        }
        Ok(())
    })?;

    Ok((planned_files, chunk_targets, stats, verified_dirs))
}

// ---------------------------------------------------------------------------
// Read planning — group chunks by pack and coalesce adjacent ranges
// ---------------------------------------------------------------------------

/// Aggregated write targets and expected logical size for a single chunk.
struct ChunkTargets {
    expected_size: u32,
    targets: SmallVec<[WriteTarget; 1]>,
}

/// Plan reads using a lookup closure that returns `(pack_id, pack_offset, stored_size)`.
/// The closure abstracts over ChunkIndex vs MmapRestoreCache.
#[cfg(test)]
fn plan_reads<L>(
    file_items: &[(&Item, PathBuf)],
    lookup: L,
) -> Result<(Vec<PlannedFile>, Vec<ReadGroup>)>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let mut files: Vec<PlannedFile> = Vec::with_capacity(file_items.len());

    // Collect all (ChunkId → ChunkTargets) across all files.
    let mut chunk_targets: HashMap<ChunkId, ChunkTargets> = HashMap::new();

    for (file_idx, (item, target_path)) in file_items.iter().enumerate() {
        let mut file_offset: u64 = 0;
        for chunk_ref in &item.chunks {
            let entry = chunk_targets
                .entry(chunk_ref.id)
                .or_insert_with(|| ChunkTargets {
                    expected_size: chunk_ref.size,
                    targets: SmallVec::new(),
                });
            if entry.expected_size != chunk_ref.size {
                return Err(VgerError::InvalidFormat(format!(
                    "chunk {} has inconsistent logical sizes in snapshot metadata: {} vs {}",
                    chunk_ref.id, entry.expected_size, chunk_ref.size
                )));
            }
            entry.targets.push(WriteTarget {
                file_idx,
                file_offset,
            });
            file_offset += chunk_ref.size as u64;
        }
        files.push(PlannedFile {
            target_path: target_path.clone(),
            total_size: file_offset,
            mode: item.mode,
            mtime: item.mtime,
            xattrs: item.xattrs.clone(),
        });
    }

    let groups = build_read_groups(chunk_targets, lookup)?;
    Ok((files, groups))
}

/// Look up each unique chunk's pack location and coalesce into ReadGroups.
/// Consumes `chunk_targets` by value.
fn build_read_groups<L>(
    chunk_targets: HashMap<ChunkId, ChunkTargets>,
    lookup: L,
) -> Result<Vec<ReadGroup>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let mut pack_blobs: HashMap<PackId, Vec<PlannedBlob>> = HashMap::new();

    for (chunk_id, ct) in chunk_targets {
        let (pack_id, pack_offset, stored_size) =
            lookup(&chunk_id).ok_or(VgerError::ChunkNotInIndex(chunk_id))?;
        pack_blobs.entry(pack_id).or_default().push(PlannedBlob {
            chunk_id,
            pack_offset,
            stored_size,
            expected_size: ct.expected_size,
            targets: ct.targets,
        });
    }

    // For each pack: sort blobs by offset, then coalesce into ReadGroups.
    let mut groups: Vec<ReadGroup> = Vec::new();

    for (pack_id, mut blobs) in pack_blobs {
        blobs.sort_by_key(|b| b.pack_offset);

        let mut iter = blobs.into_iter();
        let first = iter.next().unwrap(); // pack_blobs only has non-empty vecs

        let mut cur = ReadGroup {
            pack_id,
            read_start: first.pack_offset,
            read_end: first.pack_offset + first.stored_size as u64,
            blobs: vec![first],
        };

        for blob in iter {
            let blob_end = blob.pack_offset + blob.stored_size as u64;
            let gap = blob.pack_offset.saturating_sub(cur.read_end);
            let merged_size = blob_end - cur.read_start;

            if gap <= MAX_COALESCE_GAP && merged_size <= MAX_READ_SIZE {
                // Coalesce into the current group.
                cur.read_end = blob_end;
                cur.blobs.push(blob);
            } else {
                // Start a new group.
                groups.push(cur);
                cur = ReadGroup {
                    pack_id,
                    read_start: blob.pack_offset,
                    read_end: blob_end,
                    blobs: vec![blob],
                };
            }
        }
        groups.push(cur);
    }

    Ok(groups)
}

// ---------------------------------------------------------------------------
// Parallel restore execution
// ---------------------------------------------------------------------------

fn execute_parallel_restore(
    files: &[PlannedFile],
    groups: Vec<ReadGroup>,
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
) -> Result<u64> {
    if groups.is_empty() {
        return Ok(0);
    }

    let bytes_written = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);
    let first_error = Mutex::new(None::<VgerError>);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(MAX_READER_THREADS)
        .build()
        .map_err(|e| VgerError::Other(format!("failed to build thread pool: {e}")))?;

    pool.in_place_scope(|s| {
        for group in &groups {
            let bytes_written = &bytes_written;
            let cancelled = &cancelled;
            let first_error = &first_error;

            s.spawn(move |_| {
                if cancelled.load(Ordering::Acquire) {
                    return;
                }
                if let Err(e) =
                    process_read_group(group, files, storage, crypto, bytes_written, cancelled)
                {
                    cancelled.store(true, Ordering::Release);
                    if let Ok(mut slot) = first_error.lock() {
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                    }
                }
            });
        }
    });

    if let Ok(mut slot) = first_error.lock() {
        if let Some(err) = slot.take() {
            return Err(err);
        }
    } else if cancelled.load(Ordering::Acquire) {
        return Err(VgerError::Other(
            "parallel restore cancelled after worker error".into(),
        ));
    }

    Ok(bytes_written.load(Ordering::Relaxed))
}

/// Process a single coalesced read group: download the range, decrypt and
/// decompress each blob, then write the plaintext data to target files.
///
/// Buffers are local to this function — reused across blobs within the group,
/// freed when the group finishes. This avoids TLS high-water persistence
/// while still amortizing allocations across the ~149 blobs per group.
fn process_read_group(
    group: &ReadGroup,
    files: &[PlannedFile],
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    bytes_written: &AtomicU64,
    cancelled: &AtomicBool,
) -> Result<()> {
    if cancelled.load(Ordering::Acquire) {
        return Ok(());
    }

    let pack_key = group.pack_id.storage_key();
    let read_len = group.read_end - group.read_start;

    let data = storage
        .get_range(&pack_key, group.read_start, read_len)?
        .ok_or_else(|| VgerError::Other(format!("pack not found: {}", group.pack_id)))?;

    // Local buffers — reused across blobs within this group, freed on return.
    let mut decrypt_buf = Vec::new();
    let mut decompress_buf = Vec::new();
    let mut file_handles: HashMap<usize, std::fs::File> = HashMap::new();

    for blob in &group.blobs {
        if cancelled.load(Ordering::Acquire) {
            return Ok(());
        }

        let local_offset = (blob.pack_offset - group.read_start) as usize;
        let local_end = local_offset + blob.stored_size as usize;

        if local_end > data.len() {
            return Err(VgerError::Other(format!(
                "blob extends beyond downloaded range in pack {}",
                group.pack_id
            )));
        }

        let raw = &data[local_offset..local_end];
        unpack_object_expect_with_context_into(
            raw,
            ObjectType::ChunkData,
            &blob.chunk_id.0,
            crypto,
            &mut decrypt_buf,
        )
        .map_err(|e| {
            VgerError::Other(format!(
                "chunk {} in pack {} (offset {}, size {}): {e}",
                blob.chunk_id, group.pack_id, blob.pack_offset, blob.stored_size
            ))
        })?;
        compress::decompress_into_with_hint(
            &decrypt_buf,
            Some(blob.expected_size as usize),
            &mut decompress_buf,
        )
        .map_err(|e| {
            VgerError::Other(format!(
                "chunk {} in pack {} (offset {}, size {}): {e}",
                blob.chunk_id, group.pack_id, blob.pack_offset, blob.stored_size
            ))
        })?;

        if decompress_buf.len() != blob.expected_size as usize {
            return Err(VgerError::InvalidFormat(format!(
                "chunk {} in pack {} (offset {}, size {}) size mismatch after restore decode: \
                 expected {} bytes, got {} bytes",
                blob.chunk_id,
                group.pack_id,
                blob.pack_offset,
                blob.stored_size,
                blob.expected_size,
                decompress_buf.len()
            )));
        }

        for target in &blob.targets {
            if !file_handles.contains_key(&target.file_idx) {
                let pf = &files[target.file_idx];
                if file_handles.len() >= MAX_OPEN_FILES_PER_GROUP {
                    if let Some(evict_idx) = file_handles.keys().next().copied() {
                        file_handles.remove(&evict_idx);
                    }
                }
                let handle = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&pf.target_path)
                    .map_err(|e| {
                        VgerError::Other(format!(
                            "failed to open {} for writing: {e}",
                            pf.target_path.display()
                        ))
                    })?;
                file_handles.insert(target.file_idx, handle);
            }
            let fh = file_handles
                .get_mut(&target.file_idx)
                .ok_or_else(|| VgerError::Other("missing file handle in restore worker".into()))?;
            #[cfg(unix)]
            fh.write_all_at(&decompress_buf, target.file_offset)?;
            #[cfg(not(unix))]
            {
                fh.seek(std::io::SeekFrom::Start(target.file_offset))?;
                fh.write_all(&decompress_buf)?;
            }
            bytes_written.fetch_add(decompress_buf.len() as u64, Ordering::Relaxed);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a path matches the selection set.
/// A path matches if it's exactly in the set, or any prefix (ancestor) is in the set.
fn path_matches_selection(path: &str, selected: &HashSet<String>) -> bool {
    if selected.contains(path) {
        return true;
    }
    // Check if any ancestor is selected
    let mut current = path;
    while let Some(slash_idx) = current.rfind('/') {
        current = &current[..slash_idx];
        if selected.contains(current) {
            return true;
        }
    }
    false
}

fn apply_item_xattrs(target: &Path, xattrs: Option<&HashMap<String, Vec<u8>>>) {
    let Some(xattrs) = xattrs else {
        return;
    };

    let mut names: Vec<&str> = xattrs.keys().map(String::as_str).collect();
    names.sort_unstable();

    for name in names {
        let Some(value) = xattrs.get(name) else {
            continue;
        };

        #[cfg(unix)]
        if let Err(e) = xattr::set(target, name, value) {
            tracing::warn!(
                path = %target.display(),
                attr = %name,
                error = %e,
                "failed to restore extended attribute"
            );
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            let _ = name;
            let _ = value;
        }
    }
}

#[derive(Debug, Default)]
pub struct RestoreStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub total_bytes: u64,
}

fn sanitize_item_path(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(VgerError::InvalidFormat(format!(
            "refusing to restore absolute path: {raw}"
        )));
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VgerError::InvalidFormat(format!(
                    "refusing to restore unsafe path: {raw}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(VgerError::InvalidFormat(format!(
            "refusing to restore empty path: {raw}"
        )));
    }
    Ok(out)
}

fn ensure_parent_exists_within_root(target: &Path, root: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        ensure_path_within_root(parent, root)?;
        std::fs::create_dir_all(parent)?;
        ensure_path_within_root(parent, root)?;
    }
    Ok(())
}

fn ensure_path_within_root(path: &Path, root: &Path) -> Result<()> {
    let mut cursor = Some(path);
    while let Some(candidate) = cursor {
        if candidate.exists() {
            let canonical = candidate
                .canonicalize()
                .map_err(|e| VgerError::Other(format!("path check failed: {e}")))?;
            if !canonical.starts_with(root) {
                return Err(VgerError::InvalidFormat(format!(
                    "refusing to restore outside destination: {}",
                    path.display()
                )));
            }
            return Ok(());
        }
        cursor = candidate.parent();
    }
    Err(VgerError::InvalidFormat(format!(
        "invalid restore target path: {}",
        path.display()
    )))
}

fn split_unix_nanos(total_nanos: i64) -> (i64, u32) {
    let secs = total_nanos.div_euclid(1_000_000_000);
    let nanos = total_nanos.rem_euclid(1_000_000_000) as u32;
    (secs, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::compress::Compression;
    use crate::crypto::chunk_id::ChunkId;
    use crate::crypto::pack_id::PackId;
    use crate::crypto::PlaintextEngine;
    use crate::index::ChunkIndex;
    use crate::repo::format::pack_object_with_context;
    use crate::snapshot::item::{ChunkRef, Item, ItemType};
    use crate::storage::StorageBackend;
    use crate::testutil::{test_chunk_id_key, MemoryBackend};

    #[test]
    fn split_unix_nanos_handles_negative_values() {
        let (secs, nanos) = split_unix_nanos(-1);
        assert_eq!(secs, -1);
        assert_eq!(nanos, 999_999_999);
    }

    #[test]
    fn split_unix_nanos_handles_positive_values() {
        let (secs, nanos) = split_unix_nanos(1_500_000_000);
        assert_eq!(secs, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn sanitize_rejects_parent_dir_traversal() {
        let err = sanitize_item_path("../etc/passwd").unwrap_err().to_string();
        assert!(err.contains("unsafe path"));
    }

    // -----------------------------------------------------------------------
    // plan_reads tests
    // -----------------------------------------------------------------------

    fn dummy_chunk_id(byte: u8) -> ChunkId {
        ChunkId([byte; 32])
    }

    fn dummy_pack_id(byte: u8) -> PackId {
        PackId([byte; 32])
    }

    /// Helper: create a lookup closure from a ChunkIndex.
    fn index_lookup(index: &ChunkIndex) -> impl Fn(&ChunkId) -> Option<(PackId, u64, u32)> + '_ {
        move |id| {
            index
                .get(id)
                .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
        }
    }

    fn make_file_item(path: &str, chunks: Vec<(u8, u32)>) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: chunks.iter().map(|(_, s)| *s as u64).sum(),
            chunks: chunks
                .into_iter()
                .map(|(id_byte, size)| ChunkRef {
                    id: dummy_chunk_id(id_byte),
                    size,
                    csize: size, // not used by plan_reads
                })
                .collect(),
            link_target: None,
            xattrs: None,
        }
    }

    #[test]
    fn plan_reads_single_blob_per_pack() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        let item = make_file_item("a.txt", vec![(0xAA, 200)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 200);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 1);
        assert_eq!(groups[0].read_start, 1000);
        assert_eq!(groups[0].read_end, 1100);
    }

    #[test]
    fn plan_reads_coalesces_adjacent_blobs() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // Two blobs close together in the same pack (gap = 4 bytes for length prefix)
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);
        index.add(dummy_chunk_id(0xBB), 100, pack, 1104); // 1000 + 100 + 4 = 1104

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 500);
        // Both blobs should be coalesced into one ReadGroup
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 2);
        assert_eq!(groups[0].read_start, 1000);
        assert_eq!(groups[0].read_end, 1204);
    }

    #[test]
    fn plan_reads_splits_on_large_gap() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // Two blobs far apart (gap > MAX_COALESCE_GAP)
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);
        index.add(
            dummy_chunk_id(0xBB),
            100,
            pack,
            1000 + 100 + MAX_COALESCE_GAP + 1,
        );

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Should be split into two ReadGroups
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].blobs.len(), 1);
        assert_eq!(groups[1].blobs.len(), 1);
    }

    #[test]
    fn plan_reads_splits_on_max_read_size() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // First blob takes up almost MAX_READ_SIZE, second would push it over
        let big_size = MAX_READ_SIZE as u32 - 100;
        index.add(dummy_chunk_id(0xAA), big_size, pack, 1000);
        index.add(dummy_chunk_id(0xBB), 200, pack, 1000 + big_size as u64 + 4);

        let item = make_file_item("a.txt", vec![(0xAA, 5000), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Should be split because merged_size > MAX_READ_SIZE
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn plan_reads_dedup_across_files() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        // Two files sharing the same chunk
        let item_a = make_file_item("a.txt", vec![(0xAA, 200)]);
        let item_b = make_file_item("b.txt", vec![(0xAA, 200)]);
        let file_items = vec![
            (&item_a, PathBuf::from("/tmp/out/a.txt")),
            (&item_b, PathBuf::from("/tmp/out/b.txt")),
        ];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 2);
        // Only one ReadGroup since it's the same chunk
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 1);
        // The blob should have two write targets
        assert_eq!(groups[0].blobs[0].targets.len(), 2);
    }

    #[test]
    fn plan_reads_rejects_inconsistent_logical_chunk_sizes() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        let item_a = make_file_item("a.txt", vec![(0xAA, 200)]);
        let item_b = make_file_item("b.txt", vec![(0xAA, 300)]);
        let file_items = vec![
            (&item_a, PathBuf::from("/tmp/out/a.txt")),
            (&item_b, PathBuf::from("/tmp/out/b.txt")),
        ];

        let err = match plan_reads(&file_items, index_lookup(&index)) {
            Ok(_) => panic!("expected inconsistent logical size error"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("inconsistent logical sizes"));
    }

    #[test]
    fn plan_reads_empty_file_no_groups() {
        let index = ChunkIndex::new();
        let item = make_file_item("empty.txt", vec![]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/empty.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 0);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn plan_reads_multiple_packs() {
        let pack_a = dummy_pack_id(1);
        let pack_b = dummy_pack_id(2);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack_a, 1000);
        index.add(dummy_chunk_id(0xBB), 100, pack_b, 2000);

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Separate packs → separate ReadGroups
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn process_read_group_rejects_decode_size_mismatch() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("out.bin");
        let file = std::fs::File::create(&out).unwrap();
        file.set_len(5).unwrap();

        let files = vec![PlannedFile {
            target_path: out.clone(),
            total_size: 5,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
        }];

        let payload = b"abc";
        let compressed = crate::compress::compress(Compression::None, payload).unwrap();
        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &dummy_chunk_id(0xAA).0,
            &compressed,
            &crypto,
        )
        .unwrap();

        let pack_id = dummy_pack_id(9);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = ReadGroup {
            pack_id,
            read_start: 0,
            read_end: packed.len() as u64,
            blobs: vec![PlannedBlob {
                chunk_id: dummy_chunk_id(0xAA),
                pack_offset: 0,
                stored_size: packed.len() as u32,
                expected_size: 5,
                targets: smallvec::smallvec![WriteTarget {
                    file_idx: 0,
                    file_offset: 0,
                }],
            }],
        };

        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let err = process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("size mismatch after restore decode"),
            "expected size mismatch error, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // stream_and_plan tests
    // -----------------------------------------------------------------------

    fn make_dir_item(path: &str, mode: u32) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::Directory,
            mode,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        }
    }

    fn make_symlink_item(path: &str, target: &str) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::Symlink,
            mode: 0o777,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: Some(target.to_string()),
            xattrs: None,
        }
    }

    fn serialize_items(items: &[Item]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for item in items {
            bytes.extend_from_slice(&rmp_serde::to_vec(item).unwrap());
        }
        bytes
    }

    #[test]
    fn stream_and_plan_dirs_before_files() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        // Serialize in reverse order: file before its parent dir.
        let items = vec![
            make_file_item("mydir/a.txt", vec![(0xAA, 100)]),
            make_dir_item("mydir", 0o755),
        ];
        let stream = serialize_items(&items);

        let (planned_files, chunk_targets, stats, _verified_dirs) =
            stream_and_plan(&stream, dest, &mut |_| true, false).unwrap();

        // Directory was created (pass 1 runs before pass 2).
        assert!(dest.join("mydir").is_dir());
        assert_eq!(stats.dirs, 1);

        // File is in planned_files.
        assert_eq!(planned_files.len(), 1);
        assert!(planned_files[0].target_path.ends_with("mydir/a.txt"));
        assert_eq!(chunk_targets.len(), 1);
    }

    #[test]
    fn stream_and_plan_respects_filter() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_dir_item("included", 0o755),
            make_dir_item("excluded", 0o755),
            make_file_item("included/a.txt", vec![(0xAA, 100)]),
            make_file_item("excluded/b.txt", vec![(0xBB, 200)]),
        ];
        let stream = serialize_items(&items);

        let (planned_files, _chunk_targets, stats, _verified_dirs) = stream_and_plan(
            &stream,
            dest,
            &mut |p: &str| p.starts_with("included"),
            false,
        )
        .unwrap();

        // Only the included directory was created.
        assert!(dest.join("included").is_dir());
        assert!(!dest.join("excluded").exists());
        assert_eq!(stats.dirs, 1);

        // Only the included file is planned.
        assert_eq!(planned_files.len(), 1);
        assert!(planned_files[0].target_path.ends_with("included/a.txt"));
    }

    #[test]
    fn stream_and_plan_only_retains_files() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let n_dirs = 5;
        let m_files = 3;
        let mut items: Vec<Item> = Vec::new();
        for i in 0..n_dirs {
            items.push(make_dir_item(&format!("dir{i}"), 0o755));
        }
        for i in 0..m_files {
            items.push(make_file_item(
                &format!("dir0/file{i}.txt"),
                vec![((0xA0 + i) as u8, 100)],
            ));
        }
        // Add a symlink too — should not be in planned_files.
        items.push(make_symlink_item("dir0/link", "file0.txt"));
        let stream = serialize_items(&items);

        let (planned_files, _chunk_targets, stats, _verified_dirs) =
            stream_and_plan(&stream, dest, &mut |_| true, false).unwrap();

        assert_eq!(planned_files.len(), m_files as usize);
        assert_eq!(stats.dirs, n_dirs);
        assert_eq!(stats.symlinks, 1);
    }

    #[test]
    fn stream_and_plan_decode_failure_leaves_partial_dirs() {
        // Extraction is not transactional: a decode error partway through the
        // stream may leave already-created directories on disk.  This test
        // documents that behavior as intentional.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let mut stream = serialize_items(&[make_dir_item("aaa", 0o755)]);
        // Append garbage bytes to trigger a decode error after the first item.
        stream.extend_from_slice(&[0xFF, 0xFF, 0xFF]);

        let result = stream_and_plan(&stream, dest, &mut |_| true, false);
        assert!(result.is_err());
        // The directory from before the corrupt bytes was still created.
        assert!(dest.join("aaa").is_dir());
    }
}
