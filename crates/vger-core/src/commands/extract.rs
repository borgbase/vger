use std::collections::{HashMap, HashSet};
use std::io::{Seek, Write as IoWrite};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tracing::{debug, info};

use crate::compress;
use crate::config::VgerConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::crypto::pack_id::PackId;
use crate::crypto::CryptoEngine;
use crate::error::{Result, VgerError};
use crate::index::ChunkIndex;
use crate::platform::fs;
use crate::repo::file_cache::FileCache;
use crate::repo::format::{unpack_object_expect, ObjectType};
use crate::snapshot::item::{Item, ItemType};
use crate::storage::StorageBackend;

use super::util::open_repo;

// ---------------------------------------------------------------------------
// Constants for coalesced parallel restore
// ---------------------------------------------------------------------------

/// Maximum gap (in bytes) between blobs in the same pack that will be coalesced
/// into a single range read rather than issuing separate requests.
const MAX_COALESCE_GAP: u64 = 256 * 1024; // 256 KiB

/// Maximum size of a single coalesced range read.
const MAX_READ_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB

/// Number of parallel reader threads for downloading pack data.
const MAX_READER_THREADS: usize = 8;

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
    targets: Vec<WriteTarget>,
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

/// Run `vger extract`.
pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    pattern: Option<&str>,
    xattrs_enabled: bool,
) -> Result<ExtractStats> {
    let filter = pattern
        .map(|p| {
            globset::GlobBuilder::new(p)
                .literal_separator(false)
                .build()
                .map(|g| g.compile_matcher())
        })
        .transpose()
        .map_err(|e| VgerError::Config(format!("invalid pattern: {e}")))?;

    extract_with_filter(
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

/// Run `vger extract` for a selected set of paths.
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
) -> Result<ExtractStats> {
    extract_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        |path| path_matches_selection(path, selected_paths),
    )
}

// ---------------------------------------------------------------------------
// Core extract logic — phased approach
// ---------------------------------------------------------------------------

fn extract_with_filter<F>(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    xattrs_enabled: bool,
    mut include_path: F,
) -> Result<ExtractStats>
where
    F: FnMut(&str) -> bool,
{
    let mut repo = open_repo(config, passphrase)?;
    // Shrink blob cache for restore — the parallel pipeline reads pack data
    // directly via storage.get_range(), so the cache only serves the small
    // item-stream tree-pack chunks. 2 MiB is plenty.
    repo.set_blob_cache_max_bytes(2 * 1024 * 1024);
    repo.restore_file_cache(FileCache::new()); // not needed during extract
    let xattrs_enabled = if xattrs_enabled && !fs::xattrs_supported() {
        tracing::warn!(
            "xattrs requested but not supported on this platform; continuing without xattrs"
        );
        false
    } else {
        xattrs_enabled
    };

    // Resolve "latest" or exact snapshot name
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();
    if resolved_name != snapshot_name {
        info!("Resolved '{}' to snapshot {}", snapshot_name, resolved_name);
    }

    let items = super::list::load_snapshot_items(&mut repo, &resolved_name)?;

    let dest_path = Path::new(dest);
    std::fs::create_dir_all(dest_path)?;
    let dest_root = dest_path
        .canonicalize()
        .map_err(|e| VgerError::Other(format!("invalid destination '{}': {e}", dest)))?;

    let mut sorted_items = items;
    sort_items_for_extract(&mut sorted_items);

    let mut stats = ExtractStats::default();

    // Phase 1: Create directories + symlinks; collect file items for parallel restore.
    let mut file_items: Vec<(&Item, PathBuf)> = Vec::new();

    for item in &sorted_items {
        if !include_path(&item.path) {
            continue;
        }

        let rel = sanitize_item_path(&item.path)?;
        let target = dest_root.join(&rel);

        match item.entry_type {
            ItemType::Directory => {
                ensure_path_within_root(&target, &dest_root)?;
                std::fs::create_dir_all(&target)?;
                ensure_path_within_root(&target, &dest_root)?;
                let _ = fs::apply_mode(&target, item.mode);
                if xattrs_enabled {
                    apply_item_xattrs(&target, item.xattrs.as_ref());
                }
                stats.dirs += 1;
            }
            ItemType::Symlink => {
                if let Some(ref link_target) = item.link_target {
                    ensure_parent_exists_within_root(&target, &dest_root)?;
                    let _ = std::fs::remove_file(&target);
                    fs::create_symlink(Path::new(link_target), &target)?;
                    if xattrs_enabled {
                        apply_item_xattrs(&target, item.xattrs.as_ref());
                    }
                    stats.symlinks += 1;
                }
            }
            ItemType::RegularFile => {
                file_items.push((item, target));
            }
        }
    }

    if !file_items.is_empty() {
        // Phase 2: Plan reads — group chunks by pack, coalesce adjacent ranges.
        let (planned_files, groups) = plan_reads(&file_items, repo.chunk_index())?;
        drop(file_items); // release borrows on sorted_items
        drop(sorted_items); // free all Item memory
                            // Free chunk index memory — all pack locations are now in PlannedBlob structs.
                            // After this point repo is only used for .storage and .crypto.
        repo.clear_chunk_index();

        debug!(
            "planned {} coalesced read groups for {} files",
            groups.len(),
            planned_files.len(),
        );

        // Phase 3: Pre-create files with the correct size.
        for pf in &planned_files {
            ensure_parent_exists_within_root(&pf.target_path, &dest_root)?;
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
            let mtime = filetime::FileTime::from_unix_time(mtime_secs, mtime_nanos);
            let _ = filetime::set_file_mtime(&pf.target_path, mtime);
        }

        stats.files = planned_files.len() as u64;
        stats.total_bytes = bytes_written;
    }

    info!(
        "Extracted {} files, {} dirs, {} symlinks ({} bytes)",
        stats.files, stats.dirs, stats.symlinks, stats.total_bytes
    );

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Read planning — group chunks by pack and coalesce adjacent ranges
// ---------------------------------------------------------------------------

fn plan_reads(
    file_items: &[(&Item, PathBuf)],
    chunk_index: &ChunkIndex,
) -> Result<(Vec<PlannedFile>, Vec<ReadGroup>)> {
    let mut files: Vec<PlannedFile> = Vec::with_capacity(file_items.len());

    // Build pack_blobs inline while traversing file chunks.
    let mut pack_blobs: HashMap<PackId, Vec<PlannedBlob>> = HashMap::new();
    let mut blob_positions: HashMap<ChunkId, (PackId, usize)> = HashMap::new();

    for (file_idx, (item, target_path)) in file_items.iter().enumerate() {
        let mut file_offset: u64 = 0;
        for chunk_ref in &item.chunks {
            if let Some((pack_id, blob_idx)) = blob_positions.get(&chunk_ref.id).copied() {
                let blob = pack_blobs
                    .get_mut(&pack_id)
                    .and_then(|blobs| blobs.get_mut(blob_idx))
                    .expect("blob_positions must reference an existing planned blob");
                if blob.expected_size != chunk_ref.size {
                    return Err(VgerError::InvalidFormat(format!(
                        "chunk {} has inconsistent logical sizes in snapshot metadata: {} vs {}",
                        chunk_ref.id, blob.expected_size, chunk_ref.size
                    )));
                }
                blob.targets.push(WriteTarget {
                    file_idx,
                    file_offset,
                });
            } else {
                let entry = chunk_index.get(&chunk_ref.id).ok_or_else(|| {
                    VgerError::Other(format!("chunk not found in index: {}", chunk_ref.id))
                })?;
                let blobs = pack_blobs.entry(entry.pack_id).or_default();
                let blob_idx = blobs.len();
                blobs.push(PlannedBlob {
                    chunk_id: chunk_ref.id,
                    pack_offset: entry.pack_offset,
                    stored_size: entry.stored_size,
                    expected_size: chunk_ref.size,
                    targets: vec![WriteTarget {
                        file_idx,
                        file_offset,
                    }],
                });
                blob_positions.insert(chunk_ref.id, (entry.pack_id, blob_idx));
            }
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

    Ok((files, groups))
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
        let compressed = unpack_object_expect(raw, ObjectType::ChunkData, crypto)?;
        let plaintext = compress::decompress(&compressed)?;
        drop(compressed); // free decrypted buffer before file writes

        if plaintext.len() != blob.expected_size as usize {
            return Err(VgerError::InvalidFormat(format!(
                "chunk {} size mismatch after restore decode: expected {} bytes, got {} bytes",
                blob.chunk_id,
                blob.expected_size,
                plaintext.len()
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
            fh.seek(std::io::SeekFrom::Start(target.file_offset))?;
            fh.write_all(&plaintext)?;
            bytes_written.fetch_add(plaintext.len() as u64, Ordering::Relaxed);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sort_items_for_extract(items: &mut [Item]) {
    items.sort_by(|a, b| {
        let type_ord = |t: &ItemType| match t {
            ItemType::Directory => 0,
            ItemType::Symlink => 1,
            ItemType::RegularFile => 2,
        };
        type_ord(&a.entry_type)
            .cmp(&type_ord(&b.entry_type))
            .then_with(|| a.path.cmp(&b.path))
    });
}

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
pub struct ExtractStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub total_bytes: u64,
}

fn sanitize_item_path(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(VgerError::InvalidFormat(format!(
            "refusing to extract absolute path: {raw}"
        )));
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VgerError::InvalidFormat(format!(
                    "refusing to extract unsafe path: {raw}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(VgerError::InvalidFormat(format!(
            "refusing to extract empty path: {raw}"
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
                    "refusing to extract outside destination: {}",
                    path.display()
                )));
            }
            return Ok(());
        }
        cursor = candidate.parent();
    }
    Err(VgerError::InvalidFormat(format!(
        "invalid extraction target path: {}",
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
    use crate::repo::format::pack_object;
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

        let (files, groups) = plan_reads(&file_items, &index).unwrap();
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

        let (files, groups) = plan_reads(&file_items, &index).unwrap();
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

        let (_files, groups) = plan_reads(&file_items, &index).unwrap();
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

        let (_files, groups) = plan_reads(&file_items, &index).unwrap();
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

        let (files, groups) = plan_reads(&file_items, &index).unwrap();
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

        let err = match plan_reads(&file_items, &index) {
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

        let (files, groups) = plan_reads(&file_items, &index).unwrap();
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

        let (_files, groups) = plan_reads(&file_items, &index).unwrap();
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
        let packed = pack_object(ObjectType::ChunkData, &compressed, &crypto).unwrap();

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
                targets: vec![WriteTarget {
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
        assert!(err.contains("size mismatch after restore decode"));
    }
}
