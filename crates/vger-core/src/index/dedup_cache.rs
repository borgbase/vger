use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use tracing::{debug, warn};
use xorf::{Filter, Xor8};

use crate::crypto::chunk_id::ChunkId;
use crate::crypto::pack_id::PackId;
use crate::error::Result;
use crate::index::ChunkIndex;

/// Magic bytes at the start of the dedup cache file.
const MAGIC: &[u8; 8] = b"VGDEDUP\0";

/// Current format version.
const VERSION: u32 = 1;

/// Size of the fixed header in bytes.
const HEADER_SIZE: usize = 28;

/// Size of each entry: 32-byte ChunkId + 4-byte stored_size.
const ENTRY_SIZE: usize = 36;

// ---------------------------------------------------------------------------
// Path helper
// ---------------------------------------------------------------------------

/// Return the local filesystem path for the dedup cache file.
/// `~/.cache/vger/<repo_id_hex>/dedup_cache` (same directory as file cache).
pub fn dedup_cache_path(repo_id: &[u8]) -> Option<PathBuf> {
    dirs::cache_dir().map(|base| {
        base.join("vger")
            .join(hex::encode(repo_id))
            .join("dedup_cache")
    })
}

// ---------------------------------------------------------------------------
// Cache writer
// ---------------------------------------------------------------------------

/// Build the dedup cache binary file from the full chunk index.
/// Writes atomically via temp-file + rename.
pub fn build_dedup_cache(index: &ChunkIndex, generation: u64, repo_id: &[u8]) -> Result<()> {
    let Some(path) = dedup_cache_path(repo_id) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    build_dedup_cache_to_path(index, generation, &path)
}

/// Build the dedup cache to an explicit path (used by tests).
pub fn build_dedup_cache_to_path(index: &ChunkIndex, generation: u64, path: &Path) -> Result<()> {
    // Collect and sort entries by ChunkId bytes.
    let mut entries: Vec<(ChunkId, u32)> = index
        .iter()
        .map(|(id, entry)| (*id, entry.stored_size))
        .collect();
    entries.sort_unstable_by(|a, b| a.0 .0.cmp(&b.0 .0));

    let entry_count = entries.len() as u32;

    // Stream directly to a temp file via BufWriter to avoid a second
    // in-memory copy of the entire output.
    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    // Header
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Entries
    for (chunk_id, stored_size) in &entries {
        w.write_all(&chunk_id.0)?;
        w.write_all(&stored_size.to_le_bytes())?;
    }

    w.flush()?;
    drop(w);

    // Atomic rename into place.
    std::fs::rename(&tmp_path, path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote dedup cache"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// mmap'd cache reader
// ---------------------------------------------------------------------------

/// Memory-mapped reader over the sorted dedup cache binary file.
/// Lookups use binary search over fixed-size 36-byte entries.
pub struct MmapDedupCache {
    mmap: Mmap,
    entry_count: u32,
    index_generation: u64,
}

impl MmapDedupCache {
    /// Open and validate the dedup cache file.
    /// Returns `None` on any mismatch (missing file, wrong magic/version/generation,
    /// unexpected file size) — the caller should fall back to the HashMap path.
    pub fn open(repo_id: &[u8], expected_generation: u64) -> Option<Self> {
        // Generation 0 means "no cache ever written".
        if expected_generation == 0 {
            return None;
        }

        let path = dedup_cache_path(repo_id)?;
        Self::open_path(&path, expected_generation)
    }

    /// Open and validate a dedup cache file at an explicit path (used by tests).
    pub fn open_path(path: &Path, expected_generation: u64) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }

        let file = std::fs::File::open(path).ok()?;

        // SAFETY: we only read the file, and the file is written atomically
        // (temp + rename) so it's always in a consistent state.
        let mmap = unsafe { Mmap::map(&file) }.ok()?;

        if mmap.len() < HEADER_SIZE {
            debug!("dedup cache: file too small for header");
            return None;
        }

        // Validate magic
        if &mmap[0..8] != MAGIC {
            debug!("dedup cache: bad magic");
            return None;
        }

        // Validate version
        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version != VERSION {
            debug!(version, "dedup cache: unsupported version");
            return None;
        }

        // Validate generation
        let index_generation = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        if index_generation != expected_generation {
            debug!(
                cache_gen = index_generation,
                expected_gen = expected_generation,
                "dedup cache: generation mismatch"
            );
            return None;
        }

        let entry_count = u32::from_le_bytes(mmap[20..24].try_into().unwrap());

        // Validate file size
        let expected_size = HEADER_SIZE + (entry_count as usize) * ENTRY_SIZE;
        if mmap.len() != expected_size {
            debug!(
                actual = mmap.len(),
                expected = expected_size,
                "dedup cache: file size mismatch"
            );
            return None;
        }

        debug!(
            entries = entry_count,
            generation = index_generation,
            "opened dedup cache"
        );

        Some(Self {
            mmap,
            entry_count,
            index_generation,
        })
    }

    /// Look up a chunk ID using binary search. Returns the stored_size if found.
    pub fn get_stored_size(&self, chunk_id: &ChunkId) -> Option<u32> {
        if self.entry_count == 0 {
            return None;
        }

        let target = &chunk_id.0;
        let data = &self.mmap[HEADER_SIZE..];

        let mut lo: usize = 0;
        let mut hi: usize = self.entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = mid * ENTRY_SIZE;
            let entry_id = &data[offset..offset + 32];

            match entry_id.cmp(target.as_slice()) {
                std::cmp::Ordering::Equal => {
                    let size_offset = offset + 32;
                    let stored_size =
                        u32::from_le_bytes(data[size_offset..size_offset + 4].try_into().unwrap());
                    return Some(stored_size);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        None
    }

    /// Return the index_generation from the cache header.
    pub fn generation(&self) -> u64 {
        self.index_generation
    }

    /// Return the number of entries in the cache.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Iterate over all chunk IDs as u64 keys (first 8 bytes, LE) for xor filter construction.
    fn iter_u64_keys(&self) -> impl Iterator<Item = u64> + '_ {
        let data = &self.mmap[HEADER_SIZE..];
        (0..self.entry_count as usize).map(move |i| {
            let offset = i * ENTRY_SIZE;
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
        })
    }
}

// ---------------------------------------------------------------------------
// Xor filter helpers
// ---------------------------------------------------------------------------

/// Extract the first 8 bytes of a ChunkId as a little-endian u64.
/// BLAKE2b output has excellent entropy, so this is a high-quality hash key.
fn chunk_id_to_u64(id: &ChunkId) -> u64 {
    u64::from_le_bytes(id.0[..8].try_into().unwrap())
}

/// Build an Xor8 filter from the mmap'd cache entries.
/// Returns `None` if the cache is empty or construction fails.
fn build_xor_filter(cache: &MmapDedupCache) -> Option<Xor8> {
    if cache.entry_count() == 0 {
        return None;
    }

    let keys: Vec<u64> = cache.iter_u64_keys().collect();

    // Xor8::from may loop internally on seed collisions. For BLAKE2b-derived
    // keys the entropy is excellent, so this should succeed quickly.
    // Wrap in catch_unwind as a safety net.
    match std::panic::catch_unwind(|| Xor8::from(keys.as_slice())) {
        Ok(filter) => {
            debug!(
                entries = cache.entry_count(),
                fingerprint_bytes = filter.fingerprints.len(),
                "built xor filter"
            );
            Some(filter)
        }
        Err(_) => {
            warn!("xor filter construction panicked; falling back to mmap-only lookups");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// TieredDedupIndex
// ---------------------------------------------------------------------------

/// Three-tier dedup index for memory-efficient backup.
///
/// Lookup order:
/// 1. `session_new` HashMap — chunks added during this backup session (tiny, O(1))
/// 2. Xor filter — probabilistic negative filter (~0.4% FPR, ~1.2 bytes/entry)
/// 3. mmap binary search — confirms filter hit, returns stored_size (OS-paged, near-zero RSS)
pub struct TieredDedupIndex {
    xor_filter: Option<Xor8>,
    mmap_cache: MmapDedupCache,
    session_new: HashMap<ChunkId, u32>,
}

impl TieredDedupIndex {
    /// Create a new tiered index from an opened mmap cache.
    pub fn new(mmap_cache: MmapDedupCache) -> Self {
        let xor_filter = build_xor_filter(&mmap_cache);
        Self {
            xor_filter,
            mmap_cache,
            session_new: HashMap::new(),
        }
    }

    /// Check if a chunk exists in any tier.
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.get_stored_size(id).is_some()
    }

    /// Look up a chunk's stored size across all tiers.
    pub fn get_stored_size(&self, id: &ChunkId) -> Option<u32> {
        // Tier 1: session-new chunks
        if let Some(&size) = self.session_new.get(id) {
            return Some(size);
        }

        // Tier 2: xor filter (probabilistic negative)
        if let Some(ref filter) = self.xor_filter {
            let key = chunk_id_to_u64(id);
            if !filter.contains(&key) {
                // Definite negative — skip mmap lookup.
                return None;
            }
        }

        // Tier 3: mmap binary search (confirms filter hit or used when no filter)
        self.mmap_cache.get_stored_size(id)
    }

    /// Insert a new chunk discovered during this backup session.
    pub fn insert(&mut self, id: ChunkId, stored_size: u32) {
        self.session_new.insert(id, stored_size);
    }

    /// Number of entries in the session-new HashMap.
    pub fn session_new_len(&self) -> usize {
        self.session_new.len()
    }
}

impl std::fmt::Debug for TieredDedupIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredDedupIndex")
            .field("has_xor_filter", &self.xor_filter.is_some())
            .field("mmap_entries", &self.mmap_cache.entry_count())
            .field("session_new", &self.session_new.len())
            .finish()
    }
}

// ===========================================================================
// Restore cache — separate mmap'd file with full pack location data
// ===========================================================================

/// Magic bytes at the start of the restore cache file.
const RESTORE_MAGIC: &[u8; 8] = b"VGIDX\0\0\0";

/// Current restore cache format version.
const RESTORE_VERSION: u32 = 1;

/// Size of the restore cache header in bytes.
const RESTORE_HEADER_SIZE: usize = 28;

/// Size of each restore cache entry:
/// 32-byte ChunkId + 4-byte stored_size + 32-byte PackId + 8-byte pack_offset.
const RESTORE_ENTRY_SIZE: usize = 76;

/// Return the local filesystem path for the restore cache file.
pub fn restore_cache_path(repo_id: &[u8]) -> Option<PathBuf> {
    dirs::cache_dir().map(|base| {
        base.join("vger")
            .join(hex::encode(repo_id))
            .join("restore_cache")
    })
}

/// Build the restore cache binary file from the full chunk index.
/// Writes atomically via temp-file + rename.
pub fn build_restore_cache(index: &ChunkIndex, generation: u64, repo_id: &[u8]) -> Result<()> {
    let Some(path) = restore_cache_path(repo_id) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    build_restore_cache_to_path(index, generation, &path)
}

/// Build the restore cache to an explicit path (used by tests).
pub fn build_restore_cache_to_path(index: &ChunkIndex, generation: u64, path: &Path) -> Result<()> {
    // Collect and sort entries by ChunkId bytes.
    let mut entries: Vec<(ChunkId, u32, PackId, u64)> = index
        .iter()
        .map(|(id, entry)| (*id, entry.stored_size, entry.pack_id, entry.pack_offset))
        .collect();
    entries.sort_unstable_by(|a, b| a.0 .0.cmp(&b.0 .0));

    let entry_count = entries.len() as u32;

    let tmp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    // Header
    w.write_all(RESTORE_MAGIC)?;
    w.write_all(&RESTORE_VERSION.to_le_bytes())?;
    w.write_all(&generation.to_le_bytes())?;
    w.write_all(&entry_count.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved

    // Entries
    for (chunk_id, stored_size, pack_id, pack_offset) in &entries {
        w.write_all(&chunk_id.0)?;
        w.write_all(&stored_size.to_le_bytes())?;
        w.write_all(&pack_id.0)?;
        w.write_all(&pack_offset.to_le_bytes())?;
    }

    w.flush()?;
    drop(w);

    std::fs::rename(&tmp_path, path)?;

    debug!(
        entries = entry_count,
        path = %path.display(),
        "wrote restore cache"
    );

    Ok(())
}

/// Memory-mapped reader over the sorted restore cache binary file.
/// Lookups use binary search over fixed-size 76-byte entries.
pub struct MmapRestoreCache {
    mmap: Mmap,
    entry_count: u32,
}

impl MmapRestoreCache {
    /// Open and validate the restore cache file.
    /// Returns `None` on any mismatch (missing file, wrong magic/version/generation,
    /// unexpected file size).
    pub fn open(repo_id: &[u8], expected_generation: u64) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }
        let path = restore_cache_path(repo_id)?;
        Self::open_path(&path, expected_generation)
    }

    /// Open and validate a restore cache file at an explicit path (used by tests).
    pub fn open_path(path: &Path, expected_generation: u64) -> Option<Self> {
        if expected_generation == 0 {
            return None;
        }

        let file = std::fs::File::open(path).ok()?;

        // SAFETY: we only read the file, and the file is written atomically.
        let mmap = unsafe { Mmap::map(&file) }.ok()?;

        if mmap.len() < RESTORE_HEADER_SIZE {
            debug!("restore cache: file too small for header");
            return None;
        }

        if &mmap[0..8] != RESTORE_MAGIC {
            debug!("restore cache: bad magic");
            return None;
        }

        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version != RESTORE_VERSION {
            debug!(version, "restore cache: unsupported version");
            return None;
        }

        let index_generation = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        if index_generation != expected_generation {
            debug!(
                cache_gen = index_generation,
                expected_gen = expected_generation,
                "restore cache: generation mismatch"
            );
            return None;
        }

        let entry_count = u32::from_le_bytes(mmap[20..24].try_into().unwrap());

        let expected_size = RESTORE_HEADER_SIZE + (entry_count as usize) * RESTORE_ENTRY_SIZE;
        if mmap.len() != expected_size {
            debug!(
                actual = mmap.len(),
                expected = expected_size,
                "restore cache: file size mismatch"
            );
            return None;
        }

        debug!(
            entries = entry_count,
            generation = index_generation,
            "opened restore cache"
        );

        Some(Self { mmap, entry_count })
    }

    /// Look up a chunk ID using binary search.
    /// Returns `(pack_id, pack_offset, stored_size)` if found.
    pub fn lookup(&self, chunk_id: &ChunkId) -> Option<(PackId, u64, u32)> {
        if self.entry_count == 0 {
            return None;
        }

        let target = &chunk_id.0;
        let data = &self.mmap[RESTORE_HEADER_SIZE..];

        let mut lo: usize = 0;
        let mut hi: usize = self.entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = mid * RESTORE_ENTRY_SIZE;
            let entry_id = &data[offset..offset + 32];

            match entry_id.cmp(target.as_slice()) {
                std::cmp::Ordering::Equal => {
                    let stored_size =
                        u32::from_le_bytes(data[offset + 32..offset + 36].try_into().unwrap());
                    let mut pack_bytes = [0u8; 32];
                    pack_bytes.copy_from_slice(&data[offset + 36..offset + 68]);
                    let pack_id = PackId(pack_bytes);
                    let pack_offset =
                        u64::from_le_bytes(data[offset + 68..offset + 76].try_into().unwrap());
                    return Some((pack_id, pack_offset, stored_size));
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        None
    }

    /// Return the number of entries in the cache.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_to_u64_extracts_first_8_bytes() {
        let mut id = ChunkId([0u8; 32]);
        id.0[0..8].copy_from_slice(&42u64.to_le_bytes());
        assert_eq!(chunk_id_to_u64(&id), 42);
    }

    #[test]
    fn dedup_cache_path_returns_some() {
        let repo_id = [0xABu8; 32];
        let path = dedup_cache_path(&repo_id);
        assert!(path.is_some());
        let p = path.unwrap();
        assert!(p.to_string_lossy().contains("dedup_cache"));
        assert!(p.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    #[test]
    fn build_and_read_dedup_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        let mut index = ChunkIndex::new();
        let pack_id = crate::crypto::pack_id::PackId([0x01; 32]);

        // Insert some test entries
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            index.add(chunk_id, 100 + i as u32, pack_id, i as u64 * 100);
        }

        let generation = 12345u64;

        // Build cache
        build_dedup_cache_to_path(&index, generation, &path).unwrap();

        // Open and validate
        let cache = MmapDedupCache::open_path(&path, generation).unwrap();
        assert_eq!(cache.entry_count(), 10);
        assert_eq!(cache.generation(), generation);

        // Look up each entry
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            assert_eq!(cache.get_stored_size(&chunk_id), Some(100 + i as u32));
        }

        // Look up a non-existent entry
        let missing = ChunkId([0xEE; 32]);
        assert_eq!(cache.get_stored_size(&missing), None);
    }

    #[test]
    fn mmap_cache_rejects_wrong_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        let index = ChunkIndex::new();
        let generation = 99u64;

        build_dedup_cache_to_path(&index, generation, &path).unwrap();

        // Wrong generation should return None
        assert!(MmapDedupCache::open_path(&path, 100).is_none());

        // Correct generation should work (even with 0 entries)
        assert!(MmapDedupCache::open_path(&path, 99).is_some());
    }

    #[test]
    fn mmap_cache_rejects_generation_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        // Generation 0 means "no cache ever written" — always returns None.
        assert!(MmapDedupCache::open_path(&path, 0).is_none());
    }

    #[test]
    fn tiered_index_session_new_takes_priority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup_cache");

        let mut index = ChunkIndex::new();
        let pack_id = crate::crypto::pack_id::PackId([0x01; 32]);
        let chunk_id = ChunkId([0xAA; 32]);
        index.add(chunk_id, 100, pack_id, 0);

        let generation = 42u64;
        build_dedup_cache_to_path(&index, generation, &path).unwrap();

        let cache = MmapDedupCache::open_path(&path, generation).unwrap();
        let mut tiered = TieredDedupIndex::new(cache);

        // Override in session_new with different size
        tiered.insert(chunk_id, 999);

        // Should return session_new value
        assert_eq!(tiered.get_stored_size(&chunk_id), Some(999));
    }

    // -------------------------------------------------------------------
    // Restore cache tests
    // -------------------------------------------------------------------

    #[test]
    fn restore_cache_path_returns_some() {
        let repo_id = [0xCDu8; 32];
        let path = restore_cache_path(&repo_id);
        assert!(path.is_some());
        let p = path.unwrap();
        assert!(p.to_string_lossy().contains("restore_cache"));
        assert!(p.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    #[test]
    fn build_and_read_restore_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore_cache");

        let mut index = ChunkIndex::new();
        let pack_a = PackId([0x01; 32]);
        let pack_b = PackId([0x02; 32]);

        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            let pack = if i < 5 { pack_a } else { pack_b };
            index.add(chunk_id, 100 + i as u32, pack, i as u64 * 1000);
        }

        let generation = 777u64;
        build_restore_cache_to_path(&index, generation, &path).unwrap();

        let cache = MmapRestoreCache::open_path(&path, generation).unwrap();
        assert_eq!(cache.entry_count(), 10);

        // Look up each entry
        for i in 0u8..10 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let chunk_id = ChunkId(id_bytes);
            let result = cache.lookup(&chunk_id);
            assert!(result.is_some(), "chunk {i} not found in restore cache");
            let (pack_id, pack_offset, stored_size) = result.unwrap();
            let expected_pack = if i < 5 { pack_a } else { pack_b };
            assert_eq!(pack_id, expected_pack);
            assert_eq!(pack_offset, i as u64 * 1000);
            assert_eq!(stored_size, 100 + i as u32);
        }

        // Non-existent entry
        let missing = ChunkId([0xFF; 32]);
        assert!(cache.lookup(&missing).is_none());
    }

    #[test]
    fn restore_cache_rejects_wrong_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore_cache");

        let index = ChunkIndex::new();
        build_restore_cache_to_path(&index, 50, &path).unwrap();

        assert!(MmapRestoreCache::open_path(&path, 51).is_none());
        assert!(MmapRestoreCache::open_path(&path, 50).is_some());
    }

    #[test]
    fn restore_cache_rejects_generation_zero() {
        assert!(MmapRestoreCache::open_path(Path::new("/nonexistent"), 0).is_none());
    }
}
