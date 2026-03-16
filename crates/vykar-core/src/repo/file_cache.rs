use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::platform::paths;
use crate::snapshot::item::ChunkRef;
use vykar_crypto::CryptoEngine;
use vykar_types::error::Result;
use vykar_types::snapshot_id::SnapshotId;

use super::format::{
    pack_object_streaming_with_context, unpack_object_expect_with_context, ObjectType,
};

/// Compute the per-repo cache directory.
///
/// With `cache_dir_override`: `<override>/<repo_id_hex>/`
/// Without: `<platform_cache_dir>/vykar/<repo_id_hex>/`
pub(crate) fn repo_cache_dir(repo_id: &[u8], cache_dir_override: Option<&Path>) -> Option<PathBuf> {
    let base = match cache_dir_override {
        Some(dir) => Some(dir.to_path_buf()),
        None => paths::cache_dir().map(|d| d.join("vykar")),
    };
    base.map(|b| b.join(hex::encode(repo_id)))
}

/// 16-byte BLAKE2b hash of a file path, used as a compact HashMap key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PathHash([u8; 16]);

impl serde::Serialize for PathHash {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for PathHash {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct PathHashVisitor;

        impl<'de> serde::de::Visitor<'de> for PathHashVisitor {
            type Value = PathHash;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("16-byte path hash")
            }

            fn visit_bytes<E: serde::de::Error>(
                self,
                v: &[u8],
            ) -> std::result::Result<PathHash, E> {
                if v.len() != 16 {
                    return Err(E::invalid_length(v.len(), &"16 bytes"));
                }
                let mut arr = [0u8; 16];
                arr.copy_from_slice(v);
                Ok(PathHash(arr))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<PathHash, E> {
                Ok(hash_path(v))
            }
        }

        deserializer.deserialize_any(PathHashVisitor)
    }
}

fn hash_path(path: &str) -> PathHash {
    let mut hasher = Blake2bVar::new(16).expect("valid output size");
    hasher.update(path.as_bytes());
    let mut out = [0u8; 16];
    hasher.finalize_variable(&mut out).expect("correct length");
    PathHash(out)
}

/// Cached filesystem metadata for a file, used to skip re-reading unchanged files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCacheEntry {
    pub device: u64,
    pub inode: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub size: u64,
    pub chunk_refs: Vec<ChunkRef>,
}

/// A per-source-label section of the file cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSection {
    /// The snapshot ID that anchors this section's validity.
    pub anchor_snapshot_id: SnapshotId,
    /// The source_paths that were active when this section was built.
    pub source_paths: Vec<String>,
    /// The actual cached entries.
    pub(crate) entries: HashMap<PathHash, FileCacheEntry>,
}

/// Maps path hashes to their cached metadata and chunk references,
/// scoped by source series (source_label).
///
/// Each source label gets its own `CacheSection` so that dump-only sources
/// don't overwrite filesystem source caches, and vice versa.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileCache {
    sections: BTreeMap<String, CacheSection>,
    /// Runtime-only: the source_label currently targeted by `insert` (write cache)
    /// or searched by `lookup` (read cache).
    #[serde(skip)]
    active_label: Option<String>,
}

impl FileCache {
    pub fn new() -> Self {
        Self {
            sections: BTreeMap::new(),
            active_label: None,
        }
    }

    /// Start a new section for the given source label on the **write** cache.
    /// Creates an empty section and sets it as the active insert target.
    pub fn begin_section(&mut self, source_label: &str) {
        // Insert a placeholder section with a zeroed anchor — will be finalized
        // after the snapshot ID is generated.
        self.sections.insert(
            source_label.to_string(),
            CacheSection {
                anchor_snapshot_id: SnapshotId([0u8; 32]),
                source_paths: Vec::new(),
                entries: HashMap::new(),
            },
        );
        self.active_label = Some(source_label.to_string());
    }

    /// Set which section `lookup` and `contains` will search.
    /// Called on the **read** cache before walk begins.
    pub fn set_active_for_lookup(&mut self, source_label: &str) {
        self.active_label = Some(source_label.to_string());
    }

    /// Look up a file in the active section. Returns the cached chunk refs only if all
    /// metadata fields match exactly (device, inode, mtime_ns, ctime_ns, size).
    pub fn lookup(
        &self,
        path: &str,
        device: u64,
        inode: u64,
        mtime_ns: i64,
        ctime_ns: i64,
        size: u64,
    ) -> Option<&Vec<ChunkRef>> {
        let label = self.active_label.as_ref()?;
        let section = self.sections.get(label)?;
        let key = hash_path(path);
        let entry = section.entries.get(&key)?;
        if entry.device == device
            && entry.inode == inode
            && entry.mtime_ns == mtime_ns
            && entry.ctime_ns == ctime_ns
            && entry.size == size
        {
            Some(&entry.chunk_refs)
        } else {
            None
        }
    }

    /// Insert or update a file's cache entry in the active section.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        path: &str,
        device: u64,
        inode: u64,
        mtime_ns: i64,
        ctime_ns: i64,
        size: u64,
        chunk_refs: Vec<ChunkRef>,
    ) {
        let label = self
            .active_label
            .as_ref()
            .expect("insert called without active section");
        let section = self
            .sections
            .get_mut(label)
            .expect("insert called without active section");
        section.entries.insert(
            hash_path(path),
            FileCacheEntry {
                device,
                inode,
                mtime_ns,
                ctime_ns,
                size,
                chunk_refs,
            },
        );
    }

    /// Check if the active section has an entry for this path.
    pub fn contains(&self, path: &str) -> bool {
        let Some(label) = self.active_label.as_ref() else {
            return false;
        };
        let Some(section) = self.sections.get(label) else {
            return false;
        };
        section.entries.contains_key(&hash_path(path))
    }

    /// Finalize the active section with a snapshot ID and source paths.
    /// Called on the **write** cache after the snapshot ID is generated.
    pub fn finalize_section(&mut self, snapshot_id: SnapshotId, source_paths: Vec<String>) {
        let label = self
            .active_label
            .as_ref()
            .expect("finalize_section called without active section");
        let section = self
            .sections
            .get_mut(label)
            .expect("finalize_section called without active section");
        section.anchor_snapshot_id = snapshot_id;
        section.source_paths = source_paths;
    }

    /// Extract the finalized active section for merging into the persistent cache.
    pub fn take_active_section(&mut self) -> Option<(String, CacheSection)> {
        let label = self.active_label.take()?;
        self.sections.remove(&label).map(|s| (label, s))
    }

    /// Replace one section in the persistent cache, leaving others untouched.
    pub fn merge_section(&mut self, label: &str, section: CacheSection) {
        self.sections.insert(label.to_string(), section);
    }

    /// Remove sections whose anchor snapshot no longer exists.
    /// Returns the number of sections invalidated.
    pub fn invalidate_missing_snapshots(&mut self, exists: &dyn Fn(&SnapshotId) -> bool) -> usize {
        let before = self.sections.len();
        self.sections
            .retain(|_label, section| exists(&section.anchor_snapshot_id));
        before - self.sections.len()
    }

    /// Check whether a section exists for the given label AND its stored
    /// source_paths match the provided paths exactly.
    pub fn validate_section(&self, source_label: &str, source_paths: &[String]) -> bool {
        match self.sections.get(source_label) {
            Some(section) => section.source_paths == source_paths,
            None => false,
        }
    }

    /// Total entries across all sections.
    pub fn len(&self) -> usize {
        self.sections.values().map(|s| s.entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.sections.values().all(|s| s.entries.is_empty())
    }

    /// Maximum plausible cache entries per byte of plaintext.
    /// Each serialized entry is at least ~40 bytes (16-byte key + metadata).
    const MIN_BYTES_PER_ENTRY: usize = 40;

    /// Decode from msgpack plaintext. Old format (flat HashMap) returns empty
    /// cache for one-time migration.
    ///
    /// Applies a safety cap: rejects caches that claim more entries than the
    /// plaintext could possibly encode, preventing allocation bombs from
    /// corrupted local cache files.
    pub fn decode_from_plaintext(plaintext: &[u8]) -> Result<Self> {
        // Reject absurdly large inputs early (256 MiB should cover any real cache).
        if plaintext.len() > 256 * 1024 * 1024 {
            debug!(
                "file cache: plaintext too large ({}), starting fresh",
                plaintext.len()
            );
            return Ok(Self::new());
        }

        match rmp_serde::from_slice::<FileCache>(plaintext) {
            Ok(cache) => {
                // Post-decode sanity: reject if total entry count exceeds what
                // the plaintext could encode. This catches msgpack containers
                // with inflated length headers that rmp_serde happened to fill.
                let max_entries = plaintext.len() / Self::MIN_BYTES_PER_ENTRY;
                if cache.len() > max_entries {
                    debug!(
                        entries = cache.len(),
                        max_entries,
                        "file cache: entry count exceeds plausible limit, starting fresh"
                    );
                    return Ok(Self::new());
                }
                Ok(cache)
            }
            Err(_) => {
                // Old format or corrupted — return empty cache.
                debug!("file cache: failed to decode, starting fresh");
                Ok(Self::new())
            }
        }
    }

    /// Return the local filesystem path for the cache file.
    fn cache_path(repo_id: &[u8], cache_dir_override: Option<&Path>) -> Option<PathBuf> {
        repo_cache_dir(repo_id, cache_dir_override).map(|d| d.join("filecache"))
    }

    /// Load the file cache from local disk. Returns an empty cache if the
    /// file doesn't exist or can't be read (backward-compatible).
    pub fn load(
        repo_id: &[u8],
        crypto: &dyn CryptoEngine,
        cache_dir_override: Option<&Path>,
    ) -> Self {
        let Some(path) = Self::cache_path(repo_id, cache_dir_override) else {
            return Self::new();
        };
        let plaintext = {
            let data = match std::fs::read(&path) {
                Ok(d) => d,
                Err(_) => return Self::new(),
            };
            match unpack_object_expect_with_context(
                &data,
                ObjectType::FileCache,
                b"filecache",
                crypto,
            ) {
                Ok(pt) => pt,
                Err(_) => {
                    debug!("file cache: failed to decrypt, starting fresh");
                    return Self::new();
                }
            }
        };
        match Self::decode_from_plaintext(&plaintext) {
            Ok(cache) => cache,
            Err(e) => {
                debug!("file cache: failed to deserialize: {e}, starting fresh");
                Self::new()
            }
        }
    }

    /// Save the file cache to local disk.
    pub fn save(
        &self,
        repo_id: &[u8],
        crypto: &dyn CryptoEngine,
        cache_dir_override: Option<&Path>,
    ) -> Result<()> {
        let Some(path) = Self::cache_path(repo_id, cache_dir_override) else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let estimated = self.len().saturating_mul(120);
        let packed = pack_object_streaming_with_context(
            ObjectType::FileCache,
            b"filecache",
            estimated,
            crypto,
            |buf| Ok(rmp_serde::encode::write(buf, self)?),
        )?;
        debug!(
            entries = self.len(),
            sections = self.sections.len(),
            estimated_bytes = estimated,
            actual_bytes = packed.len(),
            "file cache serialized"
        );
        std::fs::write(&path, packed)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Parent reuse index — runtime-only cold-start fallback
// ---------------------------------------------------------------------------

use crate::snapshot::item::{Item, ItemType};

/// Runtime-only reuse index built from the latest matching snapshot.
/// Not persisted. Used as fallback when no valid local cache section exists.
pub struct ParentReuseIndex {
    entries: HashMap<PathHash, ParentEntry>,
}

struct ParentEntry {
    mtime_ns: i64,
    ctime_ns: i64,
    size: u64,
    chunk_refs: Vec<ChunkRef>,
}

/// Incremental builder for `ParentReuseIndex`.
///
/// Fed items one at a time inside a streaming callback. Call `finish()` to
/// obtain the index if the legacy gate was never tripped.
pub struct ParentReuseBuilder {
    entries: HashMap<PathHash, ParentEntry>,
    /// Pairs of (original_basename, walk_root) for multi-path reconstruction,
    /// or a single walk_root for single-path mode.
    walk_roots: Vec<(String, String)>,
    multi_path: bool,
    /// Set to true when a filesystem file lacks ctime (legacy gate).
    legacy_abort: bool,
}

impl ParentReuseBuilder {
    /// Create a builder.
    ///
    /// `walk_roots`: for each source, the `(original_basename, walk_root)` pair.
    /// - `original_basename`: the basename used in snapshot item paths (from the
    ///   raw configured source path, before canonicalization).
    /// - `walk_root`: the absolute path the walker uses to construct abs_paths
    ///   (canonicalized, matching what cache lookups will see at runtime).
    ///
    /// In single-path mode, only the walk_root of the first entry is used.
    pub fn new(walk_roots: Vec<(String, String)>, multi_path: bool) -> Self {
        Self {
            entries: HashMap::new(),
            walk_roots,
            multi_path,
            legacy_abort: false,
        }
    }

    /// Feed a single item. Takes ownership to avoid cloning chunk_refs.
    /// Returns `false` if the legacy gate was tripped (caller may stop early).
    pub fn push(&mut self, item: Item) -> bool {
        if self.legacy_abort {
            return false;
        }
        if item.entry_type != ItemType::RegularFile {
            return true;
        }
        if item.path.starts_with(".vykar-dumps/") {
            return true;
        }
        let Some(ctime_ns) = item.ctime else {
            self.legacy_abort = true;
            return false;
        };

        let abs_path = reconstruct_abs_path(&item.path, &self.walk_roots, self.multi_path);
        self.entries.insert(
            hash_path(&abs_path),
            ParentEntry {
                mtime_ns: item.mtime,
                ctime_ns,
                size: item.size,
                chunk_refs: item.chunks,
            },
        );
        true
    }

    /// Consume the builder and return the index, or `None` if the legacy gate
    /// was tripped.
    pub fn finish(self) -> Option<ParentReuseIndex> {
        if self.legacy_abort {
            None
        } else {
            Some(ParentReuseIndex {
                entries: self.entries,
            })
        }
    }
}

impl ParentReuseIndex {
    /// Look up a file in the parent index. Matches on (path, size, mtime, ctime).
    /// No device/inode check (not available in snapshots).
    pub fn lookup(
        &self,
        abs_path: &str,
        size: u64,
        mtime_ns: i64,
        ctime_ns: i64,
    ) -> Option<&[ChunkRef]> {
        let entry = self.entries.get(&hash_path(abs_path))?;
        if entry.size == size && entry.mtime_ns == mtime_ns && entry.ctime_ns == ctime_ns {
            Some(&entry.chunk_refs)
        } else {
            None
        }
    }
}

/// Reconstruct the absolute path that the walker will use for cache lookups,
/// given a snapshot item path and the `(original_basename, walk_root)` pairs.
///
/// Uses `Path::join` + `to_string_lossy` to produce the same string form
/// as the walker's `abs_path.to_string_lossy()`.
fn reconstruct_abs_path(
    item_path: &str,
    walk_roots: &[(String, String)],
    multi_path: bool,
) -> String {
    if multi_path {
        // In multi-path mode, item.path has a basename prefix: "docs/a.txt"
        // where "docs" is the original basename of the configured source path
        // (before canonicalization). Match against the original_basename and
        // join with the walk_root (the canonicalized path the walker uses).
        if let Some(slash_pos) = item_path.find('/') {
            let prefix = &item_path[..slash_pos];
            let rel = &item_path[slash_pos + 1..];
            for (basename, walk_root) in walk_roots {
                if basename == prefix {
                    return Path::new(walk_root).join(rel).to_string_lossy().to_string();
                }
            }
        }
        // Fallback: can't reconstruct — use item path as-is (won't match).
        item_path.to_string()
    } else if let Some((_basename, walk_root)) = walk_roots.first() {
        // Single-path: join with the walk root.
        Path::new(walk_root)
            .join(item_path)
            .to_string_lossy()
            .to_string()
    } else {
        item_path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_types::chunk_id::ChunkId;

    fn sample_chunk_refs() -> Vec<ChunkRef> {
        vec![ChunkRef {
            id: ChunkId([0xAA; 32]),
            size: 1024,
            csize: 512,
        }]
    }

    #[test]
    fn section_based_insert_and_lookup() {
        let mut cache = FileCache::new();
        cache.begin_section("source-1");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn lookup_requires_active_section() {
        let mut cache = FileCache::new();
        cache.begin_section("source-1");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        // Change active to a different label — should not find the entry.
        cache.set_active_for_lookup("source-2");
        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());

        // Switch back — should find it again.
        cache.set_active_for_lookup("source-1");
        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_some());
    }

    #[test]
    fn lookup_miss_wrong_path() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/other.txt", 1, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_mtime() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 9999999999, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_ctime() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 9999999999, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_size() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 8192);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_inode() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 1, 2000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_miss_changed_device() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );

        let result = cache.lookup("/tmp/test.txt", 2, 1000, 1234567890, 1234567890, 4096);
        assert!(result.is_none());
    }

    #[test]
    fn insert_overwrites_existing() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );
        cache.insert(
            "/tmp/test.txt",
            1,
            1000,
            9999999999,
            9999999999,
            8192,
            vec![],
        );

        assert_eq!(cache.len(), 1);
        assert!(cache
            .lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096)
            .is_none());
        assert!(cache
            .lookup("/tmp/test.txt", 1, 1000, 9999999999, 9999999999, 8192)
            .is_some());
    }

    #[test]
    fn empty_cache() {
        let cache = FileCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn independent_sections() {
        let mut cache = FileCache::new();

        cache.begin_section("label-a");
        cache.insert("/a/file.txt", 1, 100, 111, 111, 4096, sample_chunk_refs());

        cache.begin_section("label-b");
        cache.insert("/b/file.txt", 1, 200, 222, 222, 8192, sample_chunk_refs());

        // Looking up in label-b should not find label-a's entry.
        assert!(cache
            .lookup("/a/file.txt", 1, 100, 111, 111, 4096)
            .is_none());
        assert!(cache
            .lookup("/b/file.txt", 1, 200, 222, 222, 8192)
            .is_some());

        // Switch to label-a.
        cache.set_active_for_lookup("label-a");
        assert!(cache
            .lookup("/a/file.txt", 1, 100, 111, 111, 4096)
            .is_some());
        assert!(cache
            .lookup("/b/file.txt", 1, 200, 222, 222, 8192)
            .is_none());

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn invalidate_missing_snapshots() {
        let mut cache = FileCache::new();
        let id_a = SnapshotId([0xAA; 32]);
        let id_b = SnapshotId([0xBB; 32]);

        cache.sections.insert(
            "label-a".into(),
            CacheSection {
                anchor_snapshot_id: id_a,
                source_paths: vec!["/a".into()],
                entries: HashMap::new(),
            },
        );
        cache.sections.insert(
            "label-b".into(),
            CacheSection {
                anchor_snapshot_id: id_b,
                source_paths: vec!["/b".into()],
                entries: HashMap::new(),
            },
        );

        // Only id_a exists — id_b's section should be invalidated.
        let removed = cache.invalidate_missing_snapshots(&|id| *id == id_a);
        assert_eq!(removed, 1);
        assert!(cache.sections.contains_key("label-a"));
        assert!(!cache.sections.contains_key("label-b"));
    }

    #[test]
    fn validate_section_checks_paths() {
        let mut cache = FileCache::new();
        cache.sections.insert(
            "my-source".into(),
            CacheSection {
                anchor_snapshot_id: SnapshotId([0x11; 32]),
                source_paths: vec!["/data".into(), "/config".into()],
                entries: HashMap::new(),
            },
        );

        assert!(cache.validate_section("my-source", &["/data".into(), "/config".into()]));
        assert!(!cache.validate_section("my-source", &["/data".into()]));
        assert!(!cache.validate_section("other", &["/data".into()]));
    }

    #[test]
    fn merge_section_replaces_only_target() {
        let mut cache = FileCache::new();
        let id_a = SnapshotId([0xAA; 32]);
        let id_b = SnapshotId([0xBB; 32]);

        cache.sections.insert(
            "label-a".into(),
            CacheSection {
                anchor_snapshot_id: id_a,
                source_paths: vec![],
                entries: HashMap::new(),
            },
        );

        let new_section = CacheSection {
            anchor_snapshot_id: id_b,
            source_paths: vec!["/new".into()],
            entries: HashMap::new(),
        };
        cache.merge_section("label-b", new_section);

        assert_eq!(cache.sections.len(), 2);
        assert!(cache.sections.contains_key("label-a"));
        assert!(cache.sections.contains_key("label-b"));
    }

    #[test]
    fn take_active_section() {
        let mut cache = FileCache::new();
        cache.begin_section("test");
        cache.insert("/a.txt", 1, 1, 1, 1, 100, sample_chunk_refs());
        cache.finalize_section(SnapshotId([0x42; 32]), vec!["/src".into()]);

        let taken = cache.take_active_section();
        assert!(taken.is_some());
        let (label, section) = taken.unwrap();
        assert_eq!(label, "test");
        assert_eq!(section.entries.len(), 1);
        assert_eq!(section.anchor_snapshot_id, SnapshotId([0x42; 32]));
        assert!(cache.sections.is_empty());
    }

    #[test]
    fn contains_checks_active_section() {
        let mut cache = FileCache::new();
        cache.begin_section("s");
        cache.insert("/a.txt", 1, 1, 1, 1, 100, sample_chunk_refs());

        assert!(cache.contains("/a.txt"));
        assert!(!cache.contains("/b.txt"));
    }

    #[test]
    fn round_trip_serialization() {
        let mut cache = FileCache::new();
        cache.begin_section("source-1");
        for i in 0..10 {
            cache.insert(
                &format!("/tmp/file_{i}.txt"),
                1,
                1000 + i as u64,
                1234567890,
                1234567890,
                4096,
                sample_chunk_refs(),
            );
        }
        cache.finalize_section(SnapshotId([0xDD; 32]), vec!["/tmp".into()]);

        let plaintext = rmp_serde::to_vec(&cache).unwrap();
        let decoded = FileCache::decode_from_plaintext(&plaintext).unwrap();
        assert_eq!(decoded.sections.len(), 1);
        assert!(decoded.sections.contains_key("source-1"));
        let section = &decoded.sections["source-1"];
        assert_eq!(section.entries.len(), 10);
        assert_eq!(section.anchor_snapshot_id, SnapshotId([0xDD; 32]));
    }

    #[test]
    fn old_format_returns_empty_cache() {
        // Old flat-map format — should gracefully return empty.
        #[derive(Serialize)]
        struct OldFileCache {
            entries: HashMap<String, FileCacheEntry>,
        }
        let mut entries = HashMap::new();
        entries.insert(
            "/tmp/old.txt".to_string(),
            FileCacheEntry {
                device: 1,
                inode: 1,
                mtime_ns: 1,
                ctime_ns: 1,
                size: 100,
                chunk_refs: sample_chunk_refs(),
            },
        );
        let old = OldFileCache { entries };
        let plaintext = rmp_serde::to_vec(&old).unwrap();
        let decoded = FileCache::decode_from_plaintext(&plaintext).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn bogus_data_returns_empty_cache() {
        let garbage = vec![0xFF, 0xFE, 0xFD];
        let result = FileCache::decode_from_plaintext(&garbage);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn repo_cache_dir_default() {
        let repo_id = [0xABu8; 32];
        let dir = super::repo_cache_dir(&repo_id, None);
        assert!(dir.is_some());
        let d = dir.unwrap();
        assert!(d.to_string_lossy().contains("vykar"));
        assert!(d.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    #[test]
    fn repo_cache_dir_with_override() {
        let repo_id = [0xCDu8; 32];
        let temp = tempfile::tempdir().unwrap();
        let override_root = temp.path().join("vykar-cache");
        let dir = super::repo_cache_dir(&repo_id, Some(override_root.as_path())).unwrap();
        assert!(dir.starts_with(&override_root));
        assert!(dir.to_string_lossy().contains(&hex::encode(repo_id)));
    }

    /// Helper: build a ParentReuseIndex from a Vec<Item> using the builder.
    /// Accepts raw source paths and derives walk_roots (basename = walk_root basename).
    fn build_parent_index(
        items: Vec<Item>,
        source_paths: &[String],
        multi_path: bool,
    ) -> Option<ParentReuseIndex> {
        let walk_roots: Vec<(String, String)> = source_paths
            .iter()
            .map(|sp| {
                let p = Path::new(sp);
                let basename = p
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| sp.clone());
                (basename, sp.clone())
            })
            .collect();
        let mut builder = ParentReuseBuilder::new(walk_roots, multi_path);
        for item in items {
            builder.push(item);
        }
        builder.finish()
    }

    #[test]
    fn parent_reuse_index_basic() {
        let items = vec![
            Item {
                path: "a.txt".into(),
                entry_type: ItemType::RegularFile,
                mode: 0o644,
                uid: 0,
                gid: 0,
                user: None,
                group: None,
                mtime: 1000,
                atime: None,
                ctime: Some(2000),
                size: 4096,
                chunks: sample_chunk_refs(),
                link_target: None,
                xattrs: None,
            },
            Item {
                path: "dir".into(),
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
            },
        ];

        let idx = build_parent_index(items, &["/src".into()], false).unwrap();
        // Should find the file
        let hit = idx.lookup("/src/a.txt", 4096, 1000, 2000);
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().len(), 1);

        // Wrong mtime — miss
        assert!(idx.lookup("/src/a.txt", 4096, 9999, 2000).is_none());
        // Wrong ctime — miss
        assert!(idx.lookup("/src/a.txt", 4096, 1000, 9999).is_none());
    }

    #[test]
    fn parent_reuse_index_legacy_gate() {
        let items = vec![Item {
            path: "a.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1000,
            atime: None,
            ctime: None, // No ctime — legacy
            size: 4096,
            chunks: sample_chunk_refs(),
            link_target: None,
            xattrs: None,
        }];

        let result = build_parent_index(items, &["/src".into()], false);
        assert!(result.is_none(), "legacy gate should prevent parent index");
    }

    #[test]
    fn parent_reuse_index_ignores_dumps() {
        let items = vec![
            Item {
                path: ".vykar-dumps/pg_dump".into(),
                entry_type: ItemType::RegularFile,
                mode: 0o644,
                uid: 0,
                gid: 0,
                user: None,
                group: None,
                mtime: 1000,
                atime: None,
                ctime: None, // Dumps have no ctime — should not trip legacy gate
                size: 4096,
                chunks: sample_chunk_refs(),
                link_target: None,
                xattrs: None,
            },
            Item {
                path: "real.txt".into(),
                entry_type: ItemType::RegularFile,
                mode: 0o644,
                uid: 0,
                gid: 0,
                user: None,
                group: None,
                mtime: 2000,
                atime: None,
                ctime: Some(3000),
                size: 8192,
                chunks: sample_chunk_refs(),
                link_target: None,
                xattrs: None,
            },
        ];

        let idx = build_parent_index(items, &["/src".into()], false).unwrap();
        // Dump item should not be indexed
        assert!(idx
            .lookup("/src/.vykar-dumps/pg_dump", 4096, 1000, 0)
            .is_none());
        // Real file should be indexed
        assert!(idx.lookup("/src/real.txt", 8192, 2000, 3000).is_some());
    }

    #[test]
    fn parent_reuse_index_multi_path() {
        let items = vec![Item {
            path: "home/a.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1000,
            atime: None,
            ctime: Some(2000),
            size: 4096,
            chunks: sample_chunk_refs(),
            link_target: None,
            xattrs: None,
        }];

        let idx =
            build_parent_index(items, &["/mnt/home".into(), "/mnt/data".into()], true).unwrap();
        // "home/a.txt" → matches source_path "/mnt/home" (basename "home"), reconstructs to "/mnt/home/a.txt"
        assert!(idx.lookup("/mnt/home/a.txt", 4096, 1000, 2000).is_some());
    }

    #[test]
    fn reconstruct_abs_path_single() {
        let roots = vec![("data".into(), "/data".into())];
        let p = reconstruct_abs_path("dir/file.txt", &roots, false);
        assert_eq!(p, "/data/dir/file.txt");
    }

    #[test]
    fn reconstruct_abs_path_single_trailing_slash() {
        // Trailing slash in walk_root must not produce double-slash.
        let roots = vec![("data".into(), "/data/".into())];
        let p = reconstruct_abs_path("dir/file.txt", &roots, false);
        assert_eq!(p, "/data/dir/file.txt");
    }

    #[test]
    fn reconstruct_abs_path_multi() {
        let roots = vec![
            ("data".into(), "/mnt/data".into()),
            ("home".into(), "/mnt/home".into()),
        ];
        let p = reconstruct_abs_path("data/sub/file.txt", &roots, true);
        assert_eq!(p, "/mnt/data/sub/file.txt");
    }

    #[test]
    fn reconstruct_abs_path_multi_trailing_slash() {
        let roots = vec![
            ("data".into(), "/mnt/data/".into()),
            ("home".into(), "/mnt/home/".into()),
        ];
        let p = reconstruct_abs_path("data/sub/file.txt", &roots, true);
        assert_eq!(p, "/mnt/data/sub/file.txt");
    }

    #[test]
    fn reconstruct_abs_path_symlink_multi() {
        // Symlink: docs -> /mnt/real-docs. The snapshot uses "docs" as prefix
        // (original basename), but the walk root is the canonicalized target.
        let roots = vec![
            ("docs".into(), "/mnt/real-docs".into()),
            ("config".into(), "/etc".into()),
        ];
        let p = reconstruct_abs_path("docs/readme.txt", &roots, true);
        assert_eq!(p, "/mnt/real-docs/readme.txt");
    }
}
