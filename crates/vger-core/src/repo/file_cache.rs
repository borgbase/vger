use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::crypto::CryptoEngine;
use crate::error::{Result, VgerError};
use crate::snapshot::item::ChunkRef;

use super::format::{pack_object_streaming, unpack_object_expect, ObjectType};

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

/// Maps absolute file paths to their cached metadata and chunk references.
/// Used to skip re-reading, re-chunking, and re-compressing unchanged files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileCache {
    entries: HashMap<String, FileCacheEntry>,
}

impl FileCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
        }
    }

    /// Look up a file in the cache. Returns the cached chunk refs only if all
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
        let entry = self.entries.get(path)?;
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

    /// Insert or update a file's cache entry.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        path: String,
        device: u64,
        inode: u64,
        mtime_ns: i64,
        ctime_ns: i64,
        size: u64,
        chunk_refs: Vec<ChunkRef>,
    ) {
        self.entries.insert(
            path,
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

    /// Return the cached entry for a path, if present.
    pub fn get(&self, path: &str) -> Option<&FileCacheEntry> {
        self.entries.get(path)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove entries whose chunk IDs are not present in the index.
    /// Returns the number of entries removed.
    ///
    /// SAFETY INVARIANT: This must be called before backup begins. Cache-hit
    /// paths skip per-file `chunk_exists` checks and rely on this pre-sanitization
    /// to guarantee that every ChunkRef in every surviving entry is valid.
    pub fn prune_stale_entries(
        &mut self,
        chunk_exists: &dyn Fn(&crate::crypto::chunk_id::ChunkId) -> bool,
    ) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_path, entry| entry.chunk_refs.iter().all(|cr| chunk_exists(&cr.id)));
        before - self.entries.len()
    }

    /// Decode from msgpack plaintext with pre-allocated HashMap capacity.
    /// Parses the msgpack envelope to extract the map entry count, applies a
    /// safety cap based on plaintext length, then deserializes entries into a
    /// pre-sized HashMap. Falls back to rmp_serde::from_slice on parse error.
    fn decode_from_plaintext(plaintext: &[u8]) -> Result<Self> {
        match Self::try_decode_preallocated(plaintext) {
            Ok(cache) => Ok(cache),
            Err(_) => {
                // Envelope parsing failed — fall back to derived Deserialize.
                Ok(rmp_serde::from_slice(plaintext)?)
            }
        }
    }

    fn try_decode_preallocated(plaintext: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(plaintext);

        // Peek at first byte to determine envelope shape.
        let marker = rmp::decode::read_marker(&mut cursor)
            .map_err(|e| VgerError::Other(format!("file cache: bad marker: {e:?}")))?;

        let map_len = match marker {
            // Struct-as-array: [entries_map]
            rmp::Marker::FixArray(1) | rmp::Marker::Array16 | rmp::Marker::Array32 => {
                cursor.set_position(0);
                let outer = rmp::decode::read_array_len(&mut cursor)
                    .map_err(|e| VgerError::Other(format!("file cache: {e}")))?;
                if outer != 1 {
                    return Err(VgerError::Other(
                        "file cache: expected 1-field struct".into(),
                    ));
                }
                rmp::decode::read_map_len(&mut cursor)
                    .map_err(|e| VgerError::Other(format!("file cache: {e}")))?
                    as usize
            }
            // Struct-as-map: {"entries": entries_map}
            rmp::Marker::FixMap(1) | rmp::Marker::Map16 | rmp::Marker::Map32 => {
                cursor.set_position(0);
                let outer = rmp::decode::read_map_len(&mut cursor)
                    .map_err(|e| VgerError::Other(format!("file cache: {e}")))?;
                if outer != 1 {
                    return Err(VgerError::Other(
                        "file cache: expected 1-field struct".into(),
                    ));
                }
                // Skip key string ("entries") by reading it through the decoder
                let mut key_buf = [0u8; 16]; // "entries" is 7 bytes; 16 is plenty
                rmp::decode::read_str(&mut cursor, &mut key_buf)
                    .map_err(|e| VgerError::Other(format!("file cache: {e}")))?;
                rmp::decode::read_map_len(&mut cursor)
                    .map_err(|e| VgerError::Other(format!("file cache: {e}")))?
                    as usize
            }
            _ => return Err(VgerError::Other("file cache: unexpected envelope".into())),
        };

        // Safety cap: each map entry is at least ~8 bytes in msgpack.
        let max_entries = plaintext.len() / 8;
        let capacity = map_len.min(max_entries);

        let pos = cursor.position() as usize;
        if pos > plaintext.len() {
            return Err(VgerError::Other("file cache: truncated".into()));
        }
        let remaining = &plaintext[pos..];
        let mut de = rmp_serde::Deserializer::new(remaining);
        let mut entries = HashMap::with_capacity(capacity);
        for _ in 0..map_len {
            let key: String = Deserialize::deserialize(&mut de)?;
            let val: FileCacheEntry = Deserialize::deserialize(&mut de)?;
            entries.insert(key, val);
        }
        Ok(FileCache { entries })
    }

    /// Return the local filesystem path for the cache file.
    /// Platform cache dir + `vger/<repo_id_hex>/filecache`
    /// (macOS: `~/Library/Caches/vger/…`, Linux: `~/.cache/vger/…`)
    fn cache_path(repo_id: &[u8]) -> Option<PathBuf> {
        dirs::cache_dir().map(|base| {
            base.join("vger")
                .join(hex::encode(repo_id))
                .join("filecache")
        })
    }

    /// Load the file cache from local disk. Returns an empty cache if the
    /// file doesn't exist or can't be read (backward-compatible).
    pub fn load(repo_id: &[u8], crypto: &dyn CryptoEngine) -> Self {
        let Some(path) = Self::cache_path(repo_id) else {
            return Self::new();
        };
        // Scope `data` so the encrypted blob (~64M) is dropped before
        // deserialization allocates the HashMap.
        let plaintext = {
            let data = match std::fs::read(&path) {
                Ok(d) => d,
                Err(_) => return Self::new(),
            };
            match unpack_object_expect(&data, ObjectType::FileCache, crypto) {
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
    pub fn save(&self, repo_id: &[u8], crypto: &dyn CryptoEngine) -> Result<()> {
        let Some(path) = Self::cache_path(repo_id) else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Stream-serialize directly into the output buffer to avoid a separate
        // plaintext allocation (~89M savings for large caches).
        let estimated = self.entries.len().saturating_mul(240);
        let packed = pack_object_streaming(ObjectType::FileCache, estimated, crypto, |buf| {
            Ok(rmp_serde::encode::write(buf, self)?)
        })?;
        debug!(
            entries = self.entries.len(),
            estimated_bytes = estimated,
            actual_bytes = packed.len(),
            "file cache serialized"
        );
        std::fs::write(&path, packed)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::chunk_id::ChunkId;

    fn sample_chunk_refs() -> Vec<ChunkRef> {
        vec![ChunkRef {
            id: ChunkId([0xAA; 32]),
            size: 1024,
            csize: 512,
        }]
    }

    #[test]
    fn lookup_hit() {
        let mut cache = FileCache::new();
        cache.insert(
            "/tmp/test.txt".into(),
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
    fn lookup_miss_wrong_path() {
        let mut cache = FileCache::new();
        cache.insert(
            "/tmp/test.txt".into(),
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
        cache.insert(
            "/tmp/test.txt".into(),
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
        cache.insert(
            "/tmp/test.txt".into(),
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
        cache.insert(
            "/tmp/test.txt".into(),
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
        cache.insert(
            "/tmp/test.txt".into(),
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
        cache.insert(
            "/tmp/test.txt".into(),
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
        cache.insert(
            "/tmp/test.txt".into(),
            1,
            1000,
            1234567890,
            1234567890,
            4096,
            sample_chunk_refs(),
        );
        cache.insert(
            "/tmp/test.txt".into(),
            1,
            1000,
            9999999999,
            9999999999,
            8192,
            vec![],
        );

        assert_eq!(cache.len(), 1);
        // Old metadata should not match
        assert!(cache
            .lookup("/tmp/test.txt", 1, 1000, 1234567890, 1234567890, 4096)
            .is_none());
        // New metadata should match
        assert!(cache
            .lookup("/tmp/test.txt", 1, 1000, 9999999999, 9999999999, 8192)
            .is_some());
    }

    #[test]
    fn empty_cache() {
        let cache = FileCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(cache.lookup("/any/path", 0, 0, 0, 0, 0).is_none());
    }

    #[test]
    fn decode_from_plaintext_round_trip() {
        let mut cache = FileCache::new();
        for i in 0..100 {
            cache.insert(
                format!("/tmp/file_{i}.txt"),
                1,
                1000 + i as u64,
                1234567890,
                1234567890,
                4096,
                sample_chunk_refs(),
            );
        }

        // Serialize with derived Serialize (struct-as-array).
        let plaintext = rmp_serde::to_vec(&cache).unwrap();

        // Decode with the pre-allocating decoder.
        let decoded = FileCache::decode_from_plaintext(&plaintext).unwrap();
        assert_eq!(decoded.len(), 100);
        for i in 0..100 {
            let path = format!("/tmp/file_{i}.txt");
            assert!(
                decoded
                    .lookup(&path, 1, 1000 + i as u64, 1234567890, 1234567890, 4096)
                    .is_some(),
                "missing entry for {path}"
            );
        }
    }

    #[test]
    fn decode_from_plaintext_empty_cache() {
        let cache = FileCache::new();
        let plaintext = rmp_serde::to_vec(&cache).unwrap();
        let decoded = FileCache::decode_from_plaintext(&plaintext).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn decode_from_plaintext_bogus_map_len_capped() {
        // Craft a msgpack byte sequence: array(1) then a map header claiming 2^30 entries.
        // The safety cap should limit allocation to plaintext.len() / 8.
        let mut buf = Vec::new();
        // FixArray(1)
        rmp::encode::write_array_len(&mut buf, 1).unwrap();
        // Map32 with absurd length
        rmp::encode::write_map_len(&mut buf, 1 << 30).unwrap();
        // No actual data follows — deserialization should fail gracefully, not OOM.
        let result = FileCache::decode_from_plaintext(&buf);
        // Should error (not panic/OOM), either from try_decode or fallback.
        assert!(result.is_err());
    }

    #[test]
    fn decode_from_plaintext_falls_back_on_garbage() {
        // Completely invalid msgpack — should fall through to rmp_serde and error.
        let garbage = vec![0xFF, 0xFE, 0xFD];
        let result = FileCache::decode_from_plaintext(&garbage);
        assert!(result.is_err());
    }
}
