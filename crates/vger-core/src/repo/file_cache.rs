use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::crypto::CryptoEngine;
use crate::error::Result;
use crate::snapshot::item::ChunkRef;

use super::format::{pack_object, unpack_object_expect, ObjectType};

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
        let Ok(data) = std::fs::read(&path) else {
            return Self::new();
        };
        let Ok(plaintext) = unpack_object_expect(&data, ObjectType::FileCache, crypto) else {
            debug!("file cache: failed to decrypt, starting fresh");
            return Self::new();
        };
        match rmp_serde::from_slice(&plaintext) {
            Ok(cache) => cache,
            Err(e) => {
                debug!("file cache: failed to deserialize: {e}, starting fresh");
                Self::new()
            }
        }
    }

    /// Save the file cache to local disk. Errors are logged but non-fatal.
    pub fn save(&self, repo_id: &[u8], crypto: &dyn CryptoEngine) -> Result<()> {
        let Some(path) = Self::cache_path(repo_id) else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let cache_bytes = rmp_serde::to_vec(self)?;
        let packed = pack_object(ObjectType::FileCache, &cache_bytes, crypto)?;
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
}
