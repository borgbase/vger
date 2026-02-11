use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::crypto::chunk_id::ChunkId;
use crate::crypto::pack_id::PackId;

/// In-memory index of all chunks in the repository.
/// Maps chunk_id -> (refcount, stored_size, pack_id, pack_offset).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkIndex {
    entries: HashMap<ChunkId, ChunkIndexEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ChunkIndexEntry {
    pub refcount: u32,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Returns `true` if this chunk already exists (dedup hit).
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.entries.contains_key(id)
    }

    /// Add a new chunk entry with its pack location.
    pub fn add(&mut self, id: ChunkId, stored_size: u32, pack_id: PackId, pack_offset: u64) {
        self.entries
            .entry(id)
            .and_modify(|e| e.refcount += 1)
            .or_insert(ChunkIndexEntry {
                refcount: 1,
                stored_size,
                pack_id,
                pack_offset,
            });
    }

    /// Increment the refcount for an existing chunk without changing its location.
    pub fn increment_refcount(&mut self, id: &ChunkId) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.refcount += 1;
        }
    }

    pub fn get(&self, id: &ChunkId) -> Option<&ChunkIndexEntry> {
        self.entries.get(id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChunkId, &ChunkIndexEntry)> {
        self.entries.iter()
    }

    /// Decrement refcount for a chunk. Returns the new refcount and stored_size.
    /// If refcount reaches 0, the entry is removed from the index.
    /// Returns None if the chunk is not in the index.
    pub fn decrement(&mut self, id: &ChunkId) -> Option<(u32, u32)> {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.refcount = entry.refcount.saturating_sub(1);
            let rc = entry.refcount;
            let size = entry.stored_size;
            if rc == 0 {
                self.entries.remove(id);
            }
            Some((rc, size))
        } else {
            None
        }
    }

    /// Update the storage location of an existing chunk (used by compact).
    /// Returns `true` if the chunk was found and updated.
    pub fn update_location(&mut self, id: &ChunkId, pack_id: PackId, pack_offset: u64, stored_size: u32) -> bool {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.pack_id = pack_id;
            entry.pack_offset = pack_offset;
            entry.stored_size = stored_size;
            true
        } else {
            false
        }
    }

    /// Count distinct pack IDs across all entries.
    pub fn count_distinct_packs(&self) -> usize {
        let packs: std::collections::HashSet<PackId> =
            self.entries.values().map(|e| e.pack_id).collect();
        packs.len()
    }
}
