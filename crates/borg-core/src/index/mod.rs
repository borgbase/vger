use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::crypto::chunk_id::ChunkId;

/// In-memory index of all chunks in the repository.
/// Maps chunk_id -> (refcount, stored_size).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkIndex {
    entries: HashMap<ChunkId, ChunkIndexEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ChunkIndexEntry {
    pub refcount: u32,
    pub stored_size: u32,
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

    /// Add or increment refcount for a chunk.
    pub fn add(&mut self, id: ChunkId, stored_size: u32) {
        self.entries
            .entry(id)
            .and_modify(|e| e.refcount += 1)
            .or_insert(ChunkIndexEntry {
                refcount: 1,
                stored_size,
            });
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

    /// Decrement refcount for a chunk. Returns the new refcount.
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
}
