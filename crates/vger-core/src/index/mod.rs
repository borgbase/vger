pub mod dedup_cache;

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tracing::debug;

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

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
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
    pub fn update_location(
        &mut self,
        id: &ChunkId,
        pack_id: PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> bool {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.pack_id = pack_id;
            entry.pack_offset = pack_offset;
            entry.stored_size = stored_size;
            true
        } else {
            false
        }
    }

    /// Retain only entries whose ChunkId is in `needed`, then shrink to fit.
    /// Used to reduce memory after loading the full index for a restore.
    pub fn retain_chunks(&mut self, needed: &HashSet<ChunkId>) {
        self.entries.retain(|id, _| needed.contains(id));
        self.entries.shrink_to_fit();
    }

    /// Count distinct pack IDs across all entries.
    pub fn count_distinct_packs(&self) -> usize {
        let packs: std::collections::HashSet<PackId> =
            self.entries.values().map(|e| e.pack_id).collect();
        packs.len()
    }
}

/// Lightweight dedup-only index that stores only chunk_id → stored_size.
///
/// Used during backup to reduce memory: ~68 bytes per entry vs ~112 bytes
/// for the full `ChunkIndex`. For 10M chunks this saves ~400 MB of RAM.
///
/// Does not track refcounts, pack locations, or offsets — those are recorded
/// in an `IndexDelta` and merged back into the full index at save time.
#[derive(Debug)]
pub struct DedupIndex {
    entries: HashMap<ChunkId, u32>,
}

impl DedupIndex {
    /// Build a dedup index from the full chunk index, keeping only chunk_id → stored_size.
    pub fn from_chunk_index(full: &ChunkIndex) -> Self {
        let entries: HashMap<ChunkId, u32> = full
            .entries
            .iter()
            .map(|(id, entry)| (*id, entry.stored_size))
            .collect();
        debug!(
            "built dedup index with {} entries from full index",
            entries.len()
        );
        Self { entries }
    }

    /// Check if a chunk exists (dedup hit).
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.entries.contains_key(id)
    }

    /// Get the stored size for a chunk.
    pub fn get_stored_size(&self, id: &ChunkId) -> Option<u32> {
        self.entries.get(id).copied()
    }

    /// Insert a new chunk (used when new chunks are committed during backup).
    pub fn insert(&mut self, id: ChunkId, stored_size: u32) {
        self.entries.insert(id, stored_size);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Records all index mutations that happen while in dedup-only mode.
///
/// At save time, these are applied to a freshly-loaded full `ChunkIndex`.
#[derive(Debug, Default)]
pub struct IndexDelta {
    /// New chunk entries added during this session.
    pub new_entries: Vec<NewChunkEntry>,
    /// Refcount increments for chunks that already existed in the index.
    pub refcount_bumps: HashMap<ChunkId, u32>,
}

/// A new chunk entry recorded during dedup-mode backup.
#[derive(Debug, Clone)]
pub struct NewChunkEntry {
    pub chunk_id: ChunkId,
    pub stored_size: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
    pub refcount: u32,
}

impl IndexDelta {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if this delta contains no mutations.
    pub fn is_empty(&self) -> bool {
        self.new_entries.is_empty() && self.refcount_bumps.is_empty()
    }

    /// Record a refcount bump for an existing chunk.
    pub fn bump_refcount(&mut self, id: &ChunkId) {
        *self.refcount_bumps.entry(*id).or_insert(0) += 1;
    }

    /// Record a new chunk entry.
    pub fn add_new_entry(
        &mut self,
        chunk_id: ChunkId,
        stored_size: u32,
        pack_id: PackId,
        pack_offset: u64,
        refcount: u32,
    ) {
        self.new_entries.push(NewChunkEntry {
            chunk_id,
            stored_size,
            pack_id,
            pack_offset,
            refcount,
        });
    }

    /// Apply this delta to a full `ChunkIndex`.
    pub fn apply_to(self, index: &mut ChunkIndex) {
        // Apply new entries first
        for entry in self.new_entries {
            index.add(
                entry.chunk_id,
                entry.stored_size,
                entry.pack_id,
                entry.pack_offset,
            );
            // add() sets refcount=1; apply remaining refs
            for _ in 1..entry.refcount {
                index.increment_refcount(&entry.chunk_id);
            }
        }

        // Apply refcount bumps for pre-existing chunks
        for (id, count) in self.refcount_bumps {
            for _ in 0..count {
                index.increment_refcount(&id);
            }
        }
    }
}
