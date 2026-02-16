use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::crypto::chunk_id::ChunkId;
use crate::crypto::pack_id::PackId;
use crate::crypto::CryptoEngine;
use crate::error::{Result, VgerError};
use crate::storage::StorageBackend;

use super::format::{pack_object, unpack_object_expect, ObjectType};

/// Magic bytes at the start of every pack file.
pub const PACK_MAGIC: &[u8; 8] = b"VGERPACK";
/// Pack format version.
pub const PACK_VERSION: u8 = 1;
/// Size of the pack header (magic + version byte).
pub const PACK_HEADER_SIZE: usize = 9;

/// Distinguishes data packs (file content) from tree packs (item-stream metadata).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackType {
    Data,
    Tree,
}

/// One entry in the pack's trailing header. Describes a single blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackHeaderEntry {
    pub obj_type: u8,
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub length: u32,
    pub uncompressed_size: u32,
}

/// A blob buffered in memory before being flushed to a pack file.
struct BufferedBlob {
    obj_type: u8,
    chunk_id: ChunkId,
    encrypted_data: Vec<u8>,
    uncompressed_size: u32,
}

/// Maximum number of blobs in a single pack file.
/// Prevents pathological cases where many tiny chunks create a pack with a huge header.
pub const MAX_BLOBS_PER_PACK: usize = 10_000;

/// Maximum age of a pack writer before it should be flushed (in seconds).
/// Forces periodic flushes even if the pack isn't full, preventing stale data
/// from sitting in memory indefinitely during long backups.
pub const PACK_MAX_AGE_SECS: u64 = 300;

/// Tuple describing one chunk's location and refcount in a sealed/flushed pack.
pub type PackedChunkEntry = (ChunkId, u32, u64, u32);

/// Result of sealing a pack in-memory: (pack_id, serialized pack bytes, chunk entries).
pub type SealedPackResult = (PackId, Vec<u8>, Vec<PackedChunkEntry>);

/// Result of flushing a pack to storage: (pack_id, chunk entries).
pub type FlushedPackResult = (PackId, Vec<PackedChunkEntry>);

/// Accumulates encrypted blobs and flushes them as pack files.
pub struct PackWriter {
    pack_type: PackType,
    target_size: usize,
    buffer: Vec<BufferedBlob>,
    current_size: usize,
    /// chunk_id -> (stored_size, refcount) for pending (not-yet-flushed) blobs.
    pending: HashMap<ChunkId, (u32, u32)>,
    /// When the first blob was added to the current buffer.
    first_blob_time: Option<Instant>,
}

impl PackWriter {
    pub fn new(pack_type: PackType, target_size: usize) -> Self {
        Self {
            pack_type,
            target_size,
            buffer: Vec::new(),
            current_size: 0,
            pending: HashMap::new(),
            first_blob_time: None,
        }
    }

    /// Add an encrypted blob to the pack buffer. Returns the offset within the pack
    /// where the blob data starts (after the 4-byte length prefix).
    pub fn add_blob(
        &mut self,
        obj_type: u8,
        chunk_id: ChunkId,
        encrypted_blob: Vec<u8>,
        uncompressed_size: u32,
    ) -> u64 {
        let blob_len = encrypted_blob.len() as u32;
        // Offset accounts for: pack header + bytes already buffered + this blob's 4B len prefix.
        let offset = PACK_HEADER_SIZE as u64 + self.current_size as u64 + 4;

        self.current_size += 4 + encrypted_blob.len(); // 4B length prefix + blob data
        if self.first_blob_time.is_none() {
            self.first_blob_time = Some(Instant::now());
        }
        self.pending.insert(chunk_id, (blob_len, 1));
        self.buffer.push(BufferedBlob {
            obj_type,
            chunk_id,
            encrypted_data: encrypted_blob,
            uncompressed_size,
        });

        offset
    }

    /// Check if a chunk is pending in this writer (not yet flushed).
    pub fn contains_pending(&self, chunk_id: &ChunkId) -> bool {
        self.pending.contains_key(chunk_id)
    }

    /// Increment refcount for a pending chunk (dedup hit within the same pack).
    pub fn increment_pending(&mut self, chunk_id: &ChunkId) {
        if let Some(entry) = self.pending.get_mut(chunk_id) {
            entry.1 += 1;
        }
    }

    /// Get stored size for a pending chunk.
    pub fn get_pending_stored_size(&self, chunk_id: &ChunkId) -> Option<u32> {
        self.pending.get(chunk_id).map(|(size, _)| *size)
    }

    /// Whether the current buffer should be flushed.
    ///
    /// Returns true when any of these conditions are met:
    /// - Pack has reached its target byte size
    /// - Pack has reached the maximum blob count (10,000)
    /// - Pack has been open longer than the max age (300 seconds)
    pub fn should_flush(&self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        if self.current_size >= self.target_size {
            return true;
        }
        if self.buffer.len() >= MAX_BLOBS_PER_PACK {
            return true;
        }
        if let Some(first_time) = self.first_blob_time {
            if first_time.elapsed().as_secs() >= PACK_MAX_AGE_SECS {
                return true;
            }
        }
        false
    }

    /// Whether there are any pending blobs.
    pub fn has_pending(&self) -> bool {
        !self.buffer.is_empty()
    }

    pub fn pack_type(&self) -> PackType {
        self.pack_type
    }

    /// Assemble buffered blobs into a pack, compute its PackId, and clear internal state.
    /// Does NOT write to storage — the caller is responsible for uploading `pack_bytes`.
    /// Returns (pack_id, pack_bytes, vec of (chunk_id, stored_size, offset, refcount)).
    pub fn seal(&mut self, crypto: &dyn CryptoEngine) -> Result<SealedPackResult> {
        if self.buffer.is_empty() {
            return Err(VgerError::Other("cannot seal empty pack writer".into()));
        }

        // Build header entries
        let mut header_entries: Vec<PackHeaderEntry> = Vec::with_capacity(self.buffer.len());
        let mut pack_bytes: Vec<u8> =
            Vec::with_capacity(PACK_HEADER_SIZE + self.current_size + 1024);

        // Write pack magic + version
        pack_bytes.extend_from_slice(PACK_MAGIC);
        pack_bytes.push(PACK_VERSION);

        // Write each blob: [4B length LE][blob data]
        for blob in &self.buffer {
            let blob_len = blob.encrypted_data.len() as u32;
            let offset = pack_bytes.len() as u64 + 4; // offset past the length prefix

            pack_bytes.extend_from_slice(&blob_len.to_le_bytes());
            pack_bytes.extend_from_slice(&blob.encrypted_data);

            header_entries.push(PackHeaderEntry {
                obj_type: blob.obj_type,
                chunk_id: blob.chunk_id,
                offset,
                length: blob_len,
                uncompressed_size: blob.uncompressed_size,
            });
        }

        // Serialize and encrypt the header
        let header_bytes = rmp_serde::to_vec(&header_entries)?;
        let encrypted_header = pack_object(ObjectType::PackHeader, &header_bytes, crypto)?;
        let header_len = encrypted_header.len() as u32;
        pack_bytes.extend_from_slice(&encrypted_header);
        pack_bytes.extend_from_slice(&header_len.to_le_bytes());

        // Compute pack ID = BLAKE2b-256 of entire pack contents
        let pack_id = PackId::compute(&pack_bytes);

        // Collect results with refcounts from pending map
        let mut results: Vec<PackedChunkEntry> = Vec::with_capacity(header_entries.len());
        for entry in &header_entries {
            let refcount = self
                .pending
                .get(&entry.chunk_id)
                .map(|(_, rc)| *rc)
                .unwrap_or(1);
            results.push((entry.chunk_id, entry.length, entry.offset, refcount));
        }

        // Clear state
        self.buffer.clear();
        self.current_size = 0;
        self.pending.clear();
        self.first_blob_time = None;

        Ok((pack_id, pack_bytes, results))
    }

    /// Flush the buffered blobs into a pack file (seal + upload).
    /// Returns (pack_id, vec of (chunk_id, stored_size, offset, refcount)).
    pub fn flush(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<FlushedPackResult> {
        let (pack_id, pack_bytes, results) = self.seal(crypto)?;
        storage.put(&pack_id.storage_key(), &pack_bytes)?;
        Ok((pack_id, results))
    }
}

/// Read a single blob from a pack file using a range read.
pub fn read_blob_from_pack(
    storage: &dyn StorageBackend,
    pack_id: &PackId,
    offset: u64,
    length: u32,
) -> Result<Vec<u8>> {
    let data = storage
        .get_range(&pack_id.storage_key(), offset, length as u64)?
        .ok_or_else(|| VgerError::Other(format!("pack not found: {pack_id}")))?;
    Ok(data)
}

/// Read and decrypt the trailing header from a pack file.
pub fn read_pack_header(
    storage: &dyn StorageBackend,
    pack_id: &PackId,
    crypto: &dyn CryptoEngine,
) -> Result<Vec<PackHeaderEntry>> {
    let pack_data = storage
        .get(&pack_id.storage_key())?
        .ok_or_else(|| VgerError::Other(format!("pack not found: {pack_id}")))?;

    if pack_data.len() < PACK_HEADER_SIZE + 4 {
        return Err(VgerError::InvalidFormat("pack too small".into()));
    }

    // Read header length from last 4 bytes
    let len_offset = pack_data.len() - 4;
    let header_len = u32::from_le_bytes(
        pack_data[len_offset..len_offset + 4]
            .try_into()
            .map_err(|_| VgerError::InvalidFormat("invalid pack header length field".into()))?,
    ) as usize;

    if header_len + 4 > pack_data.len() - PACK_HEADER_SIZE {
        return Err(VgerError::InvalidFormat(
            "invalid pack header length".into(),
        ));
    }

    let header_start = len_offset - header_len;
    let encrypted_header = &pack_data[header_start..len_offset];

    let header_bytes = unpack_object_expect(encrypted_header, ObjectType::PackHeader, crypto)?;
    let entries: Vec<PackHeaderEntry> = rmp_serde::from_slice(&header_bytes)?;
    Ok(entries)
}

/// Forward-scan a pack file using per-blob length prefixes (for recovery).
/// Returns (offset, length) pairs for each blob.
pub fn scan_pack_blobs(storage: &dyn StorageBackend, pack_id: &PackId) -> Result<Vec<(u64, u32)>> {
    let pack_data = storage
        .get(&pack_id.storage_key())?
        .ok_or_else(|| VgerError::Other(format!("pack not found: {pack_id}")))?;

    if pack_data.len() < PACK_HEADER_SIZE {
        return Err(VgerError::InvalidFormat("pack too small".into()));
    }

    // Verify magic
    if &pack_data[..8] != PACK_MAGIC {
        return Err(VgerError::InvalidFormat("invalid pack magic".into()));
    }

    let mut pos = PACK_HEADER_SIZE;
    let mut blobs = Vec::new();

    if pack_data.len() < PACK_HEADER_SIZE + 4 {
        return Err(VgerError::InvalidFormat("pack too small for header".into()));
    }

    // Read header length from last 4 bytes to know where blobs end
    let len_offset = pack_data.len() - 4;
    let header_len = u32::from_le_bytes(
        pack_data[len_offset..len_offset + 4]
            .try_into()
            .map_err(|_| VgerError::InvalidFormat("invalid pack header length field".into()))?,
    ) as usize;

    if header_len > len_offset.saturating_sub(PACK_HEADER_SIZE) {
        return Err(VgerError::InvalidFormat(
            "invalid pack header length".into(),
        ));
    }

    let blobs_end = len_offset - header_len;

    while pos + 4 <= blobs_end {
        let blob_len = u32::from_le_bytes(
            pack_data[pos..pos + 4]
                .try_into()
                .map_err(|_| VgerError::InvalidFormat("invalid blob length field".into()))?,
        );
        let blob_offset = (pos + 4) as u64;
        if pos + 4 + blob_len as usize > blobs_end {
            break;
        }
        blobs.push((blob_offset, blob_len));
        pos += 4 + blob_len as usize;
    }

    Ok(blobs)
}

/// Compute the dynamic target pack size for data packs.
pub fn compute_data_pack_target(
    num_data_packs: usize,
    min_pack_size: u32,
    max_pack_size: u32,
) -> usize {
    let min = min_pack_size as f64;
    let max = max_pack_size as f64;
    let target = min * (num_data_packs as f64 / 100.0).sqrt();
    target.clamp(min, max) as usize
}

/// Compute the target pack size for tree packs.
pub fn compute_tree_pack_target(min_pack_size: u32) -> usize {
    let four_mib = 4 * 1024 * 1024;
    std::cmp::min(min_pack_size as usize, four_mib)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_chunk_id(byte: u8) -> ChunkId {
        ChunkId([byte; 32])
    }

    #[test]
    fn should_flush_on_size() {
        let mut w = PackWriter::new(PackType::Data, 100);
        assert!(!w.should_flush());
        w.add_blob(1, dummy_chunk_id(0), vec![0u8; 120], 120);
        assert!(w.should_flush());
    }

    #[test]
    fn should_flush_on_blob_count() {
        // Use a very large target size so size-based flush never triggers
        let mut w = PackWriter::new(PackType::Data, usize::MAX);
        for i in 0..MAX_BLOBS_PER_PACK {
            assert!(!w.should_flush(), "should not flush at {i} blobs");
            let mut id_bytes = [0u8; 32];
            id_bytes[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            w.add_blob(1, ChunkId(id_bytes), vec![1], 1);
        }
        assert!(w.should_flush());
    }

    #[test]
    fn seal_resets_first_blob_time() {
        let mut w = PackWriter::new(PackType::Data, usize::MAX);
        w.add_blob(1, dummy_chunk_id(0), vec![0u8; 10], 10);
        assert!(w.first_blob_time.is_some());

        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let _ = w.seal(&engine).unwrap();
        assert!(w.first_blob_time.is_none());
    }
}
