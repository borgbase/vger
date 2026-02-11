use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::crypto::chunk_id::ChunkId;
use crate::crypto::pack_id::PackId;
use crate::crypto::CryptoEngine;
use crate::error::{VgerError, Result};
use crate::storage::StorageBackend;

use super::format::{pack_object, unpack_object, ObjectType};

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

/// Accumulates encrypted blobs and flushes them as pack files.
pub struct PackWriter {
    pack_type: PackType,
    target_size: usize,
    buffer: Vec<BufferedBlob>,
    current_size: usize,
    /// chunk_id -> (stored_size, refcount) for pending (not-yet-flushed) blobs.
    pending: HashMap<ChunkId, (u32, u32)>,
}

impl PackWriter {
    pub fn new(pack_type: PackType, target_size: usize) -> Self {
        Self {
            pack_type,
            target_size,
            buffer: Vec::new(),
            current_size: 0,
            pending: HashMap::new(),
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
        // Offset accounts for: pack header (9B) + all previous blobs (each: 4B len + data)
        let offset = if self.buffer.is_empty() {
            PACK_HEADER_SIZE as u64 + 4 // 4B length prefix for this blob
        } else {
            PACK_HEADER_SIZE as u64
                + self.buffer.iter().map(|b| 4 + b.encrypted_data.len() as u64).sum::<u64>()
                + 4 // 4B length prefix for this blob
        };

        self.current_size += 4 + encrypted_blob.len(); // 4B length prefix + blob data
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

    /// Whether the current buffer has reached the target pack size.
    pub fn should_flush(&self) -> bool {
        self.current_size >= self.target_size
    }

    /// Whether there are any pending blobs.
    pub fn has_pending(&self) -> bool {
        !self.buffer.is_empty()
    }

    pub fn pack_type(&self) -> PackType {
        self.pack_type
    }

    /// Flush the buffered blobs into a pack file.
    /// Returns (pack_id, vec of (chunk_id, stored_size, offset, refcount)).
    pub fn flush(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<(PackId, Vec<(ChunkId, u32, u64, u32)>)> {
        if self.buffer.is_empty() {
            return Err(VgerError::Other("cannot flush empty pack writer".into()));
        }

        // Build header entries
        let mut header_entries: Vec<PackHeaderEntry> = Vec::with_capacity(self.buffer.len());
        let mut pack_bytes: Vec<u8> = Vec::with_capacity(PACK_HEADER_SIZE + self.current_size + 1024);

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

        // Write to storage
        storage.put(&pack_id.storage_key(), &pack_bytes)?;

        // Collect results with refcounts from pending map
        let mut results: Vec<(ChunkId, u32, u64, u32)> = Vec::with_capacity(header_entries.len());
        for entry in &header_entries {
            let refcount = self.pending.get(&entry.chunk_id).map(|(_, rc)| *rc).unwrap_or(1);
            results.push((entry.chunk_id, entry.length, entry.offset, refcount));
        }

        // Clear state
        self.buffer.clear();
        self.current_size = 0;
        self.pending.clear();

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
    let header_len =
        u32::from_le_bytes(pack_data[len_offset..len_offset + 4].try_into().unwrap()) as usize;

    if header_len + 4 > pack_data.len() - PACK_HEADER_SIZE {
        return Err(VgerError::InvalidFormat("invalid pack header length".into()));
    }

    let header_start = len_offset - header_len;
    let encrypted_header = &pack_data[header_start..len_offset];

    let (_obj_type, header_bytes) = unpack_object(encrypted_header, crypto)?;
    let entries: Vec<PackHeaderEntry> = rmp_serde::from_slice(&header_bytes)?;
    Ok(entries)
}

/// Forward-scan a pack file using per-blob length prefixes (for recovery).
/// Returns (offset, length) pairs for each blob.
pub fn scan_pack_blobs(
    storage: &dyn StorageBackend,
    pack_id: &PackId,
) -> Result<Vec<(u64, u32)>> {
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

    // Read header length from last 4 bytes to know where blobs end
    let len_offset = pack_data.len() - 4;
    let header_len =
        u32::from_le_bytes(pack_data[len_offset..len_offset + 4].try_into().unwrap()) as usize;
    let blobs_end = len_offset - header_len;

    while pos + 4 <= blobs_end {
        let blob_len =
            u32::from_le_bytes(pack_data[pos..pos + 4].try_into().unwrap());
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
pub fn compute_data_pack_target(num_data_packs: usize, min_pack_size: u32, max_pack_size: u32) -> usize {
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
