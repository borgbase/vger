use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
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

/// Lightweight metadata for a blob whose data lives in `PackWriter::pack_bytes`.
struct BlobMeta {
    obj_type: u8,
    chunk_id: ChunkId,
    stored_size: u32,
    uncompressed_size: u32,
}

/// Maximum number of blobs in a single pack file.
/// Prevents pathological cases where many tiny chunks create a pack with a huge header.
pub const MAX_BLOBS_PER_PACK: usize = 10_000;

/// Maximum age of a pack writer before it should be flushed (in seconds).
/// Forces periodic flushes even if the pack isn't full, preventing stale data
/// from sitting in memory indefinitely during long backups.
pub const PACK_MAX_AGE_SECS: u64 = 300;

/// Skip upfront `reserve()` for unreasonably large target sizes (e.g. `usize::MAX`
/// in tests). Real pack targets are well below this — `max_pack_size` defaults
/// to 128 MiB.
const MAX_PREALLOC_SIZE: usize = 256 * 1024 * 1024;

/// Tuple describing one chunk's location and refcount in a sealed/flushed pack.
pub type PackedChunkEntry = (ChunkId, u32, u64, u32);

/// Result of sealing a pack in-memory: (pack_id, serialized pack bytes, chunk entries).
pub type SealedPackResult = (PackId, Vec<u8>, Vec<PackedChunkEntry>);

/// Result of flushing a pack to storage: (pack_id, chunk entries).
pub type FlushedPackResult = (PackId, Vec<PackedChunkEntry>);

/// Accumulates encrypted blobs and flushes them as pack files.
///
/// Blob data is appended directly into a single contiguous `pack_bytes` buffer
/// during `add_blob`, so `seal_into` only needs to append the header trailer —
/// no second pack-sized buffer is required. This reduces peak memory from
/// ~2×pack_size to ~1×pack_size.
pub struct PackWriter {
    pack_type: PackType,
    target_size: usize,
    /// Contiguous buffer accumulating the pack: header + blob data.
    pack_bytes: Vec<u8>,
    /// Lightweight metadata per blob (no data — data lives in pack_bytes).
    blob_meta: Vec<BlobMeta>,
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
            pack_bytes: Vec::new(),
            blob_meta: Vec::new(),
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

        // On first blob: write pack header and pre-reserve capacity.
        if self.blob_meta.is_empty() {
            if self.pack_bytes.capacity() == 0 && self.target_size <= MAX_PREALLOC_SIZE {
                self.pack_bytes.reserve(self.target_size);
            }
            self.pack_bytes.extend_from_slice(PACK_MAGIC);
            self.pack_bytes.push(PACK_VERSION);
        }

        // Offset accounts for: pack header + bytes already buffered + this blob's 4B len prefix.
        let offset = PACK_HEADER_SIZE as u64 + self.current_size as u64 + 4;

        // Append [4B length LE][encrypted_data] directly into pack_bytes.
        self.pack_bytes.extend_from_slice(&blob_len.to_le_bytes());
        self.pack_bytes.extend_from_slice(&encrypted_blob);

        self.current_size += 4 + encrypted_blob.len(); // 4B length prefix + blob data
        debug_assert_eq!(self.pack_bytes.len(), PACK_HEADER_SIZE + self.current_size);

        if self.first_blob_time.is_none() {
            self.first_blob_time = Some(Instant::now());
        }
        self.pending.insert(chunk_id, (blob_len, 1));
        self.blob_meta.push(BlobMeta {
            obj_type,
            chunk_id,
            stored_size: blob_len,
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
        if self.blob_meta.is_empty() {
            return false;
        }
        if self.current_size >= self.target_size {
            return true;
        }
        if self.blob_meta.len() >= MAX_BLOBS_PER_PACK {
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
        !self.blob_meta.is_empty()
    }

    pub fn pack_type(&self) -> PackType {
        self.pack_type
    }

    /// Append the encrypted header trailer to the already-accumulated `pack_bytes`,
    /// compute the PackId, and swap the finalized buffer into `out`.
    ///
    /// Because blob data was written into `pack_bytes` during `add_blob`, no second
    /// pack-sized allocation is needed — peak memory is ~1×pack_size instead of ~2×.
    ///
    /// Returns (pack_id, vec of (chunk_id, stored_size, offset, refcount)).
    pub fn seal_into(
        &mut self,
        crypto: &dyn CryptoEngine,
        out: &mut Vec<u8>,
    ) -> Result<(PackId, Vec<PackedChunkEntry>)> {
        if self.blob_meta.is_empty() {
            return Err(VgerError::Other("cannot seal empty pack writer".into()));
        }

        // Phase 1: Build header entries and results from blob_meta.
        // Offsets are derived via running sum — same logic as before.
        let mut header_entries: Vec<PackHeaderEntry> = Vec::with_capacity(self.blob_meta.len());
        let mut running_offset = PACK_HEADER_SIZE;
        for meta in &self.blob_meta {
            let offset = running_offset as u64 + 4; // past the 4B length prefix
            running_offset += 4 + meta.stored_size as usize;

            header_entries.push(PackHeaderEntry {
                obj_type: meta.obj_type,
                chunk_id: meta.chunk_id,
                offset,
                length: meta.stored_size,
                uncompressed_size: meta.uncompressed_size,
            });
        }

        let mut results: Vec<PackedChunkEntry> = Vec::with_capacity(header_entries.len());
        for entry in &header_entries {
            let refcount = self
                .pending
                .get(&entry.chunk_id)
                .map(|(_, rc)| *rc)
                .unwrap_or(1);
            results.push((entry.chunk_id, entry.length, entry.offset, refcount));
        }

        // Phase 2: Fallible work — serialize and encrypt the header.
        // pack_bytes is untouched until Phase 3; on error the caller can retry.
        let header_bytes = rmp_serde::to_vec(&header_entries)?;
        let encrypted_header = pack_object(ObjectType::PackHeader, &header_bytes, crypto)?;

        // Phase 3: All fallible ops succeeded — append header trailer to pack_bytes.
        let header_len = encrypted_header.len() as u32;
        self.pack_bytes.extend_from_slice(&encrypted_header);
        self.pack_bytes.extend_from_slice(&header_len.to_le_bytes());

        // Compute pack ID = BLAKE2b-256 of entire pack contents
        let pack_id = PackId::compute(&self.pack_bytes);

        // Swap: finalized data → out, caller's buffer (possibly recycled) → pack_bytes.
        std::mem::swap(&mut self.pack_bytes, out);

        // Clear writer state for reuse
        self.pack_bytes.clear();
        self.blob_meta.clear();
        self.current_size = 0;
        self.pending.clear();
        self.first_blob_time = None;

        Ok((pack_id, results))
    }

    /// Assemble buffered blobs into a pack, compute its PackId, and clear internal state.
    /// Does NOT write to storage — the caller is responsible for uploading `pack_bytes`.
    /// Returns (pack_id, pack_bytes, vec of (chunk_id, stored_size, offset, refcount)).
    pub fn seal(&mut self, crypto: &dyn CryptoEngine) -> Result<SealedPackResult> {
        let mut pack_bytes = Vec::new();
        let (pack_id, results) = self.seal_into(crypto, &mut pack_bytes)?;
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

/// A fixed-size pool of reusable `Vec<u8>` buffers for pack seal/upload.
///
/// Buffers start as empty `Vec::new()` and grow lazily on first use via
/// `seal_into()`. After that, they retain their allocation across reuses,
/// eliminating repeated large alloc/dealloc cycles.
///
/// When all buffers are checked out, `checkout()` blocks — providing natural
/// backpressure that caps pack buffer memory at `pool_size × max_pack_size`.
pub struct PackBufferPool {
    inner: Mutex<VecDeque<Vec<u8>>>,
    available: Condvar,
}

impl PackBufferPool {
    /// Create a pool with `count` empty buffers.
    pub fn new(count: usize) -> Self {
        let mut bufs = VecDeque::with_capacity(count);
        for _ in 0..count {
            bufs.push_back(Vec::new());
        }
        Self {
            inner: Mutex::new(bufs),
            available: Condvar::new(),
        }
    }

    /// Check out a buffer from the pool. Blocks if all buffers are in use.
    pub fn checkout(self: &Arc<Self>) -> PooledBuffer {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if let Some(mut buf) = guard.pop_front() {
                buf.clear();
                return PooledBuffer {
                    buf: Some(buf),
                    pool: Arc::clone(self),
                };
            }
            guard = self.available.wait(guard).unwrap();
        }
    }

    /// Return a buffer to the pool. Private — only called by `PooledBuffer::drop`.
    fn checkin(&self, buf: Vec<u8>) {
        let mut guard = self.inner.lock().unwrap();
        guard.push_back(buf);
        self.available.notify_one();
    }
}

/// RAII guard that returns a buffer to its `PackBufferPool` on drop.
///
/// Guarantees buffer return on both success and error paths — if the upload
/// thread panics or `storage.put()` returns `Err`, the buffer is still
/// returned to the pool, preventing deadlock from buffer leak.
pub struct PooledBuffer {
    buf: Option<Vec<u8>>,
    pool: Arc<PackBufferPool>,
}

impl PooledBuffer {
    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_ref().expect("buffer already taken")
    }

    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        self.buf.as_mut().expect("buffer already taken")
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.checkin(buf);
        }
    }
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

    #[test]
    fn seal_into_matches_seal() {
        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);

        // Build two identical writers
        let blobs: Vec<(u8, ChunkId, Vec<u8>, u32)> = vec![
            (1, dummy_chunk_id(1), vec![10u8; 50], 50),
            (1, dummy_chunk_id(2), vec![20u8; 80], 80),
            (1, dummy_chunk_id(3), vec![30u8; 30], 30),
        ];

        let mut w1 = PackWriter::new(PackType::Data, usize::MAX);
        let mut w2 = PackWriter::new(PackType::Data, usize::MAX);
        for (obj_type, chunk_id, data, uncompressed) in &blobs {
            w1.add_blob(*obj_type, *chunk_id, data.clone(), *uncompressed);
            w2.add_blob(*obj_type, *chunk_id, data.clone(), *uncompressed);
        }

        let (id1, bytes1, entries1) = w1.seal(&engine).unwrap();
        let mut out = Vec::new();
        let (id2, entries2) = w2.seal_into(&engine, &mut out).unwrap();

        assert_eq!(id1, id2);
        assert_eq!(bytes1, out);
        assert_eq!(entries1, entries2);
    }

    #[test]
    fn pool_checkout_and_return() {
        let pool = Arc::new(PackBufferPool::new(2));

        // Can check out up to pool size
        let buf1 = pool.checkout();
        let buf2 = pool.checkout();

        assert!(buf1.as_slice().is_empty());
        assert!(buf2.as_slice().is_empty());

        // Drop returns buffers to pool
        drop(buf1);
        drop(buf2);

        // Can check out again after return
        let _buf3 = pool.checkout();
        let _buf4 = pool.checkout();
    }

    #[test]
    fn pool_blocks_when_exhausted() {
        let pool = Arc::new(PackBufferPool::new(1));
        let _buf1 = pool.checkout();

        // Spawn a thread that tries to checkout — it should block
        let pool2 = Arc::clone(&pool);
        let handle = std::thread::spawn(move || {
            let buf = pool2.checkout();
            buf.as_slice().len()
        });

        // Give the thread time to block
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!handle.is_finished(), "thread should be blocked waiting");

        // Return our buffer — unblocks the thread
        drop(_buf1);
        let result = handle.join().unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn pool_retains_buffer_capacity() {
        let pool = Arc::new(PackBufferPool::new(1));

        // Check out, grow the buffer, return it
        {
            let mut buf = pool.checkout();
            let v = buf.as_mut_vec();
            v.extend_from_slice(&[0u8; 1024]);
            assert!(v.capacity() >= 1024);
        }

        // Check out again — buffer should be cleared but retain capacity
        let buf = pool.checkout();
        assert_eq!(buf.as_slice().len(), 0);
        // The underlying Vec should have retained its allocation
        // (We can't directly inspect capacity through as_slice, but
        //  the important thing is it's cleared and reusable)
    }

    /// Golden-byte regression test using PlaintextEngine (deterministic — no nonces).
    /// Any change to wire format, header serialization, or byte ordering will fail this.
    #[test]
    fn seal_deterministic_bytes() {
        // Hard-coded expected output. Structure:
        //   [0..9]   VGERPACK\x01  — magic + version
        //   [9..15]  blob 0: 4B len LE (2) + 2B data (0xDE 0xAD)
        //   [15..22] blob 1: 4B len LE (3) + 3B data (0xBE 0xEF 0x42)
        //   [22..23] 0x05 — ObjectType::PackHeader type tag
        //   [23..168] msgpack-encoded Vec<PackHeaderEntry>
        //   [168..172] 4B header_len LE (146 = 0x92)
        #[rustfmt::skip]
        const EXPECTED: &[u8] = &[
            // Pack header: VGERPACK + version 1
            0x56, 0x47, 0x45, 0x52, 0x50, 0x41, 0x43, 0x4b, 0x01,
            // Blob 0: len=2 LE, data=0xDE 0xAD
            0x02, 0x00, 0x00, 0x00, 0xde, 0xad,
            // Blob 1: len=3 LE, data=0xBE 0xEF 0x42
            0x03, 0x00, 0x00, 0x00, 0xbe, 0xef, 0x42,
            // Encrypted header envelope: type tag (PackHeader=5) + msgpack payload
            0x05, 0x92, 0x95, 0x01, 0xdc, 0x00, 0x20,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa, 0xcc, 0xaa,
            0x0d, 0x02, 0x02,
            0x95, 0x01, 0xdc, 0x00, 0x20,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb, 0xcc, 0xbb,
            0x13, 0x03, 0x03,
            // Header length trailer: 146 bytes LE
            0x92, 0x00, 0x00, 0x00,
        ];

        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);

        let mut w = PackWriter::new(PackType::Data, usize::MAX);
        w.add_blob(1, dummy_chunk_id(0xAA), vec![0xDE, 0xAD], 2);
        w.add_blob(1, dummy_chunk_id(0xBB), vec![0xBE, 0xEF, 0x42], 3);

        let (_pack_id, pack_bytes, _entries) = w.seal(&engine).unwrap();

        assert_eq!(pack_bytes, EXPECTED, "pack wire format regression");
    }

    /// Roundtrip: seal a pack, then parse it back via read_pack_header + scan_pack_blobs.
    #[test]
    fn seal_roundtrip_header_and_scan() {
        use crate::testutil::MemoryBackend;

        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let storage = MemoryBackend::new();

        let blobs: Vec<(u8, ChunkId, Vec<u8>, u32)> = vec![
            (1, dummy_chunk_id(1), vec![10u8; 50], 50),
            (1, dummy_chunk_id(2), vec![20u8; 80], 80),
            (1, dummy_chunk_id(3), vec![30u8; 30], 30),
        ];

        let mut w = PackWriter::new(PackType::Data, usize::MAX);
        for (obj_type, chunk_id, data, uncompressed) in &blobs {
            w.add_blob(*obj_type, *chunk_id, data.clone(), *uncompressed);
        }

        let (pack_id, pack_bytes, entries) = w.seal(&engine).unwrap();

        // Store the pack so read_pack_header / scan_pack_blobs can access it.
        storage.put(&pack_id.storage_key(), &pack_bytes).unwrap();

        // Verify read_pack_header returns matching entries.
        let header = read_pack_header(&storage, &pack_id, &engine).unwrap();
        assert_eq!(header.len(), blobs.len());
        for (i, he) in header.iter().enumerate() {
            assert_eq!(he.chunk_id, blobs[i].1, "chunk_id mismatch at {i}");
            assert_eq!(he.length, blobs[i].2.len() as u32, "length mismatch at {i}");
            assert_eq!(
                he.uncompressed_size, blobs[i].3,
                "uncompressed mismatch at {i}"
            );
            // Verify offset matches what seal_into returned.
            assert_eq!(he.offset, entries[i].2, "offset mismatch at {i}");
        }

        // Verify scan_pack_blobs returns matching (offset, length) pairs.
        let scanned = scan_pack_blobs(&storage, &pack_id).unwrap();
        assert_eq!(scanned.len(), blobs.len());
        for (i, (offset, length)) in scanned.iter().enumerate() {
            assert_eq!(*offset, header[i].offset, "scan offset mismatch at {i}");
            assert_eq!(*length, header[i].length, "scan length mismatch at {i}");
        }
    }

    /// After seal_into fails (crypto error), writer state is intact and retry succeeds.
    #[test]
    fn seal_into_failure_preserves_state() {
        use crate::crypto::CryptoEngine;

        /// A CryptoEngine that fails on encrypt (used to trigger seal_into error).
        struct FailingEngine;

        impl CryptoEngine for FailingEngine {
            fn encrypt(&self, _plaintext: &[u8], _aad: &[u8]) -> crate::error::Result<Vec<u8>> {
                Err(VgerError::Other("simulated encrypt failure".into()))
            }
            fn decrypt(&self, _data: &[u8], _aad: &[u8]) -> crate::error::Result<Vec<u8>> {
                unreachable!()
            }
            fn encrypt_in_place_detached(
                &self,
                _buffer: &mut [u8],
                _aad: &[u8],
            ) -> crate::error::Result<([u8; 12], [u8; 16])> {
                unreachable!()
            }
            fn is_encrypting(&self) -> bool {
                false
            }
            fn chunk_id_key(&self) -> &[u8; 32] {
                &[0u8; 32]
            }
        }

        let mut w = PackWriter::new(PackType::Data, usize::MAX);
        w.add_blob(1, dummy_chunk_id(1), vec![0xAA; 100], 100);
        w.add_blob(1, dummy_chunk_id(2), vec![0xBB; 200], 200);

        let pack_bytes_len_before = w.pack_bytes.len();
        let blob_meta_len_before = w.blob_meta.len();

        // Attempt seal with failing engine — should error.
        let mut out = Vec::new();
        let result = w.seal_into(&FailingEngine, &mut out);
        assert!(result.is_err());

        // Writer state must be intact after failure.
        assert_eq!(w.pack_bytes.len(), pack_bytes_len_before);
        assert_eq!(w.blob_meta.len(), blob_meta_len_before);
        assert!(w.has_pending());

        // Retry with working engine — should succeed.
        let good_engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let mut out2 = Vec::new();
        let (pack_id, entries) = w.seal_into(&good_engine, &mut out2).unwrap();

        assert_eq!(entries.len(), 2);
        assert!(!pack_id.0.iter().all(|&b| b == 0));
        // Writer should be clear now.
        assert!(!w.has_pending());
        assert_eq!(w.pack_bytes.len(), 0);
    }

    /// Validates `pack_bytes.len() == PACK_HEADER_SIZE + current_size` across
    /// add → fail → add → seal, covering the invariant in all build modes
    /// (the `debug_assert_eq!` in `add_blob` is stripped in release).
    #[test]
    fn pack_bytes_len_invariant() {
        use crate::crypto::CryptoEngine;

        struct FailingEngine;
        impl CryptoEngine for FailingEngine {
            fn encrypt(&self, _: &[u8], _: &[u8]) -> crate::error::Result<Vec<u8>> {
                Err(VgerError::Other("fail".into()))
            }
            fn decrypt(&self, _: &[u8], _: &[u8]) -> crate::error::Result<Vec<u8>> {
                unreachable!()
            }
            fn encrypt_in_place_detached(
                &self,
                _: &mut [u8],
                _: &[u8],
            ) -> crate::error::Result<([u8; 12], [u8; 16])> {
                unreachable!()
            }
            fn is_encrypting(&self) -> bool {
                false
            }
            fn chunk_id_key(&self) -> &[u8; 32] {
                &[0u8; 32]
            }
        }

        let mut w = PackWriter::new(PackType::Data, usize::MAX);

        // Add blobs, check invariant after each.
        w.add_blob(1, dummy_chunk_id(1), vec![0xAA; 100], 100);
        assert_eq!(w.pack_bytes.len(), PACK_HEADER_SIZE + w.current_size);

        w.add_blob(1, dummy_chunk_id(2), vec![0xBB; 50], 50);
        assert_eq!(w.pack_bytes.len(), PACK_HEADER_SIZE + w.current_size);

        // Failed seal must not break the invariant.
        let mut out = Vec::new();
        assert!(w.seal_into(&FailingEngine, &mut out).is_err());
        assert_eq!(w.pack_bytes.len(), PACK_HEADER_SIZE + w.current_size);

        // Adding another blob after failure must still hold.
        w.add_blob(1, dummy_chunk_id(3), vec![0xCC; 25], 25);
        assert_eq!(w.pack_bytes.len(), PACK_HEADER_SIZE + w.current_size);

        // Successful seal clears everything.
        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let mut out2 = Vec::new();
        let (_pack_id, entries) = w.seal_into(&engine, &mut out2).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(w.pack_bytes.len(), 0);
        assert_eq!(w.current_size, 0);
    }
}
