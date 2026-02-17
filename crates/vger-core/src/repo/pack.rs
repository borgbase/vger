use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::CHUNK_MAX_SIZE_HARD_CAP;
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

/// Lightweight metadata for a blob whose data lives in the pack buffer.
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

/// Default max blob overhead for the backup path: bounded by the chunk cap,
/// plus encryption envelope (1-byte type tag + 12-byte nonce + 16-byte GCM tag +
/// 1-byte compression tag = 30 bytes, rounded up to 1024 for margin), plus
/// 4-byte length prefix.
pub const DEFAULT_MAX_BLOB_OVERHEAD: usize = CHUNK_MAX_SIZE_HARD_CAP as usize + 1024 + 4;

/// Headroom for the encrypted pack header trailer.
/// Conservative estimate: 10K entries × ~73 bytes each + encryption envelope + 4-byte trailer.
const HEADER_HEADROOM: usize = 1024 * 1024; // 1 MiB

/// Tuple describing one chunk's location and refcount in a sealed/flushed pack.
pub type PackedChunkEntry = (ChunkId, u32, u64, u32);

/// Result of flushing a pack to storage: (pack_id, chunk entries).
pub type FlushedPackResult = (PackId, Vec<PackedChunkEntry>);

/// Mmap-backed buffer for data packs.
struct MmapBuffer {
    mmap: MmapMut,
    _file: File, // keep fd alive (anonymous temp file)
    write_pos: usize,
    capacity: usize,
}

/// Buffer backing a pack writer — mmap'd temp file for data packs, heap Vec for tree packs.
enum PackBuffer {
    Mmap(MmapBuffer),
    Memory(Vec<u8>),
}

/// A sealed pack ready for upload. Destructurable so callers can split ownership
/// between the main thread (pack_id + entries) and upload thread (data).
pub struct SealedPack {
    pub pack_id: PackId,
    pub entries: Vec<PackedChunkEntry>,
    pub data: SealedData,
}

/// Sealed pack data — either mmap-backed or heap-backed.
pub enum SealedData {
    Mmap {
        mmap: MmapMut,
        _file: File,
        len: usize,
    },
    Memory(Vec<u8>),
}

// Compile-time assertion: SealedData must be Send for thread::spawn upload.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<SealedData>();
    }
};

impl SealedData {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            SealedData::Mmap { mmap, len, .. } => &mmap[..*len],
            SealedData::Memory(v) => v.as_slice(),
        }
    }
}

/// Accumulates encrypted blobs and flushes them as pack files.
///
/// Data packs use mmap'd temporary files: the OS kernel manages which pages
/// stay resident vs. get paged out, giving graceful degradation under memory
/// pressure instead of fixed heap allocation.
///
/// Tree packs stay as `Vec<u8>` — they're capped at ~4 MiB and accessed
/// frequently during serialization, so mmap overhead isn't worthwhile.
pub struct PackWriter {
    pack_type: PackType,
    target_size: usize,
    max_blob_overhead: usize,
    /// Data packs: mmap'd temp file. Tree packs: Vec<u8>. None until first blob.
    buffer: Option<PackBuffer>,
    /// Lightweight metadata per blob (no data — data lives in the buffer).
    blob_meta: Vec<BlobMeta>,
    current_size: usize,
    /// chunk_id -> (stored_size, refcount) for pending (not-yet-flushed) blobs.
    pending: HashMap<ChunkId, (u32, u32)>,
    /// When the first blob was added to the current buffer.
    first_blob_time: Option<Instant>,
    /// Directory for mmap temp files. None = system temp.
    temp_dir: Option<PathBuf>,
}

impl PackWriter {
    /// Create a new pack writer with explicit max_blob_overhead.
    ///
    /// `max_blob_overhead` controls the mmap allocation size:
    ///   `alloc_size = target_size + max_blob_overhead + HEADER_HEADROOM`
    ///
    /// For backup, use `DEFAULT_MAX_BLOB_OVERHEAD`. For compact, pre-scan
    /// existing blobs to find the maximum per-blob append size.
    pub fn new(
        pack_type: PackType,
        target_size: usize,
        max_blob_overhead: usize,
        temp_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            pack_type,
            target_size,
            max_blob_overhead,
            buffer: None,
            blob_meta: Vec::new(),
            current_size: 0,
            pending: HashMap::new(),
            first_blob_time: None,
            temp_dir,
        }
    }

    /// Create a new pack writer using `DEFAULT_MAX_BLOB_OVERHEAD` (for backup).
    pub fn new_default(pack_type: PackType, target_size: usize, temp_dir: Option<PathBuf>) -> Self {
        Self::new(pack_type, target_size, DEFAULT_MAX_BLOB_OVERHEAD, temp_dir)
    }

    /// Compute the mmap allocation size for data packs.
    fn alloc_size(&self) -> usize {
        self.target_size
            .saturating_add(self.max_blob_overhead)
            .saturating_add(HEADER_HEADROOM)
    }

    /// Initialize the buffer on first blob.
    fn init_buffer(&mut self) -> Result<()> {
        let alloc_size = self.alloc_size();

        match self.pack_type {
            PackType::Data => {
                // Try mmap'd temp file; fall back to Vec on failure or low disk space.
                match self.try_create_mmap(alloc_size) {
                    Ok(buf) => self.buffer = Some(PackBuffer::Mmap(buf)),
                    Err(e) => {
                        warn!("mmap pack buffer failed ({e}), falling back to Vec");
                        let mut v = Vec::with_capacity(self.target_size.min(512 * 1024 * 1024));
                        v.extend_from_slice(PACK_MAGIC);
                        v.push(PACK_VERSION);
                        self.buffer = Some(PackBuffer::Memory(v));
                        return Ok(());
                    }
                }
            }
            PackType::Tree => {
                let mut v = Vec::with_capacity(self.target_size.min(512 * 1024 * 1024));
                v.extend_from_slice(PACK_MAGIC);
                v.push(PACK_VERSION);
                self.buffer = Some(PackBuffer::Memory(v));
            }
        }

        // Write pack magic + version into mmap buffer if we used mmap
        if let Some(PackBuffer::Mmap(ref mut mb)) = self.buffer {
            mb.mmap[..8].copy_from_slice(PACK_MAGIC);
            mb.mmap[8] = PACK_VERSION;
            mb.write_pos = PACK_HEADER_SIZE;
        }

        Ok(())
    }

    /// Try to create an mmap'd temp file of the given size.
    fn try_create_mmap(&self, alloc_size: usize) -> Result<MmapBuffer> {
        // Best-effort pre-flight check: verify the target temp dir has enough
        // free space. This is racy (other processes can consume space between
        // check and use) and only checks per-pack (not cumulative across
        // concurrent mmaps).
        let required = (alloc_size as u64).saturating_mul(2);

        let file = if let Some(ref dir) = self.temp_dir {
            let dir_ok = match temp_dir_free_space(Some(dir)) {
                Some(free) if free < required => {
                    warn!(
                        "{} has {free} bytes free (need {required}), using system temp",
                        dir.display()
                    );
                    false
                }
                _ => true, // unknown or sufficient
            };
            if dir_ok {
                std::fs::create_dir_all(dir)
                    .and_then(|()| tempfile::tempfile_in(dir))
                    .or_else(|e| {
                        warn!(
                            "temp file in {} failed ({e}), using system temp",
                            dir.display()
                        );
                        tempfile::tempfile()
                    })
            } else {
                tempfile::tempfile()
            }
        } else {
            // No configured dir — check system temp space.
            if let Some(free) = temp_dir_free_space(None) {
                if free < required {
                    return Err(VgerError::Other(format!(
                        "temp dir has {free} bytes free, need at least {required}"
                    )));
                }
            }
            tempfile::tempfile()
        }
        .map_err(|e| VgerError::Other(format!("failed to create temp file for pack mmap: {e}")))?;
        file.set_len(alloc_size as u64)
            .map_err(|e| VgerError::Other(format!("failed to set temp file length: {e}")))?;

        // SAFETY: The file is an anonymous temp file exclusively owned by this
        // process. No other process or thread accesses it. The mmap region is
        // valid for the lifetime of the file.
        let mmap = unsafe { MmapMut::map_mut(&file) }
            .map_err(|e| VgerError::Other(format!("failed to mmap temp file: {e}")))?;

        // Hint to the kernel: we write sequentially.
        #[cfg(unix)]
        {
            let _ = mmap.advise(memmap2::Advice::Sequential);
        }

        Ok(MmapBuffer {
            capacity: mmap.len(),
            mmap,
            _file: file,
            write_pos: 0,
        })
    }

    /// Add an encrypted blob to the pack buffer. Returns the offset within the pack
    /// where the blob data starts (after the 4-byte length prefix).
    pub fn add_blob(
        &mut self,
        obj_type: u8,
        chunk_id: ChunkId,
        encrypted_blob: Vec<u8>,
        uncompressed_size: u32,
    ) -> Result<u64> {
        let blob_len = encrypted_blob.len() as u32;

        // On first blob: initialize the buffer.
        if self.blob_meta.is_empty() {
            self.init_buffer()?;
        }

        // Offset accounts for: pack header + bytes already buffered + this blob's 4B len prefix.
        let offset = PACK_HEADER_SIZE as u64 + self.current_size as u64 + 4;

        // Append [4B length LE][encrypted_data] into the buffer.
        match self.buffer.as_mut().expect("buffer initialized above") {
            PackBuffer::Mmap(mb) => {
                let needed = 4 + encrypted_blob.len();
                if mb.write_pos + needed > mb.capacity {
                    return Err(VgerError::Other(format!(
                        "blob would overflow mmap buffer: write_pos={}, needed={needed}, capacity={}",
                        mb.write_pos, mb.capacity
                    )));
                }
                let pos = mb.write_pos;
                mb.mmap[pos..pos + 4].copy_from_slice(&blob_len.to_le_bytes());
                mb.mmap[pos + 4..pos + 4 + encrypted_blob.len()].copy_from_slice(&encrypted_blob);
                mb.write_pos += needed;
            }
            PackBuffer::Memory(v) => {
                v.extend_from_slice(&blob_len.to_le_bytes());
                v.extend_from_slice(&encrypted_blob);
            }
        }

        self.current_size += 4 + encrypted_blob.len();

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

        Ok(offset)
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

    /// The target pack size in bytes.
    pub fn target_size(&self) -> usize {
        self.target_size
    }

    /// Seal the pack: append encrypted header trailer, compute PackId, return
    /// a `SealedPack` that can be destructured for upload.
    ///
    /// The only fallible step is header encryption (`pack_object`). On crypto
    /// failure the buffer is untouched (header not yet written), metadata intact,
    /// retry safe.
    pub fn seal(&mut self, crypto: &dyn CryptoEngine) -> Result<SealedPack> {
        if self.blob_meta.is_empty() {
            return Err(VgerError::Other("cannot seal empty pack writer".into()));
        }

        // Phase 1: Build header entries and results from blob_meta.
        let mut header_entries: Vec<PackHeaderEntry> = Vec::with_capacity(self.blob_meta.len());
        let mut running_offset = PACK_HEADER_SIZE;
        for meta in &self.blob_meta {
            let offset = running_offset as u64 + 4;
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
        // Buffer is untouched until Phase 3; on error the caller can retry.
        let header_bytes = rmp_serde::to_vec(&header_entries)?;
        let encrypted_header = pack_object(ObjectType::PackHeader, &header_bytes, crypto)?;

        // Phase 3: All fallible ops succeeded — append header trailer to buffer.
        let header_len = encrypted_header.len() as u32;

        let sealed_data = match self.buffer.take().expect("buffer was initialized") {
            PackBuffer::Mmap(mut mb) => {
                let needed = encrypted_header.len() + 4;
                let pos = mb.write_pos;
                mb.mmap[pos..pos + encrypted_header.len()].copy_from_slice(&encrypted_header);
                mb.mmap[pos + encrypted_header.len()..pos + needed]
                    .copy_from_slice(&header_len.to_le_bytes());
                mb.write_pos += needed;
                let len = mb.write_pos;
                SealedData::Mmap {
                    mmap: mb.mmap,
                    _file: mb._file,
                    len,
                }
            }
            PackBuffer::Memory(mut v) => {
                v.extend_from_slice(&encrypted_header);
                v.extend_from_slice(&header_len.to_le_bytes());
                SealedData::Memory(v)
            }
        };

        let pack_id = PackId::compute(sealed_data.as_slice());

        // Clear writer state for reuse
        self.blob_meta.clear();
        self.current_size = 0;
        self.pending.clear();
        self.first_blob_time = None;

        Ok(SealedPack {
            pack_id,
            entries: results,
            data: sealed_data,
        })
    }

    /// Flush the buffered blobs into a pack file (seal + upload).
    /// Returns (pack_id, vec of (chunk_id, stored_size, offset, refcount)).
    pub fn flush(
        &mut self,
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
    ) -> Result<FlushedPackResult> {
        let SealedPack {
            pack_id,
            entries,
            data,
        } = self.seal(crypto)?;
        storage.put(&pack_id.storage_key(), data.as_slice())?;
        Ok((pack_id, entries))
    }
}

/// Best-effort check: return free bytes in the given directory (or system temp), or None if
/// the check is unsupported or fails.
#[cfg(unix)]
fn temp_dir_free_space(dir: Option<&std::path::Path>) -> Option<u64> {
    use std::ffi::CString;

    let tmp = match dir {
        Some(d) => d.to_path_buf(),
        None => std::env::temp_dir(),
    };
    let c_path = CString::new(tmp.as_os_str().as_encoded_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret == 0 {
        #[allow(clippy::unnecessary_cast)] // f_bavail/f_frsize types vary by platform
        Some(stat.f_bavail as u64 * stat.f_frsize as u64)
    } else {
        None
    }
}

#[cfg(not(unix))]
fn temp_dir_free_space(_dir: Option<&std::path::Path>) -> Option<u64> {
    None
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
        let mut w = PackWriter::new_default(PackType::Data, 100, None);
        assert!(!w.should_flush());
        w.add_blob(1, dummy_chunk_id(0), vec![0u8; 120], 120)
            .unwrap();
        assert!(w.should_flush());
    }

    #[test]
    fn should_flush_on_blob_count() {
        // Use a very large target size so size-based flush never triggers
        let mut w = PackWriter::new_default(PackType::Data, usize::MAX, None);
        for i in 0..MAX_BLOBS_PER_PACK {
            assert!(!w.should_flush(), "should not flush at {i} blobs");
            let mut id_bytes = [0u8; 32];
            id_bytes[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            w.add_blob(1, ChunkId(id_bytes), vec![1], 1).unwrap();
        }
        assert!(w.should_flush());
    }

    #[test]
    fn seal_resets_first_blob_time() {
        let mut w = PackWriter::new_default(PackType::Data, usize::MAX, None);
        w.add_blob(1, dummy_chunk_id(0), vec![0u8; 10], 10).unwrap();
        assert!(w.first_blob_time.is_some());

        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let _ = w.seal(&engine).unwrap();
        assert!(w.first_blob_time.is_none());
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

        let mut w = PackWriter::new_default(PackType::Data, usize::MAX, None);
        w.add_blob(1, dummy_chunk_id(0xAA), vec![0xDE, 0xAD], 2)
            .unwrap();
        w.add_blob(1, dummy_chunk_id(0xBB), vec![0xBE, 0xEF, 0x42], 3)
            .unwrap();

        let sealed = w.seal(&engine).unwrap();

        assert_eq!(
            sealed.data.as_slice(),
            EXPECTED,
            "pack wire format regression"
        );
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

        let mut w = PackWriter::new_default(PackType::Data, usize::MAX, None);
        for (obj_type, chunk_id, data, uncompressed) in &blobs {
            w.add_blob(*obj_type, *chunk_id, data.clone(), *uncompressed)
                .unwrap();
        }

        let sealed = w.seal(&engine).unwrap();

        // Store the pack so read_pack_header / scan_pack_blobs can access it.
        storage
            .put(&sealed.pack_id.storage_key(), sealed.data.as_slice())
            .unwrap();

        // Verify read_pack_header returns matching entries.
        let header = read_pack_header(&storage, &sealed.pack_id, &engine).unwrap();
        assert_eq!(header.len(), blobs.len());
        for (i, he) in header.iter().enumerate() {
            assert_eq!(he.chunk_id, blobs[i].1, "chunk_id mismatch at {i}");
            assert_eq!(he.length, blobs[i].2.len() as u32, "length mismatch at {i}");
            assert_eq!(
                he.uncompressed_size, blobs[i].3,
                "uncompressed mismatch at {i}"
            );
            // Verify offset matches what seal returned.
            assert_eq!(he.offset, sealed.entries[i].2, "offset mismatch at {i}");
        }

        // Verify scan_pack_blobs returns matching (offset, length) pairs.
        let scanned = scan_pack_blobs(&storage, &sealed.pack_id).unwrap();
        assert_eq!(scanned.len(), blobs.len());
        for (i, (offset, length)) in scanned.iter().enumerate() {
            assert_eq!(*offset, header[i].offset, "scan offset mismatch at {i}");
            assert_eq!(*length, header[i].length, "scan length mismatch at {i}");
        }
    }

    /// After seal fails (crypto error), writer state is intact and retry succeeds.
    #[test]
    fn seal_failure_preserves_state() {
        use crate::crypto::CryptoEngine;

        /// A CryptoEngine that fails on encrypt (used to trigger seal error).
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

        let mut w = PackWriter::new_default(PackType::Data, usize::MAX, None);
        w.add_blob(1, dummy_chunk_id(1), vec![0xAA; 100], 100)
            .unwrap();
        w.add_blob(1, dummy_chunk_id(2), vec![0xBB; 200], 200)
            .unwrap();

        let blob_meta_len_before = w.blob_meta.len();

        // Attempt seal with failing engine — should error.
        let result = w.seal(&FailingEngine);
        assert!(result.is_err());

        // Writer state must be intact after failure.
        assert_eq!(w.blob_meta.len(), blob_meta_len_before);
        assert!(w.has_pending());
        assert!(w.buffer.is_some());

        // Retry with working engine — should succeed.
        let good_engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let sealed = w.seal(&good_engine).unwrap();

        assert_eq!(sealed.entries.len(), 2);
        assert!(!sealed.pack_id.0.iter().all(|&b| b == 0));
        // Writer should be clear now.
        assert!(!w.has_pending());
        assert!(w.buffer.is_none());
    }

    /// Data packs use mmap, tree packs use Vec.
    #[test]
    fn data_packs_use_mmap_tree_packs_use_vec() {
        let mut data_w = PackWriter::new_default(PackType::Data, 1024, None);
        data_w
            .add_blob(1, dummy_chunk_id(0), vec![0u8; 10], 10)
            .unwrap();
        assert!(
            matches!(data_w.buffer, Some(PackBuffer::Mmap(_))),
            "data pack should use mmap buffer"
        );

        let mut tree_w = PackWriter::new_default(PackType::Tree, 1024, None);
        tree_w
            .add_blob(1, dummy_chunk_id(0), vec![0u8; 10], 10)
            .unwrap();
        assert!(
            matches!(tree_w.buffer, Some(PackBuffer::Memory(_))),
            "tree pack should use Vec buffer"
        );
    }

    /// Validates `current_size` tracks correctly across add → fail → add → seal.
    #[test]
    fn current_size_invariant() {
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

        let mut w = PackWriter::new_default(PackType::Tree, usize::MAX, None);

        // Add blobs, check invariant after each (Vec path).
        w.add_blob(1, dummy_chunk_id(1), vec![0xAA; 100], 100)
            .unwrap();
        if let Some(PackBuffer::Memory(ref v)) = w.buffer {
            assert_eq!(v.len(), PACK_HEADER_SIZE + w.current_size);
        }

        w.add_blob(1, dummy_chunk_id(2), vec![0xBB; 50], 50)
            .unwrap();
        if let Some(PackBuffer::Memory(ref v)) = w.buffer {
            assert_eq!(v.len(), PACK_HEADER_SIZE + w.current_size);
        }

        // Failed seal must not break the invariant.
        assert!(w.seal(&FailingEngine).is_err());
        if let Some(PackBuffer::Memory(ref v)) = w.buffer {
            assert_eq!(v.len(), PACK_HEADER_SIZE + w.current_size);
        }

        // Adding another blob after failure must still hold.
        w.add_blob(1, dummy_chunk_id(3), vec![0xCC; 25], 25)
            .unwrap();
        if let Some(PackBuffer::Memory(ref v)) = w.buffer {
            assert_eq!(v.len(), PACK_HEADER_SIZE + w.current_size);
        }

        // Successful seal clears everything.
        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let sealed = w.seal(&engine).unwrap();
        assert_eq!(sealed.entries.len(), 3);
        assert!(w.buffer.is_none());
        assert_eq!(w.current_size, 0);
    }

    /// Bounds-check error when a blob would overflow the mmap buffer.
    #[test]
    fn mmap_overflow_returns_error() {
        // Small target + overhead so the mmap is alloc_size = 64 + 64 + 1 MiB ≈ 1 MiB.
        let mut w = PackWriter::new(PackType::Data, 64, 64, None);
        // First small blob succeeds.
        w.add_blob(1, dummy_chunk_id(0), vec![0u8; 10], 10).unwrap();
        // A blob larger than the remaining mmap capacity (~1 MiB) should fail.
        let big = vec![0u8; 2 * 1024 * 1024];
        let result = w.add_blob(1, dummy_chunk_id(1), big, 2 * 1024 * 1024);
        assert!(result.is_err(), "should fail when blob overflows mmap");
    }

    /// Backup-path writer with small data still seals correctly (mmap path).
    #[test]
    fn small_backup_on_mmap() {
        let mut w = PackWriter::new_default(PackType::Data, 256 * 1024, None);

        w.add_blob(1, dummy_chunk_id(0), vec![0xAB; 1024], 1024)
            .unwrap();

        let engine = crate::crypto::PlaintextEngine::new(&[0u8; 32]);
        let sealed = w.seal(&engine).unwrap();
        assert_eq!(sealed.entries.len(), 1);
        // Output should be small: header(9) + len(4) + blob(1024) + trailer < 2 KiB
        assert!(
            sealed.data.as_slice().len() < 2048,
            "sealed output unexpectedly large: {}",
            sealed.data.as_slice().len()
        );
    }

    #[test]
    fn data_pack_falls_back_to_system_temp_when_configured_dir_unusable() {
        let temp = tempfile::tempdir().unwrap();
        let blocker = temp.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let unusable_dir = blocker.join("subdir");

        let mut w = PackWriter::new_default(PackType::Data, 1024, Some(unusable_dir));
        w.add_blob(1, dummy_chunk_id(1), vec![0u8; 16], 16).unwrap();

        assert!(
            matches!(w.buffer, Some(PackBuffer::Mmap(_))),
            "expected mmap buffer after falling back to system temp"
        );
    }
}
