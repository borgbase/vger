use crate::compress::Compression;
use crate::crypto::chunk_id::ChunkId;
use crate::crypto::CryptoEngine;
use crate::error::Result;
use crate::repo::format::{pack_object, ObjectType};

pub(crate) struct PreparedChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) uncompressed_size: u32,
    pub(crate) packed: Vec<u8>,
}

/// A chunk that was only hashed (xor filter said "probably exists").
pub(crate) struct HashedChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) data: Vec<u8>,
}

/// Result of worker-side classify: either fully transformed or hash-only.
pub(crate) enum WorkerChunk {
    /// Filter miss or no filter: already compressed+encrypted.
    Prepared(PreparedChunk),
    /// Filter hit: only hashed, raw data retained for false-positive fallback.
    Hashed(HashedChunk),
}

/// Classify a single chunk: hash → xor filter check → transform or hash-only.
pub(super) fn classify_chunk(
    chunk_id: ChunkId,
    data: Vec<u8>,
    dedup_filter: Option<&xorf::Xor8>,
    compression: Compression,
    crypto: &dyn CryptoEngine,
) -> Result<WorkerChunk> {
    if let Some(filter) = dedup_filter {
        use xorf::Filter;
        let key = crate::index::dedup_cache::chunk_id_to_u64(&chunk_id);
        if filter.contains(&key) {
            return Ok(WorkerChunk::Hashed(HashedChunk { chunk_id, data }));
        }
    }
    let compressed = crate::compress::compress(compression, &data)?;
    let packed = pack_object(ObjectType::ChunkData, &compressed, crypto)?;
    Ok(WorkerChunk::Prepared(PreparedChunk {
        chunk_id,
        uncompressed_size: data.len() as u32,
        packed,
    }))
}
