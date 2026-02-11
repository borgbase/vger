pub mod item;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::ChunkerConfig;
use crate::crypto::chunk_id::ChunkId;

/// Metadata for a single archive, stored at `archives/<id>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveMeta {
    pub name: String,
    pub hostname: String,
    pub username: String,
    pub time: DateTime<Utc>,
    pub time_end: DateTime<Utc>,
    pub chunker_params: ChunkerConfig,
    #[serde(default)]
    pub comment: String,
    /// Chunk IDs that contain the serialized item stream.
    pub item_ptrs: Vec<ChunkId>,
    #[serde(default)]
    pub stats: ArchiveStats,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArchiveStats {
    pub nfiles: u64,
    pub original_size: u64,
    pub compressed_size: u64,
    pub deduplicated_size: u64,
}
