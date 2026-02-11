use blake2::digest::consts::U32;
use blake2::digest::Mac;
use blake2::Blake2bMac;
use serde::{Deserialize, Serialize};
use std::fmt;

type KeyedBlake2b256 = Blake2bMac<U32>;

/// A 32-byte chunk identifier computed as keyed BLAKE2b-256.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    /// Compute a chunk ID using keyed BLAKE2b-256 (BLAKE2b-MAC with 32-byte output).
    pub fn compute(key: &[u8; 32], data: &[u8]) -> Self {
        let mut hasher = KeyedBlake2b256::new_from_slice(key)
            .expect("valid 32-byte key for BLAKE2b");
        Mac::update(&mut hasher, data);
        let result = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&result.into_bytes());
        ChunkId(out)
    }

    /// Hex-encode the full chunk ID for use as a storage key.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// First byte as a two-char hex string, used for shard directory.
    pub fn shard_prefix(&self) -> String {
        hex::encode(&self.0[..1])
    }

    /// Storage key path: `data/<shard>/<full_hex>`.
    pub fn storage_key(&self) -> String {
        format!("data/{}/{}", self.shard_prefix(), self.to_hex())
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkId({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}
