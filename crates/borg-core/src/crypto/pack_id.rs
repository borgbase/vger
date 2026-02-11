use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A 32-byte pack file identifier computed as unkeyed BLAKE2b-256.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PackId(pub [u8; 32]);

impl PackId {
    /// Compute a pack ID as unkeyed BLAKE2b-256 of the entire pack contents.
    pub fn compute(data: &[u8]) -> Self {
        let mut hasher = Blake2bVar::new(32).expect("valid output size");
        hasher.update(data);
        let mut out = [0u8; 32];
        hasher.finalize_variable(&mut out).expect("correct length");
        PackId(out)
    }

    /// Hex-encode the full pack ID.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// First byte as a two-char hex string, used for shard directory.
    pub fn shard_prefix(&self) -> String {
        hex::encode(&self.0[..1])
    }

    /// Storage key path: `packs/<shard>/<full_hex>`.
    pub fn storage_key(&self) -> String {
        format!("packs/{}/{}", self.shard_prefix(), self.to_hex())
    }
}

impl fmt::Debug for PackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PackId({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for PackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}
