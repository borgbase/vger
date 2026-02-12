use serde::{Deserialize, Serialize};

use crate::config::CompressionAlgorithm;
use crate::error::{Result, VgerError};

const TAG_NONE: u8 = 0x00;
const TAG_LZ4: u8 = 0x01;
const TAG_ZSTD: u8 = 0x02;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Compression {
    None,
    #[default]
    Lz4,
    Zstd {
        level: i32,
    },
}

impl Compression {
    /// Parse from config string like "lz4", "zstd", "none".
    pub fn from_config(algorithm: &str, zstd_level: i32) -> Result<Self> {
        match algorithm {
            "none" => Ok(Compression::None),
            "lz4" => Ok(Compression::Lz4),
            "zstd" => Ok(Compression::Zstd { level: zstd_level }),
            other => Err(VgerError::Config(format!(
                "unknown compression algorithm: {other}"
            ))),
        }
    }

    pub fn from_algorithm(algorithm: CompressionAlgorithm, zstd_level: i32) -> Self {
        match algorithm {
            CompressionAlgorithm::None => Compression::None,
            CompressionAlgorithm::Lz4 => Compression::Lz4,
            CompressionAlgorithm::Zstd => Compression::Zstd { level: zstd_level },
        }
    }
}

/// Compress data and prepend a 1-byte tag identifying the codec.
pub fn compress(compression: Compression, data: &[u8]) -> Result<Vec<u8>> {
    let (tag, compressed) = match compression {
        Compression::None => (TAG_NONE, data.to_vec()),
        Compression::Lz4 => (TAG_LZ4, lz4_flex::compress_prepend_size(data)),
        Compression::Zstd { level } => {
            let c = zstd::encode_all(std::io::Cursor::new(data), level)
                .map_err(|e| VgerError::Other(format!("zstd compress: {e}")))?;
            (TAG_ZSTD, c)
        }
    };

    let mut out = Vec::with_capacity(1 + compressed.len());
    out.push(tag);
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decompress data by reading the 1-byte tag prefix and dispatching.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(VgerError::Decompression("empty data".into()));
    }
    let tag = data[0];
    let payload = &data[1..];
    match tag {
        TAG_NONE => Ok(payload.to_vec()),
        TAG_LZ4 => lz4_flex::decompress_size_prepended(payload)
            .map_err(|e| VgerError::Decompression(format!("lz4: {e}"))),
        TAG_ZSTD => zstd::decode_all(std::io::Cursor::new(payload))
            .map_err(|e| VgerError::Decompression(format!("zstd: {e}"))),
        _ => Err(VgerError::UnknownCompressionTag(tag)),
    }
}
