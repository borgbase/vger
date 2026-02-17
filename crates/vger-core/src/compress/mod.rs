use std::io::Read;

use serde::{Deserialize, Serialize};

use crate::config::CompressionAlgorithm;
use crate::error::{Result, VgerError};

const TAG_NONE: u8 = 0x00;
const TAG_LZ4: u8 = 0x01;
const TAG_ZSTD: u8 = 0x02;

/// Maximum decompressed output size (32 MiB = 4× max chunk size).
/// Prevents decompression bombs from consuming unbounded memory.
const MAX_DECOMPRESS_SIZE: u64 = 32 * 1024 * 1024;

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
    match compression {
        Compression::None => {
            let mut out = Vec::with_capacity(1 + data.len());
            out.push(TAG_NONE);
            out.extend_from_slice(data);
            Ok(out)
        }
        Compression::Lz4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            let mut out = Vec::with_capacity(1 + compressed.len());
            out.push(TAG_LZ4);
            out.extend_from_slice(&compressed);
            Ok(out)
        }
        Compression::Zstd { level } => {
            use std::cell::RefCell;
            thread_local! {
                static ZSTD_CX: RefCell<Option<(i32, zstd::bulk::Compressor<'static>)>> =
                    const { RefCell::new(None) };
            }

            ZSTD_CX.with(|cell| {
                let mut slot = cell.borrow_mut();

                // Lazily init or reinit if the compression level changed.
                if !matches!(slot.as_ref(), Some((l, _)) if *l == level) {
                    let cx = zstd::bulk::Compressor::new(level)
                        .map_err(|e| VgerError::Other(format!("zstd init: {e}")))?;
                    *slot = Some((level, cx));
                }
                let (_, cx) = slot.as_mut().unwrap();

                let compressed = cx
                    .compress(data)
                    .map_err(|e| VgerError::Other(format!("zstd compress: {e}")))?;
                let mut out = Vec::with_capacity(1 + compressed.len());
                out.push(TAG_ZSTD);
                out.extend_from_slice(&compressed);
                Ok(out)
            })
        }
    }
}

/// Decompress data by reading the 1-byte tag prefix and dispatching.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    decompress_with_hint(data, None)
}

/// Decompress data by reading the 1-byte tag prefix and dispatching.
///
/// `expected_size` is a best-effort capacity hint used to reduce `Vec` growth
/// during streaming decode. It is always capped by `MAX_DECOMPRESS_SIZE` and
/// never bypasses size-limit checks.
pub fn decompress_with_hint(data: &[u8], expected_size: Option<usize>) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(VgerError::Decompression("empty data".into()));
    }
    let tag = data[0];
    let payload = &data[1..];
    match tag {
        TAG_NONE => Ok(payload.to_vec()),
        TAG_LZ4 => {
            if payload.len() < 4 {
                return Err(VgerError::Decompression("lz4: payload too short".into()));
            }
            let uncompressed_size = u32::from_le_bytes(payload[..4].try_into().unwrap()) as u64;
            if uncompressed_size > MAX_DECOMPRESS_SIZE {
                return Err(VgerError::Decompression(format!(
                    "lz4: decompressed size ({uncompressed_size}) exceeds limit of {MAX_DECOMPRESS_SIZE} bytes"
                )));
            }
            lz4_flex::decompress_size_prepended(payload)
                .map_err(|e| VgerError::Decompression(format!("lz4: {e}")))
        }
        TAG_ZSTD => {
            let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(payload))
                .map_err(|e| VgerError::Decompression(format!("zstd init: {e}")))?;
            let hinted_capacity = expected_size
                .unwrap_or(0)
                .min(MAX_DECOMPRESS_SIZE as usize);
            let mut output = Vec::with_capacity(hinted_capacity);
            decoder
                .by_ref()
                .take(MAX_DECOMPRESS_SIZE + 1)
                .read_to_end(&mut output)
                .map_err(|e| VgerError::Decompression(format!("zstd: {e}")))?;
            if output.len() as u64 > MAX_DECOMPRESS_SIZE {
                return Err(VgerError::Decompression(format!(
                    "zstd: decompressed size exceeds limit of {} bytes",
                    MAX_DECOMPRESS_SIZE
                )));
            }
            Ok(output)
        }
        _ => Err(VgerError::UnknownCompressionTag(tag)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decompress_rejects_lz4_bomb() {
        // Craft a payload with a huge size prefix (1 GiB) but tiny compressed data
        let mut bomb = (1u32 << 30).to_le_bytes().to_vec();
        bomb.extend_from_slice(&[0u8; 10]);
        // Prepend LZ4 tag
        let mut data = vec![TAG_LZ4];
        data.extend_from_slice(&bomb);
        assert!(decompress(&data).is_err());
    }

    #[test]
    fn decompress_rejects_lz4_short_payload() {
        // Only tag + 2 bytes (need at least 4 for size prefix)
        let data = vec![TAG_LZ4, 0x00, 0x00];
        assert!(decompress(&data).is_err());
    }

    #[test]
    fn compress_decompress_lz4_roundtrip() {
        let original = b"hello world, this is a test of lz4 compression";
        let compressed = compress(Compression::Lz4, original).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn decompress_with_hint_matches_decompress() {
        let payloads: &[&[u8]] = &[b"", b"short", b"this payload is long enough to compress"];
        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];

        for codec in codecs {
            for payload in payloads {
                let encoded = compress(codec, payload).unwrap();
                let plain_a = decompress(&encoded).unwrap();
                let plain_b = decompress_with_hint(&encoded, Some(payload.len())).unwrap();
                assert_eq!(plain_a, plain_b);
            }
        }
    }

    #[test]
    fn decompress_with_hint_caps_large_hint() {
        let payload = vec![0xAB; 1024];
        let encoded = compress(Compression::Zstd { level: 3 }, &payload).unwrap();
        let decoded = decompress_with_hint(&encoded, Some(usize::MAX)).unwrap();
        assert_eq!(decoded, payload);
    }
}
