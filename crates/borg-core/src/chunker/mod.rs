use fastcdc::v2020::FastCDC;

use crate::config::ChunkerConfig;

/// Chunk a byte slice using FastCDC content-defined chunking.
/// Returns a vector of `(offset, length)` pairs.
pub fn chunk_data(data: &[u8], config: &ChunkerConfig) -> Vec<(usize, usize)> {
    let chunker = FastCDC::new(data, config.min_size, config.avg_size, config.max_size);
    chunker.map(|chunk| (chunk.offset, chunk.length)).collect()
}
