pub mod aes_gcm;
pub mod chacha20_poly1305;
pub mod chunk_id;
pub mod key;
pub mod pack_id;
pub mod select;

use crate::error::Result;

/// Trait for encrypting and decrypting repository objects.
pub trait CryptoEngine: Send + Sync {
    /// Encrypt plaintext. Returns `[nonce][ciphertext+tag]`.
    /// `aad` is authenticated but not encrypted (e.g., the type tag byte).
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    /// Decrypt data produced by `encrypt`.
    /// `aad` must match what was passed during encryption.
    fn decrypt(&self, data: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    /// Encrypt `buffer` in-place and return `(nonce, tag)`.
    /// Avoids allocating a separate ciphertext buffer.
    fn encrypt_in_place_detached(
        &self,
        buffer: &mut [u8],
        aad: &[u8],
    ) -> Result<([u8; 12], [u8; 16])>;

    /// Whether this engine actually encrypts data.
    /// `PlaintextEngine` returns false; real ciphers return true.
    fn is_encrypting(&self) -> bool;

    /// The key used for computing chunk IDs (keyed BLAKE2b-256).
    fn chunk_id_key(&self) -> &[u8; 32];
}

/// No-encryption engine. Still computes deterministic chunk IDs.
pub struct PlaintextEngine {
    chunk_id_key: [u8; 32],
}

impl PlaintextEngine {
    pub fn new(chunk_id_key: &[u8; 32]) -> Self {
        Self {
            chunk_id_key: *chunk_id_key,
        }
    }
}

impl CryptoEngine for PlaintextEngine {
    fn encrypt(&self, plaintext: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(&self, data: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn encrypt_in_place_detached(
        &self,
        _buffer: &mut [u8],
        _aad: &[u8],
    ) -> Result<([u8; 12], [u8; 16])> {
        Ok(([0u8; 12], [0u8; 16]))
    }

    fn is_encrypting(&self) -> bool {
        false
    }

    fn chunk_id_key(&self) -> &[u8; 32] {
        &self.chunk_id_key
    }
}
