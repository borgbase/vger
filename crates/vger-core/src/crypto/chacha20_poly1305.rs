use chacha20poly1305::aead::{Aead, AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use rand::RngCore;

use super::CryptoEngine;
use crate::error::{Result, VgerError};

/// ChaCha20-Poly1305 authenticated encryption engine.
pub struct ChaCha20Poly1305Engine {
    cipher: ChaCha20Poly1305,
    chunk_id_key: [u8; 32],
}

impl ChaCha20Poly1305Engine {
    pub fn new(encryption_key: &[u8; 32], chunk_id_key: &[u8; 32]) -> Self {
        let cipher = ChaCha20Poly1305::new_from_slice(encryption_key)
            .expect("valid 32-byte key for ChaCha20-Poly1305");
        Self {
            cipher,
            chunk_id_key: *chunk_id_key,
        }
    }
}

impl CryptoEngine for ChaCha20Poly1305Engine {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let mut rng = rand::thread_rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let payload = chacha20poly1305::aead::Payload {
            msg: plaintext,
            aad,
        };
        let ciphertext = self
            .cipher
            .encrypt(nonce, payload)
            .map_err(|e| VgerError::Other(format!("ChaCha20-Poly1305 encrypt: {e}")))?;

        // Wire format: [12-byte nonce][ciphertext with appended 16-byte tag]
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decrypt(&self, data: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        if data.len() < 12 + 16 {
            return Err(VgerError::DecryptionFailed);
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        let payload = chacha20poly1305::aead::Payload {
            msg: ciphertext,
            aad,
        };
        self.cipher
            .decrypt(nonce, payload)
            .map_err(|_| VgerError::DecryptionFailed)
    }

    fn encrypt_in_place_detached(
        &self,
        buffer: &mut [u8],
        aad: &[u8],
    ) -> crate::error::Result<([u8; 12], [u8; 16])> {
        let mut rng = rand::thread_rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let tag = self
            .cipher
            .encrypt_in_place_detached(nonce, aad, buffer)
            .map_err(|e| VgerError::Other(format!("ChaCha20-Poly1305 encrypt_in_place: {e}")))?;

        let mut tag_bytes = [0u8; 16];
        tag_bytes.copy_from_slice(&tag);
        Ok((nonce_bytes, tag_bytes))
    }

    fn is_encrypting(&self) -> bool {
        true
    }

    fn chunk_id_key(&self) -> &[u8; 32] {
        &self.chunk_id_key
    }
}
