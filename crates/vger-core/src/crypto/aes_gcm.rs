use aes_gcm::aead::{Aead, AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;

use super::CryptoEngine;
use crate::error::{Result, VgerError};

/// AES-256-GCM authenticated encryption engine.
pub struct Aes256GcmEngine {
    cipher: Aes256Gcm,
    chunk_id_key: [u8; 32],
}

impl Aes256GcmEngine {
    pub fn new(encryption_key: &[u8; 32], chunk_id_key: &[u8; 32]) -> Self {
        let cipher =
            Aes256Gcm::new_from_slice(encryption_key).expect("valid 32-byte key for AES-256-GCM");
        Self {
            cipher,
            chunk_id_key: *chunk_id_key,
        }
    }
}

impl CryptoEngine for Aes256GcmEngine {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let mut rng = rand::thread_rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let payload = aes_gcm::aead::Payload {
            msg: plaintext,
            aad,
        };
        let ciphertext = self
            .cipher
            .encrypt(nonce, payload)
            .map_err(|e| VgerError::Other(format!("AES-GCM encrypt: {e}")))?;

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

        let payload = aes_gcm::aead::Payload {
            msg: ciphertext,
            aad,
        };
        self.cipher
            .decrypt(nonce, payload)
            .map_err(|_| VgerError::DecryptionFailed)
    }

    fn decrypt_into(&self, data: &[u8], aad: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if data.len() < 12 + 16 {
            return Err(VgerError::DecryptionFailed);
        }
        let (nonce_bytes, ct_and_tag) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        let (ciphertext, tag_bytes) = ct_and_tag.split_at(ct_and_tag.len() - 16);
        let tag = aes_gcm::Tag::from_slice(tag_bytes);
        output.clear();
        output.extend_from_slice(ciphertext); // reuses existing capacity
        self.cipher
            .decrypt_in_place_detached(nonce, aad, output, tag)
            .map_err(|_| VgerError::DecryptionFailed)?;
        Ok(())
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
            .map_err(|e| VgerError::Other(format!("AES-GCM encrypt_in_place: {e}")))?;

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
