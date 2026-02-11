use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::Argon2;
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::error::{VgerError, Result};

/// The master key material — never stored in plaintext on disk.
pub struct MasterKey {
    pub encryption_key: [u8; 32],
    pub chunk_id_key: [u8; 32],
}

/// Serialized payload inside the encrypted key blob.
#[derive(Serialize, Deserialize)]
struct MasterKeyPayload {
    encryption_key: Vec<u8>,
    chunk_id_key: Vec<u8>,
}

/// KDF parameters stored alongside the encrypted key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KdfParams {
    pub algorithm: String,
    pub time_cost: u32,
    pub memory_cost: u32,
    pub parallelism: u32,
    pub salt: Vec<u8>,
}

/// On-disk format stored at `keys/repokey`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedKey {
    pub kdf: KdfParams,
    pub nonce: Vec<u8>,
    pub encrypted_payload: Vec<u8>,
}

impl MasterKey {
    /// Generate a new random master key.
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let mut encryption_key = [0u8; 32];
        let mut chunk_id_key = [0u8; 32];
        rng.fill_bytes(&mut encryption_key);
        rng.fill_bytes(&mut chunk_id_key);
        Self {
            encryption_key,
            chunk_id_key,
        }
    }

    /// Encrypt the master key with a passphrase using Argon2id + AES-256-GCM.
    pub fn to_encrypted(&self, passphrase: &str) -> Result<EncryptedKey> {
        let mut rng = rand::thread_rng();

        // Generate salt
        let mut salt = vec![0u8; 32];
        rng.fill_bytes(&mut salt);

        // Derive a wrapping key from the passphrase
        let kdf = KdfParams {
            algorithm: "argon2id".to_string(),
            time_cost: 3,
            memory_cost: 65536, // 64 MiB
            parallelism: 4,
            salt: salt.clone(),
        };
        let wrapping_key = derive_key_from_passphrase(passphrase, &kdf)?;

        // Serialize the master key payload
        let payload = MasterKeyPayload {
            encryption_key: self.encryption_key.to_vec(),
            chunk_id_key: self.chunk_id_key.to_vec(),
        };
        let plaintext = rmp_serde::to_vec(&payload)?;

        // Encrypt with AES-256-GCM
        let cipher = Aes256Gcm::new_from_slice(&wrapping_key)
            .map_err(|e| VgerError::KeyDerivation(format!("cipher init: {e}")))?;
        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_ref())
            .map_err(|e| VgerError::KeyDerivation(format!("encrypt: {e}")))?;

        Ok(EncryptedKey {
            kdf,
            nonce: nonce_bytes.to_vec(),
            encrypted_payload: ciphertext,
        })
    }

    /// Decrypt the master key from its on-disk format.
    pub fn from_encrypted(encrypted: &EncryptedKey, passphrase: &str) -> Result<Self> {
        let wrapping_key = derive_key_from_passphrase(passphrase, &encrypted.kdf)?;

        let cipher = Aes256Gcm::new_from_slice(&wrapping_key)
            .map_err(|_| VgerError::DecryptionFailed)?;
        let nonce = Nonce::from_slice(&encrypted.nonce);
        let plaintext = cipher
            .decrypt(nonce, encrypted.encrypted_payload.as_ref())
            .map_err(|_| VgerError::DecryptionFailed)?;

        let payload: MasterKeyPayload = rmp_serde::from_slice(&plaintext)
            .map_err(|_| VgerError::DecryptionFailed)?;

        let mut encryption_key = [0u8; 32];
        let mut chunk_id_key = [0u8; 32];
        if payload.encryption_key.len() != 32 || payload.chunk_id_key.len() != 32 {
            return Err(VgerError::DecryptionFailed);
        }
        encryption_key.copy_from_slice(&payload.encryption_key);
        chunk_id_key.copy_from_slice(&payload.chunk_id_key);

        Ok(Self {
            encryption_key,
            chunk_id_key,
        })
    }
}

/// Derive a 32-byte key from a passphrase using Argon2id.
fn derive_key_from_passphrase(passphrase: &str, kdf: &KdfParams) -> Result<[u8; 32]> {
    let params = argon2::Params::new(kdf.memory_cost, kdf.time_cost, kdf.parallelism, Some(32))
        .map_err(|e| VgerError::KeyDerivation(format!("argon2 params: {e}")))?;
    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

    let mut output = [0u8; 32];
    argon2
        .hash_password_into(passphrase.as_bytes(), &kdf.salt, &mut output)
        .map_err(|e| VgerError::KeyDerivation(format!("argon2 hash: {e}")))?;
    Ok(output)
}
