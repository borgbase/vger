pub mod format;
pub mod lock;
pub mod manifest;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::compress;
use crate::config::ChunkerConfig;
use crate::crypto::key::{EncryptedKey, MasterKey};
use crate::crypto::{self, CryptoEngine, PlaintextEngine};
use crate::error::{BorgError, Result};
use crate::index::ChunkIndex;
use crate::storage::StorageBackend;

use self::format::{pack_object, unpack_object, ObjectType};
use self::manifest::Manifest;

/// Persisted (unencrypted) at the `config` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub version: u32,
    pub id: Vec<u8>, // 32 bytes
    pub chunker_params: ChunkerConfig,
    pub encryption: EncryptionMode,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionMode {
    None,
    Aes256Gcm,
}

/// A handle to an opened repository.
pub struct Repository {
    pub storage: Box<dyn StorageBackend>,
    pub crypto: Box<dyn CryptoEngine>,
    pub manifest: Manifest,
    pub chunk_index: ChunkIndex,
    pub config: RepoConfig,
}

impl Repository {
    /// Initialize a new repository.
    pub fn init(
        storage: Box<dyn StorageBackend>,
        encryption: EncryptionMode,
        chunker_params: ChunkerConfig,
        passphrase: Option<&str>,
    ) -> Result<Self> {
        // Check that the repo doesn't already exist
        if storage.exists("config")? {
            return Err(BorgError::RepoAlreadyExists("repository".into()));
        }

        // Generate repo ID
        let mut rng = rand::thread_rng();
        let mut repo_id = vec![0u8; 32];
        rng.fill_bytes(&mut repo_id);

        let repo_config = RepoConfig {
            version: 1,
            id: repo_id,
            chunker_params: chunker_params.clone(),
            encryption: encryption.clone(),
            created: Utc::now(),
        };

        // Generate master key and crypto engine
        let (crypto, encrypted_key): (Box<dyn CryptoEngine>, Option<EncryptedKey>) =
            match &encryption {
                EncryptionMode::None => {
                    let mut chunk_id_key = [0u8; 32];
                    rng.fill_bytes(&mut chunk_id_key);
                    (Box::new(PlaintextEngine::new(&chunk_id_key)), None)
                }
                EncryptionMode::Aes256Gcm => {
                    let master_key = MasterKey::generate();
                    let pass = passphrase.ok_or_else(|| {
                        BorgError::Config("passphrase required for encrypted repository".into())
                    })?;
                    let enc_key = master_key.to_encrypted(pass)?;
                    let engine = crypto::aes_gcm::Aes256GcmEngine::new(
                        &master_key.encryption_key,
                        &master_key.chunk_id_key,
                    );
                    (Box::new(engine), Some(enc_key))
                }
            };

        // Store config (unencrypted)
        let config_data = rmp_serde::to_vec(&repo_config)?;
        storage.put("config", &config_data)?;

        // Store encrypted key if applicable
        if let Some(enc_key) = &encrypted_key {
            storage.create_dir("keys/")?;
            let key_data = rmp_serde::to_vec(enc_key)?;
            storage.put("keys/repokey", &key_data)?;
        }

        // Store empty manifest
        let manifest = Manifest::new();
        let manifest_bytes = rmp_serde::to_vec(&manifest)?;
        let manifest_packed = pack_object(ObjectType::Manifest, &manifest_bytes, crypto.as_ref())?;
        storage.put("manifest", &manifest_packed)?;

        // Store empty chunk index
        let chunk_index = ChunkIndex::new();
        let index_bytes = rmp_serde::to_vec(&chunk_index)?;
        let index_packed = pack_object(ObjectType::ChunkIndex, &index_bytes, crypto.as_ref())?;
        storage.put("index", &index_packed)?;

        // Create directory structure
        storage.create_dir("archives/")?;
        storage.create_dir("locks/")?;
        for i in 0u8..=255 {
            storage.create_dir(&format!("data/{:02x}/", i))?;
        }

        Ok(Repository {
            storage,
            crypto,
            manifest,
            chunk_index,
            config: repo_config,
        })
    }

    /// Open an existing repository.
    pub fn open(storage: Box<dyn StorageBackend>, passphrase: Option<&str>) -> Result<Self> {
        // Read config
        let config_data = storage
            .get("config")?
            .ok_or_else(|| BorgError::RepoNotFound("config not found".into()))?;
        let repo_config: RepoConfig = rmp_serde::from_slice(&config_data)?;

        if repo_config.version != 1 {
            return Err(BorgError::UnsupportedVersion(repo_config.version));
        }

        // Build crypto engine
        let crypto: Box<dyn CryptoEngine> = match &repo_config.encryption {
            EncryptionMode::None => {
                // For plaintext repos, we still need the chunk_id_key.
                // We store it in the encrypted key blob even for "none" mode...
                // Actually for none mode we just generate a deterministic key from repo ID.
                let mut chunk_id_key = [0u8; 32];
                // Use BLAKE2b of repo ID as the chunk ID key for plaintext repos
                use blake2::digest::{Update, VariableOutput};
                use blake2::Blake2bVar;
                let mut hasher = Blake2bVar::new(32).unwrap();
                hasher.update(&repo_config.id);
                hasher.finalize_variable(&mut chunk_id_key).unwrap();
                Box::new(PlaintextEngine::new(&chunk_id_key))
            }
            EncryptionMode::Aes256Gcm => {
                let key_data = storage
                    .get("keys/repokey")?
                    .ok_or_else(|| BorgError::InvalidFormat("missing keys/repokey".into()))?;
                let enc_key: EncryptedKey = rmp_serde::from_slice(&key_data)?;
                let pass = passphrase.ok_or_else(|| {
                    BorgError::Config("passphrase required for encrypted repository".into())
                })?;
                let master_key = MasterKey::from_encrypted(&enc_key, pass)?;
                let engine = crypto::aes_gcm::Aes256GcmEngine::new(
                    &master_key.encryption_key,
                    &master_key.chunk_id_key,
                );
                Box::new(engine)
            }
        };

        // Read manifest
        let manifest_data = storage
            .get("manifest")?
            .ok_or_else(|| BorgError::InvalidFormat("missing manifest".into()))?;
        let (obj_type, manifest_bytes) = unpack_object(&manifest_data, crypto.as_ref())?;
        if obj_type != ObjectType::Manifest {
            return Err(BorgError::InvalidFormat("manifest has wrong type tag".into()));
        }
        let manifest: Manifest = rmp_serde::from_slice(&manifest_bytes)?;

        // Read chunk index
        let chunk_index = if let Some(index_data) = storage.get("index")? {
            let (_obj_type, index_bytes) = unpack_object(&index_data, crypto.as_ref())?;
            rmp_serde::from_slice(&index_bytes)?
        } else {
            ChunkIndex::new()
        };

        Ok(Repository {
            storage,
            crypto,
            manifest,
            chunk_index,
            config: repo_config,
        })
    }

    /// Save the manifest and chunk index back to storage.
    pub fn save_state(&self) -> Result<()> {
        // Save manifest
        let manifest_bytes = rmp_serde::to_vec(&self.manifest)?;
        let manifest_packed =
            pack_object(ObjectType::Manifest, &manifest_bytes, self.crypto.as_ref())?;
        self.storage.put("manifest", &manifest_packed)?;

        // Save chunk index
        let index_bytes = rmp_serde::to_vec(&self.chunk_index)?;
        let index_packed =
            pack_object(ObjectType::ChunkIndex, &index_bytes, self.crypto.as_ref())?;
        self.storage.put("index", &index_packed)?;

        Ok(())
    }

    /// Store a chunk in the repository. Returns (stored_size, was_new).
    /// If the chunk already exists (dedup), just increments the refcount.
    pub fn store_chunk(
        &mut self,
        data: &[u8],
        compression: compress::Compression,
    ) -> Result<(crate::crypto::chunk_id::ChunkId, u32, bool)> {
        let chunk_id =
            crate::crypto::chunk_id::ChunkId::compute(self.crypto.chunk_id_key(), data);

        if self.chunk_index.contains(&chunk_id) {
            // Dedup hit — just increment refcount
            let stored_size = self.chunk_index.get(&chunk_id).unwrap().stored_size;
            self.chunk_index.add(chunk_id, stored_size);
            return Ok((chunk_id, stored_size, false));
        }

        // Compress
        let compressed = compress::compress(compression, data)?;

        // Encrypt and wrap in repo object envelope
        let packed = pack_object(ObjectType::ChunkData, &compressed, self.crypto.as_ref())?;
        let stored_size = packed.len() as u32;

        // Store
        self.storage.put(&chunk_id.storage_key(), &packed)?;

        // Update index
        self.chunk_index.add(chunk_id, stored_size);

        Ok((chunk_id, stored_size, true))
    }

    /// Read and decrypt a chunk from the repository.
    pub fn read_chunk(&self, chunk_id: &crate::crypto::chunk_id::ChunkId) -> Result<Vec<u8>> {
        let key = chunk_id.storage_key();
        let data = self
            .storage
            .get(&key)?
            .ok_or_else(|| BorgError::Other(format!("chunk not found: {chunk_id}")))?;
        let (_obj_type, compressed) = unpack_object(&data, self.crypto.as_ref())?;
        compress::decompress(&compressed)
    }
}
