pub mod format;
pub mod lock;
pub mod manifest;
pub mod pack;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::compress;
use crate::config::{ChunkerConfig, RepositoryConfig};
use crate::crypto::chunk_id::ChunkId;
use crate::crypto::key::{EncryptedKey, MasterKey};
use crate::crypto::{self, CryptoEngine, PlaintextEngine};
use crate::error::{Result, VgerError};
use crate::index::ChunkIndex;
use crate::storage::StorageBackend;

use self::format::{pack_object, unpack_object, ObjectType};
use self::manifest::Manifest;
use self::pack::{
    compute_data_pack_target, compute_tree_pack_target, read_blob_from_pack, PackType, PackWriter,
};

/// Persisted (unencrypted) at the `config` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub version: u32,
    pub id: Vec<u8>, // 32 bytes
    pub chunker_params: ChunkerConfig,
    pub encryption: EncryptionMode,
    pub created: DateTime<Utc>,
    #[serde(default = "default_min_pack_size")]
    pub min_pack_size: u32,
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: u32,
}

fn default_min_pack_size() -> u32 {
    32 * 1024 * 1024
}

fn default_max_pack_size() -> u32 {
    512 * 1024 * 1024
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
    data_pack_writer: PackWriter,
    tree_pack_writer: PackWriter,
}

impl Repository {
    /// Initialize a new repository.
    pub fn init(
        storage: Box<dyn StorageBackend>,
        encryption: EncryptionMode,
        chunker_params: ChunkerConfig,
        passphrase: Option<&str>,
        repo_config_opts: Option<&RepositoryConfig>,
    ) -> Result<Self> {
        // Check that the repo doesn't already exist
        if storage.exists("config")? {
            return Err(VgerError::RepoAlreadyExists("repository".into()));
        }

        // Generate repo ID
        let mut rng = rand::thread_rng();
        let mut repo_id = vec![0u8; 32];
        rng.fill_bytes(&mut repo_id);

        let min_pack_size = repo_config_opts
            .map(|c| c.min_pack_size)
            .unwrap_or_else(default_min_pack_size);
        let max_pack_size = repo_config_opts
            .map(|c| c.max_pack_size)
            .unwrap_or_else(default_max_pack_size);

        let repo_config = RepoConfig {
            version: 1,
            id: repo_id,
            chunker_params: chunker_params.clone(),
            encryption: encryption.clone(),
            created: Utc::now(),
            min_pack_size,
            max_pack_size,
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
                        VgerError::Config("passphrase required for encrypted repository".into())
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
        storage.create_dir("snapshots/")?;
        storage.create_dir("locks/")?;
        for i in 0u8..=255 {
            storage.create_dir(&format!("packs/{:02x}/", i))?;
        }

        // No packs yet, so data target = min_pack_size
        let data_target = min_pack_size as usize;
        let tree_target = compute_tree_pack_target(min_pack_size);

        Ok(Repository {
            storage,
            crypto,
            manifest,
            chunk_index,
            config: repo_config,
            data_pack_writer: PackWriter::new(PackType::Data, data_target),
            tree_pack_writer: PackWriter::new(PackType::Tree, tree_target),
        })
    }

    /// Open an existing repository.
    pub fn open(storage: Box<dyn StorageBackend>, passphrase: Option<&str>) -> Result<Self> {
        // Read config
        let config_data = storage
            .get("config")?
            .ok_or_else(|| VgerError::RepoNotFound("config not found".into()))?;
        let repo_config: RepoConfig = rmp_serde::from_slice(&config_data)?;

        if repo_config.version != 1 {
            return Err(VgerError::UnsupportedVersion(repo_config.version));
        }

        // Build crypto engine
        let crypto: Box<dyn CryptoEngine> = match &repo_config.encryption {
            EncryptionMode::None => {
                let mut chunk_id_key = [0u8; 32];
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
                    .ok_or_else(|| VgerError::InvalidFormat("missing keys/repokey".into()))?;
                let enc_key: EncryptedKey = rmp_serde::from_slice(&key_data)?;
                let pass = passphrase.ok_or_else(|| {
                    VgerError::Config("passphrase required for encrypted repository".into())
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
            .ok_or_else(|| VgerError::InvalidFormat("missing manifest".into()))?;
        let (obj_type, manifest_bytes) = unpack_object(&manifest_data, crypto.as_ref())?;
        if obj_type != ObjectType::Manifest {
            return Err(VgerError::InvalidFormat(
                "manifest has wrong type tag".into(),
            ));
        }
        let manifest: Manifest = rmp_serde::from_slice(&manifest_bytes)?;

        // Read chunk index
        let chunk_index = if let Some(index_data) = storage.get("index")? {
            let (_obj_type, index_bytes) = unpack_object(&index_data, crypto.as_ref())?;
            rmp_serde::from_slice(&index_bytes)?
        } else {
            ChunkIndex::new()
        };

        // Compute dynamic pack target sizes
        let num_data_packs = chunk_index.count_distinct_packs();
        let data_target = compute_data_pack_target(
            num_data_packs,
            repo_config.min_pack_size,
            repo_config.max_pack_size,
        );
        let tree_target = compute_tree_pack_target(repo_config.min_pack_size);

        Ok(Repository {
            storage,
            crypto,
            manifest,
            chunk_index,
            config: repo_config,
            data_pack_writer: PackWriter::new(PackType::Data, data_target),
            tree_pack_writer: PackWriter::new(PackType::Tree, tree_target),
        })
    }

    /// Save the manifest and chunk index back to storage.
    /// Flushes any pending pack writes first.
    pub fn save_state(&mut self) -> Result<()> {
        // Flush any pending packs
        self.flush_packs()?;

        // Save manifest
        let manifest_bytes = rmp_serde::to_vec(&self.manifest)?;
        let manifest_packed =
            pack_object(ObjectType::Manifest, &manifest_bytes, self.crypto.as_ref())?;
        self.storage.put("manifest", &manifest_packed)?;

        // Save chunk index
        let index_bytes = rmp_serde::to_vec(&self.chunk_index)?;
        let index_packed = pack_object(ObjectType::ChunkIndex, &index_bytes, self.crypto.as_ref())?;
        self.storage.put("index", &index_packed)?;

        Ok(())
    }

    /// Store a chunk in the repository. Returns (chunk_id, stored_size, was_new).
    /// If the chunk already exists (dedup), just increments the refcount.
    pub fn store_chunk(
        &mut self,
        data: &[u8],
        compression: compress::Compression,
        pack_type: PackType,
    ) -> Result<(ChunkId, u32, bool)> {
        let chunk_id = ChunkId::compute(self.crypto.chunk_id_key(), data);

        // Dedup check against committed index
        if self.chunk_index.contains(&chunk_id) {
            let stored_size = self.chunk_index.get(&chunk_id).unwrap().stored_size;
            self.chunk_index.increment_refcount(&chunk_id);
            return Ok((chunk_id, stored_size, false));
        }

        // Dedup check against pending blobs in both pack writers
        if self.data_pack_writer.contains_pending(&chunk_id) {
            let stored_size = self
                .data_pack_writer
                .get_pending_stored_size(&chunk_id)
                .unwrap();
            self.data_pack_writer.increment_pending(&chunk_id);
            return Ok((chunk_id, stored_size, false));
        }
        if self.tree_pack_writer.contains_pending(&chunk_id) {
            let stored_size = self
                .tree_pack_writer
                .get_pending_stored_size(&chunk_id)
                .unwrap();
            self.tree_pack_writer.increment_pending(&chunk_id);
            return Ok((chunk_id, stored_size, false));
        }

        // Compress
        let compressed = compress::compress(compression, data)?;
        let uncompressed_size = data.len() as u32;

        // Encrypt and wrap in repo object envelope
        let packed = pack_object(ObjectType::ChunkData, &compressed, self.crypto.as_ref())?;
        let stored_size = packed.len() as u32;

        // Add to the appropriate pack writer
        let writer = match pack_type {
            PackType::Data => &mut self.data_pack_writer,
            PackType::Tree => &mut self.tree_pack_writer,
        };
        writer.add_blob(
            ObjectType::ChunkData as u8,
            chunk_id,
            packed,
            uncompressed_size,
        );

        // Flush if target size reached
        if writer.should_flush() {
            // Need to flush — use the free function to avoid borrow issues
            match pack_type {
                PackType::Data => {
                    flush_writer(
                        &mut self.data_pack_writer,
                        self.storage.as_ref(),
                        self.crypto.as_ref(),
                        &mut self.chunk_index,
                    )?;
                }
                PackType::Tree => {
                    flush_writer(
                        &mut self.tree_pack_writer,
                        self.storage.as_ref(),
                        self.crypto.as_ref(),
                        &mut self.chunk_index,
                    )?;
                }
            }
        }

        Ok((chunk_id, stored_size, true))
    }

    /// Read and decrypt a chunk from the repository.
    pub fn read_chunk(&self, chunk_id: &ChunkId) -> Result<Vec<u8>> {
        let entry = self
            .chunk_index
            .get(chunk_id)
            .ok_or_else(|| VgerError::Other(format!("chunk not found: {chunk_id}")))?;

        let blob_data = read_blob_from_pack(
            self.storage.as_ref(),
            &entry.pack_id,
            entry.pack_offset,
            entry.stored_size,
        )?;

        let (_obj_type, compressed) = unpack_object(&blob_data, self.crypto.as_ref())?;
        compress::decompress(&compressed)
    }

    /// Flush all pending pack writes.
    pub fn flush_packs(&mut self) -> Result<()> {
        if self.data_pack_writer.has_pending() {
            flush_writer(
                &mut self.data_pack_writer,
                self.storage.as_ref(),
                self.crypto.as_ref(),
                &mut self.chunk_index,
            )?;
        }
        if self.tree_pack_writer.has_pending() {
            flush_writer(
                &mut self.tree_pack_writer,
                self.storage.as_ref(),
                self.crypto.as_ref(),
                &mut self.chunk_index,
            )?;
        }
        Ok(())
    }
}

/// Flush a single PackWriter and update the ChunkIndex with committed entries.
fn flush_writer(
    writer: &mut PackWriter,
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    index: &mut ChunkIndex,
) -> Result<()> {
    let (pack_id, entries) = writer.flush(storage, crypto)?;
    for (chunk_id, stored_size, offset, refcount) in entries {
        // Add entry with first ref, then increment for additional refs
        index.add(chunk_id, stored_size, pack_id, offset);
        for _ in 1..refcount {
            index.increment_refcount(&chunk_id);
        }
    }
    Ok(())
}
