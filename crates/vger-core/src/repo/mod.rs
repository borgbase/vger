pub mod file_cache;
pub mod format;
pub mod lock;
pub mod manifest;
pub mod pack;

use std::collections::{HashMap as StdHashMap, VecDeque};
use std::sync::Arc;
use std::thread::JoinHandle;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::compress;
use crate::config::{ChunkerConfig, RepositoryConfig};
use crate::crypto::chunk_id::ChunkId;
use crate::crypto::key::{EncryptedKey, MasterKey};
use crate::crypto::{self, CryptoEngine, PlaintextEngine};
use crate::error::{Result, VgerError};
use crate::index::{ChunkIndex, DedupIndex, IndexDelta};
use crate::storage::StorageBackend;

use self::file_cache::FileCache;
use self::format::{pack_object, unpack_object_expect, ObjectType};
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

/// Default maximum number of in-flight background pack uploads.
const DEFAULT_IN_FLIGHT_PACK_UPLOADS: usize = 4;

/// Maximum total weight (bytes) of cached blobs in the blob cache.
const BLOB_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024; // 32 MiB

/// FIFO blob cache bounded by total weight in bytes.
/// Caches decrypted+decompressed chunks to avoid redundant storage reads.
struct BlobCache {
    entries: StdHashMap<ChunkId, Vec<u8>>,
    order: VecDeque<ChunkId>,
    current_bytes: usize,
    max_bytes: usize,
}

impl BlobCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: StdHashMap::new(),
            order: VecDeque::new(),
            current_bytes: 0,
            max_bytes,
        }
    }

    fn get(&self, id: &ChunkId) -> Option<&[u8]> {
        self.entries.get(id).map(Vec::as_slice)
    }

    fn insert(&mut self, id: ChunkId, data: Vec<u8>) {
        let data_len = data.len();
        // Don't cache items larger than the entire cache
        if data_len > self.max_bytes {
            return;
        }
        // Evict oldest entries until there's room
        while self.current_bytes + data_len > self.max_bytes {
            if let Some(evicted_id) = self.order.pop_front() {
                if let Some(evicted_data) = self.entries.remove(&evicted_id) {
                    self.current_bytes -= evicted_data.len();
                }
            } else {
                break;
            }
        }
        self.current_bytes += data_len;
        self.entries.insert(id, data);
        self.order.push_back(id);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionMode {
    None,
    Aes256Gcm,
    Chacha20Poly1305,
}

impl EncryptionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            EncryptionMode::None => "none",
            EncryptionMode::Aes256Gcm => "aes256gcm",
            EncryptionMode::Chacha20Poly1305 => "chacha20poly1305",
        }
    }
}

/// A handle to an opened repository.
pub struct Repository {
    pub storage: Arc<dyn StorageBackend>,
    pub crypto: Box<dyn CryptoEngine>,
    pub manifest: Manifest,
    pub chunk_index: ChunkIndex,
    pub config: RepoConfig,
    pub file_cache: FileCache,
    data_pack_writer: PackWriter,
    tree_pack_writer: PackWriter,
    /// Background pack upload threads waiting to be joined.
    pending_uploads: VecDeque<JoinHandle<Result<()>>>,
    /// Lightweight dedup-only index used during backup to save memory.
    /// When active, `chunk_index` is empty and all lookups go through this.
    dedup_index: Option<DedupIndex>,
    /// Tracks index mutations while in dedup mode, applied at save time.
    index_delta: Option<IndexDelta>,
    /// Weight-bounded cache for decrypted chunks (used during restore).
    blob_cache: BlobCache,
    /// Configurable limit for in-flight background pack uploads.
    max_in_flight_uploads: usize,
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
        let storage: Arc<dyn StorageBackend> = Arc::from(storage);

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
                EncryptionMode::Chacha20Poly1305 => {
                    let master_key = MasterKey::generate();
                    let pass = passphrase.ok_or_else(|| {
                        VgerError::Config("passphrase required for encrypted repository".into())
                    })?;
                    let enc_key = master_key.to_encrypted(pass)?;
                    let engine = crypto::chacha20_poly1305::ChaCha20Poly1305Engine::new(
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
            file_cache: FileCache::new(),
            data_pack_writer: PackWriter::new(PackType::Data, data_target),
            tree_pack_writer: PackWriter::new(PackType::Tree, tree_target),
            pending_uploads: VecDeque::new(),
            dedup_index: None,
            index_delta: None,
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            max_in_flight_uploads: DEFAULT_IN_FLIGHT_PACK_UPLOADS,
        })
    }

    /// Open an existing repository.
    pub fn open(storage: Box<dyn StorageBackend>, passphrase: Option<&str>) -> Result<Self> {
        let storage: Arc<dyn StorageBackend> = Arc::from(storage);

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
            EncryptionMode::Chacha20Poly1305 => {
                let key_data = storage
                    .get("keys/repokey")?
                    .ok_or_else(|| VgerError::InvalidFormat("missing keys/repokey".into()))?;
                let enc_key: EncryptedKey = rmp_serde::from_slice(&key_data)?;
                let pass = passphrase.ok_or_else(|| {
                    VgerError::Config("passphrase required for encrypted repository".into())
                })?;
                let master_key = MasterKey::from_encrypted(&enc_key, pass)?;
                let engine = crypto::chacha20_poly1305::ChaCha20Poly1305Engine::new(
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
        let manifest_bytes =
            unpack_object_expect(&manifest_data, ObjectType::Manifest, crypto.as_ref())?;
        let manifest: Manifest = rmp_serde::from_slice(&manifest_bytes)?;

        // Read chunk index
        let chunk_index = if let Some(index_data) = storage.get("index")? {
            let index_bytes =
                unpack_object_expect(&index_data, ObjectType::ChunkIndex, crypto.as_ref())?;
            rmp_serde::from_slice(&index_bytes)?
        } else {
            ChunkIndex::new()
        };

        // Load file cache from local disk (not from the repo).
        let file_cache = FileCache::load(&repo_config.id, crypto.as_ref());

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
            file_cache,
            data_pack_writer: PackWriter::new(PackType::Data, data_target),
            tree_pack_writer: PackWriter::new(PackType::Tree, tree_target),
            pending_uploads: VecDeque::new(),
            dedup_index: None,
            index_delta: None,
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            max_in_flight_uploads: DEFAULT_IN_FLIGHT_PACK_UPLOADS,
        })
    }

    /// Wait for one background pack upload to finish (if any).
    fn wait_one_pending_upload(&mut self) -> Result<()> {
        if let Some(handle) = self.pending_uploads.pop_front() {
            handle
                .join()
                .map_err(|_| VgerError::Other("pack upload thread panicked".into()))??;
        }
        Ok(())
    }

    /// Wait for all background pack uploads to finish.
    fn wait_pending_uploads(&mut self) -> Result<()> {
        while !self.pending_uploads.is_empty() {
            self.wait_one_pending_upload()?;
        }
        Ok(())
    }

    /// Apply backpressure to keep the number of in-flight uploads bounded.
    fn cap_pending_uploads(&mut self) -> Result<()> {
        while self.pending_uploads.len() >= self.max_in_flight_uploads {
            self.wait_one_pending_upload()?;
        }
        Ok(())
    }

    /// Set the maximum number of in-flight background pack uploads.
    pub fn set_max_in_flight_uploads(&mut self, n: usize) {
        self.max_in_flight_uploads = n.max(1);
    }

    /// Switch to dedup-only index mode to reduce memory during backup.
    ///
    /// Builds a lightweight `DedupIndex` (chunk_id → stored_size only) from the
    /// full `ChunkIndex`, then drops the full index to reclaim memory. All
    /// mutations are recorded in an `IndexDelta` and merged back at save time.
    ///
    /// For 10M chunks this reduces steady-state memory from ~800 MB to ~450 MB.
    pub fn enable_dedup_mode(&mut self) {
        if self.dedup_index.is_some() {
            return; // already enabled
        }
        let dedup = DedupIndex::from_chunk_index(&self.chunk_index);
        // Drop the full index to reclaim memory
        self.chunk_index = ChunkIndex::new();
        self.dedup_index = Some(dedup);
        self.index_delta = Some(IndexDelta::new());
    }

    /// Check if a chunk exists in the index (works in both normal and dedup modes).
    pub fn chunk_exists(&self, id: &ChunkId) -> bool {
        if let Some(ref dedup) = self.dedup_index {
            return dedup.contains(id);
        }
        self.chunk_index.contains(id)
    }

    /// Increment the refcount for a chunk (works in both normal and dedup modes).
    pub fn increment_chunk_ref(&mut self, id: &ChunkId) {
        if let Some(ref mut delta) = self.index_delta {
            delta.bump_refcount(id);
            return;
        }
        self.chunk_index.increment_refcount(id);
    }

    /// Save the manifest and chunk index back to storage.
    /// Flushes any pending pack writes first.
    /// In dedup mode, reloads the full index from storage and applies the delta.
    pub fn save_state(&mut self) -> Result<()> {
        // Flush any pending packs
        self.flush_packs()?;

        // If in dedup mode, reload the full index and apply our delta
        if let Some(delta) = self.index_delta.take() {
            self.dedup_index = None;
            let mut full_index = self.reload_full_index()?;
            delta.apply_to(&mut full_index);
            self.chunk_index = full_index;
        }

        // Save manifest
        let manifest_bytes = rmp_serde::to_vec(&self.manifest)?;
        let manifest_packed =
            pack_object(ObjectType::Manifest, &manifest_bytes, self.crypto.as_ref())?;
        self.storage.put("manifest", &manifest_packed)?;

        // Save chunk index
        let index_bytes = rmp_serde::to_vec(&self.chunk_index)?;
        let index_packed = pack_object(ObjectType::ChunkIndex, &index_bytes, self.crypto.as_ref())?;
        self.storage.put("index", &index_packed)?;

        // Save file cache to local disk (not to the repo).
        self.file_cache
            .save(&self.config.id, self.crypto.as_ref())?;

        Ok(())
    }

    /// Reload the full chunk index from storage.
    fn reload_full_index(&self) -> Result<ChunkIndex> {
        if let Some(index_data) = self.storage.get("index")? {
            let index_bytes =
                unpack_object_expect(&index_data, ObjectType::ChunkIndex, self.crypto.as_ref())?;
            Ok(rmp_serde::from_slice(&index_bytes)?)
        } else {
            Ok(ChunkIndex::new())
        }
    }

    /// Increment refcount if this chunk already exists in committed or pending state.
    /// Returns stored size when found. Works in both normal and dedup modes.
    pub fn bump_ref_if_exists(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        // Dedup check against committed index (or dedup index in dedup mode)
        if let Some(ref dedup) = self.dedup_index {
            if let Some(stored_size) = dedup.get_stored_size(chunk_id) {
                if let Some(ref mut delta) = self.index_delta {
                    delta.bump_refcount(chunk_id);
                }
                return Some(stored_size);
            }
        } else if self.chunk_index.contains(chunk_id) {
            let stored_size = self.chunk_index.get(chunk_id).unwrap().stored_size;
            self.chunk_index.increment_refcount(chunk_id);
            return Some(stored_size);
        }

        // Dedup check against pending blobs in both pack writers
        if self.data_pack_writer.contains_pending(chunk_id) {
            let stored_size = self
                .data_pack_writer
                .get_pending_stored_size(chunk_id)
                .unwrap();
            self.data_pack_writer.increment_pending(chunk_id);
            return Some(stored_size);
        }
        if self.tree_pack_writer.contains_pending(chunk_id) {
            let stored_size = self
                .tree_pack_writer
                .get_pending_stored_size(chunk_id)
                .unwrap();
            self.tree_pack_writer.increment_pending(chunk_id);
            return Some(stored_size);
        }

        None
    }

    /// Commit a pre-compressed and pre-encrypted chunk to the selected pack writer.
    /// Returns the stored size in bytes.
    pub fn commit_prepacked_chunk(
        &mut self,
        chunk_id: ChunkId,
        packed: Vec<u8>,
        uncompressed_size: u32,
        pack_type: PackType,
    ) -> Result<u32> {
        let stored_size = packed.len() as u32;

        // Add blob and check flush in a scoped borrow
        let should_flush = {
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
            writer.should_flush()
        };

        if should_flush {
            self.flush_writer_async(pack_type)?;
        }

        Ok(stored_size)
    }

    /// Seal a pack writer and upload in the background.
    /// The index is updated immediately; the upload proceeds in a separate thread.
    fn flush_writer_async(&mut self, pack_type: PackType) -> Result<()> {
        // Keep upload fan-out bounded to avoid excessive memory/thread pressure.
        self.cap_pending_uploads()?;

        // Seal — each match arm borrows a specific field, allowing disjoint borrows with crypto
        let (pack_id, pack_bytes, entries) = match pack_type {
            PackType::Data => self.data_pack_writer.seal(self.crypto.as_ref())?,
            PackType::Tree => self.tree_pack_writer.seal(self.crypto.as_ref())?,
        };

        // Update index immediately — offsets and PackId are known before upload
        if self.dedup_index.is_some() {
            // Dedup mode: update lightweight index + record in delta
            for (chunk_id, stored_size, offset, refcount) in entries {
                if let Some(ref mut dedup) = self.dedup_index {
                    dedup.insert(chunk_id, stored_size);
                }
                if let Some(ref mut delta) = self.index_delta {
                    delta.add_new_entry(chunk_id, stored_size, pack_id, offset, refcount);
                }
            }
        } else {
            // Normal mode: update full index directly
            for (chunk_id, stored_size, offset, refcount) in entries {
                self.chunk_index.add(chunk_id, stored_size, pack_id, offset);
                for _ in 1..refcount {
                    self.chunk_index.increment_refcount(&chunk_id);
                }
            }
        }

        // Upload in background thread
        let storage = Arc::clone(&self.storage);
        let key = pack_id.storage_key();
        self.pending_uploads.push_back(std::thread::spawn(move || {
            storage.put_owned(&key, pack_bytes)
        }));

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

        if let Some(stored_size) = self.bump_ref_if_exists(&chunk_id) {
            return Ok((chunk_id, stored_size, false));
        }

        // Compress
        let compressed = compress::compress(compression, data)?;
        let uncompressed_size = data.len() as u32;

        // Encrypt and wrap in repo object envelope
        let packed = pack_object(ObjectType::ChunkData, &compressed, self.crypto.as_ref())?;
        let stored_size =
            self.commit_prepacked_chunk(chunk_id, packed, uncompressed_size, pack_type)?;

        Ok((chunk_id, stored_size, true))
    }

    /// Read and decrypt a chunk from the repository.
    /// Results are cached in a weight-bounded blob cache for faster repeated access.
    pub fn read_chunk(&mut self, chunk_id: &ChunkId) -> Result<Vec<u8>> {
        // Check cache first
        if let Some(cached) = self.blob_cache.get(chunk_id) {
            return Ok(cached.to_vec());
        }

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

        let compressed =
            unpack_object_expect(&blob_data, ObjectType::ChunkData, self.crypto.as_ref())?;
        let plaintext = compress::decompress(&compressed)?;

        // Cache the result
        self.blob_cache.insert(*chunk_id, plaintext.clone());
        Ok(plaintext)
    }

    /// Flush all pending pack writes and wait for background uploads.
    pub fn flush_packs(&mut self) -> Result<()> {
        if self.data_pack_writer.has_pending() {
            self.flush_writer_async(PackType::Data)?;
        }
        if self.tree_pack_writer.has_pending() {
            self.flush_writer_async(PackType::Tree)?;
        }
        // Wait for all background uploads to complete before returning.
        self.wait_pending_uploads()?;
        Ok(())
    }
}
