pub mod file_cache;
pub mod format;
pub mod lock;
pub mod manifest;
pub mod pack;

use std::collections::{HashMap as StdHashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use tracing::{debug, warn};

use crate::compress;
use crate::config::{
    default_max_pack_size, default_min_pack_size, ChunkerConfig, RepositoryConfig,
    DEFAULT_UPLOAD_CONCURRENCY,
};
use crate::index::dedup_cache::{self, TieredDedupIndex};
use crate::index::{
    ChunkIndex, DedupIndex, IndexDelta, PendingChunkEntry, PendingIndexJournal, RecoveredChunkEntry,
};
use vger_crypto::key::{EncryptedKey, MasterKey};
use vger_crypto::{self as crypto, CryptoEngine, PlaintextEngine};
use vger_storage::StorageBackend;
use vger_types::chunk_id::ChunkId;
use vger_types::error::{Result, VgerError};
use vger_types::pack_id::PackId;

use self::file_cache::FileCache;
use self::format::{
    pack_object_streaming_with_context, pack_object_with_context,
    unpack_object_expect_with_context, ObjectType,
};
use self::manifest::Manifest;
use self::pack::{
    compute_data_pack_target, compute_tree_pack_target, read_blob_from_pack, PackType, PackWriter,
    SealedPack,
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

/// Maximum total weight (bytes) of cached blobs in the blob cache.
const BLOB_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024; // 32 MiB

/// Extra upload handles allowed beyond `max_in_flight_uploads` before blocking.
const UPLOAD_QUEUE_HEADROOM: usize = 2;

const MANIFEST_OBJECT_CONTEXT: &[u8] = b"manifest";
const INDEX_OBJECT_CONTEXT: &[u8] = b"index";
const PENDING_INDEX_KEY: &str = "pending_index";
const PENDING_INDEX_OBJECT_CONTEXT: &[u8] = b"pending_index";

/// Number of new packs between debounced `pending_index` writes.
const JOURNAL_WRITE_INTERVAL: usize = 8;

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
    pub crypto: Arc<dyn CryptoEngine>,
    manifest: Manifest,
    chunk_index: ChunkIndex,
    pub config: RepoConfig,
    file_cache: FileCache,
    data_pack_writer: PackWriter,
    tree_pack_writer: PackWriter,
    /// Background pack upload threads waiting to be joined.
    pending_uploads: Vec<JoinHandle<Result<()>>>,
    /// Lightweight dedup-only index used during backup to save memory.
    /// When active, `chunk_index` is empty and all lookups go through this.
    dedup_index: Option<DedupIndex>,
    /// Three-tier dedup index (xor filter + mmap + session HashMap).
    /// When active, both `chunk_index` and `dedup_index` are empty.
    tiered_dedup: Option<TieredDedupIndex>,
    /// Tracks index mutations while in dedup mode, applied at save time.
    index_delta: Option<IndexDelta>,
    /// Weight-bounded cache for decrypted chunks (used during restore).
    blob_cache: BlobCache,
    /// Configurable limit for in-flight background pack uploads.
    max_in_flight_uploads: usize,
    /// Whether the manifest has been modified since last persist.
    manifest_dirty: bool,
    /// Whether the chunk index has been modified since last persist.
    index_dirty: bool,
    /// Whether the file cache has been modified since last persist.
    file_cache_dirty: bool,
    /// Whether to rebuild the local dedup cache at save time.
    rebuild_dedup_cache: bool,
    /// Override for the cache directory root (from config `cache_dir`).
    cache_dir_override: Option<PathBuf>,
    /// Journal of chunk→pack mappings for packs flushed in the current session.
    /// Written to storage periodically so interrupted backups can recover.
    pending_journal: PendingIndexJournal,
    /// Number of packs in journal when last written to storage (for debouncing).
    pending_journal_last_written: usize,
    /// Chunks recovered from a previous interrupted session's `pending_index`.
    /// Promoted into the active dedup structure on dedup hit.
    recovered_chunks: StdHashMap<ChunkId, RecoveredChunkEntry>,
    /// Number of distinct packs in the persisted index at load time.
    persisted_pack_count: usize,
    /// Number of packs flushed during the current session (data + tree).
    session_packs_flushed: usize,
}

impl Repository {
    /// Initialize a new repository.
    pub fn init(
        storage: Box<dyn StorageBackend>,
        encryption: EncryptionMode,
        chunker_params: ChunkerConfig,
        passphrase: Option<&str>,
        repo_config_opts: Option<&RepositoryConfig>,
        cache_dir: Option<PathBuf>,
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

        if max_pack_size > 512 * 1024 * 1024 {
            return Err(VgerError::Config(format!(
                "max_pack_size ({max_pack_size}) exceeds hard limit of 512 MiB"
            )));
        }

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
        let (crypto, encrypted_key): (Arc<dyn CryptoEngine>, Option<EncryptedKey>) =
            match &encryption {
                EncryptionMode::None => {
                    let mut chunk_id_key = [0u8; 32];
                    rng.fill_bytes(&mut chunk_id_key);
                    (Arc::new(PlaintextEngine::new(&chunk_id_key)), None)
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
                    (Arc::new(engine), Some(enc_key))
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
                    (Arc::new(engine), Some(enc_key))
                }
            };

        // Try server-side init (creates keys/, snapshots/, locks/, packs/* in one request).
        // Placed after all validation and crypto setup so a failure above doesn't
        // leave a partially scaffolded repo on the server.
        let server_did_init = match storage.server_init() {
            Ok(()) => true,
            Err(VgerError::UnsupportedBackend(_)) => false,
            Err(err) => return Err(err),
        };

        // Store config (unencrypted)
        let config_data = rmp_serde::to_vec(&repo_config)?;
        storage.put("config", &config_data)?;

        // Store encrypted key if applicable
        if let Some(enc_key) = &encrypted_key {
            if !server_did_init {
                storage.create_dir("keys/")?;
            }
            let key_data = rmp_serde::to_vec(enc_key)?;
            storage.put("keys/repokey", &key_data)?;
        }

        // Store empty manifest
        let manifest = Manifest::new();
        let manifest_bytes = rmp_serde::to_vec(&manifest)?;
        let manifest_packed = pack_object_with_context(
            ObjectType::Manifest,
            MANIFEST_OBJECT_CONTEXT,
            &manifest_bytes,
            crypto.as_ref(),
        )?;
        storage.put("manifest", &manifest_packed)?;

        // Store empty chunk index (compressed with ZSTD)
        let chunk_index = ChunkIndex::new();
        let index_packed = pack_object_streaming_with_context(
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            64, // empty index is tiny
            crypto.as_ref(),
            |buf| {
                compress::compress_stream_zstd(buf, 3, |encoder| {
                    rmp_serde::encode::write(encoder, &chunk_index)?;
                    Ok(())
                })
            },
        )?;
        storage.put("index", &index_packed)?;

        // Create directory structure (skip if server already did it)
        if !server_did_init {
            storage.create_dir("snapshots/")?;
            storage.create_dir("locks/")?;
            for i in 0u8..=255 {
                storage.create_dir(&format!("packs/{:02x}/", i))?;
            }
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
            pending_uploads: Vec::new(),
            dedup_index: None,
            tiered_dedup: None,
            index_delta: None,
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            max_in_flight_uploads: DEFAULT_UPLOAD_CONCURRENCY,
            manifest_dirty: false,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            pending_journal: PendingIndexJournal::new(),
            pending_journal_last_written: 0,
            recovered_chunks: StdHashMap::new(),
            persisted_pack_count: 0,
            session_packs_flushed: 0,
        })
    }

    /// Open an existing repository.
    pub fn open(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let mut repo = Self::open_base(storage, passphrase, cache_dir)?;
        repo.load_chunk_index()?;
        Ok(repo)
    }

    /// Open a repository without loading the chunk index.
    /// Suitable for read-only operations (restore, list) that either don't need
    /// the index or will load it lazily via `load_chunk_index()`.
    pub fn open_without_index(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::open_base(storage, passphrase, cache_dir)
    }

    /// Open a repository without loading the chunk index or file cache.
    /// Suitable for read-only operations (e.g. restore) that need neither.
    pub fn open_without_index_or_cache(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::open_base_inner(storage, passphrase, cache_dir, true)
    }

    /// Shared open logic: reads config, builds crypto, loads manifest and file cache.
    /// Does NOT load the chunk index — callers either load it themselves or skip it.
    fn open_base(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::open_base_inner(storage, passphrase, cache_dir, false)
    }

    fn open_base_inner(
        storage: Box<dyn StorageBackend>,
        passphrase: Option<&str>,
        cache_dir: Option<PathBuf>,
        skip_file_cache: bool,
    ) -> Result<Self> {
        let storage: Arc<dyn StorageBackend> = Arc::from(storage);

        // Read config
        let config_data = storage
            .get("config")?
            .ok_or_else(|| VgerError::RepoNotFound("config not found".into()))?;
        let repo_config: RepoConfig = rmp_serde::from_slice(&config_data)?;

        if repo_config.version != 1 {
            return Err(VgerError::UnsupportedVersion(repo_config.version));
        }

        if repo_config.max_pack_size > 512 * 1024 * 1024 {
            return Err(VgerError::Config(format!(
                "max_pack_size ({}) exceeds hard limit of 512 MiB",
                repo_config.max_pack_size
            )));
        }

        // Build crypto engine
        let crypto: Arc<dyn CryptoEngine> = match &repo_config.encryption {
            EncryptionMode::None => {
                let mut chunk_id_key = [0u8; 32];
                use blake2::digest::{Update, VariableOutput};
                use blake2::Blake2bVar;
                let mut hasher = Blake2bVar::new(32).unwrap();
                hasher.update(&repo_config.id);
                hasher.finalize_variable(&mut chunk_id_key).unwrap();
                Arc::new(PlaintextEngine::new(&chunk_id_key))
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
                Arc::new(engine)
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
                Arc::new(engine)
            }
        };

        // Read manifest
        let manifest_data = storage
            .get("manifest")?
            .ok_or_else(|| VgerError::InvalidFormat("missing manifest".into()))?;
        let manifest_bytes = unpack_object_expect_with_context(
            &manifest_data,
            ObjectType::Manifest,
            MANIFEST_OBJECT_CONTEXT,
            crypto.as_ref(),
        )?;
        let manifest: Manifest = rmp_serde::from_slice(&manifest_bytes)?;

        // Load file cache from local disk (not from the repo).
        let file_cache = if skip_file_cache {
            FileCache::new()
        } else {
            FileCache::load(&repo_config.id, crypto.as_ref(), cache_dir.as_deref())
        };

        // Use min_pack_size as default pack target (recalculated after index load).
        let data_target = repo_config.min_pack_size as usize;
        let tree_target = compute_tree_pack_target(repo_config.min_pack_size);

        Ok(Repository {
            storage,
            crypto,
            manifest,
            chunk_index: ChunkIndex::new(),
            config: repo_config,
            file_cache,
            data_pack_writer: PackWriter::new(PackType::Data, data_target),
            tree_pack_writer: PackWriter::new(PackType::Tree, tree_target),
            pending_uploads: Vec::new(),
            dedup_index: None,
            tiered_dedup: None,
            index_delta: None,
            blob_cache: BlobCache::new(BLOB_CACHE_MAX_BYTES),
            max_in_flight_uploads: DEFAULT_UPLOAD_CONCURRENCY,
            manifest_dirty: false,
            index_dirty: false,
            file_cache_dirty: false,
            rebuild_dedup_cache: false,
            cache_dir_override: cache_dir,
            pending_journal: PendingIndexJournal::new(),
            pending_journal_last_written: 0,
            recovered_chunks: StdHashMap::new(),
            persisted_pack_count: 0,
            session_packs_flushed: 0,
        })
    }

    /// Load the chunk index from storage on demand (using local blob cache).
    /// Can be called after `open_without_index()` to lazily load the index.
    /// Also recalculates the data pack writer target from the loaded index.
    pub fn load_chunk_index(&mut self) -> Result<()> {
        self.chunk_index = self.reload_full_index_cached()?;
        self.rebase_pack_target_from_index();
        Ok(())
    }

    /// Load the chunk index from storage, bypassing the local blob cache.
    /// Use this for operations like `check` that must verify what's actually
    /// in the remote repository.
    pub fn load_chunk_index_uncached(&mut self) -> Result<()> {
        self.chunk_index = self.reload_full_index()?;
        self.rebase_pack_target_from_index();
        Ok(())
    }

    /// Recompute pack-target state from `self.chunk_index`.
    ///
    /// Sets `persisted_pack_count` from the index, resets `session_packs_flushed`,
    /// and updates the data pack writer's target size. Called after any operation
    /// that brings `chunk_index` in sync with persisted storage (load or save).
    // TODO: count_distinct_packs() includes tree packs. Should filter to data
    // packs only for more accurate pack target sizing.
    fn rebase_pack_target_from_index(&mut self) {
        let num_packs = self.chunk_index.count_distinct_packs();
        self.persisted_pack_count = num_packs;
        self.session_packs_flushed = 0;
        let data_target = compute_data_pack_target(
            num_packs,
            self.config.min_pack_size,
            self.config.max_pack_size,
        );
        self.data_pack_writer.set_target_size(data_target);
    }

    /// Mark the manifest as needing persistence on the next `save_state()`.
    pub fn mark_manifest_dirty(&mut self) {
        self.manifest_dirty = true;
    }

    /// Mark the chunk index as needing persistence on the next `save_state()`.
    pub fn mark_index_dirty(&mut self) {
        self.index_dirty = true;
    }

    /// Mark the file cache as needing persistence on the next `save_state()`.
    pub fn mark_file_cache_dirty(&mut self) {
        self.file_cache_dirty = true;
    }

    /// Try to open the mmap'd restore cache for this repository.
    /// Returns `None` if the cache is missing, stale, or corrupt.
    pub fn open_restore_cache(&self) -> Option<dedup_cache::MmapRestoreCache> {
        dedup_cache::MmapRestoreCache::open(
            &self.config.id,
            self.manifest.index_generation,
            self.cache_dir_override.as_deref(),
        )
    }

    // ----- Accessors for private fields -----

    /// Read-only access to the manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Mutable access to the manifest. Automatically marks it dirty.
    pub fn manifest_mut(&mut self) -> &mut Manifest {
        self.manifest_dirty = true;
        &mut self.manifest
    }

    /// Read-only access to the chunk index.
    pub fn chunk_index(&self) -> &ChunkIndex {
        &self.chunk_index
    }

    /// Current data pack target size in bytes (for testing).
    #[cfg(test)]
    pub(crate) fn data_pack_target(&self) -> usize {
        self.data_pack_writer.target_size()
    }

    /// Mutable access to the chunk index. Automatically marks it dirty.
    pub fn chunk_index_mut(&mut self) -> &mut ChunkIndex {
        self.index_dirty = true;
        &mut self.chunk_index
    }

    /// Replace the chunk index with an empty one (frees memory).
    /// Does not mark dirty — intended for memory optimization (e.g. restore).
    pub fn clear_chunk_index(&mut self) {
        self.chunk_index = ChunkIndex::new();
    }

    /// Filter the chunk index to only retain entries for the given chunks.
    /// Does not mark dirty — this is a read-only memory optimization.
    pub fn retain_chunk_index(
        &mut self,
        needed: &std::collections::HashSet<vger_types::chunk_id::ChunkId>,
    ) {
        self.chunk_index.retain_chunks(needed);
    }

    /// Read-only access to the file cache.
    pub fn file_cache(&self) -> &FileCache {
        &self.file_cache
    }

    /// Temporarily take the file cache out of the repository.
    /// Does not mark dirty — use `restore_file_cache` to put it back,
    /// or `set_file_cache` to replace it (which marks dirty).
    pub fn take_file_cache(&mut self) -> FileCache {
        std::mem::take(&mut self.file_cache)
    }

    /// Put a previously-taken file cache back without marking dirty.
    pub fn restore_file_cache(&mut self, cache: FileCache) {
        self.file_cache = cache;
    }

    /// Replace the file cache and mark it dirty.
    pub fn set_file_cache(&mut self, cache: FileCache) {
        self.file_cache = cache;
        self.file_cache_dirty = true;
    }

    /// Join all finished upload threads, propagating the first error.
    fn drain_finished_uploads(&mut self) -> Result<()> {
        let mut i = 0;
        while i < self.pending_uploads.len() {
            if self.pending_uploads[i].is_finished() {
                let handle = self.pending_uploads.swap_remove(i);
                handle
                    .join()
                    .map_err(|_| VgerError::Other("pack upload thread panicked".into()))??;
                // Don't increment i — swap_remove moved last element to position i
            } else {
                i += 1;
            }
        }
        self.maybe_write_pending_index();
        Ok(())
    }

    /// Wait for all background pack uploads to finish.
    fn wait_pending_uploads(&mut self) -> Result<()> {
        let mut first_err: Option<VgerError> = None;
        for handle in self.pending_uploads.drain(..) {
            let res = handle
                .join()
                .map_err(|_| VgerError::Other("pack upload thread panicked".into()))
                .and_then(|r| r);
            if first_err.is_none() {
                if let Err(e) = res {
                    first_err = Some(e);
                }
            }
        }
        // Final flush of journal before returning (best-effort).
        if !self.pending_journal.is_empty() {
            self.write_pending_index_best_effort();
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Apply backpressure to keep the number of in-flight uploads bounded.
    fn cap_pending_uploads(&mut self) -> Result<()> {
        self.drain_finished_uploads()?;
        if self.pending_uploads.len()
            >= self
                .max_in_flight_uploads
                .saturating_add(UPLOAD_QUEUE_HEADROOM)
        {
            // All slots + buffer full — block on one handle.
            let handle = self.pending_uploads.swap_remove(0);
            handle
                .join()
                .map_err(|_| VgerError::Other("pack upload thread panicked".into()))??;
            self.maybe_write_pending_index();
        }
        Ok(())
    }

    /// Set the maximum number of in-flight background pack uploads.
    pub fn set_max_in_flight_uploads(&mut self, n: usize) {
        self.max_in_flight_uploads = n.max(1);
    }

    /// Replace the blob cache with a new one of the given capacity.
    pub fn set_blob_cache_max_bytes(&mut self, max_bytes: usize) {
        self.blob_cache = BlobCache::new(max_bytes);
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

    /// Switch to tiered dedup mode for minimal memory usage during backup.
    ///
    /// Tries to open a local mmap'd dedup cache validated against the manifest's
    /// `index_generation`. On success: builds an xor filter, drops the full
    /// `ChunkIndex`, and routes all lookups through the three-tier structure
    /// (~12 MB RSS for 10M chunks instead of ~680 MB).
    ///
    /// On failure (no cache, stale generation, corrupt file): falls back to the
    /// existing `DedupIndex` HashMap path.
    pub fn enable_tiered_dedup_mode(&mut self) {
        if self.tiered_dedup.is_some() || self.dedup_index.is_some() {
            return; // already in a dedup mode
        }

        self.rebuild_dedup_cache = true;
        let generation = self.manifest.index_generation;
        if let Some(mmap_cache) = dedup_cache::MmapDedupCache::open(
            &self.config.id,
            generation,
            self.cache_dir_override.as_deref(),
        ) {
            let tiered = TieredDedupIndex::new(mmap_cache);
            debug!(?tiered, "tiered dedup mode: using mmap cache");
            // Drop the full index to reclaim memory.
            self.chunk_index = ChunkIndex::new();
            self.tiered_dedup = Some(tiered);
            self.index_delta = Some(IndexDelta::new());
        } else {
            debug!("tiered dedup mode: no valid cache, falling back to DedupIndex");
            self.enable_dedup_mode();
        }
    }

    /// Return the pre-built xor filter from whichever dedup mode is active.
    /// Returns `None` for truly empty repos (first backup) or when no dedup mode is active.
    pub fn dedup_filter(&self) -> Option<std::sync::Arc<xorf::Xor8>> {
        if let Some(ref tiered) = self.tiered_dedup {
            return tiered.xor_filter();
        }
        if let Some(ref dedup) = self.dedup_index {
            return dedup.xor_filter();
        }
        None
    }

    /// Check if a chunk exists in the index (works in normal, dedup, and tiered modes).
    pub fn chunk_exists(&self, id: &ChunkId) -> bool {
        if let Some(ref tiered) = self.tiered_dedup {
            return tiered.contains(id);
        }
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
        self.index_dirty = true;
    }

    /// Save the manifest and chunk index back to storage.
    /// Flushes any pending pack writes first.
    /// Only writes components that have been marked dirty.
    /// In dedup mode, reloads the full index from storage and applies the delta.
    pub fn save_state(&mut self) -> Result<()> {
        // Flush any pending packs
        self.flush_packs()?;

        // Drop tiered dedup index (releases mmap) before reloading full index.
        self.tiered_dedup.take();
        // If in dedup mode (either tiered or HashMap), reload the full index
        // and apply our delta. Only mark index_dirty if the delta has mutations.
        //
        // On the fast path (incremental update succeeded), we defer chunk_index
        // hydration until after file_cache.save() so the ~42M index and the
        // ~89M file_cache serialization buffer don't coexist at peak.
        let mut deferred_index_load = false;

        if let Some(delta) = self.index_delta.take() {
            self.dedup_index = None;

            if !delta.is_empty() {
                // Non-empty delta: try incremental update first
                let fast_ok = self
                    .try_incremental_index_update(&delta)
                    .unwrap_or_else(|e| {
                        warn!("incremental index update failed: {e}");
                        false
                    });

                if fast_ok {
                    // Fast path succeeded — index uploaded, caches rebuilt,
                    // manifest.index_generation and manifest_dirty already set.
                    // Defer chunk_index hydration to reduce peak memory.
                    self.rebuild_dedup_cache = false;
                    deferred_index_load = true;
                } else {
                    // Slow path: full HashMap (first run, stale cache, error)
                    let mut full_index = self.reload_full_index()?;
                    delta.apply_to(&mut full_index);
                    self.chunk_index = full_index;
                    self.index_dirty = true;
                }
            } else if self.rebuild_dedup_cache {
                // Empty delta: index unchanged, but caches may need rebuilding.
                // Try to rebuild caches from full_index_cache if available.
                let mut rebuilt_from_cache = false;
                let cd = self.cache_dir_override.as_deref();
                if let Some(cache_path) = dedup_cache::full_index_cache_path(&self.config.id, cd) {
                    if dedup_cache::MmapFullIndexCache::open(
                        &self.config.id,
                        self.manifest.index_generation,
                        cd,
                    )
                    .is_some()
                    {
                        let gen = self.manifest.index_generation;
                        let id = &self.config.id;
                        let dedup_ok = dedup_cache::build_dedup_cache_from_full_cache(
                            &cache_path,
                            gen,
                            id,
                            cd,
                        )
                        .is_ok();
                        let restore_ok = dedup_cache::build_restore_cache_from_full_cache(
                            &cache_path,
                            gen,
                            id,
                            cd,
                        )
                        .is_ok();
                        if dedup_ok && restore_ok {
                            self.rebuild_dedup_cache = false;
                            rebuilt_from_cache = true;
                            // Defer chunk_index hydration to reduce peak memory.
                            deferred_index_load = true;
                        } else {
                            warn!(
                                "cache rebuild from full_index_cache partially failed, falling back"
                            );
                        }
                    }
                }
                if !rebuilt_from_cache {
                    // No valid full cache — must reload full index for slow-path cache rebuild.
                    self.chunk_index = self.reload_full_index()?;
                }
            } else {
                // Empty delta, no cache rebuild needed.
                // Still must restore chunk_index for postcondition (deferred).
                deferred_index_load = true;
            }
        }

        // When the index changes, rotate index_generation so the local dedup
        // cache is invalidated.  Must happen before the manifest write below.
        if self.index_dirty {
            self.manifest.index_generation = rand::thread_rng().next_u64();
            self.manifest_dirty = true;
        }

        if self.manifest_dirty {
            let manifest_bytes = rmp_serde::to_vec(&self.manifest)?;
            let manifest_packed = pack_object_with_context(
                ObjectType::Manifest,
                MANIFEST_OBJECT_CONTEXT,
                &manifest_bytes,
                self.crypto.as_ref(),
            )?;
            self.storage.put("manifest", &manifest_packed)?;
            self.manifest_dirty = false;
        }

        if self.index_dirty {
            let estimated_msgpack = self.chunk_index.len().saturating_mul(80);
            let estimated = 1 + zstd::zstd_safe::compress_bound(estimated_msgpack);
            let index_packed = pack_object_streaming_with_context(
                ObjectType::ChunkIndex,
                INDEX_OBJECT_CONTEXT,
                estimated,
                self.crypto.as_ref(),
                |buf| {
                    compress::compress_stream_zstd(buf, 3, |encoder| {
                        rmp_serde::encode::write(encoder, &self.chunk_index)?;
                        Ok(())
                    })
                },
            )?;
            self.storage.put("index", &index_packed)?;
            self.index_dirty = false;
        }

        // Rebuild the local dedup cache for next backup so tiered mode can
        // activate. Written on first backup (bootstrap) and every subsequent
        // backup that used tiered/dedup mode. Non-fatal on error.
        if self.rebuild_dedup_cache {
            let cd = self.cache_dir_override.as_deref();
            if let Err(e) = dedup_cache::build_dedup_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild dedup cache: {e}");
            }
            if let Err(e) = dedup_cache::build_restore_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to rebuild restore cache: {e}");
            }
            // Also build the full index cache for next incremental update.
            if let Err(e) = dedup_cache::build_full_index_cache(
                &self.chunk_index,
                self.manifest.index_generation,
                &self.config.id,
                cd,
            ) {
                warn!("failed to build full index cache: {e}");
            }
            self.rebuild_dedup_cache = false;
        }

        // Save file cache before hydrating chunk_index to reduce peak memory.
        // Capture error instead of early-returning so we can hydrate first.
        let fc_result = if self.file_cache_dirty {
            match self.file_cache.save(
                &self.config.id,
                self.crypto.as_ref(),
                self.cache_dir_override.as_deref(),
            ) {
                Ok(()) => {
                    self.file_cache_dirty = false;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        };

        // Always hydrate chunk_index — postcondition: self.chunk_index is valid
        // on all exit paths (success and error).
        if deferred_index_load {
            // Try local full_index_cache first (fast, no storage round-trip),
            // fall back to reloading from remote storage if cache is unavailable.
            self.chunk_index = dedup_cache::load_chunk_index_from_full_cache(
                &self.config.id,
                self.manifest.index_generation,
                self.cache_dir_override.as_deref(),
            )
            .or_else(|_| self.reload_full_index())?;
        }

        // Now propagate any file cache save error
        fc_result?;

        // Reset pending journal state — save_state succeeded, so all entries
        // are now in the persisted index. The pending_index file itself is
        // deleted later by clear_pending_index() from the backup command.
        self.pending_journal = PendingIndexJournal::new();
        self.pending_journal_last_written = 0;
        self.recovered_chunks.clear();

        // Rebase pack counters and target from the now-current chunk_index
        // so a reused Repository doesn't double-count session packs.
        self.rebase_pack_target_from_index();

        Ok(())
    }

    /// Try to perform an incremental index update using the local full_index_cache.
    /// Returns `Ok(true)` on success (index uploaded, caches rebuilt, manifest updated).
    /// Returns `Ok(false)` if the cache is missing or stale (caller should use slow path).
    fn try_incremental_index_update(&mut self, delta: &IndexDelta) -> Result<bool> {
        let cd = self.cache_dir_override.as_deref();
        let cache_path = match dedup_cache::full_index_cache_path(&self.config.id, cd) {
            Some(p) => p,
            None => return Ok(false),
        };

        let old_cache = match dedup_cache::MmapFullIndexCache::open(
            &self.config.id,
            self.manifest.index_generation,
            cd,
        ) {
            Some(c) => c,
            None => return Ok(false),
        };

        debug!(
            old_entries = old_cache.entry_count(),
            new_entries = delta.new_entries.len(),
            refcount_bumps = delta.refcount_bumps.len(),
            "incremental index update: merging delta"
        );

        let new_gen = rand::thread_rng().next_u64();

        // Merge old cache + delta → new cache file
        dedup_cache::merge_full_index_cache(&old_cache, delta, new_gen, &cache_path)?;

        // Drop old mmap before we open the new one
        drop(old_cache);

        // Open the newly merged cache for serialization
        let new_cache = dedup_cache::MmapFullIndexCache::open_path(&cache_path, new_gen)
            .ok_or_else(|| {
                vger_types::error::VgerError::Other(
                    "failed to open newly merged full index cache".into(),
                )
            })?;

        // Serialize from cache → encrypted packed object
        let packed =
            dedup_cache::serialize_full_cache_to_packed_object(&new_cache, self.crypto.as_ref())?;

        // Upload
        self.storage.put("index", &packed)?;

        // Free upload buffer
        drop(packed);

        // Rebuild dedup + restore caches from full cache (streaming)
        if let Err(e) = dedup_cache::build_dedup_cache_from_full_cache(
            &cache_path,
            new_gen,
            &self.config.id,
            cd,
        ) {
            warn!("failed to rebuild dedup cache from full cache: {e}");
        }
        if let Err(e) = dedup_cache::build_restore_cache_from_full_cache(
            &cache_path,
            new_gen,
            &self.config.id,
            cd,
        ) {
            warn!("failed to rebuild restore cache from full cache: {e}");
        }

        // Update manifest generation — must happen before return so
        // load_chunk_index_from_full_cache uses the correct generation.
        self.manifest.index_generation = new_gen;
        self.manifest_dirty = true;

        Ok(true)
    }

    /// Reload the full chunk index from storage (always downloads from remote).
    fn reload_full_index(&self) -> Result<ChunkIndex> {
        if let Some(index_data) = self.storage.get("index")? {
            Self::decode_index_blob(&index_data, self.crypto.as_ref())
        } else {
            Ok(ChunkIndex::new())
        }
    }

    /// Load the index blob, trying the local blob cache first.
    /// Falls back to remote download on cache miss.
    fn load_index_blob_cached(&self) -> Result<Option<Vec<u8>>> {
        let generation = self.manifest.index_generation;
        let cache_dir = self.cache_dir_override.as_deref();

        // Try local blob cache
        if let Some(blob) =
            dedup_cache::read_index_blob_cache(&self.config.id, generation, cache_dir)
        {
            debug!("index blob cache hit (generation {generation})");
            return Ok(Some(blob));
        }

        // Cache miss — download from remote
        let Some(blob) = self.storage.get("index")? else {
            return Ok(None);
        };

        // Save to local cache (non-fatal on error)
        if let Err(e) =
            dedup_cache::write_index_blob_cache(&blob, generation, &self.config.id, cache_dir)
        {
            debug!("failed to write index blob cache: {e}");
        }

        Ok(Some(blob))
    }

    /// Reload the full chunk index, trying the local blob cache first.
    /// Falls back to remote download if the cached blob is corrupt.
    fn reload_full_index_cached(&self) -> Result<ChunkIndex> {
        if let Some(index_data) = self.load_index_blob_cached()? {
            match Self::decode_index_blob(&index_data, self.crypto.as_ref()) {
                Ok(index) => return Ok(index),
                Err(e) => {
                    warn!("index blob cache corrupt, falling back to remote: {e}");
                    // Fall through to uncached remote download
                }
            }
        } else {
            return Ok(ChunkIndex::new());
        }

        // Cached blob was corrupt — download fresh and rewrite the cache
        let Some(blob) = self.storage.get("index")? else {
            return Ok(ChunkIndex::new());
        };
        let index = Self::decode_index_blob(&blob, self.crypto.as_ref())?;

        if let Err(e) = dedup_cache::write_index_blob_cache(
            &blob,
            self.manifest.index_generation,
            &self.config.id,
            self.cache_dir_override.as_deref(),
        ) {
            debug!("failed to rewrite index blob cache: {e}");
        }

        Ok(index)
    }

    /// Decrypt, decompress, and deserialize an index blob.
    fn decode_index_blob(index_data: &[u8], crypto: &dyn CryptoEngine) -> Result<ChunkIndex> {
        let compressed = unpack_object_expect_with_context(
            index_data,
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            crypto,
        )?;
        let index_bytes = compress::decompress_metadata(&compressed)?;
        Ok(rmp_serde::from_slice(&index_bytes)?)
    }

    /// Increment refcount if this chunk already exists in committed or pending state.
    /// Returns stored size when found. Works in normal, dedup, and tiered modes.
    /// Falls back to recovered chunks from a previous interrupted session.
    pub fn bump_ref_if_exists(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        // Tiered dedup mode: check xor filter + mmap + session_new
        if let Some(ref tiered) = self.tiered_dedup {
            if let Some(stored_size) = tiered.get_stored_size(chunk_id) {
                if let Some(ref mut delta) = self.index_delta {
                    delta.bump_refcount(chunk_id);
                }
                return Some(stored_size);
            }
        } else if let Some(ref dedup) = self.dedup_index {
            // DedupIndex HashMap mode
            if let Some(stored_size) = dedup.get_stored_size(chunk_id) {
                if let Some(ref mut delta) = self.index_delta {
                    delta.bump_refcount(chunk_id);
                }
                return Some(stored_size);
            }
        } else if let Some(entry) = self.chunk_index.get(chunk_id) {
            let stored_size = entry.stored_size;
            self.chunk_index.increment_refcount(chunk_id);
            self.index_dirty = true;
            return Some(stored_size);
        }

        // Check recovered chunks before pending pack writers.
        if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
            return Some(stored_size);
        }

        self.bump_ref_pending(chunk_id)
    }

    /// Prefilter said "probably exists" — tiered: skip xor, check session_new → mmap → pending.
    /// Non-tiered: falls through to bump_ref_if_exists.
    pub fn bump_ref_prefilter_hit(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        if let Some(ref tiered) = self.tiered_dedup {
            if let Some(stored_size) = tiered.get_stored_size_skip_filter(chunk_id) {
                if let Some(ref mut delta) = self.index_delta {
                    delta.bump_refcount(chunk_id);
                }
                return Some(stored_size);
            }
            if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
                return Some(stored_size);
            }
            return self.bump_ref_pending(chunk_id);
        }
        self.bump_ref_if_exists(chunk_id)
    }

    /// Prefilter said "definitely doesn't exist" — tiered: session_new → pending only.
    /// Non-tiered: falls through to bump_ref_if_exists.
    pub fn bump_ref_prefilter_miss(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        if let Some(ref tiered) = self.tiered_dedup {
            if let Some(stored_size) = tiered.session_new_stored_size(chunk_id) {
                if let Some(ref mut delta) = self.index_delta {
                    delta.bump_refcount(chunk_id);
                }
                return Some(stored_size);
            }
            if let Some(stored_size) = self.promote_recovered_chunk(chunk_id) {
                return Some(stored_size);
            }
            return self.bump_ref_pending(chunk_id);
        }
        self.bump_ref_if_exists(chunk_id)
    }

    /// Check only pending pack writers (shared helper).
    fn bump_ref_pending(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        if let Some(s) = self.data_pack_writer.get_pending_stored_size(chunk_id) {
            self.data_pack_writer.increment_pending(chunk_id);
            return Some(s);
        }
        if let Some(s) = self.tree_pack_writer.get_pending_stored_size(chunk_id) {
            self.tree_pack_writer.increment_pending(chunk_id);
            return Some(s);
        }
        None
    }

    /// Inline false-positive path: compress + encrypt + commit a chunk whose ChunkId
    /// was already computed by the worker. Avoids re-hashing via `store_chunk`.
    pub fn commit_chunk_inline(
        &mut self,
        chunk_id: ChunkId,
        data: &[u8],
        compression: compress::Compression,
        pack_type: PackType,
    ) -> Result<u32> {
        debug_assert_eq!(
            ChunkId::compute(self.crypto.chunk_id_key(), data),
            chunk_id,
            "inline commit: chunk_id mismatch"
        );
        let compressed = compress::compress(compression, data)?;
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &chunk_id.0,
            &compressed,
            self.crypto.as_ref(),
        )?;
        self.commit_prepacked_chunk(chunk_id, packed, pack_type)
    }

    /// Commit a pre-compressed and pre-encrypted chunk to the selected pack writer.
    /// Returns the stored size in bytes.
    pub fn commit_prepacked_chunk(
        &mut self,
        chunk_id: ChunkId,
        packed: Vec<u8>,
        pack_type: PackType,
    ) -> Result<u32> {
        let stored_size = packed.len() as u32;

        // Add blob and check flush in a scoped borrow
        let should_flush = {
            let writer = match pack_type {
                PackType::Data => &mut self.data_pack_writer,
                PackType::Tree => &mut self.tree_pack_writer,
            };
            writer.add_blob(chunk_id, packed)?;
            writer.should_flush()
        };

        if should_flush {
            self.flush_writer_async(pack_type)?;
        }

        Ok(stored_size)
    }

    /// Update index entries for a freshly sealed pack.
    /// Extracted from `flush_writer_async` to share between pool and non-pool paths.
    fn apply_sealed_entries(&mut self, pack_id: PackId, entries: Vec<pack::PackedChunkEntry>) {
        if self.tiered_dedup.is_some() {
            for (chunk_id, stored_size, offset, refcount) in entries {
                if let Some(ref mut tiered) = self.tiered_dedup {
                    tiered.insert(chunk_id, stored_size);
                }
                if let Some(ref mut delta) = self.index_delta {
                    delta.add_new_entry(chunk_id, stored_size, pack_id, offset, refcount);
                }
            }
        } else if self.dedup_index.is_some() {
            for (chunk_id, stored_size, offset, refcount) in entries {
                if let Some(ref mut dedup) = self.dedup_index {
                    dedup.insert(chunk_id, stored_size);
                }
                if let Some(ref mut delta) = self.index_delta {
                    delta.add_new_entry(chunk_id, stored_size, pack_id, offset, refcount);
                }
            }
        } else {
            for (chunk_id, stored_size, offset, refcount) in entries {
                self.chunk_index.add(chunk_id, stored_size, pack_id, offset);
                for _ in 1..refcount {
                    self.chunk_index.increment_refcount(&chunk_id);
                }
            }
            self.index_dirty = true;
        }
    }

    /// Seal a pack writer and upload in the background.
    /// The index is updated immediately; the upload proceeds in a separate thread.
    ///
    /// The `SealedPack` is destructured: `entries` consumed on the main thread
    /// for index updates, `data` (owning the mmap or Vec) moved into the upload
    /// thread. `pack_id` is `Copy` so it's used in both places.
    fn flush_writer_async(&mut self, pack_type: PackType) -> Result<()> {
        // Keep upload fan-out bounded to avoid excessive memory/thread pressure.
        self.cap_pending_uploads()?;

        let SealedPack {
            pack_id,
            entries,
            data,
        } = match pack_type {
            PackType::Data => self.data_pack_writer.seal()?,
            PackType::Tree => self.tree_pack_writer.seal()?,
        };

        // Recalculate data pack target after each data pack flush.
        if pack_type == PackType::Data {
            self.session_packs_flushed += 1;
            let total = self.persisted_pack_count + self.session_packs_flushed;
            let new_target = compute_data_pack_target(
                total,
                self.config.min_pack_size,
                self.config.max_pack_size,
            );
            self.data_pack_writer.set_target_size(new_target);
        }

        // Record journal entries before apply_sealed_entries consumes them.
        let journal_chunks: Vec<PendingChunkEntry> = entries
            .iter()
            .map(
                |&(chunk_id, stored_size, offset, _refcount)| PendingChunkEntry {
                    chunk_id,
                    stored_size,
                    pack_offset: offset,
                },
            )
            .collect();
        self.pending_journal.record_pack(pack_id, journal_chunks);

        self.apply_sealed_entries(pack_id, entries);

        let storage = Arc::clone(&self.storage);
        let key = pack_id.storage_key();
        self.pending_uploads
            .push(std::thread::spawn(move || data.put_to(&*storage, &key)));

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

        // Encrypt and wrap in repo object envelope
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &chunk_id.0,
            &compressed,
            self.crypto.as_ref(),
        )?;
        let stored_size = self.commit_prepacked_chunk(chunk_id, packed, pack_type)?;

        Ok((chunk_id, stored_size, true))
    }

    /// Read and decrypt a chunk from the repository.
    /// Results are cached in a weight-bounded blob cache for faster repeated access.
    pub fn read_chunk(&mut self, chunk_id: &ChunkId) -> Result<Vec<u8>> {
        let entry = *self
            .chunk_index
            .get(chunk_id)
            .ok_or_else(|| VgerError::Other(format!("chunk not found: {chunk_id}")))?;

        self.read_chunk_at(
            chunk_id,
            &entry.pack_id,
            entry.pack_offset,
            entry.stored_size,
        )
    }

    /// Read and decrypt a chunk given explicit pack location coordinates.
    /// Bypasses the chunk index — the caller supplies (pack_id, offset, stored_size)
    /// e.g. from the mmap restore cache.
    pub fn read_chunk_at(
        &mut self,
        chunk_id: &ChunkId,
        pack_id: &PackId,
        pack_offset: u64,
        stored_size: u32,
    ) -> Result<Vec<u8>> {
        if let Some(cached) = self.blob_cache.get(chunk_id) {
            return Ok(cached.to_vec());
        }

        let blob_data =
            read_blob_from_pack(self.storage.as_ref(), pack_id, pack_offset, stored_size)?;
        let compressed = unpack_object_expect_with_context(
            &blob_data,
            ObjectType::ChunkData,
            &chunk_id.0,
            self.crypto.as_ref(),
        )?;
        let plaintext = compress::decompress(&compressed)?;

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

    /// Best-effort cleanup after a failed backup or other operation.
    ///
    /// Seals any partial pack writers, waits for in-flight uploads to land,
    /// and writes the final `pending_index` journal so a subsequent run can
    /// recover. All errors are logged but never propagated.
    ///
    /// No-ops when there is nothing to clean up (no pending uploads, no
    /// journal entries, no partial packs).
    pub fn flush_on_abort(&mut self) {
        let has_partial_packs =
            self.data_pack_writer.has_pending() || self.tree_pack_writer.has_pending();
        if self.pending_uploads.is_empty() && self.pending_journal.is_empty() && !has_partial_packs
        {
            return;
        }

        warn!("saving progress for next run\u{2026}");

        // Seal and flush any partial data/tree pack writers.
        if self.data_pack_writer.has_pending() {
            if let Err(e) = self.flush_writer_async(PackType::Data) {
                warn!("flush_on_abort: failed to seal data pack: {e}");
            }
        }
        if self.tree_pack_writer.has_pending() {
            if let Err(e) = self.flush_writer_async(PackType::Tree) {
                warn!("flush_on_abort: failed to seal tree pack: {e}");
            }
        }

        // Join all in-flight upload threads so packs land on storage.
        for handle in self.pending_uploads.drain(..) {
            match handle
                .join()
                .map_err(|_| VgerError::Other("pack upload thread panicked".into()))
                .and_then(|r| r)
            {
                Ok(()) => {}
                Err(e) => warn!("flush_on_abort: upload thread failed: {e}"),
            }
        }

        // Write final pending_index so next run can recover.
        self.write_pending_index_best_effort();
    }

    // --- Pending index journal (interrupted backup recovery) ---

    /// Write the pending index journal to storage (debounced helper).
    /// Only writes if enough new packs have accumulated since the last write.
    fn maybe_write_pending_index(&mut self) {
        let current = self.pending_journal.len();
        if current >= self.pending_journal_last_written + JOURNAL_WRITE_INTERVAL {
            self.write_pending_index_best_effort();
        }
    }

    /// Serialize and write the pending index journal to storage.
    /// Best-effort: logs a warning on failure, never propagates errors.
    fn write_pending_index_best_effort(&mut self) {
        if self.pending_journal.is_empty() {
            return;
        }
        match self.write_pending_index() {
            Ok(()) => {
                self.pending_journal_last_written = self.pending_journal.len();
            }
            Err(e) => {
                warn!("failed to write pending_index: {e}");
            }
        }
    }

    /// Serialize, compress, encrypt, and write the pending index journal to storage.
    fn write_pending_index(&self) -> Result<()> {
        let wire = self.pending_journal.to_wire();
        let serialized = rmp_serde::to_vec(&wire)?;
        let compressed = compress::compress(compress::Compression::Zstd { level: 3 }, &serialized)?;
        let packed = pack_object_with_context(
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            &compressed,
            self.crypto.as_ref(),
        )?;
        self.storage.put(PENDING_INDEX_KEY, &packed)?;
        debug!(
            packs = wire.len(),
            bytes = packed.len(),
            "wrote pending_index to storage"
        );
        Ok(())
    }

    /// Recover chunk→pack mappings from a previous interrupted session's
    /// `pending_index` file. Verifies each pack exists before adding entries.
    ///
    /// Must be called inside the repo lock, before `enable_tiered_dedup_mode()`.
    /// Returns the number of recovered chunk entries.
    pub fn recover_pending_index(&mut self) -> Result<usize> {
        let data = match self.storage.get(PENDING_INDEX_KEY)? {
            Some(d) => d,
            None => return Ok(0),
        };

        let compressed = match unpack_object_expect_with_context(
            &data,
            ObjectType::PendingIndex,
            PENDING_INDEX_OBJECT_CONTEXT,
            self.crypto.as_ref(),
        ) {
            Ok(c) => c,
            Err(e) => {
                warn!("pending_index: decrypt failed, skipping recovery: {e}");
                return Ok(0);
            }
        };

        let serialized = match compress::decompress_metadata(&compressed) {
            Ok(s) => s,
            Err(e) => {
                warn!("pending_index: decompress failed, skipping recovery: {e}");
                return Ok(0);
            }
        };

        let wire: Vec<crate::index::PendingPackEntry> = match rmp_serde::from_slice(&serialized) {
            Ok(w) => w,
            Err(e) => {
                warn!("pending_index: deserialize failed, skipping recovery: {e}");
                return Ok(0);
            }
        };

        warn!(
            packs = wire.len(),
            "found pending index from interrupted session, verifying packs…"
        );

        // Batch-verify pack existence by listing shard directories instead of
        // issuing one HEAD request per pack (significant speedup for REST/S3).
        let shards: std::collections::HashSet<String> = wire
            .iter()
            .map(|e| format!("packs/{}", e.pack_id.shard_prefix()))
            .collect();
        let mut known_packs: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for shard in &shards {
            match self.storage.list(shard) {
                Ok(keys) => known_packs.extend(keys),
                Err(e) => {
                    warn!("pending_index: failed to list {shard}: {e}, falling back to per-pack checks");
                    for entry in &wire {
                        if format!("packs/{}", entry.pack_id.shard_prefix()) == *shard {
                            if self
                                .storage
                                .exists(&entry.pack_id.storage_key())
                                .unwrap_or(false)
                            {
                                known_packs.insert(entry.pack_id.storage_key());
                            }
                        }
                    }
                }
            }
        }

        let mut recovered = 0usize;
        for pack_entry in &wire {
            let pack_key = pack_entry.pack_id.storage_key();
            if !known_packs.contains(&pack_key) {
                warn!(
                    pack_id = %pack_entry.pack_id,
                    "pending_index: pack missing from storage, skipping"
                );
                continue;
            }

            for chunk in &pack_entry.chunks {
                // Only add if not already in the chunk index (e.g. from a
                // successful prior save that didn't delete pending_index).
                if !self.chunk_index.contains(&chunk.chunk_id) {
                    self.recovered_chunks.insert(
                        chunk.chunk_id,
                        RecoveredChunkEntry {
                            stored_size: chunk.stored_size,
                            pack_id: pack_entry.pack_id,
                            pack_offset: chunk.pack_offset,
                        },
                    );
                    recovered += 1;
                }
            }

            // Seed journal so re-interruption preserves these entries.
            self.pending_journal
                .record_pack(pack_entry.pack_id, pack_entry.chunks.clone());
        }

        debug!(
            packs = wire.len(),
            recovered_chunks = recovered,
            "recovered pending_index entries"
        );
        Ok(recovered)
    }

    /// Best-effort delete of the `pending_index` file from storage.
    /// Called from the backup command after `save_state()` succeeds.
    pub fn clear_pending_index(&self) {
        match self.storage.delete(PENDING_INDEX_KEY) {
            Ok(()) => {
                debug!("cleared pending_index from storage");
            }
            Err(e) => {
                warn!("failed to clear pending_index: {e}");
            }
        }
    }

    /// Promote a recovered chunk into the active dedup structure and index delta.
    /// Returns the stored size if the chunk was in `recovered_chunks`, None otherwise.
    fn promote_recovered_chunk(&mut self, chunk_id: &ChunkId) -> Option<u32> {
        let entry = self.recovered_chunks.remove(chunk_id)?;

        // Promote into active dedup structure.
        if let Some(ref mut tiered) = self.tiered_dedup {
            tiered.insert(*chunk_id, entry.stored_size);
        } else if let Some(ref mut dedup) = self.dedup_index {
            dedup.insert(*chunk_id, entry.stored_size);
        } else {
            self.chunk_index.add(
                *chunk_id,
                entry.stored_size,
                entry.pack_id,
                entry.pack_offset,
            );
            self.index_dirty = true;
            return Some(entry.stored_size);
        }

        // Record in delta as a new entry with refcount=1.
        if let Some(ref mut delta) = self.index_delta {
            delta.add_new_entry(
                *chunk_id,
                entry.stored_size,
                entry.pack_id,
                entry.pack_offset,
                1,
            );
        }

        Some(entry.stored_size)
    }

}
