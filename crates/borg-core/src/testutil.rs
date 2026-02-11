use std::collections::HashMap;
use std::sync::Mutex;

use crate::config::ChunkerConfig;
use crate::error::Result;
use crate::repo::{EncryptionMode, Repository};
use crate::storage::StorageBackend;

/// In-memory storage backend for testing. Thread-safe via Mutex.
pub struct MemoryBackend {
    data: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl StorageBackend for MemoryBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let map = self.data.lock().unwrap();
        Ok(map.get(key).cloned())
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let mut map = self.data.lock().unwrap();
        map.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let mut map = self.data.lock().unwrap();
        map.remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let map = self.data.lock().unwrap();
        Ok(map.contains_key(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let map = self.data.lock().unwrap();
        let keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix) && !k.ends_with('/'))
            .cloned()
            .collect();
        Ok(keys)
    }

    fn create_dir(&self, _key: &str) -> Result<()> {
        // No-op for in-memory backend
        Ok(())
    }
}

/// Create a plaintext repository backed by MemoryBackend.
pub fn test_repo_plaintext() -> Repository {
    let storage = Box::new(MemoryBackend::new());
    Repository::init(storage, EncryptionMode::None, ChunkerConfig::default(), None)
        .expect("failed to init test repo")
}

/// Fixed chunk ID key for deterministic tests.
pub fn test_chunk_id_key() -> [u8; 32] {
    [0xAA; 32]
}
