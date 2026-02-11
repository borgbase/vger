use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::storage::StorageBackend;

/// A simple advisory lock stored in `locks/<uuid>.json`.
#[derive(Debug, Serialize, Deserialize)]
struct LockEntry {
    hostname: String,
    pid: u32,
    time: String,
}

/// Handle to an acquired lock. Releases on drop.
pub struct LockGuard {
    key: String,
}

impl LockGuard {
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Acquire an advisory lock on the repository.
pub fn acquire_lock(storage: &dyn StorageBackend) -> Result<LockGuard> {
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".into());

    let entry = LockEntry {
        hostname,
        pid: std::process::id(),
        time: Utc::now().to_rfc3339(),
    };

    let uuid = format!("{:032x}", rand::random::<u128>());
    let key = format!("locks/{uuid}.json");
    let data = serde_json::to_vec(&entry)
        .map_err(|e| crate::error::VgerError::Other(format!("lock serialize: {e}")))?;

    storage.put(&key, &data)?;

    Ok(LockGuard { key })
}

/// Release an advisory lock.
pub fn release_lock(storage: &dyn StorageBackend, guard: LockGuard) -> Result<()> {
    storage.delete(&guard.key)?;
    Ok(())
}
