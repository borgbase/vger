use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, VgerError};
use crate::storage::StorageBackend;

/// A simple advisory lock stored in `locks/<uuid>.json`.
#[derive(Debug, Serialize, Deserialize)]
struct LockEntry {
    hostname: String,
    pid: u32,
    time: String,
}

const LOCKS_PREFIX: &str = "locks/";
const DEFAULT_STALE_LOCK_SECS: i64 = 6 * 60 * 60; // 6 hours

/// Handle to an acquired lock.
#[derive(Debug)]
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
    cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS))?;

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".into());

    let now = Utc::now();
    let entry = LockEntry {
        hostname,
        pid: std::process::id(),
        time: now.to_rfc3339(),
    };

    let uuid = format!("{:032x}", rand::random::<u128>());
    // Timestamp prefix keeps older lock keys sorted first.
    let ts = now.timestamp_micros();
    let key = format!("{LOCKS_PREFIX}{ts:020}-{uuid}.json");
    let data = serde_json::to_vec(&entry)
        .map_err(|e| crate::error::VgerError::Other(format!("lock serialize: {e}")))?;

    storage.put(&key, &data)?;

    // Determine lock winner deterministically: oldest key wins.
    let mut keys = list_lock_keys(storage)?;
    keys.sort();
    if keys.first() != Some(&key) {
        // Best-effort cleanup of the lock we just wrote.
        let _ = storage.delete(&key);
        let holder = keys
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        return Err(VgerError::Locked(holder));
    }

    Ok(LockGuard { key })
}

/// Release an advisory lock.
pub fn release_lock(storage: &dyn StorageBackend, guard: LockGuard) -> Result<()> {
    storage.delete(&guard.key)?;
    Ok(())
}

fn list_lock_keys(storage: &dyn StorageBackend) -> Result<Vec<String>> {
    let mut keys = storage.list(LOCKS_PREFIX)?;
    keys.retain(|k| k.starts_with(LOCKS_PREFIX) && k.ends_with(".json"));
    Ok(keys)
}

fn cleanup_stale_locks(storage: &dyn StorageBackend, max_age: Duration) -> Result<()> {
    let now = Utc::now();
    for key in list_lock_keys(storage)? {
        let Some(data) = storage.get(&key)? else {
            continue;
        };
        let Ok(entry) = serde_json::from_slice::<LockEntry>(&data) else {
            continue;
        };
        let Ok(acquired) = chrono::DateTime::parse_from_rfc3339(&entry.time) else {
            continue;
        };
        if now.signed_duration_since(acquired.with_timezone(&Utc)) > max_age {
            let _ = storage.delete(&key);
        }
    }
    Ok(())
}
