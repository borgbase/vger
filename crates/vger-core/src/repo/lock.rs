use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, VgerError};
use crate::storage::{BackendLockInfo, StorageBackend};

/// A simple advisory lock stored in `locks/<uuid>.json`.
#[derive(Debug, Serialize, Deserialize)]
struct LockEntry {
    hostname: String,
    pid: u32,
    time: String,
}

const LOCKS_PREFIX: &str = "locks/";
const DEFAULT_STALE_LOCK_SECS: i64 = 6 * 60 * 60; // 6 hours
const BACKEND_LOCK_ID: &str = "repo-lock";

#[derive(Debug)]
enum LockGuardKind {
    Object { key: String },
    Backend { lock_id: String },
}

/// Handle to an acquired lock.
#[derive(Debug)]
pub struct LockGuard {
    kind: LockGuardKind,
}

impl LockGuard {
    pub fn key(&self) -> &str {
        match &self.kind {
            LockGuardKind::Object { key } => key,
            LockGuardKind::Backend { lock_id } => lock_id,
        }
    }
}

/// Acquire an advisory lock on the repository.
pub fn acquire_lock(storage: &dyn StorageBackend) -> Result<LockGuard> {
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".into());
    let pid = std::process::id() as u64;

    // Prefer backend-native lock APIs when available (e.g. REST server locks).
    let backend_info = BackendLockInfo {
        hostname: hostname.clone(),
        pid,
    };
    match storage.acquire_advisory_lock(BACKEND_LOCK_ID, &backend_info) {
        Ok(()) => {
            return Ok(LockGuard {
                kind: LockGuardKind::Backend {
                    lock_id: BACKEND_LOCK_ID.to_string(),
                },
            });
        }
        Err(VgerError::UnsupportedBackend(_)) => {}
        Err(err) => return Err(err),
    }

    // Fallback to object-based lock files.
    cleanup_stale_locks(storage, Duration::seconds(DEFAULT_STALE_LOCK_SECS))?;

    let now = Utc::now();
    let entry = LockEntry {
        hostname,
        pid: pid as u32,
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

    Ok(LockGuard {
        kind: LockGuardKind::Object { key },
    })
}

/// Release an advisory lock.
pub fn release_lock(storage: &dyn StorageBackend, guard: LockGuard) -> Result<()> {
    match guard.kind {
        LockGuardKind::Object { key } => storage.delete(&key),
        LockGuardKind::Backend { lock_id } => storage.release_advisory_lock(&lock_id),
    }
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
