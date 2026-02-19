use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use chrono::{DateTime, Utc};

use crate::config::ServerSection;

/// Shared application state, wrapped in Arc for axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<AppStateInner>,
}

pub struct AppStateInner {
    pub config: ServerSection,
    pub data_dir: PathBuf,
    pub start_time: std::time::Instant,

    /// Per-repo quota usage in bytes. repo_name -> bytes_used.
    pub quota_usage: RwLock<HashMap<String, u64>>,

    /// Per-repo last backup timestamp (updated on manifest PUT).
    pub last_backup_at: RwLock<HashMap<String, DateTime<Utc>>>,

    /// Active locks: repo_name -> { lock_id -> LockInfo }
    pub locks: RwLock<HashMap<String, HashMap<String, LockInfo>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LockInfo {
    pub hostname: String,
    pub pid: u64,
    pub acquired_at: DateTime<Utc>,
    pub ttl_seconds: u64,
}

impl LockInfo {
    pub fn is_expired(&self) -> bool {
        let elapsed = Utc::now()
            .signed_duration_since(self.acquired_at)
            .num_seconds();
        let ttl_i64 = i64::try_from(self.ttl_seconds).unwrap_or(i64::MAX);
        elapsed > ttl_i64
    }
}

pub(crate) fn read_unpoisoned<'a, T>(
    lock: &'a RwLock<T>,
    lock_name: &'static str,
) -> RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!(
                lock = lock_name,
                "rwlock poisoned; continuing with inner state"
            );
            poisoned.into_inner()
        }
    }
}

pub(crate) fn write_unpoisoned<'a, T>(
    lock: &'a RwLock<T>,
    lock_name: &'static str,
) -> RwLockWriteGuard<'a, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!(
                lock = lock_name,
                "rwlock poisoned; continuing with inner state"
            );
            poisoned.into_inner()
        }
    }
}

impl AppState {
    pub fn new(config: ServerSection) -> Self {
        let configured_data_dir = PathBuf::from(&config.data_dir);
        let data_dir = configured_data_dir
            .canonicalize()
            .unwrap_or(configured_data_dir);

        // Initialize quota usage by scanning existing repos
        let quota_usage = scan_repo_sizes(&data_dir);

        Self {
            inner: Arc::new(AppStateInner {
                config,
                data_dir,
                start_time: std::time::Instant::now(),
                quota_usage: RwLock::new(quota_usage),
                last_backup_at: RwLock::new(HashMap::new()),
                locks: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Resolve a repo directory path, ensuring it stays within data_dir.
    pub fn repo_path(&self, repo: &str) -> Option<PathBuf> {
        if !is_valid_repo_name(repo) {
            return None;
        }
        let path = self.inner.data_dir.join(repo);
        if !path.starts_with(&self.inner.data_dir) {
            return None;
        }
        if !existing_ancestor_within(&path, &self.inner.data_dir) {
            return None;
        }
        Some(path)
    }

    /// Resolve a full file path within a repo, ensuring it stays within data_dir.
    pub fn file_path(&self, repo: &str, key: &str) -> Option<PathBuf> {
        if !is_valid_storage_key(key) {
            return None;
        }
        let repo_dir = self.repo_path(repo)?;
        let trimmed = key.trim_matches('/');
        let path = if trimmed.is_empty() {
            repo_dir
        } else {
            repo_dir.join(trimmed)
        };
        if !path.starts_with(&self.inner.data_dir) {
            return None;
        }
        if !existing_ancestor_within(&path, &self.inner.data_dir) {
            return None;
        }
        Some(path)
    }

    /// Get current quota usage for a repo.
    pub fn quota_used(&self, repo: &str) -> u64 {
        read_unpoisoned(&self.inner.quota_usage, "quota_usage")
            .get(repo)
            .copied()
            .unwrap_or(0)
    }

    /// Update quota usage after a write.
    pub fn add_quota_usage(&self, repo: &str, bytes: u64) {
        let mut usage = write_unpoisoned(&self.inner.quota_usage, "quota_usage");
        let entry = usage.entry(repo.to_string()).or_insert(0);
        *entry += bytes;
    }

    /// Update quota usage after a delete.
    pub fn sub_quota_usage(&self, repo: &str, bytes: u64) {
        let mut usage = write_unpoisoned(&self.inner.quota_usage, "quota_usage");
        if let Some(entry) = usage.get_mut(repo) {
            *entry = entry.saturating_sub(bytes);
        }
    }

    /// Record that a manifest was written (backup completed).
    pub fn record_backup(&self, repo: &str) {
        let mut map = write_unpoisoned(&self.inner.last_backup_at, "last_backup_at");
        map.insert(repo.to_string(), Utc::now());
    }
}

/// Scan the data directory and compute per-repo disk usage.
fn scan_repo_sizes(data_dir: &Path) -> HashMap<String, u64> {
    let mut sizes = HashMap::new();
    let entries = match std::fs::read_dir(data_dir) {
        Ok(e) => e,
        Err(_) => return sizes,
    };
    for entry in entries.flatten() {
        if entry.path().is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                let size = dir_size(&entry.path());
                sizes.insert(name.to_string(), size);
            }
        }
    }
    sizes
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(meta) = p.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

fn is_valid_repo_name(repo: &str) -> bool {
    !repo.is_empty()
        && repo.len() <= 128
        && repo != "."
        && repo != ".."
        && repo
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}

fn is_valid_storage_key(key: &str) -> bool {
    if key.contains('\0') || key.contains('\\') {
        return false;
    }
    let trimmed = key.trim_matches('/');
    if trimmed.is_empty() {
        return true;
    }
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts
        .iter()
        .any(|part| part.is_empty() || *part == "." || *part == "..")
    {
        return false;
    }
    match parts[0] {
        "config" | "manifest" | "index" => parts.len() == 1,
        "keys" | "snapshots" | "locks" => (1..=2).contains(&parts.len()),
        "packs" => is_valid_packs_key(&parts),
        _ => false,
    }
}

fn is_valid_packs_key(parts: &[&str]) -> bool {
    if parts.is_empty() || parts[0] != "packs" {
        return false;
    }
    if parts.len() == 1 {
        return true;
    }
    let shard = parts[1];
    if shard.len() != 2 || !shard.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    if parts.len() == 2 {
        return true;
    }
    if parts.len() == 3 {
        let pack = parts[2];
        return pack.len() == 64 && pack.chars().all(|c| c.is_ascii_hexdigit());
    }
    false
}

fn existing_ancestor_within(path: &Path, base: &Path) -> bool {
    let mut cursor = Some(path);
    while let Some(candidate) = cursor {
        if candidate.exists() {
            return candidate
                .canonicalize()
                .map(|canon| canon.starts_with(base))
                .unwrap_or(false);
        }
        cursor = candidate.parent();
    }
    false
}
