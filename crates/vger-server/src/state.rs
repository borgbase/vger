use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

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
        elapsed as u64 > self.ttl_seconds
    }
}

impl AppState {
    pub fn new(config: ServerSection) -> Self {
        let data_dir = PathBuf::from(&config.data_dir);

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
        let path = self.inner.data_dir.join(repo);
        // Prevent directory traversal
        if !path.starts_with(&self.inner.data_dir) {
            return None;
        }
        Some(path)
    }

    /// Resolve a full file path within a repo, ensuring it stays within data_dir.
    pub fn file_path(&self, repo: &str, key: &str) -> Option<PathBuf> {
        let repo_dir = self.repo_path(repo)?;
        let path = repo_dir.join(key);
        if !path.starts_with(&self.inner.data_dir) {
            return None;
        }
        Some(path)
    }

    /// Get current quota usage for a repo.
    pub fn quota_used(&self, repo: &str) -> u64 {
        self.inner
            .quota_usage
            .read()
            .unwrap()
            .get(repo)
            .copied()
            .unwrap_or(0)
    }

    /// Update quota usage after a write.
    pub fn add_quota_usage(&self, repo: &str, bytes: u64) {
        let mut usage = self.inner.quota_usage.write().unwrap();
        let entry = usage.entry(repo.to_string()).or_insert(0);
        *entry += bytes;
    }

    /// Update quota usage after a delete.
    pub fn sub_quota_usage(&self, repo: &str, bytes: u64) {
        let mut usage = self.inner.quota_usage.write().unwrap();
        if let Some(entry) = usage.get_mut(repo) {
            *entry = entry.saturating_sub(bytes);
        }
    }

    /// Record that a manifest was written (backup completed).
    pub fn record_backup(&self, repo: &str) {
        let mut map = self.inner.last_backup_at.write().unwrap();
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
