use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, VgerError};

/// The manifest — list of all snapshots in the repository.
/// Stored encrypted at the `manifest` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub timestamp: DateTime<Utc>,
    pub snapshots: Vec<SnapshotEntry>,
}

/// A single entry in the manifest's snapshot list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub name: String,
    pub id: Vec<u8>, // 32 bytes, stored as vec for serde compat
    pub time: DateTime<Utc>,
    /// Label of the source that produced this snapshot.
    #[serde(default)]
    pub source_label: String,
    /// Legacy field kept for backward compatibility with existing snapshots.
    /// New snapshots always write `""`.
    #[serde(default)]
    pub label: String,
    /// Actual source paths that were backed up.
    #[serde(default)]
    pub source_paths: Vec<String>,
}

impl Manifest {
    /// Create an empty manifest.
    pub fn new() -> Self {
        Self {
            version: 1,
            timestamp: Utc::now(),
            snapshots: Vec::new(),
        }
    }

    /// Find a snapshot by name.
    pub fn find_snapshot(&self, name: &str) -> Option<&SnapshotEntry> {
        self.snapshots.iter().find(|a| a.name == name)
    }

    /// Resolve a snapshot query to a concrete entry.
    ///
    /// Resolution order:
    /// 1. `"latest"` (case-insensitive) → snapshot with the most recent `time`
    /// 2. Exact name match
    pub fn resolve_snapshot(&self, query: &str) -> Result<&SnapshotEntry> {
        if query.eq_ignore_ascii_case("latest") {
            return self
                .snapshots
                .iter()
                .max_by_key(|s| s.time)
                .ok_or_else(|| VgerError::SnapshotNotFound("latest".into()));
        }

        self.find_snapshot(query)
            .ok_or_else(|| VgerError::SnapshotNotFound(query.into()))
    }

    /// Remove a snapshot by name. Returns the removed entry, or None if not found.
    pub fn remove_snapshot(&mut self, name: &str) -> Option<SnapshotEntry> {
        if let Some(pos) = self.snapshots.iter().position(|a| a.name == name) {
            Some(self.snapshots.remove(pos))
        } else {
            None
        }
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}
