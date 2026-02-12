use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    /// User-provided annotation for this snapshot.
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
