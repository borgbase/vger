use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The manifest — list of all archives in the repository.
/// Stored encrypted at the `manifest` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub timestamp: DateTime<Utc>,
    pub archives: Vec<ArchiveEntry>,
}

/// A single entry in the manifest's archive list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveEntry {
    pub name: String,
    pub id: Vec<u8>, // 32 bytes, stored as vec for serde compat
    pub time: DateTime<Utc>,
}

impl Manifest {
    /// Create an empty manifest.
    pub fn new() -> Self {
        Self {
            version: 1,
            timestamp: Utc::now(),
            archives: Vec::new(),
        }
    }

    /// Find an archive by name.
    pub fn find_archive(&self, name: &str) -> Option<&ArchiveEntry> {
        self.archives.iter().find(|a| a.name == name)
    }

    /// Remove an archive by name. Returns the removed entry, or None if not found.
    pub fn remove_archive(&mut self, name: &str) -> Option<ArchiveEntry> {
        if let Some(pos) = self.archives.iter().position(|a| a.name == name) {
            Some(self.archives.remove(pos))
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
