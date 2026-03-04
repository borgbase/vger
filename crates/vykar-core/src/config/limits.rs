use serde::{Deserialize, Serialize};

use vykar_types::error::{Result, VykarError};

/// Default number of parallel backend connections.
const DEFAULT_CONNECTIONS: usize = 2;
/// Auto mode cap for backup worker threads (`threads: 0`).
const AUTO_THREADS_MAX: usize = 12;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimitsConfig {
    /// Parallel backend operations (SFTP pool, upload concurrency, restore threads).
    /// Range: 1–16. Default: 2.
    #[serde(default = "default_connections")]
    pub connections: usize,
    /// CPU worker threads for backup transforms.
    /// 0 = auto (`min(available_cores, 12)`), 1 = sequential.
    #[serde(default)]
    pub threads: usize,
    /// Unix process niceness target (-20..19). 0 = unchanged.
    #[serde(default)]
    pub nice: i32,
    /// Upload bandwidth cap in MiB/s. 0 = unlimited.
    #[serde(default)]
    pub upload_mib_per_sec: u64,
    /// Download bandwidth cap in MiB/s. 0 = unlimited.
    #[serde(default)]
    pub download_mib_per_sec: u64,
}

fn default_connections() -> usize {
    DEFAULT_CONNECTIONS
}

impl Default for ResourceLimitsConfig {
    fn default() -> Self {
        Self {
            connections: DEFAULT_CONNECTIONS,
            threads: 0,
            nice: 0,
            upload_mib_per_sec: 0,
            download_mib_per_sec: 0,
        }
    }
}

impl ResourceLimitsConfig {
    pub fn validate(&self) -> Result<()> {
        if !(1..=16).contains(&self.connections) {
            return Err(VykarError::Config(format!(
                "limits.connections must be in [1, 16], got {}",
                self.connections
            )));
        }
        if !(-20..=19).contains(&self.nice) {
            return Err(VykarError::Config(format!(
                "limits.nice must be in [-20, 19], got {}",
                self.nice
            )));
        }
        Ok(())
    }

    /// Resolved thread count (0 → min(available cores, 12)).
    pub fn effective_threads(&self) -> usize {
        if self.threads == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get().min(AUTO_THREADS_MAX))
                .unwrap_or(2)
        } else {
            self.threads
        }
    }

    /// Pipeline depth derived from connections.
    pub fn effective_pipeline_depth(&self) -> usize {
        self.connections.max(2)
    }

    /// Pipeline buffer size in bytes, derived from effective thread count.
    pub fn pipeline_buffer_bytes(&self) -> usize {
        self.effective_threads()
            .saturating_mul(64 * 1024 * 1024)
            .clamp(64 * 1024 * 1024, 1024 * 1024 * 1024)
    }

    /// Segment size in bytes for large-file pipeline splitting (fixed 64 MiB).
    pub fn segment_size_bytes(&self) -> usize {
        64 * 1024 * 1024
    }

    /// Transform batch size in bytes (fixed 32 MiB).
    pub fn transform_batch_bytes(&self) -> usize {
        32 * 1024 * 1024
    }

    /// Max pending chunk actions (fixed 8192).
    pub fn max_pending_actions(&self) -> usize {
        8192
    }

    /// Upload concurrency = connections.
    pub fn upload_concurrency(&self) -> usize {
        self.connections
    }

    /// Concurrency for listing/existence checks.
    pub fn listing_concurrency(&self, is_remote: bool) -> usize {
        if is_remote {
            (self.connections * 3).min(24)
        } else {
            self.connections.min(8)
        }
    }

    /// Restore reader thread count = connections.
    pub fn restore_concurrency(&self) -> usize {
        self.connections
    }

    /// Verify-data concurrency: capped at 4 (each thread holds ~128 MiB).
    pub fn verify_data_concurrency(&self) -> usize {
        self.connections.min(4)
    }
}
