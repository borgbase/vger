use serde::{Deserialize, Serialize};

use crate::error::{Result, VgerError};

/// Default maximum number of in-flight background pack uploads.
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 2;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceLimitsConfig {
    #[serde(default)]
    pub cpu: CpuLimitsConfig,
    #[serde(default)]
    pub io: IoLimitsConfig,
    #[serde(default)]
    pub network: NetworkLimitsConfig,
}

impl ResourceLimitsConfig {
    pub fn validate(&self) -> Result<()> {
        self.cpu.validate()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuLimitsConfig {
    /// Max CPU worker threads for backup transforms (0 = use all cores, 1 = sequential).
    #[serde(default)]
    pub max_threads: usize,
    /// Unix process niceness target (-20..19). 0 = unchanged.
    #[serde(default)]
    pub nice: i32,
    /// Max in-flight background pack uploads (default: 2, range: 1-16).
    #[serde(default)]
    pub max_upload_concurrency: Option<usize>,
    /// Batch size in MiB for transform flushes (default: 32, range: 4-256).
    #[serde(default)]
    pub transform_batch_mib: Option<usize>,
    /// Max pending chunk actions before flush (default: 8192, range: 64-65536).
    #[serde(default)]
    pub transform_batch_chunks: Option<usize>,
    /// Depth of the pipeline channel buffer (default: 4, 0 = disable pipeline).
    #[serde(default)]
    pub pipeline_depth: Option<usize>,
    /// Max in-flight chunk bytes in the pipeline channel (default: 256 MiB, range: 32-1024).
    #[serde(default)]
    pub pipeline_buffer_mib: Option<usize>,
}

impl CpuLimitsConfig {
    fn validate(&self) -> Result<()> {
        if !(-20..=19).contains(&self.nice) {
            return Err(VgerError::Config(format!(
                "limits.cpu.nice must be in [-20, 19], got {}",
                self.nice
            )));
        }
        if let Some(n) = self.max_upload_concurrency {
            if !(1..=16).contains(&n) {
                return Err(VgerError::Config(format!(
                    "limits.cpu.max_upload_concurrency must be in [1, 16], got {n}"
                )));
            }
        }
        if let Some(n) = self.transform_batch_mib {
            if !(4..=256).contains(&n) {
                return Err(VgerError::Config(format!(
                    "limits.cpu.transform_batch_mib must be in [4, 256], got {n}"
                )));
            }
        }
        if let Some(n) = self.transform_batch_chunks {
            if !(64..=65536).contains(&n) {
                return Err(VgerError::Config(format!(
                    "limits.cpu.transform_batch_chunks must be in [64, 65536], got {n}"
                )));
            }
        }
        if let Some(n) = self.pipeline_depth {
            if n > 64 {
                return Err(VgerError::Config(format!(
                    "limits.cpu.pipeline_depth must be in [0, 64], got {n}"
                )));
            }
        }
        if let Some(n) = self.pipeline_buffer_mib {
            if !(32..=1024).contains(&n) {
                return Err(VgerError::Config(format!(
                    "limits.cpu.pipeline_buffer_mib must be in [32, 1024], got {n}"
                )));
            }
        }
        Ok(())
    }

    /// Effective transform batch size in bytes.
    pub fn transform_batch_bytes(&self) -> usize {
        self.transform_batch_mib.unwrap_or(32) * 1024 * 1024
    }

    /// Effective max pending chunk actions.
    pub fn max_pending_actions(&self) -> usize {
        self.transform_batch_chunks.unwrap_or(8192)
    }

    /// Effective upload concurrency limit.
    pub fn upload_concurrency(&self) -> usize {
        self.max_upload_concurrency
            .unwrap_or(DEFAULT_UPLOAD_CONCURRENCY)
    }

    /// Effective pipeline depth (0 = disabled).
    pub fn effective_pipeline_depth(&self) -> usize {
        self.pipeline_depth.unwrap_or(4)
    }

    /// Effective pipeline buffer size in bytes.
    pub fn pipeline_buffer_bytes(&self) -> usize {
        self.pipeline_buffer_mib.unwrap_or(256) * 1024 * 1024
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IoLimitsConfig {
    /// Source-file read limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub read_mib_per_sec: u64,
    /// Local repository write limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub write_mib_per_sec: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkLimitsConfig {
    /// Remote backend read limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub read_mib_per_sec: u64,
    /// Remote backend write limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub write_mib_per_sec: u64,
}
