use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BorgConfig {
    pub repository: RepositoryConfig,
    #[serde(default)]
    pub encryption: EncryptionConfig,
    #[serde(default)]
    pub source_directories: Vec<String>,
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    #[serde(default)]
    pub chunker: ChunkerConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default = "default_archive_format")]
    pub archive_name_format: String,
    #[serde(default)]
    pub retention: RetentionConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Keep all archives within this time interval (e.g. "2d", "48h", "1w")
    pub keep_within: Option<String>,
    /// Keep the N most recent archives
    pub keep_last: Option<usize>,
    pub keep_hourly: Option<usize>,
    pub keep_daily: Option<usize>,
    pub keep_weekly: Option<usize>,
    pub keep_monthly: Option<usize>,
    pub keep_yearly: Option<usize>,
}

impl RetentionConfig {
    /// Returns true if at least one keep_* option is set.
    pub fn has_any_rule(&self) -> bool {
        self.keep_within.is_some()
            || self.keep_last.is_some()
            || self.keep_hourly.is_some()
            || self.keep_daily.is_some()
            || self.keep_weekly.is_some()
            || self.keep_monthly.is_some()
            || self.keep_yearly.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryConfig {
    pub path: String,
    #[serde(default = "default_backend")]
    pub backend: String,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub sftp_host: Option<String>,
    pub sftp_user: Option<String>,
    pub sftp_port: Option<u16>,
    #[serde(default = "default_min_pack_size")]
    pub min_pack_size: u32,
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    #[serde(default = "default_encryption_mode")]
    pub mode: String,
    pub passphrase: Option<String>,
    pub passcommand: Option<String>,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            mode: default_encryption_mode(),
            passphrase: None,
            passcommand: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkerConfig {
    #[serde(default = "default_min_size")]
    pub min_size: u32,
    #[serde(default = "default_avg_size")]
    pub avg_size: u32,
    #[serde(default = "default_max_size")]
    pub max_size: u32,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self {
            min_size: default_min_size(),
            avg_size: default_avg_size(),
            max_size: default_max_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: default_algorithm(),
            zstd_level: default_zstd_level(),
        }
    }
}

fn default_backend() -> String {
    "local".to_string()
}

fn default_encryption_mode() -> String {
    "aes256gcm".to_string()
}

fn default_min_size() -> u32 {
    512 * 1024 // 512 KiB
}

fn default_avg_size() -> u32 {
    2 * 1024 * 1024 // 2 MiB
}

fn default_max_size() -> u32 {
    8 * 1024 * 1024 // 8 MiB
}

fn default_algorithm() -> String {
    "lz4".to_string()
}

fn default_zstd_level() -> i32 {
    3
}

fn default_min_pack_size() -> u32 {
    32 * 1024 * 1024 // 32 MiB
}

fn default_max_pack_size() -> u32 {
    512 * 1024 * 1024 // 512 MiB
}

fn default_archive_format() -> String {
    "{hostname}-{now:%Y-%m-%dT%H:%M:%S}".to_string()
}
