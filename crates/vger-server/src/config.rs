use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub server: ServerSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSection {
    /// Address to listen on.
    #[serde(default = "default_listen")]
    pub listen: String,

    /// Root directory where repositories are stored.
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// Shared bearer token for authentication.
    pub token: String,

    /// If true, reject DELETE and overwrite operations on pack files.
    #[serde(default)]
    pub append_only: bool,

    /// Log output format: "json" or "pretty".
    #[serde(default = "default_log_format")]
    pub log_format: String,

    /// Per-repo storage quota in bytes. 0 = unlimited.
    #[serde(default)]
    pub quota_bytes: u64,

    /// Rate limit: max requests per second. 0 = unlimited.
    #[serde(default)]
    pub rate_limit_rps: u64,

    /// Rate limit: max megabytes per second. 0 = unlimited.
    #[serde(default)]
    pub rate_limit_mbps: u64,

    /// Default lock TTL in seconds.
    #[serde(default = "default_lock_ttl")]
    pub lock_ttl_seconds: u64,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: default_data_dir(),
            token: String::new(),
            append_only: false,
            log_format: default_log_format(),
            quota_bytes: 0,
            rate_limit_rps: 0,
            rate_limit_mbps: 0,
            lock_ttl_seconds: default_lock_ttl(),
        }
    }
}

fn default_listen() -> String {
    "127.0.0.1:8484".to_string()
}

fn default_data_dir() -> String {
    "/var/lib/vger".to_string()
}

fn default_log_format() -> String {
    "pretty".to_string()
}

fn default_lock_ttl() -> u64 {
    3600
}
