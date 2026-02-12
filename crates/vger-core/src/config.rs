use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VgerConfig {
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
    /// Bearer token for REST backend authentication.
    pub rest_token: Option<String>,
    /// Command to retrieve the REST token (stdout is used as the token).
    pub rest_token_command: Option<String>,
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

// --- Config resolution ---

/// Tracks where the config file was found.
#[derive(Debug, Clone)]
pub enum ConfigSource {
    /// Explicitly passed via `--config`.
    CliArg(PathBuf),
    /// Set via the `VGER_CONFIG` env var.
    EnvVar(PathBuf),
    /// Found by searching standard locations.
    SearchOrder { path: PathBuf, level: &'static str },
}

impl ConfigSource {
    pub fn path(&self) -> &Path {
        match self {
            ConfigSource::CliArg(p) => p,
            ConfigSource::EnvVar(p) => p,
            ConfigSource::SearchOrder { path, .. } => path,
        }
    }
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigSource::CliArg(p) => write!(f, "{} (--config)", p.display()),
            ConfigSource::EnvVar(p) => write!(f, "{} (VGER_CONFIG)", p.display()),
            ConfigSource::SearchOrder { path, level } => {
                write!(f, "{} ({})", path.display(), level)
            }
        }
    }
}

/// Returns search locations in priority order: project, user, system.
pub fn default_config_search_paths() -> Vec<(PathBuf, &'static str)> {
    let mut paths = vec![(PathBuf::from("vger.yaml"), "project")];

    // User config: $XDG_CONFIG_HOME/vger/config.yaml or ~/.config/vger/config.yaml
    let user_config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| dirs::home_dir().map(|h| h.join(".config")))
        .map(|base| base.join("vger").join("config.yaml"));

    if let Some(p) = user_config {
        paths.push((p, "user"));
    }

    // System config
    paths.push((PathBuf::from("/etc/vger/config.yaml"), "system"));

    paths
}

/// Resolve which config file to use.
///
/// Priority: CLI arg > `VGER_CONFIG` env var > first existing file from search paths.
/// Returns `None` if nothing is found.
pub fn resolve_config_path(cli_config: Option<&str>) -> Option<ConfigSource> {
    // 1. Explicit --config
    if let Some(path) = cli_config {
        return Some(ConfigSource::CliArg(PathBuf::from(path)));
    }

    // 2. VGER_CONFIG env var
    if let Ok(val) = std::env::var("VGER_CONFIG") {
        if !val.is_empty() {
            return Some(ConfigSource::EnvVar(PathBuf::from(val)));
        }
    }

    // 3. Search standard locations
    for (path, level) in default_config_search_paths() {
        if path.exists() {
            return Some(ConfigSource::SearchOrder { path, level });
        }
    }

    None
}

/// Load and parse a config file.
pub fn load_config(path: &Path) -> crate::error::Result<VgerConfig> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        crate::error::VgerError::Config(format!("cannot read '{}': {e}", path.display()))
    })?;
    let config: VgerConfig = serde_yaml::from_str(&contents).map_err(|e| {
        crate::error::VgerError::Config(format!("invalid config '{}': {e}", path.display()))
    })?;
    Ok(config)
}

/// Returns a minimal YAML config template suitable for bootstrapping.
pub fn minimal_config_template() -> &'static str {
    r#"# vger configuration file
# See https://github.com/your-org/vger for full documentation.

repository:
  path: /path/to/repo
  backend: local

encryption:
  mode: aes256gcm
  # passphrase: secret
  # passcommand: "pass show vger"

source_directories:
  - /home/user/documents

# exclude_patterns:
#   - "*.tmp"
#   - ".cache/**"

# retention:
#   keep_daily: 7
#   keep_weekly: 4
#   keep_monthly: 6
"#
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;

    // Tests that mutate process-global state (env vars, CWD) must be serialized.
    static GLOBAL_STATE: Mutex<()> = Mutex::new(());

    #[test]
    fn test_search_paths_order() {
        let paths = default_config_search_paths();
        assert!(paths.len() >= 2);
        assert_eq!(paths[0].1, "project");
        // Last entry should be system
        assert_eq!(paths.last().unwrap().1, "system");
        // If there are 3 entries, middle is user
        if paths.len() == 3 {
            assert_eq!(paths[1].1, "user");
        }
    }

    #[test]
    fn test_resolve_cli_arg_wins() {
        let result = resolve_config_path(Some("/tmp/override.yaml"));
        let source = result.unwrap();
        assert!(matches!(source, ConfigSource::CliArg(_)));
        assert_eq!(source.path(), Path::new("/tmp/override.yaml"));
    }

    #[test]
    fn test_resolve_env_var() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let _guard = EnvGuard::set("VGER_CONFIG", "/tmp/env-config.yaml");
        let result = resolve_config_path(None);
        let source = result.unwrap();
        assert!(matches!(source, ConfigSource::EnvVar(_)));
        assert_eq!(source.path(), Path::new("/tmp/env-config.yaml"));
    }

    #[test]
    fn test_resolve_search_finds_project() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("vger.yaml");
        fs::write(&config_path, "repository:\n  path: /tmp/repo\n").unwrap();

        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let _env_guard = EnvGuard::set("VGER_CONFIG", "");

        let result = resolve_config_path(None);
        std::env::set_current_dir(original).unwrap();

        let source = result.unwrap();
        assert!(matches!(source, ConfigSource::SearchOrder { level: "project", .. }));
    }

    #[test]
    fn test_resolve_nothing_found() {
        let _lock = GLOBAL_STATE.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let _env_guard = EnvGuard::set("VGER_CONFIG", "");
        let _xdg_guard = EnvGuard::set("XDG_CONFIG_HOME", dir.path().to_str().unwrap());

        let result = resolve_config_path(None);
        std::env::set_current_dir(original).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_minimal_template_is_valid_yaml() {
        let template = minimal_config_template();
        let parsed: Result<VgerConfig, _> = serde_yaml::from_str(template);
        assert!(parsed.is_ok(), "template should parse as valid YAML: {:?}", parsed.err());
    }

    #[test]
    fn test_load_config_missing_file() {
        let result = load_config(Path::new("/nonexistent/path/config.yaml"));
        assert!(result.is_err());
    }

    /// RAII guard to set an env var and restore its previous value on drop.
    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, val: &str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::set_var(key, val);
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}
