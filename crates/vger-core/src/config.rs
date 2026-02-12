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
    #[serde(default = "default_snapshot_format")]
    pub snapshot_name_format: String,
    #[serde(default)]
    pub retention: RetentionConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Keep all snapshots within this time interval (e.g. "2d", "48h", "1w")
    pub keep_within: Option<String>,
    /// Keep the N most recent snapshots
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

fn default_snapshot_format() -> String {
    "{hostname}-{now:%Y-%m-%dT%H:%M:%S}".to_string()
}

/// A single entry in the `repositories:` list.
/// Contains all `RepositoryConfig` fields plus optional per-repo overrides.
#[derive(Debug, Clone, Deserialize)]
pub struct RepositoryEntry {
    // Required repository fields
    pub path: String,
    #[serde(default = "default_backend")]
    pub backend: String,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub sftp_host: Option<String>,
    pub sftp_user: Option<String>,
    pub sftp_port: Option<u16>,
    pub rest_token: Option<String>,
    pub rest_token_command: Option<String>,
    pub min_pack_size: Option<u32>,
    pub max_pack_size: Option<u32>,

    /// Optional label for `--repo` selection.
    pub label: Option<String>,

    // Per-repo overrides (None = use top-level defaults)
    pub encryption: Option<EncryptionConfig>,
    pub compression: Option<CompressionConfig>,
    pub retention: Option<RetentionConfig>,
    pub source_directories: Option<Vec<String>>,
}

impl RepositoryEntry {
    fn to_repo_config(&self) -> RepositoryConfig {
        RepositoryConfig {
            path: self.path.clone(),
            backend: self.backend.clone(),
            s3_bucket: self.s3_bucket.clone(),
            s3_region: self.s3_region.clone(),
            s3_endpoint: self.s3_endpoint.clone(),
            sftp_host: self.sftp_host.clone(),
            sftp_user: self.sftp_user.clone(),
            sftp_port: self.sftp_port,
            rest_token: self.rest_token.clone(),
            rest_token_command: self.rest_token_command.clone(),
            min_pack_size: self.min_pack_size.unwrap_or_else(default_min_pack_size),
            max_pack_size: self.max_pack_size.unwrap_or_else(default_max_pack_size),
        }
    }
}

/// Intermediate deserialization struct for the YAML config file.
#[derive(Debug, Deserialize)]
struct RawConfig {
    repositories: Vec<RepositoryEntry>,
    #[serde(default)]
    encryption: EncryptionConfig,
    #[serde(default)]
    source_directories: Vec<String>,
    #[serde(default)]
    exclude_patterns: Vec<String>,
    #[serde(default)]
    chunker: ChunkerConfig,
    #[serde(default)]
    compression: CompressionConfig,
    #[serde(default = "default_snapshot_format")]
    snapshot_name_format: String,
    #[serde(default)]
    retention: RetentionConfig,
}

/// A fully resolved repository with its merged config.
#[derive(Debug, Clone)]
pub struct ResolvedRepo {
    pub label: Option<String>,
    pub config: VgerConfig,
}

/// Load and resolve a config file into one `ResolvedRepo` per repository entry.
pub fn load_and_resolve(path: &Path) -> crate::error::Result<Vec<ResolvedRepo>> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        crate::error::VgerError::Config(format!("cannot read '{}': {e}", path.display()))
    })?;
    let raw: RawConfig = serde_yaml::from_str(&contents).map_err(|e| {
        crate::error::VgerError::Config(format!("invalid config '{}': {e}", path.display()))
    })?;

    resolve_raw_config(raw)
}

fn resolve_raw_config(raw: RawConfig) -> crate::error::Result<Vec<ResolvedRepo>> {
    if raw.repositories.is_empty() {
        return Err(crate::error::VgerError::Config(
            "'repositories:' must not be empty".into(),
        ));
    }

    // Check for duplicate labels
    let mut seen = std::collections::HashSet::new();
    for label in raw.repositories.iter().filter_map(|e| e.label.as_deref()) {
        if !seen.insert(label) {
            return Err(crate::error::VgerError::Config(format!(
                "duplicate repository label: '{label}'"
            )));
        }
    }

    let repos = raw
        .repositories
        .into_iter()
        .map(|entry| {
            let label = entry.label.clone();
            ResolvedRepo {
                label,
                config: VgerConfig {
                    repository: entry.to_repo_config(),
                    encryption: entry
                        .encryption
                        .unwrap_or_else(|| raw.encryption.clone()),
                    source_directories: entry
                        .source_directories
                        .unwrap_or_else(|| raw.source_directories.clone()),
                    exclude_patterns: raw.exclude_patterns.clone(),
                    chunker: raw.chunker.clone(),
                    compression: entry
                        .compression
                        .unwrap_or_else(|| raw.compression.clone()),
                    snapshot_name_format: raw.snapshot_name_format.clone(),
                    retention: entry
                        .retention
                        .unwrap_or_else(|| raw.retention.clone()),
                },
            }
        })
        .collect();

    Ok(repos)
}

/// Select a repository by label or path from a list of resolved repos.
pub fn select_repo<'a>(repos: &'a [ResolvedRepo], selector: &str) -> Option<&'a ResolvedRepo> {
    // Try label match first
    repos
        .iter()
        .find(|r| r.label.as_deref() == Some(selector))
        .or_else(|| {
            // Fall back to path match
            repos.iter().find(|r| r.config.repository.path == selector)
        })
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

/// Load and parse a config file. Returns the first repository's config.
pub fn load_config(path: &Path) -> crate::error::Result<VgerConfig> {
    let repos = load_and_resolve(path)?;
    Ok(repos.into_iter().next().unwrap().config)
}

/// Returns a minimal YAML config template suitable for bootstrapping.
pub fn minimal_config_template() -> &'static str {
    r#"# vger configuration file
# See https://github.com/your-org/vger for full documentation.

repositories:
  - path: /path/to/repo
    label: main
    # backend: local              # "local", "s3", or "rest" (default: "local")

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

# Multiple repositories: add more entries to 'repositories:'.
# Top-level settings serve as defaults; per-repo entries can override
# encryption, compression, retention, and source_directories.
#
#  - path: s3://bucket/remote
#    label: remote
#    backend: s3
#    s3_bucket: my-bucket
#    compression:
#      algorithm: zstd
#    retention:
#      keep_daily: 30
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
        fs::write(&config_path, "repositories:\n  - path: /tmp/repo\n").unwrap();

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
        let parsed: Result<RawConfig, _> = serde_yaml::from_str(template);
        assert!(parsed.is_ok(), "template should parse as valid YAML: {:?}", parsed.err());
        let raw = parsed.unwrap();
        let repos = resolve_raw_config(raw).unwrap();
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].config.repository.path, "/path/to/repo");
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

    // --- Multi-repo tests ---

    #[test]
    fn test_single_repo() {
        let yaml = r#"
repositories:
  - path: /tmp/repo
    label: main
encryption:
  mode: none
source_directories:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].label.as_deref(), Some("main"));
        assert_eq!(repos[0].config.repository.path, "/tmp/repo");
        assert_eq!(repos[0].config.encryption.mode, "none");
        assert_eq!(repos[0].config.source_directories, vec!["/home/user"]);
    }

    #[test]
    fn test_multi_repo_basic() {
        let yaml = r#"
encryption:
  mode: aes256gcm
source_directories:
  - /home/user
compression:
  algorithm: lz4
retention:
  keep_daily: 7

repositories:
  - path: /backups/local
    label: local
  - path: /backups/remote
    label: remote
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        assert_eq!(repos[0].label.as_deref(), Some("local"));
        assert_eq!(repos[0].config.repository.path, "/backups/local");
        // Inherits top-level defaults
        assert_eq!(repos[0].config.encryption.mode, "aes256gcm");
        assert_eq!(repos[0].config.compression.algorithm, "lz4");
        assert_eq!(repos[0].config.retention.keep_daily, Some(7));
        assert_eq!(repos[0].config.source_directories, vec!["/home/user"]);

        assert_eq!(repos[1].label.as_deref(), Some("remote"));
        assert_eq!(repos[1].config.repository.path, "/backups/remote");
    }

    #[test]
    fn test_multi_repo_overrides() {
        let yaml = r#"
encryption:
  mode: aes256gcm
source_directories:
  - /home/user
compression:
  algorithm: lz4
retention:
  keep_daily: 7

repositories:
  - path: /backups/local
    label: local
  - path: /backups/remote
    label: remote
    encryption:
      mode: aes256gcm
      passcommand: "pass show vger-remote"
    compression:
      algorithm: zstd
    retention:
      keep_daily: 30
    source_directories:
      - /home/user/important
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        // First repo uses defaults
        let local = &repos[0];
        assert_eq!(local.config.compression.algorithm, "lz4");
        assert_eq!(local.config.retention.keep_daily, Some(7));
        assert_eq!(local.config.source_directories, vec!["/home/user"]);

        // Second repo uses overrides
        let remote = &repos[1];
        assert_eq!(remote.config.compression.algorithm, "zstd");
        assert_eq!(remote.config.retention.keep_daily, Some(30));
        assert_eq!(
            remote.config.encryption.passcommand.as_deref(),
            Some("pass show vger-remote")
        );
        assert_eq!(remote.config.source_directories, vec!["/home/user/important"]);
    }

    #[test]
    fn test_multi_repo_pack_size_defaults() {
        let yaml = r#"
repositories:
  - path: /backups/a
    min_pack_size: 1048576
  - path: /backups/b
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.min_pack_size, 1048576);
        assert_eq!(repos[1].config.repository.min_pack_size, default_min_pack_size());
        assert_eq!(repos[1].config.repository.max_pack_size, default_max_pack_size());
    }

    #[test]
    fn test_reject_missing_repositories() {
        let yaml = r#"
encryption:
  mode: none
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn test_reject_empty_repositories() {
        let yaml = r#"
repositories: []
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must not be empty"), "unexpected error: {msg}");
    }

    #[test]
    fn test_reject_duplicate_labels() {
        let yaml = r#"
repositories:
  - path: /backups/a
    label: same
  - path: /backups/b
    label: same
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate repository label"), "unexpected error: {msg}");
    }

    #[test]
    fn test_select_repo_by_label() {
        let repos = vec![
            ResolvedRepo {
                label: Some("local".into()),
                config: make_test_config("/backups/local"),
            },
            ResolvedRepo {
                label: Some("remote".into()),
                config: make_test_config("/backups/remote"),
            },
        ];

        let found = select_repo(&repos, "remote").unwrap();
        assert_eq!(found.config.repository.path, "/backups/remote");
    }

    #[test]
    fn test_select_repo_by_path() {
        let repos = vec![
            ResolvedRepo {
                label: Some("local".into()),
                config: make_test_config("/backups/local"),
            },
            ResolvedRepo {
                label: None,
                config: make_test_config("/backups/unlabeled"),
            },
        ];

        let found = select_repo(&repos, "/backups/unlabeled").unwrap();
        assert!(found.label.is_none());
        assert_eq!(found.config.repository.path, "/backups/unlabeled");
    }

    #[test]
    fn test_select_repo_no_match() {
        let repos = vec![ResolvedRepo {
            label: Some("local".into()),
            config: make_test_config("/backups/local"),
        }];

        assert!(select_repo(&repos, "nonexistent").is_none());
    }

    #[test]
    fn test_load_config_returns_first_repo() {
        let yaml = r#"
repositories:
  - path: /tmp/first
  - path: /tmp/second
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let config = load_config(&path).unwrap();
        assert_eq!(config.repository.path, "/tmp/first");
    }

    fn make_test_config(path: &str) -> VgerConfig {
        VgerConfig {
            repository: RepositoryConfig {
                path: path.to_string(),
                backend: "local".to_string(),
                s3_bucket: None,
                s3_region: None,
                s3_endpoint: None,
                sftp_host: None,
                sftp_user: None,
                sftp_port: None,
                rest_token: None,
                rest_token_command: None,
                min_pack_size: default_min_pack_size(),
                max_pack_size: default_max_pack_size(),
            },
            encryption: EncryptionConfig::default(),
            source_directories: vec![],
            exclude_patterns: vec![],
            chunker: ChunkerConfig::default(),
            compression: CompressionConfig::default(),
            snapshot_name_format: default_snapshot_format(),
            retention: RetentionConfig::default(),
        }
    }
}
