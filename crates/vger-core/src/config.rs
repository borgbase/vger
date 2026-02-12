use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VgerConfig {
    pub repository: RepositoryConfig,
    #[serde(default)]
    pub encryption: EncryptionConfig,
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    #[serde(default)]
    pub exclude_if_present: Vec<String>,
    #[serde(default = "default_one_file_system")]
    pub one_file_system: bool,
    #[serde(default)]
    pub git_ignore: bool,
    #[serde(default)]
    pub chunker: ChunkerConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub schedule: ScheduleConfig,
    #[serde(default)]
    pub limits: ResourceLimitsConfig,
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
pub struct ScheduleConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_schedule_every")]
    pub every: String,
    #[serde(default)]
    pub on_startup: bool,
    #[serde(default)]
    pub jitter_seconds: u64,
    #[serde(default = "default_passphrase_prompt_timeout_seconds")]
    pub passphrase_prompt_timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimitsConfig {
    #[serde(default)]
    pub cpu: CpuLimitsConfig,
    #[serde(default)]
    pub io: IoLimitsConfig,
    #[serde(default)]
    pub network: NetworkLimitsConfig,
}

impl Default for ResourceLimitsConfig {
    fn default() -> Self {
        Self {
            cpu: CpuLimitsConfig::default(),
            io: IoLimitsConfig::default(),
            network: NetworkLimitsConfig::default(),
        }
    }
}

impl ResourceLimitsConfig {
    pub fn validate(&self) -> crate::error::Result<()> {
        self.cpu.validate()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuLimitsConfig {
    /// Max CPU worker threads for backup transforms (0 = default rayon behavior).
    #[serde(default)]
    pub max_threads: usize,
    /// Unix process niceness target (-20..19). 0 = unchanged.
    #[serde(default)]
    pub nice: i32,
}

impl Default for CpuLimitsConfig {
    fn default() -> Self {
        Self {
            max_threads: 0,
            nice: 0,
        }
    }
}

impl CpuLimitsConfig {
    fn validate(&self) -> crate::error::Result<()> {
        if !(-20..=19).contains(&self.nice) {
            return Err(crate::error::VgerError::Config(format!(
                "limits.cpu.nice must be in [-20, 19], got {}",
                self.nice
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoLimitsConfig {
    /// Source-file read limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub read_mib_per_sec: u64,
    /// Local repository write limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub write_mib_per_sec: u64,
}

impl Default for IoLimitsConfig {
    fn default() -> Self {
        Self {
            read_mib_per_sec: 0,
            write_mib_per_sec: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkLimitsConfig {
    /// Remote backend read limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub read_mib_per_sec: u64,
    /// Remote backend write limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub write_mib_per_sec: u64,
}

impl Default for NetworkLimitsConfig {
    fn default() -> Self {
        Self {
            read_mib_per_sec: 0,
            write_mib_per_sec: 0,
        }
    }
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            every: default_schedule_every(),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: default_passphrase_prompt_timeout_seconds(),
        }
    }
}

impl ScheduleConfig {
    pub fn every_duration(&self) -> crate::error::Result<Duration> {
        parse_human_duration(&self.every)
    }
}

/// Valid hook prefixes.
const HOOK_PREFIXES: &[&str] = &["before", "after", "failed", "finally"];

/// Valid command suffixes for command-specific hooks.
const HOOK_COMMANDS: &[&str] = &[
    "backup", "prune", "compact", "check", "delete", "extract", "init", "list", "run",
];

/// Hook configuration: flat map of hook keys to lists of shell commands.
///
/// Valid keys are bare prefixes (`before`, `after`, `failed`, `finally`) and
/// command-specific variants (`before_backup`, `finally_prune`, etc.).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HooksConfig {
    #[serde(flatten)]
    pub hooks: HashMap<String, Vec<String>>,
}

impl HooksConfig {
    /// Validate that all keys match valid hook patterns.
    pub fn validate(&self) -> crate::error::Result<()> {
        for key in self.hooks.keys() {
            if HOOK_PREFIXES.contains(&key.as_str()) {
                continue;
            }
            // Check for prefix_command pattern
            let valid = HOOK_PREFIXES.iter().any(|prefix| {
                key.strip_prefix(prefix)
                    .and_then(|rest| rest.strip_prefix('_'))
                    .is_some_and(|cmd| HOOK_COMMANDS.contains(&cmd))
            });
            if !valid {
                return Err(crate::error::VgerError::Config(format!(
                    "invalid hook key: '{key}'"
                )));
            }
        }
        Ok(())
    }

    /// Look up commands for a hook key, returning an empty slice if absent.
    pub fn get_hooks(&self, key: &str) -> &[String] {
        self.hooks.get(key).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

/// Source-level hooks — simpler than `HooksConfig`, only bare prefixes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceHooksConfig {
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub before: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub after: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub failed: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub finally: Vec<String>,
}

/// Deserialize a YAML field that can be either a single string or a list of strings.
fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct StringOrVec;

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a string or a list of strings")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Vec<String>, E> {
            Ok(vec![v.to_string()])
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<String>, A::Error> {
            let mut v = Vec::new();
            while let Some(s) = seq.next_element()? {
                v.push(s);
            }
            Ok(v)
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

/// YAML input for a source entry — either a plain path or a rich object.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum SourceInput {
    Simple(String),
    Rich {
        path: Option<String>,
        paths: Option<Vec<String>>,
        label: Option<String>,
        #[serde(default)]
        exclude: Vec<String>,
        exclude_if_present: Option<Vec<String>>,
        one_file_system: Option<bool>,
        git_ignore: Option<bool>,
        #[serde(default)]
        hooks: SourceHooksConfig,
        retention: Option<RetentionConfig>,
        #[serde(default)]
        repos: Vec<String>,
    },
}

/// Canonical resolved source entry.
#[derive(Debug, Clone)]
pub struct SourceEntry {
    pub paths: Vec<String>,
    pub label: String,
    pub exclude: Vec<String>,
    pub exclude_if_present: Vec<String>,
    pub one_file_system: bool,
    pub git_ignore: bool,
    pub hooks: SourceHooksConfig,
    pub retention: Option<RetentionConfig>,
    pub repos: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryConfig {
    /// Repository URL: bare path, `file://`, `s3://`, `sftp://`, or `http(s)://`.
    pub url: String,
    /// S3 region (default: us-east-1).
    pub region: Option<String>,
    /// S3 access key ID.
    pub access_key_id: Option<String>,
    /// S3 secret access key.
    pub secret_access_key: Option<String>,
    /// S3 endpoint override (for S3-compatible stores when the URL heuristic is insufficient).
    pub endpoint: Option<String>,
    /// Path to SSH private key for SFTP backend.
    pub sftp_key: Option<String>,
    /// Bearer token for REST backend authentication.
    pub rest_token: Option<String>,
    #[serde(default = "default_min_pack_size")]
    pub min_pack_size: u32,
    #[serde(default = "default_max_pack_size")]
    pub max_pack_size: u32,
    /// Retry settings for remote backends (S3, SFTP, REST).
    #[serde(default)]
    pub retry: RetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    #[serde(default = "default_encryption_mode")]
    pub mode: EncryptionModeConfig,
    pub passphrase: Option<String>,
    pub passcommand: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionModeConfig {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "aes256gcm")]
    Aes256Gcm,
}

impl EncryptionModeConfig {
    pub fn as_str(self) -> &'static str {
        match self {
            EncryptionModeConfig::None => "none",
            EncryptionModeConfig::Aes256Gcm => "aes256gcm",
        }
    }
}

impl PartialEq<&str> for EncryptionModeConfig {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
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
    pub algorithm: CompressionAlgorithm,
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Zstd,
}

impl CompressionAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Lz4 => "lz4",
            CompressionAlgorithm::Zstd => "zstd",
        }
    }
}

impl PartialEq<&str> for CompressionAlgorithm {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: default_algorithm(),
            zstd_level: default_zstd_level(),
        }
    }
}

fn default_encryption_mode() -> EncryptionModeConfig {
    EncryptionModeConfig::Aes256Gcm
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

fn default_algorithm() -> CompressionAlgorithm {
    CompressionAlgorithm::Lz4
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

fn default_max_retries() -> usize {
    3
}

fn default_retry_delay_ms() -> u64 {
    1000
}

fn default_retry_max_delay_ms() -> u64 {
    60_000
}

fn default_one_file_system() -> bool {
    true
}

fn default_schedule_every() -> String {
    "24h".to_string()
}

fn default_passphrase_prompt_timeout_seconds() -> u64 {
    300
}

/// Parse a simple duration string like "30m", "4h", or "2d".
pub fn parse_human_duration(raw: &str) -> crate::error::Result<Duration> {
    let input = raw.trim();
    if input.is_empty() {
        return Err(crate::error::VgerError::Config(
            "duration must not be empty".into(),
        ));
    }

    let (num_part, unit) = match input.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => (&input[..input.len() - 1], Some(c)),
        Some(_) => (input, None),
        None => {
            return Err(crate::error::VgerError::Config(
                "duration must not be empty".into(),
            ));
        }
    };

    let value: u64 = num_part
        .parse()
        .map_err(|_| crate::error::VgerError::Config(format!("invalid duration value: '{raw}'")))?;

    let secs = match unit {
        Some('m') | Some('M') => value.saturating_mul(60),
        Some('h') | Some('H') => value.saturating_mul(60 * 60),
        Some('d') | Some('D') => value.saturating_mul(60 * 60 * 24),
        Some(other) => {
            return Err(crate::error::VgerError::Config(format!(
                "unsupported duration suffix '{other}' in '{raw}' (use m/h/d)"
            )));
        }
        None => value.saturating_mul(60 * 60 * 24),
    };

    if secs == 0 {
        return Err(crate::error::VgerError::Config(
            "duration must be greater than zero".into(),
        ));
    }

    Ok(Duration::from_secs(secs))
}

/// Retry configuration for remote storage backends (S3, SFTP, REST).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries).
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    /// Initial delay between retries in milliseconds.
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
    /// Maximum delay between retries in milliseconds.
    #[serde(default = "default_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            retry_delay_ms: default_retry_delay_ms(),
            retry_max_delay_ms: default_retry_max_delay_ms(),
        }
    }
}

/// A single entry in the `repositories:` list.
/// Contains all `RepositoryConfig` fields plus optional per-repo overrides.
#[derive(Debug, Clone, Deserialize)]
pub struct RepositoryEntry {
    /// Repository URL: bare path, `file://`, `s3://`, `sftp://`, or `http(s)://`.
    pub url: String,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub endpoint: Option<String>,
    pub sftp_key: Option<String>,
    pub rest_token: Option<String>,
    pub min_pack_size: Option<u32>,
    pub max_pack_size: Option<u32>,

    /// Optional label for `--repo` selection.
    pub label: Option<String>,

    // Per-repo overrides (None = use top-level defaults)
    pub encryption: Option<EncryptionConfig>,
    pub compression: Option<CompressionConfig>,
    pub retention: Option<RetentionConfig>,

    /// Retry settings for remote backends.
    pub retry: Option<RetryConfig>,

    /// Per-repository resource limits (full override of top-level `limits`).
    pub limits: Option<ResourceLimitsConfig>,

    /// Per-repo hooks (optional).
    #[serde(default)]
    pub hooks: Option<HooksConfig>,
}

impl RepositoryEntry {
    fn to_repo_config(&self) -> RepositoryConfig {
        RepositoryConfig {
            url: expand_tilde(&self.url),
            region: self.region.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            endpoint: self.endpoint.clone(),
            sftp_key: self.sftp_key.clone(),
            rest_token: self.rest_token.clone(),
            min_pack_size: self.min_pack_size.unwrap_or_else(default_min_pack_size),
            max_pack_size: self.max_pack_size.unwrap_or_else(default_max_pack_size),
            retry: self.retry.clone().unwrap_or_default(),
        }
    }
}

/// Intermediate deserialization struct for the YAML config file.
#[derive(Debug, Deserialize)]
struct ConfigDocument {
    repositories: Vec<RepositoryEntry>,
    #[serde(default)]
    encryption: EncryptionConfig,
    #[serde(default)]
    sources: Vec<SourceInput>,
    #[serde(default)]
    exclude_patterns: Vec<String>,
    #[serde(default)]
    exclude_if_present: Vec<String>,
    #[serde(default = "default_one_file_system")]
    one_file_system: bool,
    #[serde(default)]
    git_ignore: bool,
    #[serde(default)]
    chunker: ChunkerConfig,
    #[serde(default)]
    compression: CompressionConfig,
    #[serde(default)]
    retention: RetentionConfig,
    #[serde(default)]
    schedule: ScheduleConfig,
    #[serde(default)]
    limits: ResourceLimitsConfig,
    /// Global hooks — apply to all repositories.
    #[serde(default)]
    hooks: HooksConfig,
}

// Backward-compatible alias used by internal tests.
#[cfg(test)]
type RawConfig = ConfigDocument;

/// Expand a leading `~` or `~/` to the user's home directory.
pub fn expand_tilde(path: &str) -> String {
    if path == "~" || path.starts_with("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(&path[2..]).to_string_lossy().to_string();
        }
    }
    path.to_string()
}

/// Derive a label from a path by taking the last component (basename).
pub fn label_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string())
}

/// Normalize a list of `SourceInput` into resolved `SourceEntry` values.
///
/// - All `Simple(String)` entries are grouped into a single `SourceEntry`:
///   - If exactly 1 simple entry, label = `label_from_path()` (backward compat)
///   - If multiple, label = `"default"` and all paths are collected
/// - Each `Rich` entry is normalized individually.
///   - `path:` is sugar for `paths: [path]` — exactly one must be set.
///   - Multi-path rich entries require an explicit `label`.
fn normalize_sources(
    inputs: Vec<SourceInput>,
    default_exclude_if_present: &[String],
    default_one_file_system: bool,
    default_git_ignore: bool,
) -> crate::error::Result<Vec<SourceEntry>> {
    let mut simple_paths: Vec<String> = Vec::new();
    let mut rich_entries: Vec<SourceEntry> = Vec::new();

    for input in inputs {
        match input {
            SourceInput::Simple(path) => {
                simple_paths.push(expand_tilde(&path));
            }
            SourceInput::Rich {
                path,
                paths,
                label,
                exclude,
                exclude_if_present,
                one_file_system,
                git_ignore,
                hooks,
                retention,
                repos,
            } => {
                let resolved_paths = match (path, paths) {
                    (Some(p), None) => vec![expand_tilde(&p)],
                    (None, Some(ps)) => {
                        if ps.is_empty() {
                            return Err(crate::error::VgerError::Config(
                                "source 'paths' must not be empty".into(),
                            ));
                        }
                        ps.iter().map(|p| expand_tilde(p)).collect()
                    }
                    (Some(_), Some(_)) => {
                        return Err(crate::error::VgerError::Config(
                            "source entry cannot have both 'path' and 'paths'".into(),
                        ));
                    }
                    (None, None) => {
                        return Err(crate::error::VgerError::Config(
                            "source entry must have 'path' or 'paths'".into(),
                        ));
                    }
                };

                // Multi-path rich entries require an explicit label
                if resolved_paths.len() > 1 && label.is_none() {
                    return Err(crate::error::VgerError::Config(
                        "multi-path source entries require an explicit 'label'".into(),
                    ));
                }

                let label = label.unwrap_or_else(|| label_from_path(&resolved_paths[0]));

                // Validate no duplicate basenames within a multi-path entry
                if resolved_paths.len() > 1 {
                    let mut basenames = std::collections::HashSet::new();
                    for p in &resolved_paths {
                        let base = label_from_path(p);
                        if !basenames.insert(base.clone()) {
                            return Err(crate::error::VgerError::Config(format!(
                                "duplicate basename '{base}' in multi-path source '{label}'"
                            )));
                        }
                    }
                }

                rich_entries.push(SourceEntry {
                    paths: resolved_paths,
                    label,
                    exclude,
                    exclude_if_present: exclude_if_present
                        .unwrap_or_else(|| default_exclude_if_present.to_vec()),
                    one_file_system: one_file_system.unwrap_or(default_one_file_system),
                    git_ignore: git_ignore.unwrap_or(default_git_ignore),
                    hooks,
                    retention,
                    repos,
                });
            }
        }
    }

    let mut result = Vec::new();

    // Group all simple entries into one SourceEntry
    if !simple_paths.is_empty() {
        let label = if simple_paths.len() == 1 {
            label_from_path(&simple_paths[0])
        } else {
            // Validate no duplicate basenames
            let mut basenames = std::collections::HashSet::new();
            for p in &simple_paths {
                let base = label_from_path(p);
                if !basenames.insert(base.clone()) {
                    return Err(crate::error::VgerError::Config(format!(
                        "duplicate basename '{base}' in simple sources (use rich entries with explicit labels to disambiguate)"
                    )));
                }
            }
            "default".to_string()
        };
        result.push(SourceEntry {
            paths: simple_paths,
            label,
            exclude: Vec::new(),
            exclude_if_present: default_exclude_if_present.to_vec(),
            one_file_system: default_one_file_system,
            git_ignore: default_git_ignore,
            hooks: SourceHooksConfig::default(),
            retention: None,
            repos: Vec::new(),
        });
    }

    result.extend(rich_entries);
    Ok(result)
}

/// A fully resolved repository with its merged config.
#[derive(Debug, Clone)]
pub struct ResolvedRepo {
    pub label: Option<String>,
    pub config: VgerConfig,
    pub global_hooks: HooksConfig,
    pub repo_hooks: HooksConfig,
    pub sources: Vec<SourceEntry>,
}

/// Load and resolve a config file into one `ResolvedRepo` per repository entry.
pub fn load_and_resolve(path: &Path) -> crate::error::Result<Vec<ResolvedRepo>> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        crate::error::VgerError::Config(format!("cannot read '{}': {e}", path.display()))
    })?;
    let raw: ConfigDocument = serde_yaml::from_str(&contents).map_err(|e| {
        crate::error::VgerError::Config(format!("invalid config '{}': {e}", path.display()))
    })?;

    resolve_document(raw)
}

fn resolve_document(raw: ConfigDocument) -> crate::error::Result<Vec<ResolvedRepo>> {
    if raw.repositories.is_empty() {
        return Err(crate::error::VgerError::Config(
            "'repositories:' must not be empty".into(),
        ));
    }

    // Check for duplicate repo labels
    let mut seen = std::collections::HashSet::new();
    for label in raw.repositories.iter().filter_map(|e| e.label.as_deref()) {
        if !seen.insert(label) {
            return Err(crate::error::VgerError::Config(format!(
                "duplicate repository label: '{label}'"
            )));
        }
    }

    // Validate global hooks
    raw.hooks.validate()?;
    raw.limits.validate()?;

    // Validate per-repo hooks
    for entry in &raw.repositories {
        if let Some(ref h) = entry.hooks {
            h.validate()?;
        }
        if let Some(ref limits) = entry.limits {
            limits.validate()?;
        }
    }

    // Normalize sources
    let all_sources: Vec<SourceEntry> = normalize_sources(
        raw.sources,
        &raw.exclude_if_present,
        raw.one_file_system,
        raw.git_ignore,
    )?;

    // Check for duplicate source labels
    let mut source_labels = std::collections::HashSet::new();
    for src in &all_sources {
        if !source_labels.insert(&src.label) {
            return Err(crate::error::VgerError::Config(format!(
                "duplicate source label: '{}'",
                src.label
            )));
        }
    }

    // Validate that source `repos` references exist
    let repo_labels: std::collections::HashSet<&str> = raw
        .repositories
        .iter()
        .filter_map(|e| e.label.as_deref())
        .collect();
    for src in &all_sources {
        for repo_ref in &src.repos {
            if !repo_labels.contains(repo_ref.as_str()) {
                return Err(crate::error::VgerError::Config(format!(
                    "source '{}' references unknown repository '{repo_ref}'",
                    src.label
                )));
            }
        }
    }

    let repos = raw
        .repositories
        .into_iter()
        .map(|entry| {
            let entry_label = entry.label.clone();
            let repo_hooks = entry.hooks.clone().unwrap_or_default();

            // Filter sources for this repo: include sources whose `repos` is empty
            // (meaning all repos) or whose `repos` list contains this repo's label.
            let sources_for_repo: Vec<SourceEntry> = all_sources
                .iter()
                .filter(|src| {
                    src.repos.is_empty()
                        || entry_label
                            .as_deref()
                            .is_some_and(|l| src.repos.iter().any(|r| r == l))
                })
                .map(|src| {
                    // Merge exclude patterns: global + per-source
                    let mut merged_exclude = raw.exclude_patterns.clone();
                    merged_exclude.extend(src.exclude.clone());
                    SourceEntry {
                        paths: src.paths.clone(),
                        label: src.label.clone(),
                        exclude: merged_exclude,
                        exclude_if_present: src.exclude_if_present.clone(),
                        one_file_system: src.one_file_system,
                        git_ignore: src.git_ignore,
                        hooks: src.hooks.clone(),
                        retention: src.retention.clone(),
                        repos: src.repos.clone(),
                    }
                })
                .collect();

            ResolvedRepo {
                label: entry_label,
                config: VgerConfig {
                    repository: entry.to_repo_config(),
                    encryption: entry.encryption.unwrap_or_else(|| raw.encryption.clone()),
                    exclude_patterns: raw.exclude_patterns.clone(),
                    exclude_if_present: raw.exclude_if_present.clone(),
                    one_file_system: raw.one_file_system,
                    git_ignore: raw.git_ignore,
                    chunker: raw.chunker.clone(),
                    compression: entry.compression.unwrap_or_else(|| raw.compression.clone()),
                    retention: entry.retention.unwrap_or_else(|| raw.retention.clone()),
                    schedule: raw.schedule.clone(),
                    limits: entry.limits.unwrap_or_else(|| raw.limits.clone()),
                },
                global_hooks: raw.hooks.clone(),
                repo_hooks,
                sources: sources_for_repo,
            }
        })
        .collect();

    Ok(repos)
}

/// Select a repository by label or URL from a list of resolved repos.
pub fn select_repo<'a>(repos: &'a [ResolvedRepo], selector: &str) -> Option<&'a ResolvedRepo> {
    // Try label match first
    repos
        .iter()
        .find(|r| r.label.as_deref() == Some(selector))
        .or_else(|| {
            // Fall back to URL match
            repos.iter().find(|r| r.config.repository.url == selector)
        })
}

/// Select sources by label from a list of source entries.
///
/// Returns an error with available source labels if any selector doesn't match.
pub fn select_sources<'a>(
    sources: &'a [SourceEntry],
    selectors: &[String],
) -> std::result::Result<Vec<&'a SourceEntry>, String> {
    let mut result = Vec::new();
    for sel in selectors {
        match sources.iter().find(|s| s.label == *sel) {
            Some(s) => result.push(s),
            None => {
                let available: Vec<&str> = sources.iter().map(|s| s.label.as_str()).collect();
                return Err(format!(
                    "no source matching '{sel}'\nAvailable sources: {}",
                    available.join(", ")
                ));
            }
        }
    }
    Ok(result)
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
#[deprecated(
    note = "prefer load_and_resolve() and explicit repository selection for multi-repo configs"
)]
pub fn load_config(path: &Path) -> crate::error::Result<VgerConfig> {
    let repos = load_and_resolve(path)?;
    Ok(repos.into_iter().next().unwrap().config)
}

/// Returns a minimal YAML config template suitable for bootstrapping.
pub fn minimal_config_template() -> &'static str {
    r#"# vger configuration file
# See https://github.com/your-org/vger for full documentation.

repositories:
  - url: /path/to/repo
    label: main

encryption:
  mode: aes256gcm
  # passphrase: secret
  # passcommand: "pass show vger"

sources:
  - /home/user/documents

# exclude_patterns:
#   - "*.tmp"
#   - ".cache/**"
#
# # Skip directories if any marker file is present:
# # exclude_if_present:
# #   - .nobackup
# #   - CACHEDIR.TAG
#
# # Don't cross filesystem boundaries (default: true)
# # one_file_system: true
#
# # Respect repository .gitignore files (default: false)
# # git_ignore: false

# retention:
#   keep_daily: 7
#   keep_weekly: 4
#   keep_monthly: 6
#
# schedule:
#   enabled: false
#   every: "24h"
#   on_startup: false
#   jitter_seconds: 0
#   passphrase_prompt_timeout_seconds: 300
#
# # Optional resource limits for backup.
# # All values are MiB/s unless noted; 0 means unlimited/no change.
# #
# # limits:
# #   cpu:
# #     max_threads: 0
# #     nice: 0
# #   io:
# #     read_mib_per_sec: 0
# #     write_mib_per_sec: 0
# #   network:
# #     read_mib_per_sec: 0
# #     write_mib_per_sec: 0

# Sources support simple paths (above) or rich entries:
#
# sources:
#   - path: /home/user/documents
#     label: docs
#     exclude:
#       - "*.tmp"
#     repos:
#       - main
#     retention:
#       keep_daily: 14
#     hooks:
#       before: "echo backing up docs"
#
# Multiple repositories: add more entries to 'repositories:'.
# Top-level settings serve as defaults; per-repo entries can override
# encryption, compression, retention, and limits.
#
# URL formats:
#   Local:  /backups/repo  or  file:///backups/repo
#   S3:     s3://bucket/prefix  or  s3://endpoint:port/bucket/prefix
#   SFTP:   sftp://user@host/path
#   REST:   https://backup.example.com/repo
#
#  - url: s3://my-bucket/vger
#    label: remote
#    region: us-east-1
#    compression:
#      algorithm: zstd
#    retention:
#      keep_daily: 30
#    retry:
#      max_retries: 5
#      retry_delay_ms: 2000
#      retry_max_delay_ms: 120000
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
        fs::write(&config_path, "repositories:\n  - url: /tmp/repo\n").unwrap();

        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let _env_guard = EnvGuard::set("VGER_CONFIG", "");

        let result = resolve_config_path(None);
        std::env::set_current_dir(original).unwrap();

        let source = result.unwrap();
        assert!(matches!(
            source,
            ConfigSource::SearchOrder {
                level: "project",
                ..
            }
        ));
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
        assert!(
            parsed.is_ok(),
            "template should parse as valid YAML: {:?}",
            parsed.err()
        );
        let raw = parsed.unwrap();
        let repos = resolve_document(raw).unwrap();
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].config.repository.url, "/path/to/repo");
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user/documents"]);
        assert_eq!(repos[0].sources[0].label, "documents");
    }

    #[test]
    #[allow(deprecated)]
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
  - url: /tmp/repo
    label: main
encryption:
  mode: none
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].label.as_deref(), Some("main"));
        assert_eq!(repos[0].config.repository.url, "/tmp/repo");
        assert_eq!(repos[0].config.encryption.mode, "none");
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user"]);
        assert_eq!(repos[0].sources[0].label, "user");
    }

    #[test]
    fn test_multi_repo_basic() {
        let yaml = r#"
encryption:
  mode: aes256gcm
sources:
  - /home/user
compression:
  algorithm: lz4
retention:
  keep_daily: 7

repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        assert_eq!(repos[0].label.as_deref(), Some("local"));
        assert_eq!(repos[0].config.repository.url, "/backups/local");
        // Inherits top-level defaults
        assert_eq!(repos[0].config.encryption.mode, "aes256gcm");
        assert_eq!(repos[0].config.compression.algorithm, "lz4");
        assert_eq!(repos[0].config.retention.keep_daily, Some(7));
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user"]);

        assert_eq!(repos[1].label.as_deref(), Some("remote"));
        assert_eq!(repos[1].config.repository.url, "/backups/remote");
        assert_eq!(repos[1].sources.len(), 1);
    }

    #[test]
    fn test_multi_repo_overrides() {
        let yaml = r#"
encryption:
  mode: aes256gcm
sources:
  - /home/user
compression:
  algorithm: lz4
retention:
  keep_daily: 7

repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote
    encryption:
      mode: aes256gcm
      passcommand: "pass show vger-remote"
    compression:
      algorithm: zstd
    retention:
      keep_daily: 30
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
        assert_eq!(local.sources.len(), 1);
        assert_eq!(local.sources[0].paths, vec!["/home/user"]);

        // Second repo uses overrides
        let remote = &repos[1];
        assert_eq!(remote.config.compression.algorithm, "zstd");
        assert_eq!(remote.config.retention.keep_daily, Some(30));
        assert_eq!(
            remote.config.encryption.passcommand.as_deref(),
            Some("pass show vger-remote")
        );
        assert_eq!(remote.sources.len(), 1);
    }

    #[test]
    fn test_limits_inherit_and_repo_override() {
        let yaml = r#"
limits:
  cpu:
    max_threads: 4
    nice: 5
  io:
    read_mib_per_sec: 100
    write_mib_per_sec: 50
  network:
    read_mib_per_sec: 80
    write_mib_per_sec: 40

repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote
    limits:
      cpu:
        max_threads: 2
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos.len(), 2);

        let local = &repos[0].config.limits;
        assert_eq!(local.cpu.max_threads, 4);
        assert_eq!(local.cpu.nice, 5);
        assert_eq!(local.io.read_mib_per_sec, 100);
        assert_eq!(local.io.write_mib_per_sec, 50);
        assert_eq!(local.network.read_mib_per_sec, 80);
        assert_eq!(local.network.write_mib_per_sec, 40);

        let remote = &repos[1].config.limits;
        assert_eq!(remote.cpu.max_threads, 2);
        assert_eq!(remote.cpu.nice, 0);
        assert_eq!(remote.io.read_mib_per_sec, 0);
        assert_eq!(remote.io.write_mib_per_sec, 0);
        assert_eq!(remote.network.read_mib_per_sec, 0);
        assert_eq!(remote.network.write_mib_per_sec, 0);
    }

    #[test]
    fn test_limits_invalid_nice_rejected() {
        let yaml = r#"
limits:
  cpu:
    nice: 25
repositories:
  - url: /backups/local
sources:
  - /home/user
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        assert!(
            err.to_string().contains("limits.cpu.nice"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_multi_repo_pack_size_defaults() {
        let yaml = r#"
repositories:
  - url: /backups/a
    min_pack_size: 1048576
  - url: /backups/b
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].config.repository.min_pack_size, 1048576);
        assert_eq!(
            repos[1].config.repository.min_pack_size,
            default_min_pack_size()
        );
        assert_eq!(
            repos[1].config.repository.max_pack_size,
            default_max_pack_size()
        );
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
  - url: /backups/a
    label: same
  - url: /backups/b
    label: same
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate repository label"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_select_repo_by_label() {
        let repos = vec![
            make_test_repo("/backups/local", Some("local")),
            make_test_repo("/backups/remote", Some("remote")),
        ];

        let found = select_repo(&repos, "remote").unwrap();
        assert_eq!(found.config.repository.url, "/backups/remote");
    }

    #[test]
    fn test_select_repo_by_url() {
        let repos = vec![
            make_test_repo("/backups/local", Some("local")),
            make_test_repo("/backups/unlabeled", None),
        ];

        let found = select_repo(&repos, "/backups/unlabeled").unwrap();
        assert!(found.label.is_none());
        assert_eq!(found.config.repository.url, "/backups/unlabeled");
    }

    #[test]
    fn test_select_repo_no_match() {
        let repos = vec![make_test_repo("/backups/local", Some("local"))];

        assert!(select_repo(&repos, "nonexistent").is_none());
    }

    #[test]
    #[allow(deprecated)]
    fn test_load_config_returns_first_repo() {
        let yaml = r#"
repositories:
  - url: /tmp/first
  - url: /tmp/second
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let config = load_config(&path).unwrap();
        assert_eq!(config.repository.url, "/tmp/first");
    }

    fn make_test_repo(url: &str, label: Option<&str>) -> ResolvedRepo {
        ResolvedRepo {
            label: label.map(|s| s.to_string()),
            config: VgerConfig {
                repository: RepositoryConfig {
                    url: url.to_string(),
                    region: None,
                    access_key_id: None,
                    secret_access_key: None,
                    endpoint: None,
                    sftp_key: None,
                    rest_token: None,
                    min_pack_size: default_min_pack_size(),
                    max_pack_size: default_max_pack_size(),
                    retry: RetryConfig::default(),
                },
                encryption: EncryptionConfig::default(),
                exclude_patterns: vec![],
                exclude_if_present: vec![],
                one_file_system: default_one_file_system(),
                git_ignore: false,
                chunker: ChunkerConfig::default(),
                compression: CompressionConfig::default(),
                retention: RetentionConfig::default(),
                schedule: ScheduleConfig::default(),
                limits: ResourceLimitsConfig::default(),
            },
            global_hooks: HooksConfig::default(),
            repo_hooks: HooksConfig::default(),
            sources: vec![],
        }
    }

    // --- select_sources tests ---

    fn make_test_source(label: &str) -> SourceEntry {
        SourceEntry {
            paths: vec![format!("/home/{label}")],
            label: label.to_string(),
            exclude: Vec::new(),
            exclude_if_present: Vec::new(),
            one_file_system: default_one_file_system(),
            git_ignore: false,
            hooks: SourceHooksConfig::default(),
            retention: None,
            repos: Vec::new(),
        }
    }

    #[test]
    fn test_select_sources_by_label() {
        let sources = vec![
            make_test_source("docs"),
            make_test_source("photos"),
            make_test_source("music"),
        ];

        let result = select_sources(&sources, &["photos".into()]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].label, "photos");
    }

    #[test]
    fn test_select_sources_multiple() {
        let sources = vec![
            make_test_source("docs"),
            make_test_source("photos"),
            make_test_source("music"),
        ];

        let result = select_sources(&sources, &["docs".into(), "music".into()]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].label, "docs");
        assert_eq!(result[1].label, "music");
    }

    #[test]
    fn test_select_sources_no_match() {
        let sources = vec![make_test_source("docs"), make_test_source("photos")];

        let err = select_sources(&sources, &["nonexistent".into()]).unwrap_err();
        assert!(
            err.contains("no source matching 'nonexistent'"),
            "unexpected: {err}"
        );
        assert!(err.contains("docs"), "should list available sources: {err}");
        assert!(
            err.contains("photos"),
            "should list available sources: {err}"
        );
    }

    #[test]
    fn test_select_sources_empty_selectors() {
        let sources = vec![make_test_source("docs")];

        let result = select_sources(&sources, &[]).unwrap();
        assert!(result.is_empty());
    }

    // --- Hooks config tests ---

    #[test]
    fn test_hooks_deserialize() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
hooks:
  before_backup:
    - "pg_dump mydb > /tmp/db.sql"
  finally_backup:
    - "rm -f /tmp/db.sql"
  after:
    - "echo done"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].global_hooks.get_hooks("before_backup").len(), 1);
        assert_eq!(repos[0].global_hooks.get_hooks("finally_backup").len(), 1);
        assert_eq!(repos[0].global_hooks.get_hooks("after").len(), 1);
        assert!(repos[0].global_hooks.get_hooks("before").is_empty());
    }

    #[test]
    fn test_hooks_default_empty() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].global_hooks.is_empty());
        assert!(repos[0].repo_hooks.is_empty());
    }

    #[test]
    fn test_hooks_per_repo() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: main
    hooks:
      before:
        - "mount /mnt/nas"
      finally:
        - "umount /mnt/nas"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].global_hooks.is_empty());
        assert_eq!(repos[0].repo_hooks.get_hooks("before").len(), 1);
        assert_eq!(repos[0].repo_hooks.get_hooks("finally").len(), 1);
    }

    #[test]
    fn test_hooks_validation_rejects_bad_keys() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
hooks:
  before_invalid_command:
    - "echo nope"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid hook key"), "unexpected error: {msg}");
    }

    #[test]
    fn test_hooks_validation_rejects_bad_repo_keys() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    hooks:
      on_start:
        - "echo nope"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid hook key"), "unexpected error: {msg}");
    }

    // --- Sources tests ---

    #[test]
    fn test_sources_simple_single() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/documents
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user/documents"]);
        assert_eq!(repos[0].sources[0].label, "documents");
    }

    #[test]
    fn test_sources_simple_multiple_grouped() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/documents
  - /home/user/photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        // Multiple simple sources are grouped into a single entry
        assert_eq!(repos[0].sources.len(), 1);
        assert_eq!(
            repos[0].sources[0].paths,
            vec!["/home/user/documents", "/home/user/photos"]
        );
        assert_eq!(repos[0].sources[0].label, "default");
    }

    #[test]
    fn test_sources_rich() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: main
sources:
  - path: /home/user/documents
    label: docs
    exclude:
      - "*.tmp"
    retention:
      keep_daily: 14
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 1);
        let src = &repos[0].sources[0];
        assert_eq!(src.paths, vec!["/home/user/documents"]);
        assert_eq!(src.label, "docs");
        assert_eq!(src.exclude, vec!["*.tmp"]);
        assert_eq!(src.retention.as_ref().unwrap().keep_daily, Some(14));
    }

    #[test]
    fn test_sources_mixed_simple_and_rich() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/photos
  - path: /home/user/documents
    label: docs
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 2);
        // Simple entries come first (grouped), then rich entries
        assert_eq!(repos[0].sources[0].label, "photos");
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user/photos"]);
        assert_eq!(repos[0].sources[1].label, "docs");
        assert_eq!(repos[0].sources[1].paths, vec!["/home/user/documents"]);
    }

    #[test]
    fn test_sources_repo_targeting() {
        let yaml = r#"
repositories:
  - url: /backups/local
    label: local
  - url: /backups/remote
    label: remote

sources:
  - path: /home/user/documents
    label: docs
    repos:
      - local
  - path: /home/user/photos
    label: photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();

        // local repo should have both (docs targets local, photos targets all)
        let local = &repos[0];
        assert_eq!(local.sources.len(), 2);

        // remote repo should only have photos (docs targets local only)
        let remote = &repos[1];
        assert_eq!(remote.sources.len(), 1);
        assert_eq!(remote.sources[0].label, "photos");
    }

    #[test]
    fn test_sources_exclude_merge() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
exclude_patterns:
  - "*.cache"
sources:
  - path: /home/user/documents
    exclude:
      - "*.tmp"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let src = &repos[0].sources[0];
        // Global + per-source excludes should be merged
        assert_eq!(src.exclude, vec!["*.cache", "*.tmp"]);
    }

    #[test]
    fn test_exclusion_feature_defaults() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/documents
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].config.exclude_if_present.is_empty());
        assert!(repos[0].config.one_file_system);
        assert!(!repos[0].config.git_ignore);

        let src = &repos[0].sources[0];
        assert!(src.exclude_if_present.is_empty());
        assert!(src.one_file_system);
        assert!(!src.git_ignore);
    }

    #[test]
    fn test_source_exclusion_feature_overrides() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
exclude_if_present:
  - .nobackup
  - CACHEDIR.TAG
one_file_system: true
git_ignore: false
sources:
  - path: /home/user/documents
    label: docs
    exclude_if_present:
      - .skip
    one_file_system: false
    git_ignore: true
  - path: /home/user/photos
    label: photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let docs = repos[0].sources.iter().find(|s| s.label == "docs").unwrap();
        let photos = repos[0]
            .sources
            .iter()
            .find(|s| s.label == "photos")
            .unwrap();

        // Per-source marker list replaces global markers when set.
        assert_eq!(docs.exclude_if_present, vec![".skip"]);
        assert!(!docs.one_file_system);
        assert!(docs.git_ignore);

        // Sources without overrides inherit global defaults.
        assert_eq!(photos.exclude_if_present, vec![".nobackup", "CACHEDIR.TAG"]);
        assert!(photos.one_file_system);
        assert!(!photos.git_ignore);
    }

    #[test]
    fn test_sources_reject_duplicate_labels() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /a
    label: same
  - path: /b
    label: same
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate source label"), "unexpected: {msg}");
    }

    #[test]
    fn test_sources_reject_unknown_repo_ref() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
    label: main
sources:
  - path: /home/user
    repos:
      - nonexistent
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown repository"), "unexpected: {msg}");
    }

    #[test]
    fn test_sources_hooks_string_and_list() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user
    hooks:
      before: "echo single"
      after:
        - "echo first"
        - "echo second"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let hooks = &repos[0].sources[0].hooks;
        assert_eq!(hooks.before, vec!["echo single"]);
        assert_eq!(hooks.after, vec!["echo first", "echo second"]);
        assert!(hooks.failed.is_empty());
        assert!(hooks.finally.is_empty());
    }

    #[test]
    fn test_empty_sources_allowed() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert!(repos[0].sources.is_empty());
    }

    #[test]
    fn test_tilde_expanded_in_repo_url_and_sources() {
        let yaml = r#"
repositories:
  - url: ~/backups/repo
sources:
  - ~/documents
  - path: ~/photos
    label: pics
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        let home = dirs::home_dir().unwrap().to_string_lossy().to_string();

        // Repository URL should be expanded
        assert!(
            repos[0].config.repository.url.starts_with(&home),
            "repo url not expanded: {}",
            repos[0].config.repository.url
        );
        assert!(repos[0].config.repository.url.ends_with("/backups/repo"));

        // Simple source path should be expanded
        assert!(
            repos[0].sources[0].paths[0].starts_with(&home),
            "source path not expanded: {}",
            repos[0].sources[0].paths[0]
        );

        // Rich source path should be expanded
        assert!(
            repos[0].sources[1].paths[0].starts_with(&home),
            "rich source path not expanded: {}",
            repos[0].sources[1].paths[0]
        );
    }

    // --- Multi-path source tests ---

    #[test]
    fn test_sources_rich_paths_plural() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths:
      - /home/user/documents
      - /home/user/photos
    label: multi
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources.len(), 1);
        let src = &repos[0].sources[0];
        assert_eq!(src.paths, vec!["/home/user/documents", "/home/user/photos"]);
        assert_eq!(src.label, "multi");
    }

    #[test]
    fn test_sources_rich_path_singular_still_works() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user/documents
    label: docs
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let repos = load_and_resolve(&path).unwrap();
        assert_eq!(repos[0].sources[0].paths, vec!["/home/user/documents"]);
        assert_eq!(repos[0].sources[0].label, "docs");
    }

    #[test]
    fn test_sources_reject_both_path_and_paths() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - path: /home/user/documents
    paths:
      - /home/user/photos
    label: bad
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("cannot have both 'path' and 'paths'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_reject_neither_path_nor_paths() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - label: bad
    exclude:
      - "*.tmp"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("must have 'path' or 'paths'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_multi_path_requires_label() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths:
      - /home/user/documents
      - /home/user/photos
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("require an explicit 'label'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_reject_duplicate_basenames_in_multi_path() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths:
      - /home/user/a/docs
      - /home/user/b/docs
    label: multi
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate basename 'docs'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_reject_duplicate_basenames_in_simple_group() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - /home/user/a/docs
  - /home/user/b/docs
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate basename 'docs'"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn test_sources_reject_empty_paths_list() {
        let yaml = r#"
repositories:
  - url: /tmp/repo
sources:
  - paths: []
    label: empty
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, yaml).unwrap();

        let err = load_and_resolve(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must not be empty"), "unexpected: {msg}");
    }

    #[test]
    fn test_parse_human_duration_units() {
        assert_eq!(parse_human_duration("30m").unwrap().as_secs(), 30 * 60);
        assert_eq!(parse_human_duration("4h").unwrap().as_secs(), 4 * 60 * 60);
        assert_eq!(
            parse_human_duration("2d").unwrap().as_secs(),
            2 * 24 * 60 * 60
        );
    }

    #[test]
    fn test_parse_human_duration_plain_number_is_days() {
        assert_eq!(
            parse_human_duration("3").unwrap().as_secs(),
            3 * 24 * 60 * 60
        );
    }

    #[test]
    fn test_parse_human_duration_rejects_invalid_values() {
        assert!(parse_human_duration("").is_err());
        assert!(parse_human_duration("0h").is_err());
        assert!(parse_human_duration("5w").is_err());
    }
}
