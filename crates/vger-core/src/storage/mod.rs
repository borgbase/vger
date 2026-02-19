pub mod local_backend;
pub mod opendal_backend;
#[cfg(feature = "backend-rest")]
pub mod rest_backend;
#[cfg(feature = "backend-sftp")]
pub mod sftp_backend;

use std::sync::Arc;
use std::time::Duration;

use opendal::layers::{ConcurrentLimitLayer, RetryLayer, ThrottleLayer};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::{RepositoryConfig, RetryConfig};
use crate::error::{Result, VgerError};

/// Default concurrent request limit for S3 backends.
/// Allows for multiple in-flight pack uploads (each with multipart concurrency)
/// plus reads and metadata operations.
const DEFAULT_S3_CONCURRENT_REQUESTS: usize = 64;

/// Burst size for ThrottleLayer (256 MiB).
/// Must be larger than any single write operation (multipart chunks are 32 MiB).
const THROTTLE_BURST_BYTES: u32 = 256 * 1024 * 1024;

/// Metadata sent to backends that support native advisory lock APIs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendLockInfo {
    pub hostname: String,
    pub pid: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackBlobRef {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackOperationRequest {
    pub source_pack: String,
    pub keep_blobs: Vec<RepackBlobRef>,
    pub delete_after: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackPlanRequest {
    pub operations: Vec<RepackOperationRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackOperationResult {
    pub source_pack: String,
    pub new_pack: Option<String>,
    pub new_offsets: Vec<u64>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepackResultResponse {
    pub completed: Vec<RepackOperationResult>,
}

/// Abstract key-value storage for repository objects.
/// Keys are `/`-separated string paths (e.g. "packs/ab/ab01cd02...").
pub trait StorageBackend: Send + Sync {
    /// Read an object by key. Returns `None` if not found.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Write an object. Overwrites if it already exists.
    fn put(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Delete an object.
    fn delete(&self, key: &str) -> Result<()>;

    /// Check if an object exists.
    fn exists(&self, key: &str) -> Result<bool>;

    /// List all keys under a prefix. Returns full key paths.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Read a byte range from an object. Returns `None` if not found.
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>>;

    /// Create a directory marker (no-op for flat object stores).
    fn create_dir(&self, key: &str) -> Result<()>;

    /// Write an object from an owned buffer. Backends can override to avoid
    /// an extra copy when the caller already owns the data.
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put(key, &data)
    }

    /// Acquire an advisory lock using a backend-native API.
    ///
    /// Backends that don't support a lock API should return
    /// `VgerError::UnsupportedBackend`, so the caller can fall back to
    /// object-based lock files.
    fn acquire_advisory_lock(&self, _lock_id: &str, _info: &BackendLockInfo) -> Result<()> {
        Err(VgerError::UnsupportedBackend("advisory lock API".into()))
    }

    /// Release an advisory lock using a backend-native API.
    fn release_advisory_lock(&self, _lock_id: &str) -> Result<()> {
        Err(VgerError::UnsupportedBackend("advisory lock API".into()))
    }

    /// Execute a server-side repack plan when supported by the backend.
    fn server_repack(&self, _plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        Err(VgerError::UnsupportedBackend(
            "server-side repack API".into(),
        ))
    }

    /// Batch-delete keys using a backend-native API.
    fn batch_delete_keys(&self, _keys: &[String]) -> Result<()> {
        Err(VgerError::UnsupportedBackend("batch delete API".into()))
    }
}

impl StorageBackend for Arc<dyn StorageBackend> {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        (**self).get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        (**self).put(key, data)
    }
    fn delete(&self, key: &str) -> Result<()> {
        (**self).delete(key)
    }
    fn exists(&self, key: &str) -> Result<bool> {
        (**self).exists(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        (**self).list(prefix)
    }
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        (**self).get_range(key, offset, length)
    }
    fn create_dir(&self, key: &str) -> Result<()> {
        (**self).create_dir(key)
    }
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        (**self).put_owned(key, data)
    }
    fn acquire_advisory_lock(&self, lock_id: &str, info: &BackendLockInfo) -> Result<()> {
        (**self).acquire_advisory_lock(lock_id, info)
    }
    fn release_advisory_lock(&self, lock_id: &str) -> Result<()> {
        (**self).release_advisory_lock(lock_id)
    }
    fn server_repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        (**self).server_repack(plan)
    }
    fn batch_delete_keys(&self, keys: &[String]) -> Result<()> {
        (**self).batch_delete_keys(keys)
    }
}

/// Parsed repository URL.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedUrl {
    /// Local filesystem path.
    Local { path: String },
    /// S3 or S3-compatible storage.
    S3 {
        bucket: String,
        root: String,
        /// Custom endpoint (present when the URL host contains a `.` or port).
        endpoint: Option<String>,
    },
    /// SFTP remote storage.
    Sftp {
        user: Option<String>,
        host: String,
        port: Option<u16>,
        path: String,
    },
    /// REST backend (HTTP/HTTPS).
    Rest { url: String },
}

/// Parse a repository URL into its components.
///
/// Supported formats:
/// - Bare path (`/backups/repo`, `./relative`, `relative`) -> `Local`
/// - `file:///backups/repo` -> `Local`
/// - `s3://bucket/prefix` -> `S3` (host is bucket, AWS default endpoint)
/// - `s3://endpoint:port/bucket/prefix` -> `S3` (host with `.` or port = custom endpoint)
/// - `sftp://[user@]host[:port]/path` -> `Sftp`
/// - `http(s)://...` -> `Rest`
pub fn parse_repo_url(raw: &str) -> Result<ParsedUrl> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(VgerError::Config("repository URL must not be empty".into()));
    }

    // Bare path: starts with `/`, `./`, or `../`
    if trimmed.starts_with('/')
        || trimmed.starts_with("./")
        || trimmed.starts_with("../")
        || trimmed == "."
        || trimmed == ".."
    {
        return Ok(ParsedUrl::Local {
            path: trimmed.to_string(),
        });
    }

    // Bare relative paths (without ./) are local too.
    if !trimmed.contains("://") {
        return Ok(ParsedUrl::Local {
            path: trimmed.to_string(),
        });
    }

    // Try to parse as URL
    let url = Url::parse(trimmed)
        .map_err(|e| VgerError::Config(format!("invalid repository URL '{trimmed}': {e}")))?;

    match url.scheme() {
        "file" => {
            let path = url.path().to_string();
            if path.is_empty() {
                return Err(VgerError::Config("file:// URL has empty path".into()));
            }
            Ok(ParsedUrl::Local { path })
        }
        "s3" => parse_s3_url(&url),
        "sftp" => parse_sftp_url(&url),
        "http" | "https" => Ok(ParsedUrl::Rest {
            url: trimmed.to_string(),
        }),
        other => Err(VgerError::UnsupportedBackend(format!(
            "unsupported URL scheme: '{other}'"
        ))),
    }
}

/// Parse an `s3://` URL.
///
/// Heuristic: if the host contains a `.` or has a port, it's treated as a
/// custom endpoint and the first path segment is the bucket. Otherwise
/// the host IS the bucket (AWS default endpoint).
///
/// Custom endpoints default to HTTPS; plaintext HTTP requires explicit
/// `repository.allow_insecure_http: true` plus an `endpoint: http://...` override.
fn parse_s3_url(url: &Url) -> Result<ParsedUrl> {
    let host = url
        .host_str()
        .ok_or_else(|| VgerError::Config("s3:// URL is missing a host/bucket".into()))?;

    let has_port = url.port().is_some();
    let has_dot = host.contains('.');

    if has_dot || has_port {
        // Custom endpoint — first path segment is bucket
        let port_suffix = url.port().map(|p| format!(":{p}")).unwrap_or_default();
        let endpoint = format!("https://{host}{port_suffix}");

        let path = url.path().trim_start_matches('/');
        let (bucket, root) = path.split_once('/').unwrap_or((path, ""));
        if bucket.is_empty() {
            return Err(VgerError::Config(
                "s3:// URL with custom endpoint is missing a bucket in the path".into(),
            ));
        }
        Ok(ParsedUrl::S3 {
            bucket: bucket.to_string(),
            root: root.to_string(),
            endpoint: Some(endpoint),
        })
    } else {
        // Host is the bucket, AWS default endpoint
        let root = url.path().trim_start_matches('/').to_string();
        Ok(ParsedUrl::S3 {
            bucket: host.to_string(),
            root,
            endpoint: None,
        })
    }
}

/// Parse an `sftp://` URL.
fn parse_sftp_url(url: &Url) -> Result<ParsedUrl> {
    let host = url
        .host_str()
        .ok_or_else(|| VgerError::Config("sftp:// URL is missing a host".into()))?;

    let user = if url.username().is_empty() {
        None
    } else {
        Some(url.username().to_string())
    };

    let path = url.path().to_string();

    Ok(ParsedUrl::Sftp {
        user,
        host: host.to_string(),
        port: url.port(),
        path,
    })
}

fn enforce_secure_http(url: &str, allow_insecure_http: bool, field_name: &str) -> Result<()> {
    let lowered = url.to_ascii_lowercase();
    if lowered.starts_with("http://") {
        if allow_insecure_http {
            tracing::warn!(
                "{field_name} uses plaintext HTTP; repository.allow_insecure_http=true enables this unsafe mode"
            );
            return Ok(());
        }
        return Err(VgerError::Config(format!(
            "{field_name} uses insecure HTTP and is blocked by default. \
Use HTTPS, or set repository.allow_insecure_http: true to permit plaintext HTTP (unsafe)."
        )));
    }
    Ok(())
}

/// Apply OpenDAL's `RetryLayer` to an operator if retries are enabled.
fn apply_retry(op: Operator, retry: &RetryConfig) -> Operator {
    if retry.max_retries == 0 {
        return op;
    }
    op.layer(
        RetryLayer::new()
            .with_max_times(retry.max_retries)
            .with_min_delay(Duration::from_millis(retry.retry_delay_ms))
            .with_max_delay(Duration::from_millis(retry.retry_max_delay_ms))
            .with_jitter(),
    )
}

/// Apply OpenDAL's `ThrottleLayer` to an operator if a bandwidth limit is set.
fn apply_throttle(op: Operator, throttle_bytes_per_sec: Option<u32>) -> Operator {
    match throttle_bytes_per_sec {
        Some(bps) if bps > 0 => op.layer(ThrottleLayer::new(bps, THROTTLE_BURST_BYTES)),
        _ => op,
    }
}

/// Build a storage backend from the repository configuration.
///
/// `throttle_bytes_per_sec` applies OpenDAL's `ThrottleLayer` for S3.
/// For local, SFTP, and REST backends this parameter is ignored (use
/// `limits::wrap_backup_storage_backend` for per-operation throttling).
pub fn backend_from_config(
    cfg: &RepositoryConfig,
    throttle_bytes_per_sec: Option<u32>,
) -> Result<Box<dyn StorageBackend>> {
    let parsed = parse_repo_url(&cfg.url)?;

    if let ParsedUrl::Rest { url } = &parsed {
        enforce_secure_http(url, cfg.allow_insecure_http, "repository.url")?;
    }

    if let ParsedUrl::S3 {
        endpoint: url_endpoint,
        ..
    } = &parsed
    {
        let endpoint = cfg.endpoint.as_deref().or(url_endpoint.as_deref());
        if let Some(endpoint) = endpoint {
            enforce_secure_http(endpoint, cfg.allow_insecure_http, "repository.endpoint")?;
        }
    }

    match parsed {
        ParsedUrl::Local { path } => Ok(Box::new(local_backend::LocalBackend::new(&path)?)),
        ParsedUrl::S3 {
            bucket,
            root,
            endpoint: url_endpoint,
        } => {
            // Config `endpoint` field overrides URL-derived endpoint
            let endpoint = cfg
                .endpoint
                .as_deref()
                .map(|s| s.to_string())
                .or(url_endpoint);
            let region = cfg.region.as_deref().unwrap_or("us-east-1");
            let op = opendal_backend::OpendalBackend::s3_operator(
                &bucket,
                region,
                &root,
                endpoint.as_deref(),
                cfg.access_key_id.as_deref(),
                cfg.secret_access_key.as_deref(),
            )?;
            // Layer order (inner → outer): S3 → ConcurrentLimit → Retry → Throttle → Blocking
            let op = op.layer(ConcurrentLimitLayer::new(DEFAULT_S3_CONCURRENT_REQUESTS));
            let op = apply_retry(op, &cfg.retry);
            let op = apply_throttle(op, throttle_bytes_per_sec);
            Ok(Box::new(
                opendal_backend::OpendalBackend::from_async_operator(op)?,
            ))
        }
        #[cfg(feature = "backend-sftp")]
        ParsedUrl::Sftp {
            user,
            host,
            port,
            path,
        } => Ok(Box::new(sftp_backend::SftpBackend::new(
            &host,
            user.as_deref(),
            port,
            &path,
            cfg.sftp_key.as_deref(),
            cfg.sftp_known_hosts.as_deref(),
            cfg.sftp_max_connections,
            cfg.retry.clone(),
        )?)),
        #[cfg(not(feature = "backend-sftp"))]
        ParsedUrl::Sftp { .. } => Err(VgerError::UnsupportedBackend(
            "sftp (compile with feature 'backend-sftp')".into(),
        )),
        #[cfg(feature = "backend-rest")]
        ParsedUrl::Rest { url } => {
            let token = cfg.rest_token.as_deref();
            Ok(Box::new(rest_backend::RestBackend::new(
                &url,
                token,
                cfg.retry.clone(),
            )?))
        }
        #[cfg(not(feature = "backend-rest"))]
        ParsedUrl::Rest { .. } => Err(VgerError::UnsupportedBackend(
            "rest (compile with feature 'backend-rest')".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bare_absolute_path() {
        let parsed = parse_repo_url("/backups/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "/backups/repo".into()
            }
        );
    }

    #[test]
    fn test_bare_relative_path() {
        let parsed = parse_repo_url("./my-repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "./my-repo".into()
            }
        );
    }

    #[test]
    fn test_bare_relative_path_without_dot_prefix() {
        let parsed = parse_repo_url("my-repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "my-repo".into()
            }
        );
    }

    #[test]
    fn test_file_url() {
        let parsed = parse_repo_url("file:///backups/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Local {
                path: "/backups/repo".into()
            }
        );
    }

    #[test]
    fn test_s3_bucket_only() {
        let parsed = parse_repo_url("s3://my-bucket").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "".into(),
                endpoint: None,
            }
        );
    }

    #[test]
    fn test_s3_bucket_with_prefix() {
        let parsed = parse_repo_url("s3://my-bucket/vger").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vger".into(),
                endpoint: None,
            }
        );
    }

    #[test]
    fn test_s3_bucket_with_nested_prefix() {
        let parsed = parse_repo_url("s3://my-bucket/backups/vger").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "backups/vger".into(),
                endpoint: None,
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_with_port() {
        let parsed = parse_repo_url("s3://minio.local:9000/my-bucket/vger").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vger".into(),
                endpoint: Some("https://minio.local:9000".into()),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_with_dot() {
        let parsed = parse_repo_url("s3://s3.example.com/my-bucket/vger").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::S3 {
                bucket: "my-bucket".into(),
                root: "vger".into(),
                endpoint: Some("https://s3.example.com".into()),
            }
        );
    }

    #[test]
    fn test_s3_custom_endpoint_missing_bucket() {
        let err = parse_repo_url("s3://minio.local:9000").unwrap_err();
        assert!(err.to_string().contains("missing a bucket"));
    }

    #[test]
    fn test_sftp_basic() {
        let parsed = parse_repo_url("sftp://nas.local/backups/vger").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: None,
                host: "nas.local".into(),
                port: None,
                path: "/backups/vger".into(),
            }
        );
    }

    #[test]
    fn test_sftp_with_user() {
        let parsed = parse_repo_url("sftp://backup@nas.local/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: Some("backup".into()),
                host: "nas.local".into(),
                port: None,
                path: "/repo".into(),
            }
        );
    }

    #[test]
    fn test_sftp_with_user_and_port() {
        let parsed = parse_repo_url("sftp://backup@nas.local:2222/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Sftp {
                user: Some("backup".into()),
                host: "nas.local".into(),
                port: Some(2222),
                path: "/repo".into(),
            }
        );
    }

    #[test]
    fn test_https_rest() {
        let parsed = parse_repo_url("https://backup.example.com/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Rest {
                url: "https://backup.example.com/repo".into(),
            }
        );
    }

    #[test]
    fn test_http_rest() {
        let parsed = parse_repo_url("http://localhost:8080/repo").unwrap();
        assert_eq!(
            parsed,
            ParsedUrl::Rest {
                url: "http://localhost:8080/repo".into(),
            }
        );
    }

    #[test]
    fn test_unsupported_scheme() {
        let err = parse_repo_url("ftp://host/path").unwrap_err();
        assert!(err.to_string().contains("unsupported URL scheme"));
    }

    #[test]
    fn test_invalid_url() {
        let err = parse_repo_url("http://[::1").unwrap_err();
        assert!(err.to_string().contains("invalid repository URL"));
    }

    #[test]
    fn test_empty_url_rejected() {
        let err = parse_repo_url("   ").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    fn test_config(url: &str) -> RepositoryConfig {
        RepositoryConfig {
            url: url.to_string(),
            region: None,
            access_key_id: None,
            secret_access_key: None,
            endpoint: None,
            sftp_key: None,
            sftp_known_hosts: None,
            sftp_max_connections: None,
            rest_token: None,
            allow_insecure_http: false,
            min_pack_size: 32 * 1024 * 1024,
            max_pack_size: 128 * 1024 * 1024,
            retry: RetryConfig::default(),
        }
    }

    #[test]
    fn test_backend_rejects_http_rest_by_default() {
        let cfg = test_config("http://localhost:8080/repo");
        let err = match backend_from_config(&cfg, None) {
            Ok(_) => panic!("expected HTTP REST URL to be rejected"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("repository.url"));
        assert!(msg.contains("repository.allow_insecure_http: true"));
    }

    #[test]
    fn test_backend_rejects_http_s3_endpoint_by_default() {
        let mut cfg = test_config("s3://my-bucket/vger");
        cfg.endpoint = Some("http://minio.local:9000".into());
        let err = match backend_from_config(&cfg, None) {
            Ok(_) => panic!("expected HTTP S3 endpoint to be rejected"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("repository.endpoint"));
        assert!(msg.contains("repository.allow_insecure_http: true"));
    }

    #[test]
    fn test_backend_allows_http_rest_when_opted_in() {
        let mut cfg = test_config("http://localhost:8080/repo");
        cfg.allow_insecure_http = true;
        let backend = backend_from_config(&cfg, None);
        assert!(
            backend.is_ok(),
            "expected HTTP REST URL to be allowed when opted in"
        );
    }

    #[test]
    fn test_backend_allows_http_s3_endpoint_when_opted_in() {
        let mut cfg = test_config("s3://my-bucket/vger");
        cfg.endpoint = Some("http://minio.local:9000".into());
        cfg.allow_insecure_http = true;
        let backend = backend_from_config(&cfg, None);
        assert!(
            backend.is_ok(),
            "expected HTTP S3 endpoint to be allowed when opted in"
        );
    }
}
