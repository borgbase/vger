pub mod opendal_backend;
#[cfg(feature = "backend-rest")]
pub mod rest_backend;

use std::sync::Arc;
use std::time::Duration;

use opendal::layers::RetryLayer;
use opendal::Operator;
use url::Url;

use crate::config::{RepositoryConfig, RetryConfig};
use crate::error::{Result, VgerError};

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
/// - Bare path (`/backups/repo`, `./relative`) -> `Local`
/// - `file:///backups/repo` -> `Local`
/// - `s3://bucket/prefix` -> `S3` (host is bucket, AWS default endpoint)
/// - `s3://endpoint:port/bucket/prefix` -> `S3` (host with `.` or port = custom endpoint)
/// - `sftp://[user@]host[:port]/path` -> `Sftp`
/// - `http(s)://...` -> `Rest`
pub fn parse_repo_url(raw: &str) -> Result<ParsedUrl> {
    let trimmed = raw.trim();

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
fn parse_s3_url(url: &Url) -> Result<ParsedUrl> {
    let host = url
        .host_str()
        .ok_or_else(|| VgerError::Config("s3:// URL is missing a host/bucket".into()))?;

    let has_port = url.port().is_some();
    let has_dot = host.contains('.');

    if has_dot || has_port {
        // Custom endpoint — first path segment is bucket
        let scheme = if has_port && !host.contains("amazonaws.com") {
            "http"
        } else {
            "https"
        };
        let port_suffix = url.port().map(|p| format!(":{p}")).unwrap_or_default();
        let endpoint = format!("{scheme}://{host}{port_suffix}");

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

/// Build a storage backend from the repository configuration.
pub fn backend_from_config(cfg: &RepositoryConfig) -> Result<Box<dyn StorageBackend>> {
    let parsed = parse_repo_url(&cfg.url)?;

    match parsed {
        ParsedUrl::Local { path } => Ok(Box::new(opendal_backend::OpendalBackend::local(&path)?)),
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
            let op = apply_retry(op, &cfg.retry);
            Ok(Box::new(
                opendal_backend::OpendalBackend::from_async_operator_tuned(
                    op,
                    Some(opendal_backend::S3_UPLOAD_CHUNK_SIZE_BYTES),
                    Some(opendal_backend::S3_UPLOAD_CONCURRENCY),
                )?,
            ))
        }
        #[cfg(feature = "backend-sftp")]
        ParsedUrl::Sftp {
            user,
            host,
            port,
            path,
        } => {
            let op = opendal_backend::OpendalBackend::sftp_operator(
                &host,
                user.as_deref(),
                port,
                &path,
                cfg.sftp_key.as_deref(),
            )?;
            let op = apply_retry(op, &cfg.retry);
            Ok(Box::new(
                opendal_backend::OpendalBackend::from_async_operator(op)?,
            ))
        }
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
                endpoint: Some("http://minio.local:9000".into()),
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
        let err = parse_repo_url("not a url at all").unwrap_err();
        assert!(err.to_string().contains("invalid repository URL"));
    }
}
