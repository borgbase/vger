pub mod opendal_backend;
#[cfg(feature = "backend-rest")]
pub mod rest_backend;

use crate::config::RepositoryConfig;
use crate::error::{VgerError, Result};

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
}

/// Build a storage backend from the repository configuration.
pub fn backend_from_config(cfg: &RepositoryConfig) -> Result<Box<dyn StorageBackend>> {
    match cfg.backend.as_str() {
        "local" => Ok(Box::new(opendal_backend::OpendalBackend::local(&cfg.path)?)),
        "s3" => {
            let bucket = cfg
                .s3_bucket
                .as_deref()
                .ok_or_else(|| VgerError::Config("s3_bucket is required for S3 backend".into()))?;
            let region = cfg.s3_region.as_deref().unwrap_or("us-east-1");
            Ok(Box::new(opendal_backend::OpendalBackend::s3(
                bucket, region, &cfg.path, cfg.s3_endpoint.as_deref(),
            )?))
        }
        #[cfg(feature = "backend-rest")]
        "rest" => {
            let token = cfg.rest_token.as_deref();
            Ok(Box::new(rest_backend::RestBackend::new(&cfg.path, token)?))
        }
        #[cfg(not(feature = "backend-rest"))]
        "rest" => Err(VgerError::UnsupportedBackend(
            "rest (compile with feature 'backend-rest')".into(),
        )),
        other => Err(VgerError::UnsupportedBackend(other.into())),
    }
}
