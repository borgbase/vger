use std::sync::LazyLock;

use opendal::layers::BlockingLayer;
use opendal::{BlockingOperator, Operator};

use crate::error::{Result, VgerError};
use crate::storage::StorageBackend;

/// Tokio runtime used by async-backed storage adapters (OpenDAL S3 and native
/// SFTP) to bridge into synchronous call sites. Created lazily on first use.
pub(crate) static ASYNC_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let worker_threads = std::thread::available_parallelism()
        .map(|n| n.get().clamp(4, 8))
        .unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for blocking layer")
});

pub struct OpendalBackend {
    op: BlockingOperator,
}

impl OpendalBackend {
    /// Create a backend from an async-only OpenDAL operator (S3).
    /// Adds a `BlockingLayer` to bridge async → blocking.
    ///
    /// All write tuning (multipart chunking, concurrency, throttling,
    /// connection limits) should be applied as layers on the `Operator`
    /// *before* calling this method. OpenDAL picks sensible multipart
    /// defaults for S3 internally.
    pub fn from_async_operator(op: Operator) -> Result<Self> {
        let _guard = ASYNC_RUNTIME.enter();
        let op = op.layer(
            BlockingLayer::create()
                .map_err(|e| VgerError::Other(format!("failed to create blocking layer: {e}")))?,
        );
        Ok(Self { op: op.blocking() })
    }

    /// Build an `Operator` for S3 (without blocking conversion).
    pub fn s3_operator(
        bucket: &str,
        region: &str,
        root: &str,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
    ) -> Result<Operator> {
        let mut builder = opendal::services::S3::default()
            .bucket(bucket)
            .region(region)
            .root(root);
        if let Some(ep) = endpoint {
            builder = builder.endpoint(ep);
        }
        if let Some(key_id) = access_key_id {
            builder = builder.access_key_id(key_id);
        }
        if let Some(secret) = secret_access_key {
            builder = builder.secret_access_key(secret);
        }
        Operator::new(builder)
            .map_err(|e| VgerError::Other(format!("opendal s3 init: {e}")))
            .map(|b| b.finish())
    }

    /// Create a backend backed by AWS S3 (or S3-compatible like MinIO).
    pub fn s3(
        bucket: &str,
        region: &str,
        root: &str,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
    ) -> Result<Self> {
        Self::from_async_operator(Self::s3_operator(
            bucket,
            region,
            root,
            endpoint,
            access_key_id,
            secret_access_key,
        )?)
    }
}

impl StorageBackend for OpendalBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.op.read(key) {
            Ok(buf) => Ok(Some(buf.to_vec())),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(VgerError::from(e)),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.op.write(key, data.to_vec()).map_err(VgerError::from)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.op.delete(key).map_err(VgerError::from)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        match self.op.stat(key) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(VgerError::from(e)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let entries = self
            .op
            .list_with(prefix)
            .recursive(true)
            .call()
            .map_err(VgerError::from)?;
        for entry in entries {
            let path = entry.path().to_string();
            // Skip directory markers
            if !path.ends_with('/') {
                keys.push(path);
            }
        }
        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        match self.op.read_with(key).range(offset..offset + length).call() {
            Ok(buf) => Ok(Some(buf.to_vec())),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(VgerError::from(e)),
        }
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let dir_key = if key.ends_with('/') {
            key.to_string()
        } else {
            format!("{key}/")
        };
        self.op.create_dir(&dir_key).map_err(VgerError::from)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.op.write(key, data).map_err(VgerError::from)
    }
}
