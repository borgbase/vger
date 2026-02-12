use std::sync::LazyLock;

use opendal::layers::BlockingLayer;
use opendal::{BlockingOperator, Operator};

use crate::error::{Result, VgerError};
use crate::storage::StorageBackend;

/// Tokio runtime used by `BlockingLayer` to bridge async OpenDAL services
/// (S3, SFTP) into synchronous calls. Created lazily on first use.
static ASYNC_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let worker_threads = std::thread::available_parallelism()
        .map(|n| n.get().clamp(4, 8))
        .unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for blocking layer")
});

/// Internal default multipart chunk size for S3 uploads (32 MiB).
pub const S3_UPLOAD_CHUNK_SIZE_BYTES: usize = 32 * 1024 * 1024;
/// Internal default multipart concurrency for S3 uploads.
pub const S3_UPLOAD_CONCURRENCY: usize = 4;

pub struct OpendalBackend {
    op: BlockingOperator,
    async_op: Option<Operator>,
    write_chunk_size: Option<usize>,
    write_concurrency: Option<usize>,
}

impl OpendalBackend {
    /// Create a backend from a pre-configured `Operator` (e.g. with layers applied).
    /// Use this for services with native blocking support (e.g. local Fs).
    pub fn from_operator(op: Operator) -> Self {
        Self {
            op: op.blocking(),
            async_op: None,
            write_chunk_size: None,
            write_concurrency: None,
        }
    }

    /// Create a backend from an async-only operator (S3, SFTP).
    /// Adds a `BlockingLayer` to bridge async → blocking.
    pub fn from_async_operator(op: Operator) -> Result<Self> {
        Self::from_async_operator_tuned(op, None, None)
    }

    /// Create a backend from an async-only operator with optional write tuning.
    pub fn from_async_operator_tuned(
        op: Operator,
        write_chunk_size: Option<usize>,
        write_concurrency: Option<usize>,
    ) -> Result<Self> {
        let async_op = op.clone();
        let _guard = ASYNC_RUNTIME.enter();
        let op = op.layer(
            BlockingLayer::create()
                .map_err(|e| VgerError::Other(format!("failed to create blocking layer: {e}")))?,
        );
        Ok(Self {
            op: op.blocking(),
            async_op: Some(async_op),
            write_chunk_size,
            write_concurrency,
        })
    }

    /// Build an `Operator` for local filesystem (without blocking conversion).
    pub fn local_operator(root: &str) -> Result<Operator> {
        let builder = opendal::services::Fs::default().root(root);
        Operator::new(builder)
            .map_err(|e| VgerError::Other(format!("opendal fs init: {e}")))
            .map(|b| b.finish())
    }

    /// Create a backend backed by a local filesystem directory.
    pub fn local(root: &str) -> Result<Self> {
        Ok(Self::from_operator(Self::local_operator(root)?))
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
        Self::from_async_operator_tuned(
            Self::s3_operator(
                bucket,
                region,
                root,
                endpoint,
                access_key_id,
                secret_access_key,
            )?,
            Some(S3_UPLOAD_CHUNK_SIZE_BYTES),
            Some(S3_UPLOAD_CONCURRENCY),
        )
    }

    /// Build an `Operator` for SFTP (without blocking conversion).
    #[cfg(feature = "backend-sftp")]
    pub fn sftp_operator(
        host: &str,
        user: Option<&str>,
        port: Option<u16>,
        root: &str,
        key: Option<&str>,
    ) -> Result<Operator> {
        let port = port.unwrap_or(22);
        let endpoint = format!("ssh://{host}:{port}");
        let mut builder = opendal::services::Sftp::default()
            .endpoint(&endpoint)
            .root(root);
        if let Some(u) = user {
            builder = builder.user(u);
        }
        if let Some(k) = key {
            builder = builder.key(k);
        }
        Operator::new(builder)
            .map_err(|e| VgerError::Other(format!("opendal sftp init: {e}")))
            .map(|b| b.finish())
    }

    /// Create a backend backed by SFTP.
    #[cfg(feature = "backend-sftp")]
    pub fn sftp(
        host: &str,
        user: Option<&str>,
        port: Option<u16>,
        root: &str,
        key: Option<&str>,
    ) -> Result<Self> {
        Self::from_async_operator(Self::sftp_operator(host, user, port, root, key)?)
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
        // For async-backed operators (S3/SFTP), use the async operator to apply
        // multipart tuning where configured.
        if self.write_chunk_size.is_some() || self.write_concurrency.is_some() {
            if let Some(async_op) = self.async_op.as_ref() {
                // Avoid nested runtime block_on in async contexts.
                if tokio::runtime::Handle::try_current().is_err() {
                    let mut write = async_op.write_with(key, data.to_vec());
                    if let Some(chunk_size) = self.write_chunk_size {
                        write = write.chunk(chunk_size);
                    }
                    if let Some(concurrency) = self.write_concurrency {
                        write = write.concurrent(concurrency);
                    }
                    return ASYNC_RUNTIME
                        .block_on(async { write.await })
                        .map_err(VgerError::from);
                }
            }
        }

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
        let entries = self.op.list(prefix).map_err(VgerError::from)?;
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
        if self.write_chunk_size.is_some() || self.write_concurrency.is_some() {
            if let Some(async_op) = self.async_op.as_ref() {
                if tokio::runtime::Handle::try_current().is_err() {
                    let mut write = async_op.write_with(key, data);
                    if let Some(chunk_size) = self.write_chunk_size {
                        write = write.chunk(chunk_size);
                    }
                    if let Some(concurrency) = self.write_concurrency {
                        write = write.concurrent(concurrency);
                    }
                    return ASYNC_RUNTIME
                        .block_on(async { write.await })
                        .map_err(VgerError::from);
                }
            }
        }

        self.op.write(key, data).map_err(VgerError::from)
    }
}
