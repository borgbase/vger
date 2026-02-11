use opendal::{BlockingOperator, Operator};

use crate::error::{BorgError, Result};
use crate::storage::StorageBackend;

pub struct OpendalBackend {
    op: BlockingOperator,
}

impl OpendalBackend {
    /// Create a backend backed by a local filesystem directory.
    pub fn local(root: &str) -> Result<Self> {
        let builder = opendal::services::Fs::default().root(root);
        let op = Operator::new(builder)
            .map_err(|e| BorgError::Other(format!("opendal fs init: {e}")))?
            .finish()
            .blocking();
        Ok(Self { op })
    }

    /// Create a backend backed by AWS S3 (or S3-compatible like MinIO).
    pub fn s3(bucket: &str, region: &str, root: &str, endpoint: Option<&str>) -> Result<Self> {
        let mut builder = opendal::services::S3::default()
            .bucket(bucket)
            .region(region)
            .root(root);
        if let Some(ep) = endpoint {
            builder = builder.endpoint(ep);
        }
        let op = Operator::new(builder)
            .map_err(|e| BorgError::Other(format!("opendal s3 init: {e}")))?
            .finish()
            .blocking();
        Ok(Self { op })
    }
}

impl StorageBackend for OpendalBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.op.read(key) {
            Ok(buf) => Ok(Some(buf.to_vec())),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BorgError::Storage(e)),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.op
            .write(key, data.to_vec())
            .map_err(BorgError::Storage)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.op.delete(key).map_err(BorgError::Storage)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        match self.op.stat(key) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(BorgError::Storage(e)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let entries = self.op.list(prefix).map_err(BorgError::Storage)?;
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
            Err(e) => Err(BorgError::Storage(e)),
        }
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let dir_key = if key.ends_with('/') {
            key.to_string()
        } else {
            format!("{key}/")
        };
        self.op.create_dir(&dir_key).map_err(BorgError::Storage)
    }
}
