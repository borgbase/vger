use opendal::{BlockingOperator, Operator};

use crate::error::{Result, VgerError};
use crate::storage::StorageBackend;

pub struct OpendalBackend {
    op: BlockingOperator,
}

impl OpendalBackend {
    /// Create a backend backed by a local filesystem directory.
    pub fn local(root: &str) -> Result<Self> {
        let builder = opendal::services::Fs::default().root(root);
        let op = Operator::new(builder)
            .map_err(|e| VgerError::Other(format!("opendal fs init: {e}")))?
            .finish()
            .blocking();
        Ok(Self { op })
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
        let op = Operator::new(builder)
            .map_err(|e| VgerError::Other(format!("opendal s3 init: {e}")))?
            .finish()
            .blocking();
        Ok(Self { op })
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
        let op = Operator::new(builder)
            .map_err(|e| VgerError::Other(format!("opendal sftp init: {e}")))?
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
}
