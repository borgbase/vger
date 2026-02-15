use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, PathBuf};

use crate::error::{Result, VgerError};
use crate::storage::StorageBackend;

/// Storage backend for local filesystem using `std::fs` directly.
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    /// Create a backend rooted at the given directory path.
    pub fn new(root: &str) -> Result<Self> {
        let root_path = PathBuf::from(root);
        // Canonicalize if the path already exists for clearer errors and
        // correct strip_prefix behavior with symlinked roots.
        let root = if root_path.exists() {
            fs::canonicalize(&root_path)?
        } else {
            root_path
        };
        Ok(Self { root })
    }

    /// Reject storage keys that could escape the repository root.
    fn validate_key(key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(VgerError::InvalidFormat("unsafe storage key: empty".into()));
        }
        if key.starts_with('/') || key.starts_with('\\') {
            return Err(VgerError::InvalidFormat(format!(
                "unsafe storage key: absolute path '{key}'"
            )));
        }
        if key.contains('\\') {
            return Err(VgerError::InvalidFormat(format!(
                "unsafe storage key: contains backslash '{key}'"
            )));
        }
        let path = std::path::Path::new(key);
        for component in path.components() {
            if component == Component::ParentDir {
                return Err(VgerError::InvalidFormat(format!(
                    "unsafe storage key: parent traversal '{key}'"
                )));
            }
        }
        Ok(())
    }

    /// Resolve a `/`-separated storage key to a filesystem path under the root.
    fn resolve(&self, key: &str) -> Result<PathBuf> {
        Self::validate_key(key)?;
        Ok(self.root.join(key))
    }
}

impl StorageBackend for LocalBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(key)?;
        match fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.resolve(key)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let path = self.resolve(key)?;
        match fs::metadata(&path) {
            Ok(meta) => Ok(meta.is_file()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.resolve(prefix)?;
        match fs::metadata(&dir) {
            Ok(meta) if meta.is_dir() => {
                let mut keys = Vec::new();
                self.list_recursive(&dir, &mut keys)?;
                Ok(keys)
            }
            Ok(_) => Ok(Vec::new()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(key)?;
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; length as usize];
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => return Err(e.into()),
            }
        }
        buf.truncate(filled);
        Ok(Some(buf))
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let path = self.resolve(key.trim_end_matches('/'))?;
        fs::create_dir_all(&path)?;
        Ok(())
    }
}

impl LocalBackend {
    /// Recursively list all files under `dir`, adding their paths relative to
    /// `self.root` as `/`-separated keys.
    fn list_recursive(&self, dir: &std::path::Path, keys: &mut Vec<String>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                self.list_recursive(&entry.path(), keys)?;
            } else if file_type.is_file() {
                if let Ok(rel) = entry.path().strip_prefix(&self.root) {
                    // Convert to `/`-separated key, matching OpenDAL convention.
                    let key = rel
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    keys.push(key);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_key_rejects_unsafe_keys() {
        // Absolute paths
        assert!(LocalBackend::validate_key("/etc/passwd").is_err());
        assert!(LocalBackend::validate_key("\\Windows\\System32").is_err());

        // Parent traversal
        assert!(LocalBackend::validate_key("../../outside").is_err());
        assert!(LocalBackend::validate_key("foo/../../etc/passwd").is_err());

        // Backslash
        assert!(LocalBackend::validate_key("foo\\bar").is_err());

        // Empty
        assert!(LocalBackend::validate_key("").is_err());
    }

    #[test]
    fn validate_key_accepts_safe_keys() {
        assert!(LocalBackend::validate_key("config").is_ok());
        assert!(LocalBackend::validate_key("packs/ab/deadbeef").is_ok());
        assert!(LocalBackend::validate_key("snapshots/abc123").is_ok());
        assert!(LocalBackend::validate_key("index").is_ok());
        assert!(LocalBackend::validate_key("keys/repokey").is_ok());
    }

    #[test]
    fn exists_returns_false_for_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        assert!(!backend.exists("no_such_file").unwrap());
    }

    #[test]
    fn exists_returns_true_for_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        backend.put("test_file", b"hello").unwrap();
        assert!(backend.exists("test_file").unwrap());
    }

    #[test]
    fn list_returns_empty_for_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        let keys = backend.list("no_such_dir").unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn resolve_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_str().unwrap()).unwrap();
        assert!(backend.get("../../etc/passwd").is_err());
        assert!(backend.put("../escape", b"bad").is_err());
        assert!(backend.delete("/absolute").is_err());
    }
}
