use std::io::Read;

use crate::error::{VgerError, Result};
use crate::storage::StorageBackend;

/// HTTP REST backend for remote repository access via vger-server.
pub struct RestBackend {
    /// Base URL, e.g. "https://backup.example.com/myrepo"
    base_url: String,
    agent: ureq::Agent,
    token: Option<String>,
}

impl RestBackend {
    pub fn new(base_url: &str, token: Option<&str>) -> Result<Self> {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(30))
            .timeout_read(std::time::Duration::from_secs(300))
            .timeout_write(std::time::Duration::from_secs(300))
            .build();

        let base = base_url.trim_end_matches('/').to_string();

        Ok(Self {
            base_url: base,
            agent,
            token: token.map(|t| t.to_string()),
        })
    }

    fn url(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        format!("{}/{}", self.base_url, key)
    }

    fn apply_auth(&self, req: ureq::Request) -> ureq::Request {
        if let Some(ref token) = self.token {
            req.set("Authorization", &format!("Bearer {token}"))
        } else {
            req
        }
    }

    /// Batch delete multiple keys in a single request.
    pub fn batch_delete(&self, keys: &[&str]) -> Result<()> {
        let url = format!("{}?batch-delete", self.base_url);
        let req = self.apply_auth(self.agent.post(&url));
        let resp = req
            .send_json(ureq::json!(keys))
            .map_err(|e| VgerError::Other(format!("REST batch-delete: {e}")))?;
        if resp.status() >= 400 {
            return Err(VgerError::Other(format!(
                "REST batch-delete failed: HTTP {}",
                resp.status()
            )));
        }
        Ok(())
    }

    /// Get repository statistics from the server.
    pub fn stats(&self) -> Result<serde_json::Value> {
        let url = format!("{}?stats", self.base_url);
        let req = self.apply_auth(self.agent.get(&url));
        let resp = req
            .call()
            .map_err(|e| VgerError::Other(format!("REST stats: {e}")))?;
        let val: serde_json::Value = resp
            .into_json()
            .map_err(|e| VgerError::Other(format!("REST stats parse: {e}")))?;
        Ok(val)
    }

    /// Acquire a lock on the server.
    pub fn acquire_lock(&self, id: &str, info: &serde_json::Value) -> Result<()> {
        let url = format!("{}/locks/{}", self.base_url, id);
        let req = self.apply_auth(self.agent.post(&url));
        let resp = req
            .send_json(info.clone())
            .map_err(|e| VgerError::Other(format!("REST lock acquire: {e}")))?;
        if resp.status() >= 400 {
            return Err(VgerError::Other(format!(
                "REST lock acquire failed: HTTP {}",
                resp.status()
            )));
        }
        Ok(())
    }

    /// Release a lock on the server.
    pub fn release_lock(&self, id: &str) -> Result<()> {
        let url = format!("{}/locks/{}", self.base_url, id);
        let req = self.apply_auth(self.agent.delete(&url));
        let resp = req
            .call()
            .map_err(|e| VgerError::Other(format!("REST lock release: {e}")))?;
        if resp.status() >= 400 {
            return Err(VgerError::Other(format!(
                "REST lock release failed: HTTP {}",
                resp.status()
            )));
        }
        Ok(())
    }

    /// Send a repack plan to the server for server-side compaction.
    pub fn repack(&self, plan: &serde_json::Value) -> Result<serde_json::Value> {
        let url = format!("{}?repack", self.base_url);
        let req = self.apply_auth(self.agent.post(&url));
        let resp = req
            .send_json(plan.clone())
            .map_err(|e| VgerError::Other(format!("REST repack: {e}")))?;
        if resp.status() >= 400 {
            return Err(VgerError::Other(format!(
                "REST repack failed: HTTP {}",
                resp.status()
            )));
        }
        let val: serde_json::Value = resp
            .into_json()
            .map_err(|e| VgerError::Other(format!("REST repack parse: {e}")))?;
        Ok(val)
    }
}

impl StorageBackend for RestBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let url = self.url(key);
        let req = self.apply_auth(self.agent.get(&url));
        match req.call() {
            Ok(resp) => {
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(|e| VgerError::Io(e))?;
                Ok(Some(buf))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VgerError::Other(format!("REST GET {key}: {e}"))),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let url = self.url(key);
        let req = self.apply_auth(self.agent.put(&url));
        req.send_bytes(data)
            .map_err(|e| VgerError::Other(format!("REST PUT {key}: {e}")))?;
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let url = self.url(key);
        let req = self.apply_auth(self.agent.delete(&url));
        match req.call() {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(e) => Err(VgerError::Other(format!("REST DELETE {key}: {e}"))),
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let url = self.url(key);
        let req = self.apply_auth(self.agent.head(&url));
        match req.call() {
            Ok(_) => Ok(true),
            Err(ureq::Error::Status(404, _)) => Ok(false),
            Err(e) => Err(VgerError::Other(format!("REST HEAD {key}: {e}"))),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.trim_start_matches('/');
        let url = format!("{}?list", self.url(prefix));
        let req = self.apply_auth(self.agent.get(&url));
        let resp = req
            .call()
            .map_err(|e| VgerError::Other(format!("REST LIST {prefix}: {e}")))?;
        let keys: Vec<String> = resp
            .into_json()
            .map_err(|e| VgerError::Other(format!("REST LIST parse: {e}")))?;
        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let url = self.url(key);
        let end = offset + length - 1;
        let req = self
            .apply_auth(self.agent.get(&url))
            .set("Range", &format!("bytes={offset}-{end}"));
        match req.call() {
            Ok(resp) => {
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(|e| VgerError::Io(e))?;
                Ok(Some(buf))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VgerError::Other(format!("REST GET_RANGE {key}: {e}"))),
        }
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let key = key.trim_start_matches('/');
        let url = format!("{}?mkdir", self.url(key));
        let req = self.apply_auth(self.agent.post(&url));
        req.call()
            .map_err(|e| VgerError::Other(format!("REST MKDIR {key}: {e}")))?;
        Ok(())
    }
}
