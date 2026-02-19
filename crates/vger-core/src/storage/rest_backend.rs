use std::io::Read;
use std::time::Duration;

use crate::config::RetryConfig;
use crate::error::{Result, VgerError};
use crate::storage::{BackendLockInfo, RepackPlanRequest, RepackResultResponse, StorageBackend};

/// HTTP REST backend for remote repository access via vger-server.
pub struct RestBackend {
    /// Base URL, e.g. "https://backup.example.com/myrepo"
    base_url: String,
    agent: ureq::Agent,
    token: Option<String>,
    retry: RetryConfig,
}

impl RestBackend {
    pub fn new(base_url: &str, token: Option<&str>, retry: RetryConfig) -> Result<Self> {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(30))
            .timeout_read(Duration::from_secs(300))
            .timeout_write(Duration::from_secs(300))
            .build();

        let base = base_url.trim_end_matches('/').to_string();

        Ok(Self {
            base_url: base,
            agent,
            token: token.map(|t| t.to_string()),
            retry,
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

    /// Retry a closure on transient errors with exponential backoff + jitter.
    #[allow(clippy::result_large_err)]
    fn retry_call<T>(
        &self,
        op_name: &str,
        f: impl Fn() -> std::result::Result<T, ureq::Error>,
    ) -> std::result::Result<T, ureq::Error> {
        let mut delay_ms = self.retry.retry_delay_ms;
        let mut last_err = None;

        for attempt in 0..=self.retry.max_retries {
            if attempt > 0 {
                let jitter = rand::random::<u64>() % delay_ms.max(1);
                std::thread::sleep(Duration::from_millis(delay_ms + jitter));
                delay_ms = (delay_ms * 2).min(self.retry.retry_max_delay_ms);
            }
            match f() {
                Ok(val) => return Ok(val),
                Err(e) if Self::is_retryable(&e) && attempt < self.retry.max_retries => {
                    tracing::warn!(
                        "REST {op_name}: transient error (attempt {}/{}), retrying: {e}",
                        attempt + 1,
                        self.retry.max_retries,
                    );
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap())
    }

    fn is_retryable(err: &ureq::Error) -> bool {
        match err {
            ureq::Error::Transport(_) => true,
            ureq::Error::Status(code, _) => *code == 429 || *code >= 500,
        }
    }

    /// Batch delete multiple keys in a single request.
    pub fn batch_delete(&self, keys: &[String]) -> Result<()> {
        let url = format!("{}?batch-delete", self.base_url);
        let payload = keys.to_vec();
        let resp = self
            .retry_call("batch-delete", || {
                let req = self.apply_auth(self.agent.post(&url));
                req.send_json(payload.clone())
            })
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
        let resp = self
            .retry_call("stats", || {
                let req = self.apply_auth(self.agent.get(&url));
                req.call()
            })
            .map_err(|e| VgerError::Other(format!("REST stats: {e}")))?;
        let val: serde_json::Value = resp
            .into_json()
            .map_err(|e| VgerError::Other(format!("REST stats parse: {e}")))?;
        Ok(val)
    }

    /// Acquire a lock on the server.
    pub fn acquire_lock(&self, id: &str, info: &BackendLockInfo) -> Result<()> {
        let url = format!("{}/locks/{}", self.base_url, id);
        let info = info.clone();
        match self.retry_call("lock-acquire", || {
            let req = self.apply_auth(self.agent.post(&url));
            req.send_json(info.clone())
        }) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(409, _)) => Err(VgerError::Locked(id.to_string())),
            Err(e) => Err(VgerError::Other(format!("REST lock acquire: {e}"))),
        }
    }

    /// Release a lock on the server.
    pub fn release_lock(&self, id: &str) -> Result<()> {
        let url = format!("{}/locks/{}", self.base_url, id);
        match self.retry_call("lock-release", || {
            let req = self.apply_auth(self.agent.delete(&url));
            req.call()
        }) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(e) => Err(VgerError::Other(format!("REST lock release: {e}"))),
        }
    }

    /// Send a repack plan to the server for server-side compaction.
    pub fn repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        let url = format!("{}?repack", self.base_url);
        let plan = plan.clone();
        let resp = self
            .retry_call("repack", || {
                let req = self.apply_auth(self.agent.post(&url));
                req.send_json(plan.clone())
            })
            .map_err(|e| VgerError::Other(format!("REST repack: {e}")))?;
        if resp.status() >= 400 {
            return Err(VgerError::Other(format!(
                "REST repack failed: HTTP {}",
                resp.status()
            )));
        }
        let val: RepackResultResponse = resp
            .into_json()
            .map_err(|e| VgerError::Other(format!("REST repack parse: {e}")))?;
        Ok(val)
    }
}

impl RestBackend {
    /// Shared PUT implementation for both borrowed and owned data.
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let url = self.url(key);
        self.retry_call(&format!("PUT {key}"), || {
            let req = self.apply_auth(self.agent.put(&url));
            req.send_bytes(data)
        })
        .map_err(|e| VgerError::Other(format!("REST PUT {key}: {e}")))?;
        Ok(())
    }
}

impl StorageBackend for RestBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let url = self.url(key);
        match self.retry_call(&format!("GET {key}"), || {
            let req = self.apply_auth(self.agent.get(&url));
            req.call()
        }) {
            Ok(resp) => {
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(VgerError::Io)?;
                Ok(Some(buf))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VgerError::Other(format!("REST GET {key}: {e}"))),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.put_bytes(key, data)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put_bytes(key, &data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let url = self.url(key);
        match self.retry_call(&format!("DELETE {key}"), || {
            let req = self.apply_auth(self.agent.delete(&url));
            req.call()
        }) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(e) => Err(VgerError::Other(format!("REST DELETE {key}: {e}"))),
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let url = self.url(key);
        match self.retry_call(&format!("HEAD {key}"), || {
            let req = self.apply_auth(self.agent.head(&url));
            req.call()
        }) {
            Ok(_) => Ok(true),
            Err(ureq::Error::Status(404, _)) => Ok(false),
            Err(e) => Err(VgerError::Other(format!("REST HEAD {key}: {e}"))),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.trim_start_matches('/');
        let url = if prefix.is_empty() {
            format!("{}?list", self.base_url)
        } else {
            format!("{}?list", self.url(prefix))
        };
        let resp = self
            .retry_call(&format!("LIST {prefix}"), || {
                let req = self.apply_auth(self.agent.get(&url));
                req.call()
            })
            .map_err(|e| VgerError::Other(format!("REST LIST {prefix}: {e}")))?;
        let keys: Vec<String> = resp
            .into_json()
            .map_err(|e| VgerError::Other(format!("REST LIST parse: {e}")))?;
        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let url = self.url(key);
        let end = offset + length - 1;
        let range_header = format!("bytes={offset}-{end}");
        match self.retry_call(&format!("GET_RANGE {key}"), || {
            let req = self
                .apply_auth(self.agent.get(&url))
                .set("Range", &range_header);
            req.call()
        }) {
            Ok(resp) => {
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(VgerError::Io)?;
                Ok(Some(buf))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VgerError::Other(format!("REST GET_RANGE {key}: {e}"))),
        }
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let key = key.trim_start_matches('/');
        let url = format!("{}?mkdir", self.url(key));
        self.retry_call(&format!("MKDIR {key}"), || {
            let req = self.apply_auth(self.agent.post(&url));
            req.call()
        })
        .map_err(|e| VgerError::Other(format!("REST MKDIR {key}: {e}")))?;
        Ok(())
    }

    fn acquire_advisory_lock(&self, lock_id: &str, info: &BackendLockInfo) -> Result<()> {
        self.acquire_lock(lock_id, info)
    }

    fn release_advisory_lock(&self, lock_id: &str) -> Result<()> {
        self.release_lock(lock_id)
    }

    fn server_repack(&self, plan: &RepackPlanRequest) -> Result<RepackResultResponse> {
        self.repack(plan)
    }

    fn batch_delete_keys(&self, keys: &[String]) -> Result<()> {
        self.batch_delete(keys)
    }
}
