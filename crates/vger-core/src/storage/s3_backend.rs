use std::io::Read;
use std::time::Duration;

use rusty_s3::actions::{ListObjectsV2, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};

use crate::config::RetryConfig;
use crate::error::{Result, VgerError};
use crate::storage::StorageBackend;

/// Duration for presigned URL validity.
const PRESIGN_DURATION: Duration = Duration::from_secs(3600);

pub struct S3Backend {
    bucket: Bucket,
    credentials: Credentials,
    agent: ureq::Agent,
    retry: RetryConfig,
    /// Prefix (root path) prepended to all keys.
    root: String,
}

impl S3Backend {
    pub fn new(
        bucket_name: &str,
        region: &str,
        root: &str,
        endpoint: &str,
        access_key_id: &str,
        secret_access_key: &str,
        retry: RetryConfig,
    ) -> Result<Self> {
        let base_url = endpoint
            .parse()
            .map_err(|e| VgerError::Config(format!("invalid S3 endpoint URL '{endpoint}': {e}")))?;

        // Endpoint is always explicit in repository URL; use path-style addressing.
        let url_style = UrlStyle::Path;

        let bucket = Bucket::new(
            base_url,
            url_style,
            bucket_name.to_string(),
            region.to_string(),
        )
        .map_err(|e| VgerError::Config(format!("failed to create S3 bucket handle: {e}")))?;

        let credentials = Credentials::new(access_key_id, secret_access_key);

        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(30))
            .timeout_read(Duration::from_secs(300))
            .timeout_write(Duration::from_secs(300))
            .build();

        // Normalize root: strip leading/trailing slashes, ensure trailing slash if non-empty.
        let root = root.trim_matches('/').to_string();

        Ok(Self {
            bucket,
            credentials,
            agent,
            retry,
            root,
        })
    }

    /// Prepend the root prefix to a key.
    fn full_key(&self, key: &str) -> String {
        if self.root.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.root, key)
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
                        "S3 {op_name}: transient error (attempt {}/{}), retrying: {e}",
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
}

impl StorageBackend for S3Backend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .get_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        match self.retry_call(&format!("GET {key}"), || {
            self.agent.get(url.as_str()).call()
        }) {
            Ok(resp) => {
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(VgerError::Io)?;
                Ok(Some(buf))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VgerError::Other(format!("S3 GET {key}: {e}"))),
        }
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.put_bytes(key, data)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.put_bytes(key, &data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .delete_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        self.retry_call(&format!("DELETE {key}"), || {
            self.agent.delete(url.as_str()).call()
        })
        .map_err(|e| VgerError::Other(format!("S3 DELETE {key}: {e}")))?;
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .head_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        match self.retry_call(&format!("HEAD {key}"), || {
            self.agent.head(url.as_str()).call()
        }) {
            Ok(_) => Ok(true),
            Err(ureq::Error::Status(404, _)) => Ok(false),
            Err(e) => Err(VgerError::Other(format!("S3 HEAD {key}: {e}"))),
        }
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .head_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        match self.retry_call(&format!("HEAD {key}"), || {
            self.agent.head(url.as_str()).call()
        }) {
            Ok(resp) => {
                let header = resp.header("Content-Length").ok_or_else(|| {
                    VgerError::Other(format!(
                        "S3 HEAD {key}: response missing Content-Length header"
                    ))
                })?;
                let len = header.parse::<u64>().map_err(|_| {
                    VgerError::Other(format!(
                        "S3 HEAD {key}: invalid Content-Length header: {header}"
                    ))
                })?;
                Ok(Some(len))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VgerError::Other(format!("S3 HEAD {key}: {e}"))),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let root_prefix_len = if self.root.is_empty() {
            0
        } else {
            self.root.len() + 1 // +1 for the '/'
        };

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut action = self.bucket.list_objects_v2(Some(&self.credentials));
            action.query_mut().insert("prefix", &full_prefix);
            if let Some(ref token) = continuation_token {
                action.query_mut().insert("continuation-token", token);
            }
            let url = action.sign(PRESIGN_DURATION);

            let resp = self
                .retry_call(&format!("LIST {prefix}"), || {
                    self.agent.get(url.as_str()).call()
                })
                .map_err(|e| VgerError::Other(format!("S3 LIST {prefix}: {e}")))?;

            let mut body = Vec::new();
            resp.into_reader()
                .read_to_end(&mut body)
                .map_err(VgerError::Io)?;

            let parsed = ListObjectsV2::parse_response(&body).map_err(|e| {
                VgerError::Other(format!("S3 LIST {prefix}: failed to parse response: {e}"))
            })?;

            for obj in &parsed.contents {
                let key = &obj.key;
                // Skip directory markers
                if key.ends_with('/') {
                    continue;
                }
                // Strip root prefix to return relative keys
                if root_prefix_len > 0 && key.len() > root_prefix_len {
                    keys.push(key[root_prefix_len..].to_string());
                } else {
                    keys.push(key.clone());
                }
            }

            match parsed.next_continuation_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(keys)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        if length == 0 {
            return Err(VgerError::Other(format!(
                "S3 GET_RANGE {key}: zero-length read requested"
            )));
        }
        let full_key = self.full_key(key);
        let end = offset
            .checked_add(length)
            .and_then(|n| n.checked_sub(1))
            .ok_or_else(|| {
                VgerError::Other(format!(
                    "S3 GET_RANGE {key}: offset {offset} + length {length} overflows u64"
                ))
            })?;
        let range_header = format!("bytes={offset}-{end}");

        let mut action = self.bucket.get_object(Some(&self.credentials), &full_key);
        action.headers_mut().insert("Range", &range_header);
        let url = action.sign(PRESIGN_DURATION);

        match self.retry_call(&format!("GET_RANGE {key}"), || {
            self.agent
                .get(url.as_str())
                .set("Range", &range_header)
                .call()
        }) {
            Ok(resp) => {
                let status = resp.status();
                if status == 200 {
                    return Err(VgerError::Other(format!(
                        "S3 GET_RANGE {key}: server returned 200 instead of 206 (Range header ignored)"
                    )));
                }
                if status != 206 {
                    return Err(VgerError::Other(format!(
                        "S3 GET_RANGE {key}: unexpected status {status}"
                    )));
                }
                let cap = usize::try_from(length).map_err(|_| {
                    VgerError::Other(format!(
                        "S3 GET_RANGE {key}: length {length} exceeds platform usize"
                    ))
                })?;
                let mut buf = Vec::with_capacity(cap);
                resp.into_reader()
                    .take(length)
                    .read_to_end(&mut buf)
                    .map_err(VgerError::Io)?;
                if buf.len() != cap {
                    return Err(VgerError::Other(format!(
                        "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                        buf.len()
                    )));
                }
                Ok(Some(buf))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(VgerError::Other(format!("S3 GET_RANGE {key}: {e}"))),
        }
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        let dir_key = if key.ends_with('/') {
            self.full_key(key)
        } else {
            self.full_key(&format!("{key}/"))
        };
        let url = self
            .bucket
            .put_object(Some(&self.credentials), &dir_key)
            .sign(PRESIGN_DURATION);

        self.retry_call(&format!("MKDIR {key}"), || {
            self.agent.put(url.as_str()).send_bytes(&[])
        })
        .map_err(|e| VgerError::Other(format!("S3 MKDIR {key}: {e}")))?;
        Ok(())
    }
}

impl S3Backend {
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let full_key = self.full_key(key);
        let url = self
            .bucket
            .put_object(Some(&self.credentials), &full_key)
            .sign(PRESIGN_DURATION);

        self.retry_call(&format!("PUT {key}"), || {
            self.agent.put(url.as_str()).send_bytes(data)
        })
        .map_err(|e| VgerError::Other(format!("S3 PUT {key}: {e}")))?;
        Ok(())
    }
}
