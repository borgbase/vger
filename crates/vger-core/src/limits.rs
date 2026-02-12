use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use nix::errno::Errno;
use tracing::warn;

use crate::config::ResourceLimitsConfig;
use crate::error::{Result, VgerError};
use crate::storage::{parse_repo_url, ParsedUrl, StorageBackend};

const BYTES_PER_MIB: u64 = 1024 * 1024;
const FILE_READ_CHUNK_SIZE: usize = 256 * 1024;

fn mib_per_sec_to_bytes_per_sec(mib_per_sec: u64) -> u64 {
    mib_per_sec.saturating_mul(BYTES_PER_MIB)
}

#[derive(Debug)]
struct LimiterState {
    start: Instant,
    bytes_consumed: u128,
}

/// Simple process-local byte-rate limiter shared by multiple call sites.
#[derive(Debug)]
pub struct ByteRateLimiter {
    bytes_per_sec: u64,
    state: Mutex<LimiterState>,
}

impl ByteRateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            state: Mutex::new(LimiterState {
                start: Instant::now(),
                bytes_consumed: 0,
            }),
        }
    }

    pub fn from_mib_per_sec(mib_per_sec: u64) -> Option<Arc<Self>> {
        if mib_per_sec == 0 {
            None
        } else {
            Some(Arc::new(Self::new(mib_per_sec_to_bytes_per_sec(
                mib_per_sec,
            ))))
        }
    }

    pub fn consume(&self, bytes: usize) {
        if bytes == 0 || self.bytes_per_sec == 0 {
            return;
        }

        let mut state = match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.bytes_consumed = state.bytes_consumed.saturating_add(bytes as u128);

        let elapsed_secs = state.start.elapsed().as_secs_f64();
        let expected_secs = state.bytes_consumed as f64 / self.bytes_per_sec as f64;
        if expected_secs > elapsed_secs {
            std::thread::sleep(Duration::from_secs_f64(expected_secs - elapsed_secs));
        }
    }
}

pub fn read_file_with_limiter(path: &Path, limiter: Option<&ByteRateLimiter>) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(VgerError::Io)?;
    let mut data = Vec::new();
    let mut buffer = [0u8; FILE_READ_CHUNK_SIZE];

    loop {
        let n = file.read(&mut buffer).map_err(VgerError::Io)?;
        if n == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..n]);
        if let Some(limiter) = limiter {
            limiter.consume(n);
        }
    }

    Ok(data)
}

fn storage_rate_limits_for_backup(
    repo_url: &str,
    limits: &ResourceLimitsConfig,
) -> Result<(u64, u64)> {
    let parsed = parse_repo_url(repo_url)?;
    let (read_mib_per_sec, write_mib_per_sec) = match parsed {
        ParsedUrl::Local { .. } => (0, limits.io.write_mib_per_sec),
        ParsedUrl::S3 { .. } | ParsedUrl::Sftp { .. } | ParsedUrl::Rest { .. } => (
            limits.network.read_mib_per_sec,
            limits.network.write_mib_per_sec,
        ),
    };

    Ok((
        mib_per_sec_to_bytes_per_sec(read_mib_per_sec),
        mib_per_sec_to_bytes_per_sec(write_mib_per_sec),
    ))
}

pub fn wrap_backup_storage_backend(
    inner: Box<dyn StorageBackend>,
    repo_url: &str,
    limits: &ResourceLimitsConfig,
) -> Result<Box<dyn StorageBackend>> {
    let (read_bps, write_bps) = storage_rate_limits_for_backup(repo_url, limits)?;
    if read_bps == 0 && write_bps == 0 {
        return Ok(inner);
    }

    let read_limiter = (read_bps > 0).then(|| Arc::new(ByteRateLimiter::new(read_bps)));
    let write_limiter = (write_bps > 0).then(|| Arc::new(ByteRateLimiter::new(write_bps)));

    Ok(Box::new(ThrottledStorageBackend {
        inner,
        read_limiter,
        write_limiter,
    }))
}

struct ThrottledStorageBackend {
    inner: Box<dyn StorageBackend>,
    read_limiter: Option<Arc<ByteRateLimiter>>,
    write_limiter: Option<Arc<ByteRateLimiter>>,
}

impl StorageBackend for ThrottledStorageBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let out = self.inner.get(key)?;
        if let (Some(limiter), Some(data)) = (self.read_limiter.as_ref(), out.as_ref()) {
            limiter.consume(data.len());
        }
        Ok(out)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if let Some(limiter) = self.write_limiter.as_ref() {
            limiter.consume(data.len());
        }
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let out = self.inner.get_range(key, offset, length)?;
        if let (Some(limiter), Some(data)) = (self.read_limiter.as_ref(), out.as_ref()) {
            limiter.consume(data.len());
        }
        Ok(out)
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        self.inner.create_dir(key)
    }
}

/// Guard that restores process niceness when dropped.
pub struct NiceGuard {
    #[cfg(unix)]
    previous_nice: i32,
}

impl NiceGuard {
    pub fn apply(target_nice: i32) -> std::result::Result<Option<Self>, String> {
        if target_nice == 0 {
            return Ok(None);
        }

        #[cfg(unix)]
        {
            let previous = get_process_nice()?;
            set_process_nice(target_nice)?;
            return Ok(Some(Self {
                previous_nice: previous,
            }));
        }

        #[cfg(not(unix))]
        {
            let _ = target_nice;
            Err("limits.cpu.nice is not supported on this platform".to_string())
        }
    }
}

impl Drop for NiceGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Err(err) = set_process_nice(self.previous_nice) {
            warn!(
                "failed to restore process niceness to {}: {err}",
                self.previous_nice
            );
        }
    }
}

#[cfg(unix)]
fn get_process_nice() -> std::result::Result<i32, String> {
    Errno::clear();
    let value = unsafe { nix::libc::getpriority(nix::libc::PRIO_PROCESS, 0) };
    let errno = Errno::last_raw();
    if value == -1 && errno != 0 {
        return Err(format!(
            "getpriority failed: {}",
            std::io::Error::from_raw_os_error(errno)
        ));
    }
    Ok(value)
}

#[cfg(unix)]
fn set_process_nice(value: i32) -> std::result::Result<(), String> {
    Errno::clear();
    let rc = unsafe { nix::libc::setpriority(nix::libc::PRIO_PROCESS, 0, value) };
    if rc != 0 {
        let errno = Errno::last_raw();
        if errno == 0 {
            return Err("setpriority failed".to_string());
        }
        return Err(format!(
            "setpriority failed: {}",
            std::io::Error::from_raw_os_error(errno)
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CpuLimitsConfig, IoLimitsConfig, NetworkLimitsConfig};

    #[test]
    fn mib_conversion() {
        assert_eq!(mib_per_sec_to_bytes_per_sec(0), 0);
        assert_eq!(mib_per_sec_to_bytes_per_sec(1), 1024 * 1024);
        assert_eq!(mib_per_sec_to_bytes_per_sec(8), 8 * 1024 * 1024);
    }

    #[test]
    fn storage_limits_local_repo_uses_io_write() {
        let limits = ResourceLimitsConfig {
            cpu: CpuLimitsConfig::default(),
            io: IoLimitsConfig {
                read_mib_per_sec: 100,
                write_mib_per_sec: 10,
            },
            network: NetworkLimitsConfig {
                read_mib_per_sec: 200,
                write_mib_per_sec: 20,
            },
        };
        let (read_bps, write_bps) = storage_rate_limits_for_backup("/tmp/repo", &limits).unwrap();
        assert_eq!(read_bps, 0);
        assert_eq!(write_bps, 10 * 1024 * 1024);
    }

    #[test]
    fn storage_limits_remote_repo_uses_network() {
        let limits = ResourceLimitsConfig {
            cpu: CpuLimitsConfig::default(),
            io: IoLimitsConfig {
                read_mib_per_sec: 100,
                write_mib_per_sec: 10,
            },
            network: NetworkLimitsConfig {
                read_mib_per_sec: 200,
                write_mib_per_sec: 20,
            },
        };
        let (read_bps, write_bps) =
            storage_rate_limits_for_backup("https://backup.example.com/repo", &limits).unwrap();
        assert_eq!(read_bps, 200 * 1024 * 1024);
        assert_eq!(write_bps, 20 * 1024 * 1024);
    }
}
