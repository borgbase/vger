use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::VgerConfig;
use crate::repo::lock;
use crate::repo::Repository;
use crate::storage;
use vger_types::error::{Result, VgerError};

/// Extract the cache_dir override from config as a PathBuf.
pub(crate) fn cache_dir_from_config(config: &VgerConfig) -> Option<PathBuf> {
    config.cache_dir.as_deref().map(PathBuf::from)
}

/// Open a repository from config using the standard backend resolver.
pub fn open_repo(config: &VgerConfig, passphrase: Option<&str>) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository)?;
    Repository::open(backend, passphrase, cache_dir_from_config(config))
}

/// Open a repository without loading the chunk index.
/// Suitable for read-only operations that load or filter the index lazily.
pub fn open_repo_without_index(
    config: &VgerConfig,
    passphrase: Option<&str>,
) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository)?;
    Repository::open_without_index(backend, passphrase, cache_dir_from_config(config))
}

/// Open a repository without loading the chunk index or file cache.
/// Suitable for operations (e.g. restore) that need neither.
pub fn open_repo_without_index_or_cache(
    config: &VgerConfig,
    passphrase: Option<&str>,
) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository)?;
    Repository::open_without_index_or_cache(backend, passphrase, cache_dir_from_config(config))
}

/// Open a repository and execute a mutation while holding an advisory lock.
pub fn with_open_repo_lock<T>(
    config: &VgerConfig,
    passphrase: Option<&str>,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let mut repo = open_repo(config, passphrase)?;
    with_repo_lock(&mut repo, action)
}

/// Return `Err(VgerError::Interrupted)` if the shutdown flag is set.
pub fn check_interrupted(shutdown: Option<&AtomicBool>) -> Result<()> {
    if shutdown.is_some_and(|f| f.load(Ordering::Relaxed)) {
        return Err(VgerError::Interrupted);
    }
    Ok(())
}

/// Execute a repository mutation while holding an advisory lock.
/// Ensures the lock release is always attempted even when the action fails.
/// On error, performs best-effort cleanup (seals partial packs, waits for
/// in-flight uploads, writes pending_index) before releasing the lock.
pub fn with_repo_lock<T>(
    repo: &mut Repository,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let guard = lock::acquire_lock(repo.storage.as_ref())?;
    let result = action(repo);

    if result.is_err() {
        repo.flush_on_abort();
    }

    match lock::release_lock(repo.storage.as_ref(), guard) {
        Ok(()) => result,
        Err(release_err) => {
            if result.is_err() {
                tracing::warn!("failed to release repository lock: {release_err}");
                result
            } else {
                Err(release_err)
            }
        }
    }
}
