use crate::config::VgerConfig;
use crate::error::Result;
use crate::repo::lock;
use crate::repo::Repository;
use crate::storage;

/// Open a repository from config using the standard backend resolver.
pub fn open_repo(config: &VgerConfig, passphrase: Option<&str>) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository, None)?;
    Repository::open(backend, passphrase)
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

/// Execute a repository mutation while holding an advisory lock.
/// Ensures the lock release is always attempted even when the action fails.
pub fn with_repo_lock<T>(
    repo: &mut Repository,
    action: impl FnOnce(&mut Repository) -> Result<T>,
) -> Result<T> {
    let guard = lock::acquire_lock(repo.storage.as_ref())?;
    let result = action(repo);

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
