use crate::error::Result;
use crate::repo::lock;
use crate::repo::Repository;

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
