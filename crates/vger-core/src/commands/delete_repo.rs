use std::path::Path;

use tracing::info;

use crate::config::VgerConfig;
use crate::error::{Result, VgerError};
use crate::storage::{backend_from_config, parse_repo_url, ParsedUrl};

#[derive(Debug)]
pub struct DeleteRepoStats {
    pub keys_deleted: u64,
}

pub fn run(config: &VgerConfig) -> Result<DeleteRepoStats> {
    let backend = backend_from_config(&config.repository, None)?;

    // Verify this is actually a vger repository
    if !backend.exists("config")? {
        return Err(VgerError::RepoNotFound(config.repository.url.clone()));
    }

    let parsed = parse_repo_url(&config.repository.url)?;

    match parsed {
        ParsedUrl::Local { path } => {
            let repo_path = Path::new(&path);

            // Drop the backend so it releases any handles before removal
            drop(backend);

            // Count files before removing the directory tree
            let mut keys_deleted = 0u64;
            for entry in std::fs::read_dir(repo_path)?.flatten() {
                if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    keys_deleted += 1;
                }
            }

            std::fs::remove_dir_all(repo_path)?;

            info!(path = %path, keys = keys_deleted, "deleted local repository");
            Ok(DeleteRepoStats { keys_deleted })
        }
        _ => {
            // Remote repos: list all keys, try batch delete, fall back to individual deletes
            let all_keys = backend.list("")?;
            let keys_deleted = all_keys.len() as u64;

            if !all_keys.is_empty() {
                match backend.batch_delete_keys(&all_keys) {
                    Ok(()) => {}
                    Err(VgerError::UnsupportedBackend(_)) => {
                        for key in &all_keys {
                            backend.delete(key)?;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }

            info!(url = %config.repository.url, keys = keys_deleted, "deleted remote repository");
            Ok(DeleteRepoStats { keys_deleted })
        }
    }
}
