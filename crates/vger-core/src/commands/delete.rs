use crate::config::VgerConfig;
use crate::error::Result;
use crate::repo::Repository;
use crate::storage;
use tracing::warn;

use super::list::{load_snapshot_items, load_snapshot_meta};
use super::util::with_repo_lock;

pub struct DeleteStats {
    pub snapshot_name: String,
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dry_run: bool,
) -> Result<DeleteStats> {
    let backend = storage::backend_from_config(&config.repository, None)?;
    let mut repo = Repository::open(backend, passphrase)?;

    with_repo_lock(&mut repo, |repo| {
        // Verify snapshot exists
        let entry = repo
            .manifest
            .find_snapshot(snapshot_name)
            .ok_or_else(|| crate::error::VgerError::SnapshotNotFound(snapshot_name.into()))?;
        let snapshot_id_hex = hex::encode(&entry.id);

        // Load snapshot metadata and items to find all chunk refs
        let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
        let items = load_snapshot_items(repo, snapshot_name)?;

        if dry_run {
            // Count what would be freed
            let mut chunks_deleted = 0u64;
            let mut space_freed = 0u64;

            for item in &items {
                for chunk_ref in &item.chunks {
                    if let Some(entry) = repo.chunk_index.get(&chunk_ref.id) {
                        if entry.refcount == 1 {
                            chunks_deleted += 1;
                            space_freed += entry.stored_size as u64;
                        }
                    }
                }
            }
            for chunk_id in &snapshot_meta.item_ptrs {
                if let Some(entry) = repo.chunk_index.get(chunk_id) {
                    if entry.refcount == 1 {
                        chunks_deleted += 1;
                        space_freed += entry.stored_size as u64;
                    }
                }
            }

            return Ok(DeleteStats {
                snapshot_name: snapshot_name.to_string(),
                chunks_deleted,
                space_freed,
            });
        }

        // Decrement refcounts for data chunks
        // Orphaned blobs remain in pack files until a future `compact` command.
        let mut chunks_deleted = 0u64;
        let mut space_freed = 0u64;

        for item in &items {
            for chunk_ref in &item.chunks {
                if let Some((rc, size)) = repo.chunk_index.decrement(&chunk_ref.id) {
                    if rc == 0 {
                        chunks_deleted += 1;
                        space_freed += size as u64;
                    }
                }
            }
        }

        // Decrement refcounts for item-stream chunks
        for chunk_id in &snapshot_meta.item_ptrs {
            if let Some((rc, size)) = repo.chunk_index.decrement(chunk_id) {
                if rc == 0 {
                    chunks_deleted += 1;
                    space_freed += size as u64;
                }
            }
        }

        // Remove from manifest
        repo.manifest.remove_snapshot(snapshot_name);

        // Persist state
        repo.save_state()?;

        // Best-effort cleanup of snapshot metadata object.
        // If this fails after state is persisted, the repo remains consistent and
        // only leaves an orphaned metadata object.
        let snapshot_key = format!("snapshots/{snapshot_id_hex}");
        if let Err(err) = repo.storage.delete(&snapshot_key) {
            warn!(
                snapshot = %snapshot_name,
                key = %snapshot_key,
                error = %err,
                "failed to delete snapshot metadata object after state commit"
            );
        }

        Ok(DeleteStats {
            snapshot_name: snapshot_name.to_string(),
            chunks_deleted,
            space_freed,
        })
    })
}
