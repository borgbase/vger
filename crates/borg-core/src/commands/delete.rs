use crate::config::BorgConfig;
use crate::error::Result;
use crate::repo::lock;
use crate::repo::Repository;
use crate::storage;

use super::list::{load_archive_items, load_archive_meta};

pub struct DeleteStats {
    pub archive_name: String,
    pub chunks_deleted: u64,
    pub space_freed: u64,
}

pub fn run(
    config: &BorgConfig,
    passphrase: Option<&str>,
    archive_name: &str,
    dry_run: bool,
) -> Result<DeleteStats> {
    let backend = storage::backend_from_config(&config.repository)?;
    let mut repo = Repository::open(backend, passphrase)?;
    let lock_guard = lock::acquire_lock(repo.storage.as_ref())?;

    // Verify archive exists
    let entry = repo
        .manifest
        .find_archive(archive_name)
        .ok_or_else(|| crate::error::BorgError::ArchiveNotFound(archive_name.into()))?;
    let archive_id_hex = hex::encode(&entry.id);

    // Load archive metadata and items to find all chunk refs
    let archive_meta = load_archive_meta(&repo, archive_name)?;
    let items = load_archive_items(&repo, archive_name)?;

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
        for chunk_id in &archive_meta.item_ptrs {
            if let Some(entry) = repo.chunk_index.get(chunk_id) {
                if entry.refcount == 1 {
                    chunks_deleted += 1;
                    space_freed += entry.stored_size as u64;
                }
            }
        }

        lock::release_lock(repo.storage.as_ref(), lock_guard)?;
        return Ok(DeleteStats {
            archive_name: archive_name.to_string(),
            chunks_deleted,
            space_freed,
        });
    }

    // Decrement refcounts for data chunks and delete orphans
    let mut chunks_deleted = 0u64;
    let mut space_freed = 0u64;

    for item in &items {
        for chunk_ref in &item.chunks {
            if let Some((rc, size)) = repo.chunk_index.decrement(&chunk_ref.id) {
                if rc == 0 {
                    repo.storage.delete(&chunk_ref.id.storage_key())?;
                    chunks_deleted += 1;
                    space_freed += size as u64;
                }
            }
        }
    }

    // Decrement refcounts for item-stream chunks
    for chunk_id in &archive_meta.item_ptrs {
        if let Some((rc, size)) = repo.chunk_index.decrement(chunk_id) {
            if rc == 0 {
                repo.storage.delete(&chunk_id.storage_key())?;
                chunks_deleted += 1;
                space_freed += size as u64;
            }
        }
    }

    // Delete archive metadata object
    repo.storage
        .delete(&format!("archives/{archive_id_hex}"))?;

    // Remove from manifest
    repo.manifest.remove_archive(archive_name);

    // Persist state
    repo.save_state()?;
    lock::release_lock(repo.storage.as_ref(), lock_guard)?;

    Ok(DeleteStats {
        archive_name: archive_name.to_string(),
        chunks_deleted,
        space_freed,
    })
}
