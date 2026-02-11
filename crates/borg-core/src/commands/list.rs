use crate::archive::item::Item;
use crate::archive::ArchiveMeta;
use crate::config::BorgConfig;
use crate::error::{BorgError, Result};
use crate::repo::format::unpack_object;
use crate::repo::manifest::ArchiveEntry;
use crate::repo::Repository;
use crate::storage;

/// Result of a list operation.
pub enum ListResult {
    /// List of archives in the repository.
    Archives(Vec<ArchiveEntry>),
    /// Contents of a specific archive.
    Items(Vec<Item>),
}

/// Run `borg-rs list`.
pub fn run(config: &BorgConfig, passphrase: Option<&str>, archive_name: Option<&str>) -> Result<ListResult> {
    let backend = storage::backend_from_config(&config.repository)?;
    let repo = Repository::open(backend, passphrase)?;

    match archive_name {
        None => {
            // List all archives
            Ok(ListResult::Archives(repo.manifest.archives.clone()))
        }
        Some(name) => {
            // List contents of a specific archive
            let items = load_archive_items(&repo, name)?;
            Ok(ListResult::Items(items))
        }
    }
}

/// Load the ArchiveMeta for an archive by name.
pub fn load_archive_meta(repo: &Repository, archive_name: &str) -> Result<ArchiveMeta> {
    let entry = repo
        .manifest
        .find_archive(archive_name)
        .ok_or_else(|| BorgError::ArchiveNotFound(archive_name.into()))?;

    let archive_id_hex = hex::encode(&entry.id);
    let meta_data = repo
        .storage
        .get(&format!("archives/{archive_id_hex}"))?
        .ok_or_else(|| BorgError::ArchiveNotFound(archive_name.into()))?;

    let (_obj_type, meta_bytes) = unpack_object(&meta_data, repo.crypto.as_ref())?;
    Ok(rmp_serde::from_slice(&meta_bytes)?)
}

/// Load and deserialize all items from an archive.
pub fn load_archive_items(repo: &Repository, archive_name: &str) -> Result<Vec<Item>> {
    let archive_meta = load_archive_meta(repo, archive_name)?;

    // Reconstruct the item stream from item_ptrs
    let mut items_stream = Vec::new();
    for chunk_id in &archive_meta.item_ptrs {
        let chunk_data = repo.read_chunk(chunk_id)?;
        items_stream.extend_from_slice(&chunk_data);
    }

    let items: Vec<Item> = rmp_serde::from_slice(&items_stream)?;
    Ok(items)
}
