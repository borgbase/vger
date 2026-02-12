use crate::snapshot::item::Item;
use crate::snapshot::SnapshotMeta;
use crate::config::VgerConfig;
use crate::error::{VgerError, Result};
use crate::repo::format::unpack_object;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::Repository;
use crate::storage;

/// Result of a list operation.
pub enum ListResult {
    /// List of snapshots in the repository.
    Snapshots(Vec<SnapshotEntry>),
    /// Contents of a specific snapshot.
    Items(Vec<Item>),
}

/// Run `vger list`.
pub fn run(config: &VgerConfig, passphrase: Option<&str>, snapshot_name: Option<&str>) -> Result<ListResult> {
    let backend = storage::backend_from_config(&config.repository)?;
    let repo = Repository::open(backend, passphrase)?;

    match snapshot_name {
        None => {
            // List all snapshots
            Ok(ListResult::Snapshots(repo.manifest.snapshots.clone()))
        }
        Some(name) => {
            // List contents of a specific snapshot
            let items = load_snapshot_items(&repo, name)?;
            Ok(ListResult::Items(items))
        }
    }
}

/// Load the SnapshotMeta for a snapshot by name.
pub fn load_snapshot_meta(repo: &Repository, snapshot_name: &str) -> Result<SnapshotMeta> {
    let entry = repo
        .manifest
        .find_snapshot(snapshot_name)
        .ok_or_else(|| VgerError::SnapshotNotFound(snapshot_name.into()))?;

    let snapshot_id_hex = hex::encode(&entry.id);
    let meta_data = repo
        .storage
        .get(&format!("snapshots/{snapshot_id_hex}"))?
        .ok_or_else(|| VgerError::SnapshotNotFound(snapshot_name.into()))?;

    let (_obj_type, meta_bytes) = unpack_object(&meta_data, repo.crypto.as_ref())?;
    Ok(rmp_serde::from_slice(&meta_bytes)?)
}

/// Load and deserialize all items from a snapshot.
pub fn load_snapshot_items(repo: &Repository, snapshot_name: &str) -> Result<Vec<Item>> {
    let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;

    // Reconstruct the item stream from item_ptrs
    let mut items_stream = Vec::new();
    for chunk_id in &snapshot_meta.item_ptrs {
        let chunk_data = repo.read_chunk(chunk_id)?;
        items_stream.extend_from_slice(&chunk_data);
    }

    let items: Vec<Item> = rmp_serde::from_slice(&items_stream)?;
    Ok(items)
}
