use serde::Deserialize;

use crate::config::VgerConfig;
use crate::error::{Result, VgerError};
use crate::repo::format::unpack_object;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::Repository;
use crate::snapshot::item::Item;
use crate::snapshot::SnapshotMeta;
use crate::storage;

/// Result of a list operation.
pub enum ListResult {
    /// List of snapshots in the repository.
    Snapshots(Vec<SnapshotEntry>),
    /// Contents of a specific snapshot.
    Items(Vec<Item>),
}

/// Run `vger list`.
pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: Option<&str>,
) -> Result<ListResult> {
    let backend = storage::backend_from_config(&config.repository, None)?;
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

    decode_items_stream(&items_stream)
}

fn decode_items_stream(items_stream: &[u8]) -> Result<Vec<Item>> {
    if items_stream.is_empty() {
        return Ok(Vec::new());
    }

    // Items are encoded as concatenated MsgPack Item objects.
    let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(items_stream));
    let mut items = Vec::new();
    while (de.position() as usize) < items_stream.len() {
        items.push(Item::deserialize(&mut de)?);
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::decode_items_stream;
    use crate::snapshot::item::Item;

    #[test]
    fn decode_streamed_item_sequence() {
        let item = Item {
            path: "b.txt".into(),
            entry_type: crate::snapshot::item::ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
        };

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&rmp_serde::to_vec(&item).unwrap());
        bytes.extend_from_slice(&rmp_serde::to_vec(&item).unwrap());

        let decoded = decode_items_stream(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].path, "b.txt");
        assert_eq!(decoded[1].path, "b.txt");
    }
}
