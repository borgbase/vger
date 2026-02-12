use vger_core::compress::Compression;
use vger_core::config::ChunkerConfig;
use vger_core::repo::manifest::SnapshotEntry;
use vger_core::repo::pack::PackType;
use vger_core::repo::{EncryptionMode, Repository};
use vger_core::storage::opendal_backend::OpendalBackend;
use chrono::Utc;

fn init_local_repo(dir: &std::path::Path) -> Repository {
    let storage = Box::new(OpendalBackend::local(dir.to_str().unwrap()).unwrap());
    Repository::init(storage, EncryptionMode::None, ChunkerConfig::default(), None, None).unwrap()
}

fn open_local_repo(dir: &std::path::Path) -> Repository {
    let storage = Box::new(OpendalBackend::local(dir.to_str().unwrap()).unwrap());
    Repository::open(storage, None).unwrap()
}

#[test]
fn init_store_reopen_read() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // Init and store chunks
    let data1 = b"chunk one data for integration test";
    let data2 = b"chunk two data for integration test";
    let (id1, id2) = {
        let mut repo = init_local_repo(dir);
        let (id1, _, _) = repo.store_chunk(data1, Compression::Lz4, PackType::Data).unwrap();
        let (id2, _, _) = repo.store_chunk(data2, Compression::Lz4, PackType::Data).unwrap();
        repo.save_state().unwrap();
        (id1, id2)
    };

    // Reopen and verify
    let repo = open_local_repo(dir);
    assert_eq!(repo.chunk_index.len(), 2);
    let read1 = repo.read_chunk(&id1).unwrap();
    let read2 = repo.read_chunk(&id2).unwrap();
    assert_eq!(read1, data1);
    assert_eq!(read2, data2);
}

#[test]
fn manifest_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // Init and add a snapshot entry to manifest
    {
        let mut repo = init_local_repo(dir);
        repo.manifest.snapshots.push(SnapshotEntry {
            name: "test-snapshot".to_string(),
            id: vec![0x42; 32],
            time: Utc::now(),
            source_label: String::new(),
        });
        repo.save_state().unwrap();
    }

    // Reopen and check manifest
    let repo = open_local_repo(dir);
    assert_eq!(repo.manifest.snapshots.len(), 1);
    assert_eq!(repo.manifest.snapshots[0].name, "test-snapshot");
}
