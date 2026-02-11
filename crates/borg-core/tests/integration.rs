use borg_core::compress::Compression;
use borg_core::config::ChunkerConfig;
use borg_core::repo::manifest::ArchiveEntry;
use borg_core::repo::{EncryptionMode, Repository};
use borg_core::storage::opendal_backend::OpendalBackend;
use chrono::Utc;

fn init_local_repo(dir: &std::path::Path) -> Repository {
    let storage = Box::new(OpendalBackend::local(dir.to_str().unwrap()).unwrap());
    Repository::init(storage, EncryptionMode::None, ChunkerConfig::default(), None).unwrap()
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
        let (id1, _, _) = repo.store_chunk(data1, Compression::Lz4).unwrap();
        let (id2, _, _) = repo.store_chunk(data2, Compression::Lz4).unwrap();
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

    // Init and add an archive entry to manifest
    {
        let mut repo = init_local_repo(dir);
        repo.manifest.archives.push(ArchiveEntry {
            name: "test-archive".to_string(),
            id: vec![0x42; 32],
            time: Utc::now(),
        });
        repo.save_state().unwrap();
    }

    // Reopen and check manifest
    let repo = open_local_repo(dir);
    assert_eq!(repo.manifest.archives.len(), 1);
    assert_eq!(repo.manifest.archives[0].name, "test-archive");
}
