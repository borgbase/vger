use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::repo::pack::PackType;
use crate::repo::EncryptionMode;
use crate::repo::Repository;
use crate::testutil::{test_repo_plaintext, MemoryBackend};

#[test]
fn init_creates_required_keys() {
    let storage = Box::new(MemoryBackend::new());
    let repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
    )
    .unwrap();

    // config, manifest, and index should exist
    assert!(repo.storage.exists("config").unwrap());
    assert!(repo.storage.exists("manifest").unwrap());
    assert!(repo.storage.exists("index").unwrap());
}

#[test]
fn init_twice_fails() {
    let storage = Box::new(MemoryBackend::new());
    let repo = Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
    )
    .unwrap();

    // Try to init again with the same storage (clone the Arc into a new Box)
    let result = Repository::init(
        Box::new(repo.storage.clone()),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
    );
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("already exists"), "unexpected error: {err}");
}

#[test]
fn store_and_read_chunk_roundtrip() {
    let mut repo = test_repo_plaintext();
    let data = b"hello, this is chunk data for testing";
    let (chunk_id, _stored_size, is_new) = repo
        .store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    assert!(is_new);

    // Flush packs so chunks are readable
    repo.flush_packs().unwrap();

    let read_back = repo.read_chunk(&chunk_id).unwrap();
    assert_eq!(read_back, data);
}

#[test]
fn store_chunk_dedup() {
    let mut repo = test_repo_plaintext();
    let data = b"duplicate chunk data";

    let (id1, _size1, is_new1) = repo
        .store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    assert!(is_new1);

    let (id2, _size2, is_new2) = repo
        .store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    assert!(!is_new2, "second store should be a dedup hit");
    assert_eq!(id1, id2);

    // Flush packs to commit to index
    repo.flush_packs().unwrap();

    // Refcount should be 2
    let entry = repo.chunk_index.get(&id1).unwrap();
    assert_eq!(entry.refcount, 2);
}

#[test]
fn store_chunk_with_compression() {
    let mut repo = test_repo_plaintext();
    let data = b"compressible data that should survive lz4 round-trip";
    let (chunk_id, _stored_size, is_new) = repo
        .store_chunk(data, Compression::Lz4, PackType::Data)
        .unwrap();
    assert!(is_new);

    // Flush packs so chunks are readable
    repo.flush_packs().unwrap();

    let read_back = repo.read_chunk(&chunk_id).unwrap();
    assert_eq!(read_back, data);
}

#[test]
fn save_state_persists_manifest_and_index() {
    let mut repo = test_repo_plaintext();
    let data = b"persistent chunk";
    repo.store_chunk(data, Compression::None, PackType::Data)
        .unwrap();
    repo.save_state().unwrap();

    // Verify manifest and index are updated in storage
    assert!(repo.storage.exists("manifest").unwrap());
    assert!(repo.storage.exists("index").unwrap());

    // Index should have one entry
    assert_eq!(repo.chunk_index.len(), 1);
}

#[test]
fn read_missing_chunk_fails() {
    let mut repo = test_repo_plaintext();
    let fake_id = crate::crypto::chunk_id::ChunkId([0xFF; 32]);
    let result = repo.read_chunk(&fake_id);
    assert!(result.is_err());
}
