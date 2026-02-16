use crate::compress::Compression;
use crate::config::ChunkerConfig;
use crate::repo::pack::PackType;
use crate::repo::EncryptionMode;
use crate::repo::Repository;
use crate::testutil::{test_repo_plaintext, MemoryBackend, PutLog, RecordingBackend};

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
    let entry = repo.chunk_index().get(&id1).unwrap();
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
    // Mark manifest dirty so it gets written (store_chunk only marks index dirty)
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();

    // Verify manifest and index are updated in storage
    assert!(repo.storage.exists("manifest").unwrap());
    assert!(repo.storage.exists("index").unwrap());

    // Index should have one entry
    assert_eq!(repo.chunk_index().len(), 1);
}

#[test]
fn read_missing_chunk_fails() {
    let mut repo = test_repo_plaintext();
    let fake_id = crate::crypto::chunk_id::ChunkId([0xFF; 32]);
    let result = repo.read_chunk(&fake_id);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Dirty tracking tests
// ---------------------------------------------------------------------------

fn repo_on_recording_backend() -> (Repository, PutLog) {
    crate::testutil::init_test_environment();
    let (backend, log) = RecordingBackend::new();
    let repo = Repository::init(
        Box::new(backend),
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
    )
    .expect("failed to init test repo");
    (repo, log)
}

#[test]
fn save_state_no_mutations_skips_writes() {
    let (mut repo, log) = repo_on_recording_backend();

    // Clear the put log from init (which writes config, manifest, index)
    log.clear();

    // No mutations — save_state should not write manifest, index, or file cache
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        !entries.contains(&"manifest".to_string()),
        "manifest should not be written when not dirty: {entries:?}"
    );
    assert!(
        !entries.contains(&"index".to_string()),
        "index should not be written when not dirty: {entries:?}"
    );
}

#[test]
fn save_state_writes_only_dirty_components() {
    let (mut repo, log) = repo_on_recording_backend();
    log.clear();

    // Only mark manifest dirty
    repo.mark_manifest_dirty();
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        entries.contains(&"manifest".to_string()),
        "manifest should be written: {entries:?}"
    );
    assert!(
        !entries.contains(&"index".to_string()),
        "index should NOT be written: {entries:?}"
    );
}

#[test]
fn store_chunk_marks_index_dirty() {
    let (mut repo, log) = repo_on_recording_backend();
    log.clear();

    // Store a chunk — this goes through flush_writer_async (normal mode)
    repo.store_chunk(b"chunk data", Compression::None, PackType::Data)
        .unwrap();
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        entries.contains(&"index".to_string()),
        "index should be written after store_chunk: {entries:?}"
    );
}

#[test]
fn dedup_mode_empty_delta_restores_index_without_write() {
    let (mut repo, log) = repo_on_recording_backend();

    // Store some chunks in normal mode
    let data_a = b"chunk data A for dedup test";
    let (id_a, _, _) = repo
        .store_chunk(data_a, Compression::None, PackType::Data)
        .unwrap();
    repo.mark_manifest_dirty();
    repo.mark_index_dirty();
    repo.save_state().unwrap();

    // Verify chunk is in the index
    assert_eq!(repo.chunk_index().len(), 1);
    let entry_before = *repo.chunk_index().get(&id_a).unwrap();

    // Enable dedup mode (drops full index)
    repo.enable_dedup_mode();
    assert!(
        repo.chunk_index().is_empty(),
        "chunk_index should be empty in dedup mode"
    );

    log.clear();

    // save_state with no new chunks — empty delta
    repo.save_state().unwrap();

    // chunk_index should be restored from storage
    assert_eq!(
        repo.chunk_index().len(),
        1,
        "chunk_index should be restored"
    );
    let entry_after = *repo.chunk_index().get(&id_a).unwrap();
    assert_eq!(entry_before.refcount, entry_after.refcount);

    // No index write should have occurred (delta was empty)
    let entries = log.entries();
    assert!(
        !entries.contains(&"index".to_string()),
        "index should NOT be rewritten for empty delta: {entries:?}"
    );
}

#[test]
fn dedup_mode_with_delta_writes_index() {
    let (mut repo, log) = repo_on_recording_backend();

    // Store initial chunks
    repo.store_chunk(b"initial chunk data", Compression::None, PackType::Data)
        .unwrap();
    repo.mark_index_dirty();
    repo.save_state().unwrap();
    assert_eq!(repo.chunk_index().len(), 1);

    // Enable dedup mode
    repo.enable_dedup_mode();
    log.clear();

    // Store a NEW chunk in dedup mode
    let (new_id, _, is_new) = repo
        .store_chunk(
            b"new chunk in dedup mode",
            Compression::None,
            PackType::Data,
        )
        .unwrap();
    assert!(is_new);

    repo.save_state().unwrap();

    // Index should be written because delta had new entries
    let entries = log.entries();
    assert!(
        entries.contains(&"index".to_string()),
        "index should be written for non-empty delta: {entries:?}"
    );

    // chunk_index should contain both old and new entries
    assert_eq!(repo.chunk_index().len(), 2);
    assert!(repo.chunk_index().get(&new_id).is_some());
}

#[test]
fn dirty_flags_reset_after_save() {
    let (mut repo, log) = repo_on_recording_backend();

    // Mark everything dirty and save
    repo.mark_manifest_dirty();
    repo.mark_index_dirty();
    repo.mark_file_cache_dirty();
    repo.save_state().unwrap();

    // Clear log and save again — nothing should be written
    log.clear();
    repo.save_state().unwrap();

    let entries = log.entries();
    assert!(
        !entries.contains(&"manifest".to_string()),
        "manifest should not be rewritten: {entries:?}"
    );
    assert!(
        !entries.contains(&"index".to_string()),
        "index should not be rewritten: {entries:?}"
    );
}

#[test]
fn index_delta_is_empty() {
    use crate::index::IndexDelta;

    let empty = IndexDelta::new();
    assert!(empty.is_empty());

    let mut with_bump = IndexDelta::new();
    with_bump.bump_refcount(&crate::crypto::chunk_id::ChunkId([0xAA; 32]));
    assert!(!with_bump.is_empty());
}
