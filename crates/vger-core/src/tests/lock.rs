use crate::repo::lock::{acquire_lock, release_lock};
use crate::storage::StorageBackend;
use crate::testutil::MemoryBackend;

#[test]
fn acquire_and_release_lock() {
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();
    let key = guard.key().to_string();

    // Lock key should exist in storage
    assert!(storage.exists(&key).unwrap());

    // Release should remove it
    release_lock(&storage, guard).unwrap();
    assert!(!storage.exists(&key).unwrap());
}

#[test]
fn lock_key_in_locks_directory() {
    let storage = MemoryBackend::new();
    let guard = acquire_lock(&storage).unwrap();
    assert!(guard.key().starts_with("locks/"));
    assert!(guard.key().ends_with(".json"));
    release_lock(&storage, guard).unwrap();
}
