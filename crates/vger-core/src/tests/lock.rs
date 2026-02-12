use crate::repo::lock::{acquire_lock, release_lock};
use crate::storage::StorageBackend;
use crate::testutil::MemoryBackend;
use chrono::{Duration, Utc};

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

#[test]
fn second_lock_is_rejected() {
    let storage = MemoryBackend::new();
    let first = acquire_lock(&storage).unwrap();

    let second = acquire_lock(&storage);
    assert!(second.is_err(), "second lock acquisition should fail");
    let msg = second.unwrap_err().to_string();
    assert!(msg.contains("locked"), "unexpected error: {msg}");

    release_lock(&storage, first).unwrap();
}

#[test]
fn stale_lock_is_cleaned_up() {
    let storage = MemoryBackend::new();
    let stale_key = "locks/00000000000000000000-stale.json";
    let stale_time = (Utc::now() - Duration::hours(7)).to_rfc3339();
    let stale_entry = format!(r#"{{"hostname":"old","pid":1234,"time":"{stale_time}"}}"#);
    storage.put(stale_key, stale_entry.as_bytes()).unwrap();
    assert!(storage.exists(stale_key).unwrap());

    let guard = acquire_lock(&storage).unwrap();
    assert!(
        !storage.exists(stale_key).unwrap(),
        "stale lock should be removed during acquisition"
    );
    release_lock(&storage, guard).unwrap();
}
