use crate::crypto::chunk_id::ChunkId;
use crate::index::ChunkIndex;
use crate::testutil::test_chunk_id_key;

fn make_id(data: &[u8]) -> ChunkId {
    ChunkId::compute(&test_chunk_id_key(), data)
}

#[test]
fn new_index_is_empty() {
    let index = ChunkIndex::new();
    assert!(index.is_empty());
    assert_eq!(index.len(), 0);
}

#[test]
fn add_and_contains() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    assert!(!index.contains(&id));
    index.add(id, 100);
    assert!(index.contains(&id));
    assert_eq!(index.len(), 1);
}

#[test]
fn add_increments_refcount() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    index.add(id, 100);
    assert_eq!(index.get(&id).unwrap().refcount, 1);
    index.add(id, 100);
    assert_eq!(index.get(&id).unwrap().refcount, 2);
    index.add(id, 100);
    assert_eq!(index.get(&id).unwrap().refcount, 3);
    // Still only one entry
    assert_eq!(index.len(), 1);
}

#[test]
fn get_returns_none_for_missing() {
    let index = ChunkIndex::new();
    let id = make_id(b"nonexistent");
    assert!(index.get(&id).is_none());
}

#[test]
fn decrement_reduces_refcount() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    index.add(id, 200);
    index.add(id, 200);
    assert_eq!(index.get(&id).unwrap().refcount, 2);
    let result = index.decrement(&id);
    assert_eq!(result, Some((1, 200)));
    assert!(index.contains(&id));
}

#[test]
fn decrement_to_zero_removes_entry() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    index.add(id, 100);
    assert_eq!(index.get(&id).unwrap().refcount, 1);
    let result = index.decrement(&id);
    assert_eq!(result, Some((0, 100)));
    assert!(!index.contains(&id));
    assert!(index.is_empty());
}

#[test]
fn decrement_missing_returns_none() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"nonexistent");
    assert_eq!(index.decrement(&id), None);
}

#[test]
fn stored_size_preserved() {
    let mut index = ChunkIndex::new();
    let id = make_id(b"chunk1");
    index.add(id, 42);
    assert_eq!(index.get(&id).unwrap().stored_size, 42);
}

#[test]
fn serde_roundtrip() {
    let mut index = ChunkIndex::new();
    let id1 = make_id(b"chunk1");
    let id2 = make_id(b"chunk2");
    index.add(id1, 100);
    index.add(id2, 200);
    index.add(id1, 100); // refcount=2

    let serialized = rmp_serde::to_vec(&index).unwrap();
    let deserialized: ChunkIndex = rmp_serde::from_slice(&serialized).unwrap();

    assert_eq!(deserialized.len(), 2);
    assert_eq!(deserialized.get(&id1).unwrap().refcount, 2);
    assert_eq!(deserialized.get(&id1).unwrap().stored_size, 100);
    assert_eq!(deserialized.get(&id2).unwrap().refcount, 1);
    assert_eq!(deserialized.get(&id2).unwrap().stored_size, 200);
}
