use crate::crypto::chunk_id::ChunkId;
use crate::testutil::test_chunk_id_key;

#[test]
fn compute_deterministic() {
    let key = test_chunk_id_key();
    let data = b"hello world";
    let id1 = ChunkId::compute(&key, data);
    let id2 = ChunkId::compute(&key, data);
    assert_eq!(id1, id2);
}

#[test]
fn compute_different_data_different_id() {
    let key = test_chunk_id_key();
    let id1 = ChunkId::compute(&key, b"hello");
    let id2 = ChunkId::compute(&key, b"world");
    assert_ne!(id1, id2);
}

#[test]
fn compute_different_key_different_id() {
    let key1 = [0xAA; 32];
    let key2 = [0xBB; 32];
    let data = b"same data";
    let id1 = ChunkId::compute(&key1, data);
    let id2 = ChunkId::compute(&key2, data);
    assert_ne!(id1, id2);
}

#[test]
fn to_hex_length() {
    let key = test_chunk_id_key();
    let id = ChunkId::compute(&key, b"test");
    assert_eq!(id.to_hex().len(), 64);
}

#[test]
fn shard_prefix_is_first_byte() {
    let id = ChunkId([0xAB; 32]);
    assert_eq!(id.shard_prefix(), "ab");
}

#[test]
fn empty_data_produces_valid_id() {
    let key = test_chunk_id_key();
    let id = ChunkId::compute(&key, b"");
    assert_eq!(id.to_hex().len(), 64);
    assert_ne!(id.0, [0u8; 32]);
}

#[test]
fn serde_roundtrip() {
    let key = test_chunk_id_key();
    let id = ChunkId::compute(&key, b"roundtrip test");
    let serialized = rmp_serde::to_vec(&id).unwrap();
    let deserialized: ChunkId = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(id, deserialized);
}
