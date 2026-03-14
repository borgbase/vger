use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::compress::Compression;
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::testutil::{init_test_environment, MemoryBackend};
use vykar_storage::StorageBackend;
use vykar_types::error::Result;

/// Storage wrapper that delegates to `MemoryBackend` and counts `get_range()` calls.
struct CountingBackend {
    inner: MemoryBackend,
    get_range_count: Arc<AtomicU64>,
}

impl CountingBackend {
    fn new() -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        (
            Self {
                inner: MemoryBackend::new(),
                get_range_count: counter.clone(),
            },
            counter,
        )
    }
}

impl StorageBackend for CountingBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.get_range_count.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(key, offset, length)
    }
    fn create_dir(&self, key: &str) -> Result<()> {
        self.inner.create_dir(key)
    }
    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.inner.put(key, &data)
    }
}

fn repo_on_counting_backend() -> (Repository, Arc<AtomicU64>) {
    init_test_environment();
    let (backend, counter) = CountingBackend::new();
    let mut repo = Repository::init(
        Box::new(backend),
        crate::repo::EncryptionMode::None,
        crate::config::ChunkerConfig::default(),
        None,
        None,
        None,
    )
    .expect("init");
    repo.begin_write_session().expect("begin write session");
    (repo, counter)
}

/// Store N distinct chunks, flush packs, and return their IDs.
fn store_chunks(repo: &mut Repository, count: usize) -> Vec<vykar_types::chunk_id::ChunkId> {
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let data = format!("chunk-data-{i:04}-padding-to-make-it-unique");
        let (id, _, _) = repo
            .store_chunk(data.as_bytes(), Compression::None, PackType::Tree)
            .unwrap();
        ids.push(id);
    }
    repo.flush_packs().unwrap();
    ids
}

/// Resolve chunk IDs to (ChunkId, PackId, offset, stored_size) tuples via the index.
fn resolve_chunks(
    repo: &Repository,
    ids: &[vykar_types::chunk_id::ChunkId],
) -> Vec<(
    vykar_types::chunk_id::ChunkId,
    vykar_types::pack_id::PackId,
    u64,
    u32,
)> {
    ids.iter()
        .map(|id| {
            let entry = *repo.chunk_index().get(id).unwrap();
            (*id, entry.pack_id, entry.pack_offset, entry.stored_size)
        })
        .collect()
}

#[test]
fn adjacent_blobs_coalesce_into_single_read() {
    let (mut repo, counter) = repo_on_counting_backend();
    let ids = store_chunks(&mut repo, 3);
    let chunks = resolve_chunks(&repo, &ids);

    // All chunks should be in the same pack (small data, one flush).
    assert_eq!(
        chunks
            .iter()
            .map(|c| c.1)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1,
        "expected all chunks in one pack"
    );

    // Clear blob cache so everything must be fetched.
    repo.clear_blob_cache();
    counter.store(0, Ordering::Relaxed);

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out).unwrap();

    // Should have been a single coalesced get_range call.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        1,
        "expected 1 coalesced get_range"
    );

    // Verify output matches reading chunks individually.
    let mut expected = Vec::new();
    for id in &ids {
        expected.extend_from_slice(&repo.read_chunk(id).unwrap());
    }
    assert_eq!(out, expected);
}

#[test]
fn cache_hits_skip_network() {
    let (mut repo, counter) = repo_on_counting_backend();
    let ids = store_chunks(&mut repo, 3);
    let chunks = resolve_chunks(&repo, &ids);

    // Pre-populate blob cache by reading each chunk.
    for id in &ids {
        let _ = repo.read_chunk(id).unwrap();
    }

    counter.store(0, Ordering::Relaxed);
    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out).unwrap();

    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "expected 0 get_range calls (all cache hits)"
    );

    let mut expected = Vec::new();
    for id in &ids {
        expected.extend_from_slice(&repo.read_chunk(id).unwrap());
    }
    assert_eq!(out, expected);
}

#[test]
fn cross_pack_grouping() {
    let (mut repo, counter) = repo_on_counting_backend();

    // Store chunks in separate packs by flushing between each.
    let data_a = b"pack-a-chunk-data-padding-unique-aa";
    let data_b = b"pack-b-chunk-data-padding-unique-bb";

    let (id_a, _, _) = repo
        .store_chunk(data_a, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();

    let (id_b, _, _) = repo
        .store_chunk(data_b, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();

    let chunks = resolve_chunks(&repo, &[id_a, id_b]);

    // Verify they're in different packs.
    assert_ne!(
        chunks[0].1, chunks[1].1,
        "expected chunks in different packs"
    );

    repo.clear_blob_cache();
    counter.store(0, Ordering::Relaxed);

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out).unwrap();

    // One get_range per pack.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        2,
        "expected 2 get_range calls (one per pack)"
    );

    // Verify order: id_a data then id_b data.
    let expected_a = repo.read_chunk(&id_a).unwrap();
    let expected_b = repo.read_chunk(&id_b).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&expected_a);
    expected.extend_from_slice(&expected_b);
    assert_eq!(out, expected);
}

#[test]
fn output_order_preserved_across_packs() {
    let (mut repo, _counter) = repo_on_counting_backend();

    // Create chunks in two packs, but request them in interleaved order.
    let (id_a1, _, _) = repo
        .store_chunk(
            b"a1-data-unique-padding-xxxxx",
            Compression::None,
            PackType::Tree,
        )
        .unwrap();
    let (id_a2, _, _) = repo
        .store_chunk(
            b"a2-data-unique-padding-yyyyy",
            Compression::None,
            PackType::Tree,
        )
        .unwrap();
    repo.flush_packs().unwrap();

    let (id_b1, _, _) = repo
        .store_chunk(
            b"b1-data-unique-padding-zzzzz",
            Compression::None,
            PackType::Tree,
        )
        .unwrap();
    repo.flush_packs().unwrap();

    // Request order: a1, b1, a2 — interleaved across packs.
    let chunks = resolve_chunks(&repo, &[id_a1, id_b1, id_a2]);
    repo.clear_blob_cache();

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out).unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(&repo.read_chunk(&id_a1).unwrap());
    expected.extend_from_slice(&repo.read_chunk(&id_b1).unwrap());
    expected.extend_from_slice(&repo.read_chunk(&id_a2).unwrap());
    assert_eq!(
        out, expected,
        "output must follow input order regardless of pack grouping"
    );
}

#[test]
fn empty_input_is_noop() {
    let (mut repo, counter) = repo_on_counting_backend();
    counter.store(0, Ordering::Relaxed);

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&[], &mut out).unwrap();

    assert!(out.is_empty());
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[test]
fn large_gap_splits_into_separate_reads() {
    let (mut repo, counter) = repo_on_counting_backend();

    // Store: small chunk A, large gap chunk (>256 KiB), small chunk B — all in one flush.
    let data_a = b"small-chunk-a-unique-data-xxxxx";
    let gap_data = vec![0x42u8; 300 * 1024]; // 300 KiB — exceeds COALESCE_GAP
    let data_b = b"small-chunk-b-unique-data-yyyyy";

    let (id_a, _, _) = repo
        .store_chunk(data_a, Compression::None, PackType::Tree)
        .unwrap();
    let (id_gap, _, _) = repo
        .store_chunk(&gap_data, Compression::None, PackType::Tree)
        .unwrap();
    let (id_b, _, _) = repo
        .store_chunk(data_b, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();

    // All three should be in the same pack.
    let all_chunks = resolve_chunks(&repo, &[id_a, id_gap, id_b]);
    assert_eq!(
        all_chunks
            .iter()
            .map(|c| c.1)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1,
        "expected all chunks in one pack"
    );

    // Request only A and B, skipping the gap chunk.
    // The gap between A's end and B's start is > 256 KiB (the gap chunk sits between them).
    let chunks = resolve_chunks(&repo, &[id_a, id_b]);
    let gap_between = chunks[1].2 - (chunks[0].2 + chunks[0].3 as u64);
    assert!(
        gap_between > 256 * 1024,
        "gap {gap_between} should exceed COALESCE_GAP (256 KiB) to force a split"
    );

    repo.clear_blob_cache();
    counter.store(0, Ordering::Relaxed);

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out).unwrap();

    // Gap exceeds threshold — should be 2 separate get_range calls.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        2,
        "expected 2 get_range calls due to gap split"
    );

    // Verify output correctness.
    let mut expected = Vec::new();
    expected.extend_from_slice(&repo.read_chunk(&id_a).unwrap());
    expected.extend_from_slice(&repo.read_chunk(&id_b).unwrap());
    assert_eq!(out, expected);
}

#[test]
fn duplicate_chunk_ids_coalesce_into_single_range_read() {
    let (mut repo, counter) = repo_on_counting_backend();
    let ids = store_chunks(&mut repo, 2);
    let chunks = resolve_chunks(&repo, &ids);

    // Build input with duplicates: [chunk0, chunk1, chunk0, chunk1].
    let mut dup_chunks = chunks.clone();
    dup_chunks.extend_from_slice(&chunks);

    repo.clear_blob_cache();
    counter.store(0, Ordering::Relaxed);

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&dup_chunks, &mut out)
        .unwrap();

    // Duplicates sit at the same pack offset and coalesce into one range read.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        1,
        "duplicates at same offset should coalesce into a single range read"
    );

    // Output should be chunk0, chunk1, chunk0, chunk1 in order.
    let c0 = repo.read_chunk(&ids[0]).unwrap();
    let c1 = repo.read_chunk(&ids[1]).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&c0);
    expected.extend_from_slice(&c1);
    expected.extend_from_slice(&c0);
    expected.extend_from_slice(&c1);
    assert_eq!(out, expected);
}
