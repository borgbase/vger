# Memory optimization for content-addressed backup in Rust

**V'Ger's peak memory is dominated by its in-memory chunk index (~68–112 bytes/entry), and research across both academic literature and production tools shows this can be reduced by 5–20× without meaningful dedup loss.** The most impactful techniques are a tiered index architecture (probabilistic filter + mmap'd on-disk index + locality caching), streaming restore planning, and bounded pipeline buffering. Borg, Restic, Kopia, and Rustic each tackled subsets of this problem — their collective experience, combined with deduplication research from DDFS, Sparse Indexing, and MFDedup, points to a clear optimization roadmap. The Rust ecosystem offers specific advantages (zero-copy deserialization via rkyv, `memmap2`, arena allocators, crossbeam backpressure) that make many of these techniques straightforward to implement.

---

## The chunk index is the primary memory bottleneck

Every content-addressed backup tool struggles with the same fundamental problem: the dedup index grows linearly with repository size. At **112 bytes/entry** (V'Ger's full mode) or **68 bytes/entry** (dedup mode), a repository with 100M chunks consumes **6.8–11.2 GB** just for the index. This dwarfs all other memory consumers.

The existing tools confirm this pattern. Restic loads all index files into a Go map at **~82–134 bytes/entry** (after years of optimization, down from an initial ~134 bytes). Users with 3 TB repositories report OOM kills on 1 GB devices. Borg 1.x uses a C-level HashIndex at **~40–44 bytes/entry** for the repo index, plus a 44-byte/entry chunks cache and a massive files cache — totaling `chunk_count × 164 + file_count × 240` bytes. Kopia avoids the worst of this by designing its local index cache in an **mmap-compatible format**, letting the OS manage paging. Rustic achieves the best per-entry efficiency among existing tools through **tiered index loading**: backup operations load only ChunkIds (~32 bytes/entry), while restore loads the full index with pack locations (~41–45 bytes/entry) — a simple but effective insight that Restic still lacks.

Borg 2.0 took the most radical approach: it **eliminated the repository index entirely** by storing each object separately (findable by its ID without an index). This trades storage backend overhead for zero index memory, which works well for local storage but poorly for cloud backends with per-request costs.

---

## A tiered dedup index can cut memory 5–20×

Academic research and production systems converge on a three-tier architecture for memory-efficient dedup:

**Tier 0 — Probabilistic filter in memory.** A Bloom filter at 1% false-positive rate costs only **~1.2 bytes/entry** (9.6 bits), meaning 100M chunks need just **120 MB** instead of 6.8 GB. Xor filters and Binary Fuse filters are ~30% more space-efficient than Bloom (immutable, but rebuildable after each backup). The DDFS paper showed that combining a Bloom "Summary Vector" with locality caching eliminates **99% of disk I/O** for dedup lookups. False positives merely trigger a harmless on-disk verification — no dedup accuracy is lost. For V'Ger, a Xor filter (via the `xorfilter` crate) rebuilt at finalization time is ideal: immutable filters suit the append-mostly backup model.

**Tier 1 — mmap'd on-disk exact index.** Store the full index (truncated 12-byte hash + pack ID + offset + length ≈ **20–24 bytes/entry**) in a flat file with open-addressing or sorted layout. Memory-map it via `memmap2`. The OS pages in only the working set — hot entries stay resident while cold entries stay on disk. For random-access lookups, mmap is **2–6× faster** than `pread()` when pages are cached, and performs well on SSDs even when cold. Use `madvise(MADV_RANDOM)` to suppress unnecessary readahead. Truncating hashes from 32 bytes to 12 bytes is safe: the collision probability for 100M chunks with 96-bit hashes is ~10⁻⁹, and any match is verified against the full hash in the pack header.

**Tier 2 — Locality-preserved container cache.** When a chunk is found in the on-disk index, load that entire pack's fingerprint manifest into an LRU cache. Backup data exhibits strong spatial locality — chunks that were near each other in a previous backup tend to reappear together. DDFS demonstrated that this technique, combined with the Bloom filter, reduces disk I/O to **~1% of the naive approach**.

The total memory footprint of this architecture for 100M chunks: ~120 MB (Xor filter) + OS-managed mmap virtual space + ~50 MB locality cache ≈ **~200 MB active RAM**, versus 6.8 GB today. This represents a **~34× reduction** with zero dedup loss.

For even lower memory, **Sparse Indexing** (Lillibridge et al., FAST '09) indexes only sampled chunks (~1/8) as "hooks" to find similar segments, achieving near-exact dedup at **~170 MB for TB-scale repositories**. ChunkStash showed that 12.5% sampling loses only **0.1% dedup quality**. Minimal perfect hashing (via the `boomphf` crate at ~3–6 bits/entry) can further compress the in-memory component, though it requires rebuilding when chunks are added.

---

## The previous snapshot is the most powerful dedup hint

The single most impactful insight from the academic literature: **most duplicate chunks in backup N come directly from backup N-1**. MFDedup (FAST '21) demonstrated that deduplicating against just the previous version captures 95%+ of duplicates while requiring memory proportional to only one backup's chunks rather than the entire repository. Block Locality Caching (Meister et al., SYSTOR '13) confirmed that the previous backup run is the best predictor for the current one, outperforming DDFS's container-based caching over time.

V'Ger already has a file cache (device/inode/mtime/ctime/size) that reuses ChunkRefs on cache hit. This can be extended: **on a file cache miss, load the previous snapshot's tree for that subtree and use its chunk references as dedup hints before consulting the global index.** Since the previous snapshot's chunks are overwhelmingly likely to still exist, this gives high dedup coverage with bounded, predictable memory — just the previous snapshot's metadata, not the entire repository index.

Combined with the tiered index for genuinely new chunks, this approach achieves the **sweet spot identified by Fu et al. (FAST '15): ~10–15% of full index memory for 95–99%+ of dedup effectiveness**.

---

## Restore planning can be restructured to use bounded memory

V'Ger's restore planner currently builds a `chunk_targets` map (ChunkId → Vec<WriteTarget>) and loads the full chunk index for lookups. For large restores, both structures can consume hundreds of MB. Four techniques address this:

**Two-pass index scanning** avoids loading the full index. Pass 1 walks the snapshot tree and collects the set of needed ChunkIds into a compact HashSet (~32 bytes/entry, far smaller than the full index). Pass 2 streams through index files one at a time, retaining only entries matching needed chunks. This reduces index memory from O(total_repo_chunks) to O(snapshot_chunks). Restic explored this approach in issue #2523 but never implemented it.

**External sorting of chunk_targets** bounds the planning map's memory. Write (PackId, offset, ChunkId, WriteTarget) tuples to a temporary file, external merge sort by PackId + offset, then stream through the sorted file to build ReadGroups. Memory stays at O(merge_buffer_size) regardless of restore size. A simpler alternative: use SQLite as a temporary indexed store with automatic spill-to-disk.

**Compact WriteTarget representation** cuts per-entry overhead dramatically. Replace file paths with **u32 file indices** into a separate file table (saves ~100+ bytes per target). Pack offset + length into a **u64** (upper 32 bits offset, lower 32 bits length — works for files ≤ 4 GB, which covers nearly all backup targets). Use `SmallVec<[WriteTarget; 1]>` since most chunks are referenced exactly once, avoiding the 24-byte Vec heap allocation overhead for ~80% of entries.

**Partitioned restore** processes subtrees independently rather than planning everything at once. Each subtree's planning state is small. This naturally bounds memory and can improve locality of pack accesses.

Restic's evolution offers useful lessons for the execution phase: PR #3109 switched from buffering entire packs in memory to **streaming pack reads** sorted by offset with a bounded 4 MB buffer. PR #3484 introduced `StreamPack` for unified streaming blob access. V'Ger's ReadGroup coalescing (merge if gap ≤ 256K, merged ≤ 16M) already does something similar. The key additional optimization is the **Forward Assembly Area** (Lillibridge et al., FAST '13), which uses perfect knowledge of future chunk accesses (the restore plan is known in advance) to prioritize pack downloads and maintain a bounded cache of prefetched-but-not-yet-consumed chunks, achieving **5–16× faster restore** with fixed memory.

---

## Pack writer buffering can be halved through streaming compression

V'Ger buffers chunks in pack writers until they reach target size (32M–512M), with multiple pack writers potentially active simultaneously. The primary optimization: **compress chunks as they arrive rather than buffering uncompressed data.** Initialize a ZSTD streaming compressor per pack writer. Each incoming chunk is immediately compressed and appended to the compressed output buffer. Since typical data compresses at least 2:1, this **halves per-writer buffer memory**. Reuse ZSTD context objects (`ZSTD_CCtx`) across packs to avoid repeated allocation.

A **buffer pool with bounded concurrency** provides a hard memory cap. Pre-allocate a fixed pool of pack-sized buffers (e.g., 4 × target_pack_size). Writers check out buffers from the pool; the pool size caps concurrent active packs. When all buffers are in use, chunk processing blocks — creating natural backpressure. Implementation is straightforward: `crossbeam::queue::ArrayQueue<Vec<u8>>`. Total memory bound: `num_buffers × compressed_pack_size`.

For the largest pack sizes (512M target), **mmap'd temporary files** via `memmap2` offload buffer management to the OS kernel. Write compressed chunks to a memory-mapped temp file; the kernel pages out cold pages under memory pressure. This trades potential I/O for guaranteed bounded RSS. Restic already uses temporary files for pack assembly, with memory proportional to `pack_size × (backend_connections + 1)`.

**Double-buffering** (one buffer filling while another uploads) is sufficient to saturate upload bandwidth. The minimum buffering formula is `upload_bandwidth × pack_assembly_time`. For a 100 Mbps upload with 2-second assembly: ~25 MB minimum. With bounded double-buffering: `2 data_writers × target_size × 2 + 1 tree_writer × target_size × 2` gives a predictable, bounded total.

---

## Rust-specific techniques yield compounding savings

**rkyv for zero-copy index deserialization** is the highest-leverage Rust-specific optimization. Instead of deserializing the on-disk index into heap-allocated structures (msgpack → HashMap), rkyv serializes data such that the archived bytes ARE the in-memory representation. Combined with mmap, index loading becomes **O(1) time and O(0) heap allocation** — just mmap the file and cast the pointer. The rkyv benchmark shows 97.3% of theoretical maximum performance for zero-copy access versus ~4% for rmp-serde. For a 400 MB index, this eliminates 400+ MB of heap allocation entirely. Implementation complexity is high (requires deriving Archive traits on all types, breaking msgpack compatibility), but the payoff is transformative. A pragmatic migration: keep msgpack for external/wire formats, use rkyv exclusively for internal on-disk structures (chunk index, pack manifests, file cache).

**crossbeam bounded channels for rayon backpressure** solve a critical gap. Rayon's `par_iter()` eagerly subdivides work, potentially keeping many chunks in flight simultaneously. The pattern:

- Producer sends chunks through `crossbeam::channel::bounded::<Chunk>(64)` (blocks when full)
- Consumer uses `rx.iter().par_bridge().for_each(|chunk| process(chunk))`
- Channel capacity of 32–128 limits buffered data to 32–128 MB regardless of input size

Without this backpressure, memory can grow unboundedly during parallel chunk processing. With it, the pipeline processes chunks at the rate they can be consumed.

**bumpalo arena allocators** eliminate per-allocation overhead for batch operations. During backup's tree-building phase and restore's planning phase, millions of short-lived objects share the same lifetime. Arena allocation is ~10× faster than standard allocation for many small objects and eliminates **16–32 bytes of per-allocation bookkeeping**. For 1M chunks, this saves 16–32 MB of overhead. Use `bumpalo-herd` for rayon thread compatibility (pool of per-thread bump allocators).

Additional per-object savings compound across millions of entries:

- `Box<[T]>` instead of `Vec<T>` for finalized collections saves **8 bytes each** (no capacity field) — for 1M packs, 8 MB saved
- **u32 offsets** instead of u64 for within-pack positions saves 4 bytes/field — for 10M entries with 2 offset fields, 80 MB saved
- `CompactString` stores strings ≤24 bytes inline, eliminating heap allocation for most path components
- `roaring` bitmaps for chunk ID sets are **10–20× more compact** than `HashSet<u32>`, ideal for garbage collection and set operations
- The `bytes` crate enables zero-copy slicing of chunk data between pipeline stages (hashing → compression → pack writing) without copying ~1 MB per chunk

**Memory profiling** should use `dhat` in development (heap profiling with CI-friendly assertions: `assert!(stats.max_bytes <= threshold)`) and `tikv-jemallocator` with profiling in production for identifying allocation hotspots during large backups.

---

## Conclusion

The research points to a clear optimization hierarchy for V'Ger, ordered by memory impact:

**Tier 1 (GB-scale savings):** Replace the in-memory chunk index with a tiered architecture — Xor/Bloom filter (~1.2 bytes/entry) backed by an mmap'd on-disk index (~20–24 bytes/entry in rkyv/zerocopy format). Use previous-snapshot dedup hints to avoid the global index for 95%+ of chunks. This single change converts memory from O(repo_chunks × 68–112) to O(filter_size + OS_managed_pages), cutting peak usage from multiple GB to ~200 MB for 100M chunks.

**Tier 2 (hundreds of MB savings):** Restructure restore planning with two-pass index scanning, external-sorted chunk_targets, and compact u32-based WriteTarget structs. Add bounded channels for rayon backpressure to cap in-flight chunk data. Switch pack writers to streaming compression with a buffer pool.

**Tier 3 (tens of MB compounding savings):** Adopt rkyv for internal serialization formats, arena allocators for batch phases, `Box<[T]>` for finalized collections, u32 offsets, CompactString for paths, and roaring bitmaps for chunk ID sets. Each saves modestly, but they compound across millions of objects.

The academic consensus is unambiguous: **10–15% of full index memory achieves 95–99%+ of dedup effectiveness.** V'Ger can exploit this through the tiered index architecture, previous-snapshot hints, and locality-preserved caching — all validated by production systems (DDFS, Kopia) and peer-reviewed research (Sparse Indexing, MFDedup, BLC). Rust's ownership model, zero-cost abstractions, and zero GC overhead make it uniquely suited to implement these techniques with predictable, minimal memory footprints.