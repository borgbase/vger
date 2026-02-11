# Architecture Notes & Research

Living document for design research, comparisons with other backup tools, and status of implemented/planned features.

---

## Research: Lessons from rustic-rs (Feb 2025)

rustic-rs is a mature Rust backup tool (~2,900 stars) that is **fully compatible with restic repositories**. Split across several crates (`rustic_core`, `rustic_backend`, `rustic_cdc`, `aes256ctr_poly1305aes`). This section summarizes what borg-rs can learn from their architecture.

### Encryption: AES-256-CTR + Poly1305-AES vs AES-256-GCM

**rustic:** AES-256-CTR with Poly1305-AES (64-byte keys). Wire format: `[16B nonce][ciphertext][16B MAC tag]`. Inherited from restic.

**borg-rs:** AES-256-GCM with 12-byte nonces. Wire format: `[1B type_tag][12B nonce][ciphertext + 16B GCM tag]`.

**Verdict — keep AES-256-GCM:**
- NIST-standardized, hardware-accelerated (AESNI + CLMUL for GHASH)
- Simpler 32-byte keys vs rustic's 64-byte split keys
- Poly1305-AES has no AAD support in rustic's impl. borg-rs uses type_tag as AAD — a security win
- rustic chose Poly1305-AES only for restic compatibility

### Key Derivation: scrypt vs Argon2id

**rustic:** scrypt (N=2^16, r=8, p=1) — restic compatibility.

**borg-rs:** Argon2id — modern, memory-hard, OWASP/IETF recommended.

**Verdict — keep Argon2id.**

### Hashing / Chunk IDs: SHA-256 vs Keyed BLAKE2b

**rustic:** Unkeyed SHA-256 for all content-addressed IDs.

**borg-rs:** Keyed BLAKE2b-256 MAC with `chunk_id_key` derived from master key.

**Verdict — keep keyed BLAKE2b:**
- Prevents content confirmation attacks (adversary can't check if known plaintext is in backup)
- BLAKE2b is faster than SHA-256 in software
- Trade-off: rustic's unkeyed SHA-256 allows dedup across different encryption keys

### Chunking: Rabin Fingerprint vs FastCDC

**rustic:** Custom `rustic_cdc` crate with Rabin64 polynomial. Default ~8 KiB average. Also supports fixed-size chunking and hierarchical multi-level dedup.

**borg-rs:** `fastcdc` v3 crate.

**Verdict — keep FastCDC.** Newer, benchmarks faster. Could consider making chunk sizes configurable and adding fixed-size chunking as a cheap option.

### Compression: ZSTD-only vs LZ4/ZSTD/None

**rustic:** ZSTD only (or none). Tied to repo format version.

**borg-rs:** LZ4, ZSTD, and None with 1-byte tag prefix per chunk.

**Verdict — keep current approach.** Per-chunk tags are more flexible.

---

## Repository Format: Pack Files (Implemented)

Chunks are grouped into **pack files** (~32 MiB) instead of being stored as individual files. This reduces file count by 1000x+, critical for cloud storage costs (fewer PUT/GET ops) and filesystem performance (fewer inodes).

### borg-rs Pack File Format

```
[8B magic "BORGPACK\0"][1B version=1]
[4B blob_0_len LE][blob_0_data]
[4B blob_1_len LE][blob_1_data]
...
[4B blob_N_len LE][blob_N_data]
[encrypted_header][4B header_length LE]
```

- **Per-blob length prefix** (4 bytes): enables forward scanning to recover individual blobs even if the trailing header is corrupted (more resilient than restic/rustic which have no per-blob framing)
- Each blob is a complete RepoObj envelope: `[1B type_tag][12B nonce][ciphertext+16B GCM tag]`
- Each blob is independently encrypted (can read one chunk without decrypting the whole pack)
- Header at the END allows streaming writes without knowing final header size
- Header is encrypted as `pack_object(ObjectType::PackHeader, msgpack(Vec<PackHeaderEntry>))`
- Pack ID = unkeyed BLAKE2b-256 of entire pack contents, stored at `packs/<shard>/<hex_pack_id>`

### Data Packs vs Tree Packs

Two separate `PackWriter` instances:
- **Data packs** — file content chunks. Dynamic target size.
- **Tree packs** — item-stream metadata. Fixed at `min(min_pack_size, 4 MiB)` since metadata is small and read frequently.

### Dynamic Pack Sizing

Pack sizes grow with repository size. Config exposes floor and ceiling:

```yaml
repository:
  min_pack_size: 33554432     # 32 MiB (floor, default)
  max_pack_size: 536870912    # 512 MiB (ceiling, default)
```

Data pack sizing formula:
```
target = clamp(min_pack_size * sqrt(num_data_packs / 100), min_pack_size, max_pack_size)
```

| Data packs in repo | Target pack size |
|--------------------|------------------|
| < 100              | 32 MiB (floor)   |
| 1,000              | ~101 MiB         |
| 10,000             | ~320 MiB         |
| 30,000+            | 512 MiB (cap)    |

`num_data_packs` is computed at `open()` by counting distinct `pack_id` values in the ChunkIndex (zero extra I/O).

### Comparison with Other Approaches

| Aspect | Pack files (rustic/restic/borg-rs) | Individual files (old borg-rs) |
|--------|-----------------------------------|-------------------------------|
| File count | Very low (1 file per ~32 MiB) | Very high (1 file per chunk) |
| S3/cloud cost | Fewer PUT/GET ops, much cheaper | Many small PUTs, expensive at scale |
| Local FS perf | Efficient, fewer inodes | Can hit inode limits, slow listings |
| Random access | Range read from pack | Direct file read, simpler |
| GC/compaction | Need `compact` to reclaim space | Simple file deletion |
| Write speed | Faster (batched writes) | Slower (many small writes) |

The `compact` command reclaims space from orphaned blobs left behind by `delete` and `prune`. See the [Compact Command](#compact-command-implemented) section below.

### rustic/restic Pack File Format (for reference)

```
[blob1_encrypted][blob2_encrypted]...[blobN_encrypted][header_encrypted][header_length: u32 LE]
```

Header entry format (per blob):
- Uncompressed: `[type: 1B][length: 4B][id: 32B]` = 37 bytes
- Compressed:   `[type: 1B][length: 4B][uncompressed_len: 4B][id: 32B]` = 41 bytes

---

## Compact Command (Implemented)

After `delete` or `prune`, chunk refcounts are decremented and entries with refcount 0 are removed from the `ChunkIndex` — but the encrypted blob data remains in pack files. The `compact` command rewrites packs to reclaim this wasted space.

### Algorithm

**Phase 1 — Analysis (read-only):**
1. Enumerate all pack files across 256 shard dirs (`packs/00/` through `packs/ff/`)
2. Read each pack's trailing header to get `Vec<PackHeaderEntry>`
3. Classify each blob as live (exists in `ChunkIndex` at matching pack+offset) or dead
4. Compute `unused_ratio = dead_bytes / total_bytes` per pack
5. Filter packs where `unused_ratio >= threshold` (default 10%)

**Phase 2 — Repack:**
For each candidate pack (most wasteful first, respecting `--max-repack-size` cap):
1. If all blobs are dead → delete the pack file directly
2. Otherwise: read live blobs as encrypted passthrough (no decrypt/re-encrypt cycle)
3. Write into a new pack via a standalone `PackWriter`, flush to storage
4. Update `ChunkIndex` entries to point to the new pack_id/offset
5. `save_state()` — persist index before deleting old pack (crash safety)
6. Delete old pack file

### Crash Safety

The index never points to a deleted pack. Sequence: write new pack → save index → delete old pack. A crash between steps leaves an orphan old pack (harmless, cleaned up on next compact).

### CLI

```
borg-rs compact [--threshold 10] [--max-repack-size 2G] [-n/--dry-run]
```

---

### Type-Safe IDs

**rustic:** Every ID type (`PackId`, `BlobId`, `SnapshotId`, `IndexId`) is a newtype around 32-byte hash. Compile-time prevention of ID confusion.

**borg-rs:** `ChunkId` and `PackId` are newtypes. Other IDs (archive, manifest) are still raw bytes/strings.

**Recommendation:** Add newtypes for `ArchiveId`, `ManifestId`, etc. Zero-cost abstractions that prevent subtle bugs.

### Parallelism & Performance

**rustic:** `rayon` for parallel compression/encryption, `crossbeam-channel` for worker coordination, `pariter` for parallel iterators.

**borg-rs:** Currently single-threaded.

**Recommendation:** `rayon` for the chunk→compress→encrypt pipeline. Natural fit since chunks are independently processable.

### Configuration

**rustic:** TOML with profile-based hierarchical merging (via `conflate` crate), recursive profile includes, env var overrides, per-source backup configs.

**borg-rs:** YAML config inspired by Borgmatic.

**Nice-to-have later:** Profile inheritance, per-source configs, hooks system.

---

## Feature Status

### Implemented

| Feature | Description |
|---------|-------------|
| **Pack files** | Chunks grouped into ~32 MiB packs with dynamic sizing, separate data/tree packs |
| **Retention policies** | `keep_daily`, `keep_weekly`, `keep_monthly`, `keep_yearly`, `keep_last`, `keep_within` |
| **delete command** | Remove individual archives, decrement refcounts |
| **prune command** | Apply retention policies, remove expired archives |
| **check command** | Structural integrity + optional `--verify-data` for full content verification |
| **Type-safe PackId** | Newtype for pack file identifiers with `storage_key()` |
| **compact command** | Rewrite packs to reclaim space from orphaned blobs after delete/prune |

### Planned / Not Yet Implemented

| Feature | Description | Priority |
|---------|-------------|----------|
| **Parallel pipeline** | `rayon` for compress/encrypt | High |
| **File-level cache** | inode/mtime skip for unchanged files (incremental speedup) | High |
| **Type-safe IDs** | Newtypes for `ArchiveId`, `ManifestId` | Medium |
| **Hot/cold tiering** | Separate backends by access frequency | Medium |
| **Hooks** | Before/after/failed/finally per command | Medium |
| **Snapshot filtering** | By host, tag, path, date ranges | Medium |
| **FUSE mount** | Read-only mount of repository | Medium |
| **Async I/O** | Non-blocking storage operations | Medium |
| **Warm-up commands** | Prepare cold storage before access | Low |
| **Metrics** | Prometheus/OpenTelemetry | Low |
| **WebDAV server** | HTTP access to backup contents | Low |
