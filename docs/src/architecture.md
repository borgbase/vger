# Architecture

Technical reference for vger's cryptographic, chunking, compression, and storage design decisions.

---

## Cryptography

### Encryption

AEAD with 12-byte random nonces (`AES-256-GCM` or `ChaCha20-Poly1305`).

Rationale:
- Authenticated encryption with modern, audited constructions
- `auto` mode benchmarks `AES-256-GCM` vs `ChaCha20-Poly1305` at init and stores one concrete mode per repo
- Strong performance across mixed CPU capabilities (AES acceleration and non-AES acceleration)
- 32-byte symmetric keys (simpler key management than split-key schemes)
- AEAD AAD always includes the 1-byte type tag; for identity-bound objects it also includes a domain-separated object context (for example: `manifest`, `index`, snapshot ID, chunk ID, or `filecache`)

### Key Derivation

Argon2id for passphrase-to-key derivation.

Rationale:
- Modern memory-hard KDF recommended by OWASP and IETF
- Resists both GPU and ASIC brute-force attacks

### Hashing / Chunk IDs

Keyed BLAKE2b-256 MAC using a `chunk_id_key` derived from the master key.

Rationale:
- Prevents content confirmation attacks (an adversary cannot check whether known plaintext exists in the backup without the key)
- BLAKE2b is faster than SHA-256 in software
- Trade-off: keyed IDs prevent dedup across different encryption keys (acceptable for vger's single-key-per-repo model)

---

## Content Processing

### Chunking

FastCDC (content-defined chunking) via the `fastcdc` v3 crate.

Default parameters: 512 KiB min, 2 MiB average, 8 MiB max (configurable in YAML).
`chunker.max_size` is hard-capped at 16 MiB during config validation.

Rationale:
- Newer algorithm, benchmarks faster than Rabin fingerprinting
- Good deduplication ratio with configurable chunk boundaries

### Compression

Per-chunk compression with a 1-byte tag prefix. Supported algorithms: LZ4, ZSTD, and None.

Rationale:
- Per-chunk tags allow mixing algorithms within a single repository
- LZ4 for speed-sensitive workloads, ZSTD for better compression ratios
- No repository-wide format version lock-in for compression choice
- ZSTD compression reuses a thread-local compressor context per level, reducing allocation churn in parallel backup paths
- Decompression enforces a hard output cap (32 MiB) to bound memory usage and mitigate decompression-bomb inputs

### Deduplication

Content-addressed deduplication uses keyed `ChunkId` values (BLAKE2b-256 MAC). Identical plaintext produces the same `ChunkId`, so the second copy is not stored; only refcounts are incremented.

vger supports three index modes for dedup lookups:

1. **Full index mode** — in-memory `ChunkIndex` (`HashMap<ChunkId, ChunkIndexEntry>`)
2. **Dedup-only mode** — lightweight `DedupIndex` (`ChunkId -> stored_size`) plus `IndexDelta` for mutations
3. **Tiered dedup mode** — `TieredDedupIndex`:
   - session-local HashMap for new chunks in the current backup
   - Xor filter (`xorf::Xor8`) as probabilistic negative check
   - mmap-backed on-disk dedup cache for exact lookup

During backup, `enable_tiered_dedup_mode()` is used by default. If the mmap cache is missing/stale/corrupt, vger safely falls back to dedup-only HashMap mode.

**Two-level dedup check** (in `Repository::bump_ref_if_exists`):
1. **Persistent dedup tier** — full index, dedup-only index, or tiered dedup index (depending on mode)
2. **Pending pack writers** — blobs buffered in data/tree `PackWriter`s that have not yet been flushed

This prevents duplicates both across backups and within a single backup run.

---

## Serialization

All persistent data structures use **msgpack** via `rmp_serde`. Structs serialize as **positional arrays** (not named-field maps) for compactness. This means field order matters — adding or removing fields requires careful versioning, and `#[serde(skip_serializing_if)]` must not be used on `Item` fields (it would break positional deserialization of existing data).

### RepoObj Envelope

Every encrypted object stored in the repository is wrapped in a `RepoObj` envelope (`repo/format.rs`):

```text
[1-byte type_tag][12-byte nonce][ciphertext + 16-byte AEAD tag]
```

The type tag identifies the object kind via the `ObjectType` enum:

| Tag | ObjectType | Used for |
|-----|------------|----------|
| 0 | Config | Repository configuration (stored unencrypted) |
| 1 | Manifest | Snapshot list |
| 2 | SnapshotMeta | Per-snapshot metadata |
| 3 | ChunkData | Compressed file/item-stream chunks |
| 4 | ChunkIndex | Chunk-to-pack mapping |
| 5 | PackHeader | Reserved legacy tag (current pack files have no trailing header object) |
| 6 | FileCache | File-level cache (inode/mtime skip) |

The type tag byte is always included in AAD (authenticated additional data). For identity-bound objects, AAD also includes a domain-separated object context, binding ciphertext to both object type and identity (for example, `ChunkData` to its `ChunkId`, `SnapshotMeta` to snapshot ID, and manifest/index to fixed context labels).

---

## Repository Format

### On-Disk Layout

```text
<repo>/
|- config                    # Repository metadata (unencrypted msgpack)
|- keys/repokey              # Encrypted master key (Argon2id-wrapped)
|- manifest                  # Encrypted snapshot list
|- index                     # Encrypted chunk index
|- snapshots/<id>            # Encrypted snapshot metadata
|- packs/<xx>/<pack-id>      # Pack files containing compressed+encrypted chunks (256 shard dirs)
`- locks/                    # Advisory lock files
```

### Local Optimization Caches (Client Machine)

These files live under a per-repo local cache root. By default this is the platform cache directory + `vger` (for example, `~/.cache/vger/<repo_id_hex>/...` on Linux, `~/Library/Caches/vger/<repo_id_hex>/...` on macOS). If `cache_dir` is set in config, that path becomes the cache root. These are optimization artifacts, not repository source of truth.

```text
<cache>/<repo_id_hex>/
|- filecache                 # File metadata -> cached ChunkRefs
|- dedup_cache               # Sorted ChunkId -> stored_size (mmap + xor filter)
|- restore_cache             # Sorted ChunkId -> pack_id, pack_offset, stored_size (mmap)
`- full_index_cache          # Sorted full index rows for incremental index updates
```

All three index caches are validated against `manifest.index_generation`. A generation mismatch means "stale cache" and triggers safe fallback/rebuild paths.

The same per-repo cache root is also used as the preferred temp location for mmap-backed data-pack assembly files.

### Key Data Structures

**ChunkIndex** — `HashMap<ChunkId, ChunkIndexEntry>`, stored encrypted at the `index` key. The central lookup table for deduplication, restore, and compaction.

| Field | Type | Description |
|-------|------|-------------|
| refcount | u32 | Number of snapshots referencing this chunk |
| stored_size | u32 | Size in bytes as stored (compressed + encrypted) |
| pack_id | PackId | Which pack file contains this chunk |
| pack_offset | u64 | Byte offset within the pack file |

**Manifest** — the encrypted snapshot list stored at the `manifest` key.

| Field | Type | Description |
|-------|------|-------------|
| version | u32 | Format version (currently 1) |
| timestamp | DateTime | Last modification time |
| snapshots | Vec\<SnapshotEntry\> | One entry per snapshot |
| index_generation | u64 | Cache-validity token rotated when index changes; used to validate local mmap caches |

Each `SnapshotEntry` contains: `name`, `id` (32-byte random), `time`, `source_label`, `label`, `source_paths`.

**SnapshotMeta** — per-snapshot metadata stored at `snapshots/<id>`.

| Field | Type | Description |
|-------|------|-------------|
| name | String | User-provided snapshot name |
| hostname | String | Machine that created the backup |
| username | String | User that ran the backup |
| time / time_end | DateTime | Backup start and end timestamps |
| chunker_params | ChunkerConfig | CDC parameters used for this snapshot |
| item_ptrs | Vec\<ChunkId\> | Chunk IDs containing the serialized item stream |
| stats | SnapshotStats | File count, original/compressed/deduplicated sizes |
| source_label | String | Config label for the source |
| source_paths | Vec\<String\> | Directories that were backed up |
| label | String | User-provided annotation |

**Item** — a single filesystem entry within a snapshot's item stream.

| Field | Type | Description |
|-------|------|-------------|
| path | String | Relative path within the backup |
| entry_type | ItemType | `RegularFile`, `Directory`, or `Symlink` |
| mode | u32 | Unix permission bits |
| uid / gid | u32 | Owner and group IDs |
| user / group | Option\<String\> | Owner and group names |
| mtime | i64 | Modification time (nanoseconds since epoch) |
| atime / ctime | Option\<i64\> | Access and change times |
| size | u64 | Original file size |
| chunks | Vec\<ChunkRef\> | Content chunks (regular files only) |
| link_target | Option\<String\> | Symlink target |
| xattrs | Option\<HashMap\> | Extended attributes |

**ChunkRef** — reference to a stored chunk, used in `Item.chunks`:

| Field | Type | Description |
|-------|------|-------------|
| id | ChunkId | Content-addressed chunk identifier |
| size | u32 | Uncompressed (original) size |
| csize | u32 | Stored size (compressed + encrypted) |

### Pack Files

Chunks are grouped into **pack files** (~32 MiB) instead of being stored as individual files. This reduces file count by 1000x+, critical for cloud storage costs (fewer PUT/GET ops) and filesystem performance (fewer inodes).

#### Pack File Format

```text
[8B magic "VGERPACK"][1B version=1]
[4B blob_0_len LE][blob_0_data]
[4B blob_1_len LE][blob_1_data]
...
[4B blob_N_len LE][blob_N_data]
```

- **Per-blob length prefix** (4 bytes): enables forward scanning of all blobs from byte 9 to EOF
- Each blob is a complete RepoObj envelope: `[1B type_tag][12B nonce][ciphertext+16B AEAD tag]`
- Each blob is independently encrypted (can read one chunk without decrypting the whole pack)
- No trailing per-pack header object; pack analysis/compaction enumerate blobs via length-prefix scan
- Pack ID = unkeyed BLAKE2b-256 of entire pack contents, stored at `packs/<shard>/<hex_pack_id>`

#### Data Packs vs Tree Packs

Two separate `PackWriter` instances:
- **Data packs** — file content chunks. Dynamic target size. Assembled in mmap-backed anonymous temp files by default (OS-paged memory behavior under pressure).
- **Tree packs** — item-stream metadata. Fixed at `min(min_pack_size, 4 MiB)` and assembled in heap `Vec<u8>` buffers.

If mmap temp-file allocation fails (or preferred temp location is low on space), data-pack assembly falls back to system temp, then to heap buffers.

#### Dynamic Pack Sizing

Pack sizes grow with repository size. Config exposes floor and ceiling:

```yaml
repositories:
  - path: /backups/repo
    min_pack_size: 33554432     # 32 MiB (floor, default)
    max_pack_size: 134217728    # 128 MiB (default)
```

Data pack sizing formula:
```text
target = clamp(min_pack_size * sqrt(num_data_packs / 100), min_pack_size, max_pack_size)
```

`max_pack_size` has a hard ceiling of **512 MiB**. Values above that are rejected at repository init/open.

| Data packs in repo | Target pack size |
|--------------------|------------------|
| < 100              | 32 MiB (floor)   |
| 1,000              | ~101 MiB         |
| 10,000             | 128 MiB (default cap) |
| 30,000+            | 128 MiB (default cap) |

If you raise `max_pack_size`, target size can grow further, up to the 512 MiB hard ceiling.

`num_data_packs` is computed at `open()` by counting distinct `pack_id` values in the ChunkIndex (zero extra I/O).

---

## Data Flow

### Backup Pipeline

```text
open repo (full index loaded once)
  → prune stale local file-cache entries
  → enable tiered dedup mode (mmap cache + xor filter, fallback to dedup HashMap)
  → configure bounded upload concurrency + pipeline limits
  → walk sources with excludes + one_file_system + exclude_if_present
    → cache-hit path: reuse cached ChunkRefs and bump refs
    → cache-miss path:
      → pipeline path (if pipeline_depth > 0):
        → walk emits regular files and segmented large files
          (segmentation applies when file_size > segment_size;
           effective segment size is clamped to min(segment_size_mib, pipeline_buffer_mib))
        → worker threads read/chunk/hash and classify each chunk:
          - xor prefilter says "maybe present" → hash-only chunk
          - xor prefilter miss (or no filter) → compress + encrypt prepacked chunk
        → sequential consumer validates segment order, performs dedup checks
          (persistent dedup tier + pending pack writers), commits new chunks,
          and handles xor false positives via inline transform
        → ByteBudget enforces pipeline_buffer_mib as a hard in-flight memory cap
      → sequential fallback path (pipeline_depth == 0)
  → serialize items incrementally into item-stream chunks (tree packs)
  → write SnapshotMeta
  → mutate manifest
  → save_state()
    → flush packs/pending uploads (pack flush triggers: target size, 10,000 blobs, or 300s age)
    → if dedup/tiered mode: apply IndexDelta (fast path = incremental full-index-cache merge; slow path = reload full index + merge)
    → write dirty manifest/index only
    → rebuild local dedup/restore/full-index caches as needed
    → persist local file cache
```

### Restore Pipeline

```text
open repository without index (`open_without_index`)
  → resolve snapshot
  → try mmap restore cache (validated by manifest.index_generation)
  → load item stream:
    → preferred: lookup tree-pack chunk locations via restore cache
    → fallback: load full index and read item stream normally
  → stream-decode items in two passes:
    → pass 1 create directories
    → pass 2 create symlinks and plan file chunk writes
  → build coalesced pack read groups:
    → preferred: index-free lookup via restore cache
    → fallback: load full index and retain only snapshot-needed chunks
  → parallel coalesced range reads by pack/offset
    (merge when gap <= 256 KiB and merged range <= 16 MiB)
    → 6 reader workers fetch groups, decrypt + decompress-with-size-hint chunks
    → validate plaintext size and write to all targets (max 16 open files per worker)
  → restore file metadata (mode, mtime, optional xattrs)
```

### Item Stream

Snapshot metadata (the list of files, directories, and symlinks) is **not** stored as a single monolithic blob. Instead:

1. Items are serialized one-by-one as msgpack and appended to an in-memory buffer
2. When the buffer reaches ~128 KiB, it is chunked and stored as a **tree pack** chunk (with a finer CDC config: 32 KiB min / 128 KiB avg / 512 KiB max)
3. The resulting `ChunkId` values are collected into `item_ptrs` in the `SnapshotMeta`

This design means the item stream benefits from deduplication — if most files are unchanged between backups, the item-stream chunks are mostly identical and deduplicated away.

Restore now also consumes item streams incrementally (streaming deserialization) instead of materializing full `Vec<Item>` state up front.
When the mmap restore cache is valid, item-stream chunk lookups can avoid loading the full chunk index.

---

## Operations

### Locking

vger uses advisory locking to prevent concurrent mutating operations on the same repository.

- Preferred path: backend-native lock APIs (`acquire_advisory_lock` / `release_advisory_lock`) when the backend supports them (for example, vger-server)
- Fallback path: lock files at `locks/<timestamp>-<uuid>.json`
- Each lock contains: hostname, PID, and acquisition timestamp
- **Oldest-key-wins**: after writing its lock, a client lists all locks — if its key isn't lexicographically first, it deletes its own lock and returns an error
- **Stale cleanup**: locks older than 6 hours are automatically removed before each acquisition attempt
- **Commands that lock**: `backup`, `delete`, `prune`, `compact`
- **Read-only commands** (no lock): `list`, `restore`, `check`, `info`
- **Recovery**: `vger break-lock` forcibly removes stale backend/object locks when interrupted processes leave lock conflicts

When using a vger server, server-managed locks with TTL replace client-side advisory locks (see [Server Internals](server-internals.md)).

### Daemon Mode

`vger daemon` runs scheduled backup cycles as a foreground process (no cron dependency).

- **Scheduling**: sleep-loop with configurable interval (`schedule.every` human-duration string, e.g. `"6h"`). Optional random jitter (`jitter_seconds`) spreads load across hosts.
- **Cycle**: `backup → prune → compact → check` per repo, sequential. Shutdown flag checked between steps.
- **Signal handling**: two-stage — first SIGTERM/SIGINT sets a shutdown flag (finishes current step, skips remaining); second signal kills immediately.
- **Passphrase**: daemon validates at startup that all encrypted repos have a non-interactive passphrase source (`passcommand`, `passphrase`, or `VGER_PASSPHRASE` env). Cannot prompt interactively.

Configuration:
```yaml
schedule:
  enabled: true
  every: "6h"
  on_startup: false
  jitter_seconds: 0
```

### Refcount Lifecycle

Chunk refcounts track how many snapshots reference each chunk, driving the dedup → delete → compact lifecycle:

1. **Backup** — `store_chunk()` adds a new entry with refcount=1, or increments an existing entry's refcount on dedup hit
2. **Delete / Prune** — `ChunkIndex::decrement()` decreases the refcount; entries reaching 0 are removed from the index
3. **Orphaned blobs** — after delete/prune, the encrypted blob data remains in pack files (the index no longer points to it, but the bytes are still on disk)
4. **Compact** — rewrites packs to reclaim space from orphaned blobs

This design means `delete` is fast (just index updates), while space reclamation is deferred to `compact`.

### Compact

After `delete` or `prune`, chunk refcounts are decremented and entries with refcount 0 are removed from the `ChunkIndex` — but the encrypted blob data remains in pack files. The `compact` command rewrites packs to reclaim this wasted space.

#### Algorithm

**Phase 1 — Analysis (read-only, no pack downloads):**
1. Enumerate all pack files across 256 shard dirs (`packs/00/` through `packs/ff/`)
2. Query each pack's size via metadata-only calls (`HEAD`/stat), parallelized (16 threads remote, 4 local)
3. Compute live bytes per pack from the `ChunkIndex`: `live_bytes = Σ(4 + stored_size)` for each indexed blob in that pack
4. Derive `dead_bytes = (pack_size - PACK_HEADER_SIZE) - live_bytes`; packs where `live_bytes > pack_payload` are marked corrupt
5. Compute `unused_ratio = dead_bytes / pack_size` per pack
6. Track pack health counters (`packs_corrupt`, `packs_orphan`) in addition to live/dead bytes
7. Filter packs where `unused_ratio >= threshold`

**Phase 2 — Repack:**
For each candidate pack (most wasteful first, respecting `--max-repack-size` cap):
1. If backend supports `server_repack`, send a repack plan and apply returned pack remaps
2. Otherwise run client-side repack:
   - If all blobs are dead → delete the pack file directly
   - Else validate pack header (magic + version) via `get_range(0..9)` and cross-check each on-disk blob length prefix against the index's `stored_size`
   - Read live blobs as encrypted passthrough (no decrypt/re-encrypt cycle), write a new pack, update index mappings
3. Persist index/manifest updates before old pack deletion (`save_state()`)
4. Delete old pack(s)

#### Crash Safety

The index never points to a deleted pack. Sequence: write new pack → save index → delete old pack. A crash between steps leaves an orphan old pack (harmless, cleaned up on next compact).

#### CLI

```text
vger compact [--threshold N] [--max-repack-size 2G] [-n/--dry-run]
```

---

## Parallel Pipeline

Backup uses a bounded pipeline:

1. Sequential walk stage emits file work
2. Parallel workers in a crossbeam-channel pipeline read/chunk/hash files and classify chunks (hash-only vs prepacked)
3. A `ByteBudget` enforces a hard cap on in-flight pipeline bytes (`pipeline_buffer_mib`)
4. Consumer stage commits chunks and updates dedup/index state sequentially (including segment-order validation for large files)
5. Pack uploads run in background with bounded in-flight upload concurrency

Large files are split into fixed-size segments and processed through the same worker pool. Segmentation applies only when `file_size > segment_size`, and the effective segment size is clamped to `pipeline_buffer_mib`.

**Configuration:**

```yaml
limits:
  cpu:
    max_threads: 4                 # worker budget (0 = all cores)
    nice: 10                       # Unix nice value
    max_upload_concurrency: 2      # default max in-flight background pack uploads
    transform_batch_mib: 32        # flush threshold for pending transforms
    transform_batch_chunks: 8192   # flush threshold by action count
    pipeline_depth: 4              # 0 disables pipeline, >0 enables bounded pipeline
    pipeline_buffer_mib: 256       # hard cap for in-flight pipeline bytes
    segment_size_mib: 64           # large-file split threshold (16..=256), clamped by pipeline_buffer_mib
  io:
    read_mib_per_sec: 100          # disk read rate limit (0 = unlimited)
```

---

## Why This Is Notable for Backup Tools

Deduplicating backup tools are often dominated by index memory and restore-planning overhead at large chunk counts. vger's implemented architecture addresses that class of bottlenecks with:

- Tiered dedup lookups (session map + xor filter + mmap cache) instead of always materializing a full in-memory index during backup
- mmap-backed data-pack assembly, so pack bytes are OS-paged instead of always heap-resident
- Index-light restore planning (restore-cache-first, filtered-index fallback) for lower peak memory on restore
- Explicitly bounded backup pipeline memory (`pipeline_buffer_mib`) and bounded in-flight uploads
- Incremental index update paths that avoid rebuilding/uploading from a full in-memory index on every save

These optimizations are implementation choices in current vger, not future roadmap items.
