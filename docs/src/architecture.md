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
- The 1-byte type tag is passed as AAD (authenticated additional data), binding the ciphertext to its intended object type

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
| 5 | PackHeader | Trailing header inside pack files |
| 6 | FileCache | File-level cache (inode/mtime skip) |

The type tag byte is passed as AAD (authenticated additional data) to the selected AEAD mode. This binds each ciphertext to its intended object type, preventing an attacker from substituting one object type for another (e.g., swapping a manifest for a snapshot).

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

These files live in the local cache directory (`~/.cache/vger/<repo_id_hex>/...` on Linux, `~/Library/Caches/vger/<repo_id_hex>/...` on macOS). They are optimization artifacts, not repository source of truth.

```text
<cache>/<repo_id_hex>/
|- filecache                 # File metadata -> cached ChunkRefs
|- dedup_cache               # Sorted ChunkId -> stored_size (mmap + xor filter)
|- restore_cache             # Sorted ChunkId -> pack_id, pack_offset, stored_size (mmap)
`- full_index_cache          # Sorted full index rows for incremental index updates
```

All three index caches are validated against `manifest.index_generation`. A generation mismatch means "stale cache" and triggers safe fallback/rebuild paths.

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
[8B magic "VGERPACK\0"][1B version=1]
[4B blob_0_len LE][blob_0_data]
[4B blob_1_len LE][blob_1_data]
...
[4B blob_N_len LE][blob_N_data]
[encrypted_header][4B header_length LE]
```

- **Per-blob length prefix** (4 bytes): enables forward scanning to recover individual blobs even if the trailing header is corrupted
- Each blob is a complete RepoObj envelope: `[1B type_tag][12B nonce][ciphertext+16B AEAD tag]`
- Each blob is independently encrypted (can read one chunk without decrypting the whole pack)
- Header at the END allows streaming writes without knowing final header size
- Header is encrypted as `pack_object(ObjectType::PackHeader, msgpack(Vec<PackHeaderEntry>))`
- Pack ID = unkeyed BLAKE2b-256 of entire pack contents, stored at `packs/<shard>/<hex_pack_id>`

#### Data Packs vs Tree Packs

Two separate `PackWriter` instances:
- **Data packs** — file content chunks. Dynamic target size.
- **Tree packs** — item-stream metadata. Fixed at `min(min_pack_size, 4 MiB)` since metadata is small and read frequently.

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
  → configure bounded upload concurrency and activate pack buffer pool
  → walk sources with excludes
    → pipeline path (if enabled): walk → parallel read/chunk/hash/compress/encrypt → sequential commit
      → ByteBudget enforces pipeline_buffer_mib as a real in-flight memory cap
    → sequential path (fallback when pipeline disabled)
    → dedup check (persistent dedup tier + pending pack writers)
      → [new chunk] commit encrypted blob to data/tree pack writer
      → [dedup hit] bump refcount only
  → serialize items incrementally into item-stream chunks (tree packs)
  → write SnapshotMeta
  → mutate manifest
  → save_state()
    → flush packs and pending uploads
    → release pack buffer pool
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
  → parallel range reads by pack/offset, decrypt + decompress chunks, write targets
  → restore file metadata (mode, mtime, optional xattrs)
```

### Item Stream

Snapshot metadata (the list of files, directories, and symlinks) is **not** stored as a single monolithic blob. Instead:

1. Items are serialized one-by-one as msgpack and appended to an in-memory buffer
2. When the buffer reaches ~128 KiB, it is chunked and stored as a **tree pack** chunk (with a finer CDC config: 32 KiB min / 128 KiB avg / 512 KiB max)
3. The resulting `ChunkId` values are collected into `item_ptrs` in the `SnapshotMeta`

This design means the item stream benefits from deduplication — if most files are unchanged between backups, the item-stream chunks are mostly identical and deduplicated away.

Restore now also consumes item streams incrementally (streaming deserialization) instead of materializing full `Vec<Item>` state up front.

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
- **Read-only commands** (no lock): `list`, `extract`, `check`, `info`
- **Recovery**: `vger break-lock` forcibly removes stale backend/object locks when interrupted processes leave lock conflicts

When using a vger server, server-managed locks with TTL replace client-side advisory locks (see [Server Architecture](#server-architecture)).

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

**Phase 1 — Analysis (read-only):**
1. Enumerate all pack files across 256 shard dirs (`packs/00/` through `packs/ff/`)
2. Read each pack's trailing header to get `Vec<PackHeaderEntry>`
3. Classify each blob as live (exists in `ChunkIndex` at matching pack+offset) or dead
4. Compute `unused_ratio = dead_bytes / total_bytes` per pack
5. Track pack health counters (`packs_corrupt`, `packs_orphan`) in addition to live/dead bytes
6. Filter packs where `unused_ratio >= threshold` (default 10%)

**Phase 2 — Repack:**
For each candidate pack (most wasteful first, respecting `--max-repack-size` cap):
1. If backend supports `server_repack`, send a repack plan and apply returned pack remaps
2. Otherwise run client-side repack:
   - If all blobs are dead → delete the pack file directly
   - Else read live blobs as encrypted passthrough (no decrypt/re-encrypt cycle), write a new pack, update index mappings
3. Persist index/manifest updates before old pack deletion (`save_state()`)
4. Delete old pack(s)

#### Crash Safety

The index never points to a deleted pack. Sequence: write new pack → save index → delete old pack. A crash between steps leaves an orphan old pack (harmless, cleaned up on next compact).

#### CLI

```text
vger compact [--threshold 10] [--max-repack-size 2G] [-n/--dry-run]
```

---

## Parallel Pipeline

Backup uses a bounded pipeline:

1. Sequential walk stage emits file work
2. Parallel workers (pariter-based) read/chunk/hash/compress/encrypt files
3. A `ByteBudget` enforces a hard cap on in-flight pipeline bytes (`pipeline_buffer_mib`)
4. Consumer stage commits chunks and updates dedup/index state sequentially
5. Pack uploads run in background with bounded in-flight upload concurrency

The CPU-heavy transform stage uses rayon pools, while pipeline orchestration uses bounded worker queues. Large files can bypass full in-memory materialization and stream inline to keep memory predictable.

**Configuration:**

```yaml
limits:
  cpu:
    max_threads: 4                 # worker budget (0 = all cores)
    nice: 10                       # Unix nice value
    max_upload_concurrency: 4      # max in-flight background pack uploads
    transform_batch_mib: 32        # flush threshold for pending transforms
    transform_batch_chunks: 8192   # flush threshold by action count
    pipeline_depth: 4              # 0 disables pipeline, >0 enables bounded pipeline
    pipeline_buffer_mib: 256       # hard cap for in-flight pipeline bytes
  io:
    read_mib_per_sec: 100          # disk read rate limit (0 = unlimited)
```

---

## Why This Is Notable for Backup Tools

Deduplicating backup tools are often dominated by index memory and restore-planning overhead at large chunk counts. vger's implemented architecture addresses that class of bottlenecks with:

- Tiered dedup lookups (session map + xor filter + mmap cache) instead of always materializing a full in-memory index during backup
- Index-light restore planning (restore-cache-first, filtered-index fallback) for lower peak memory on extract
- Explicitly bounded backup pipeline memory (`pipeline_buffer_mib`) and bounded in-flight uploads
- Incremental index update paths that avoid rebuilding/uploading from a full in-memory index on every save

These optimizations are implementation choices in current vger, not future roadmap items.

---

## Server Architecture

vger includes a dedicated backup server (`vger-server`) for features that dumb storage (S3/WebDAV) cannot provide. The server stores data on its local filesystem, and TLS is handled by a reverse proxy. All data remains client-side encrypted — the server is opaque storage that understands repo structure but never has the encryption key.

```text
vger CLI (client)        reverse proxy (TLS)     vger-server
       │                       │                       │
       │──── HTTPS ───────────►│──── HTTP ────────────►│
       │                       │                       │──► local filesystem
```

### Crate layout

| Component | Location | Purpose |
|-----------|----------|---------|
| **vger-server** | `crates/vger-server/` | axum HTTP server with all server-side features |
| **RestBackend** | `crates/vger-core/src/storage/rest_backend.rs` | `StorageBackend` impl over HTTP (behind `backend-rest` feature) |

### REST API

Storage endpoints map 1:1 to the `StorageBackend` trait:

| Method | Path | Maps to | Notes |
|--------|------|---------|-------|
| `GET` | `/{repo}/{*path}` | `get(key)` | `200` + body or `404`. With `Range` header → `get_range` (returns `206`). |
| `HEAD` | `/{repo}/{*path}` | `exists(key)` | `200` (with Content-Length) or `404` |
| `PUT` | `/{repo}/{*path}` | `put(key, data)` | Raw bytes body. `201`/`204`. Rejected if over quota. |
| `DELETE` | `/{repo}/{*path}` | `delete(key)` | `204` or `404`. Rejected with `403` in append-only mode. |
| `GET` | `/{repo}/{*path}?list` | `list(prefix)` | JSON array of key strings |
| `POST` | `/{repo}/{*path}?mkdir` | `create_dir(key)` | `201` |

Admin endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{repo}?init` | Create repo directory scaffolding (256 shard dirs, etc.) |
| `POST` | `/{repo}?batch-delete` | Body: JSON array of keys to delete |
| `POST` | `/{repo}?repack` | Server-side compaction (see below) |
| `GET` | `/{repo}?stats` | Size, object count, last backup timestamp, quota usage |
| `GET` | `/{repo}?verify-structure` | Structural integrity check (pack magic, shard naming) |
| `GET` | `/` | List all repos |
| `GET` | `/health` | Uptime, disk space, version (unauthenticated) |

Lock endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{repo}/locks/{id}` | Acquire lock (body: `{"hostname": "...", "pid": 123}`) |
| `DELETE` | `/{repo}/locks/{id}` | Release lock |
| `GET` | `/{repo}/locks` | List active locks |

### Authentication

Single shared bearer token, constant-time compared via the `subtle` crate. Configured in `vger-server.toml`:

```toml
[server]
listen = "127.0.0.1:8484"
data_dir = "/var/lib/vger"
token = "some-secret-token"
```

`GET /health` is the only unauthenticated endpoint.

### Append-Only Enforcement

When `append_only = true`:
- `DELETE` on any path → `403 Forbidden`
- `PUT` to existing `packs/**` keys → `403` (no overwriting pack files)
- `PUT` to `manifest`, `index` → allowed (updated every backup)
- `batch-delete` → `403`
- `repack` with `delete_after: true` → `403`

This prevents a compromised client from destroying backup history.

### Quota Enforcement

Per-repo storage quota (`quota_bytes` in config). Server tracks total bytes per repo (initialized by scanning `data_dir` on startup, updated on PUT/DELETE). When a PUT would exceed the limit → `413 Payload Too Large`.

### Backup Freshness Monitoring

The server detects completed backups by observing `PUT /{repo}/manifest` (always the last write in a backup). Updates `last_backup_at` timestamp, exposed via the stats endpoint:

```json
{
  "total_bytes": 1073741824,
  "total_objects": 234,
  "total_packs": 42,
  "last_backup_at": "2026-02-11T14:30:00Z",
  "quota_bytes": 5368709120,
  "quota_used_bytes": 1073741824
}
```

### Lock Management with TTL

Server-managed locks replace advisory JSON lock files:
- Locks are held in memory with a configurable TTL (default 1 hour)
- A background task (tokio interval, every 60 seconds) removes expired locks
- Prevents orphaned locks from crashed clients

### Server-Side Compaction (Repack)

The key feature that justifies a custom server. Pack files that have high dead-blob ratios are repacked server-side, avoiding multi-gigabyte downloads over the network.

**How it works (no encryption key needed):**

Pack files contain encrypted blobs. Compaction does **encrypted passthrough** — it reads blobs by offset and repacks them without decrypting.

1. Client opens repo, downloads and decrypts the index (small)
2. Client analyzes pack headers to identify live vs dead blobs (via range reads)
3. Client sends `POST /{repo}?repack` with a plan:
   ```json
   {
     "operations": [
       {
         "source_pack": "packs/ab/ab01cd02...",
         "keep_blobs": [
           {"offset": 9, "length": 4096},
           {"offset": 8205, "length": 2048}
         ],
         "delete_after": true
       }
     ]
   }
   ```
4. Server reads live blobs from disk, writes new pack files (magic + version + length-prefixed blobs, no trailing header), deletes old packs
5. Server returns new pack keys and blob offsets so the client can update its index
6. Client writes the encrypted pack header separately, updates ChunkIndex, calls `save_state`

For packs with `keep_blobs: []`, the server simply deletes the pack.

### Structural Integrity Check

`GET /{repo}?verify-structure` checks (no encryption key needed):
- Required files exist (`config`, `manifest`, `index`, `keys/repokey`)
- Pack files follow `<2-char-hex>/<64-char-hex>` shard pattern
- No zero-byte packs (minimum valid = magic 9 bytes + header length 4 bytes = 13 bytes)
- Pack files start with `VGERPACK\0` magic bytes
- Reports stale lock count, total size, and pack counts

Full content verification (decrypt + recompute chunk IDs) stays client-side via `vger check --verify-data`.

### Server Configuration

```toml
[server]
listen = "127.0.0.1:8484"
data_dir = "/var/lib/vger"
token = "some-secret-token"
append_only = false
log_format = "json"              # "json" or "pretty"

# Optional limits
# quota_bytes = 0                # per-repo quota. 0 = unlimited.
# lock_ttl_seconds = 3600        # default lock TTL
```

### RestBackend (Client Side)

`crates/vger-core/src/storage/rest_backend.rs` implements `StorageBackend` using `ureq` (sync HTTP client, behind `backend-rest` feature flag). Connection-pooled. Maps each trait method to the corresponding HTTP verb. `get_range` sends a `Range: bytes=<start>-<end>` header and expects `206 Partial Content`. Also exposes extra methods beyond the trait: `batch_delete()`, `repack()`, `acquire_lock()`, `release_lock()`, `stats()`.

Client config:
```yaml
repositories:
  - url: https://backup.example.com/myrepo
    label: server
    rest_token: "secret-token-here"
```

---

## Feature Status

### Implemented

| Feature | Description |
|---------|-------------|
| **Pack files** | Chunks grouped into ~32 MiB packs with dynamic sizing, separate data/tree packs |
| **Retention policies** | `keep_daily`, `keep_weekly`, `keep_monthly`, `keep_yearly`, `keep_last`, `keep_within` |
| **snapshot delete command** | Remove individual snapshots, decrement refcounts |
| **prune command** | Apply retention policies, remove expired snapshots |
| **check command** | Structural integrity + optional `--verify-data` for full content verification |
| **Type-safe PackId** | Newtype for pack file identifiers with `storage_key()` |
| **compact command** | Rewrite packs to reclaim space from orphaned blobs after delete/prune |
| **REST server** | axum-based backup server with auth, append-only, quotas, freshness tracking, lock TTL, server-side compaction |
| **REST backend** | `StorageBackend` over HTTP with range-read support (behind `backend-rest` feature) |
| **Tiered dedup index** | Backup dedup via session map + xor filter + mmap dedup cache, with safe fallback to HashMap dedup mode |
| **Restore mmap cache** | Index-light extract planning via local restore cache; fallback to filtered full-index loading when needed |
| **Incremental index update** | `save_state()` fast path merges `IndexDelta` into local full-index cache and serializes index from cache |
| **Bounded parallel pipeline** | Byte-budgeted pipeline (`pipeline_buffer_mib`) with bounded worker/upload concurrency |
| **Pack buffer pool** | Reusable seal/upload buffers activated during backup and released before index-heavy save-state work |
| **Parallel transforms** | rayon-backed compression/encryption within the bounded pipeline |
| **break-lock command** | Forced stale-lock cleanup for backend/object lock recovery |
| **Compact pack health accounting** | Compact analysis reports/tracks corrupt and orphan packs in addition to reclaimable dead bytes |
| **File-level cache** | inode/mtime/ctime skip for unchanged files — avoids read, chunk, compress, encrypt. Stored locally in the platform cache dir (macOS: `~/Library/Caches/vger/<repo_id>/filecache`, Linux: `~/.cache/vger/…`) — machine-specific, not in the repo. |

### Planned / Not Yet Implemented

| Feature | Description | Priority |
|---------|-------------|----------|
| **Type-safe IDs** | Newtypes for `SnapshotId`, `ManifestId` | Medium |
| **Snapshot filtering** | By host, tag, path, date ranges | Medium |
| **Async I/O** | Non-blocking storage operations | Medium |
| **Metrics** | Prometheus/OpenTelemetry | Low |
