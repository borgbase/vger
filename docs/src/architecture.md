# Architecture

Technical reference for vger's cryptographic, chunking, compression, and storage design decisions.

---

## Encryption

AES-256-GCM with 12-byte random nonces.

Wire format: `[1B type_tag][12B nonce][ciphertext + 16B GCM tag]`

Rationale:
- NIST-standardized authenticated encryption
- Hardware-accelerated on modern CPUs (AES-NI + CLMUL for GHASH)
- 32-byte symmetric keys (simpler key management than split-key schemes)
- The 1-byte type tag is passed as AAD (authenticated additional data), binding the ciphertext to its intended object type

## Key Derivation

Argon2id for passphrase-to-key derivation.

Rationale:
- Modern memory-hard KDF recommended by OWASP and IETF
- Resists both GPU and ASIC brute-force attacks

## Hashing / Chunk IDs

Keyed BLAKE2b-256 MAC using a `chunk_id_key` derived from the master key.

Rationale:
- Prevents content confirmation attacks (an adversary cannot check whether known plaintext exists in the backup without the key)
- BLAKE2b is faster than SHA-256 in software
- Trade-off: keyed IDs prevent dedup across different encryption keys (acceptable for vger's single-key-per-repo model)

## Chunking

FastCDC (content-defined chunking) via the `fastcdc` v3 crate.

Default parameters: 512 KiB min, 2 MiB average, 8 MiB max (configurable in YAML).

Rationale:
- Newer algorithm, benchmarks faster than Rabin fingerprinting
- Good deduplication ratio with configurable chunk boundaries

## Compression

Per-chunk compression with a 1-byte tag prefix. Supported algorithms: LZ4, ZSTD, and None.

Rationale:
- Per-chunk tags allow mixing algorithms within a single repository
- LZ4 for speed-sensitive workloads, ZSTD for better compression ratios
- No repository-wide format version lock-in for compression choice

---

## Repository Layout

```text
<repo>/
|- config                    # Repository metadata (unencrypted)
|- keys/repokey              # Encrypted master key
|- manifest                  # Encrypted snapshot list
|- index                     # Encrypted chunk index
|- snapshots/<id>            # Encrypted snapshot metadata
|- packs/<xx>/<pack-id>      # Pack files containing compressed+encrypted chunks (256 shard dirs)
`- locks/                    # Advisory lock files
```

---

## Pack Files

Chunks are grouped into **pack files** (~32 MiB) instead of being stored as individual files. This reduces file count by 1000x+, critical for cloud storage costs (fewer PUT/GET ops) and filesystem performance (fewer inodes).

### Pack File Format

```text
[8B magic "VGERPACK\0"][1B version=1]
[4B blob_0_len LE][blob_0_data]
[4B blob_1_len LE][blob_1_data]
...
[4B blob_N_len LE][blob_N_data]
[encrypted_header][4B header_length LE]
```

- **Per-blob length prefix** (4 bytes): enables forward scanning to recover individual blobs even if the trailing header is corrupted
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
repositories:
  - path: /backups/repo
    min_pack_size: 33554432     # 32 MiB (floor, default)
    max_pack_size: 536870912    # 512 MiB (ceiling, default)
```

Data pack sizing formula:
```text
target = clamp(min_pack_size * sqrt(num_data_packs / 100), min_pack_size, max_pack_size)
```

| Data packs in repo | Target pack size |
|--------------------|------------------|
| < 100              | 32 MiB (floor)   |
| 1,000              | ~101 MiB         |
| 10,000             | ~320 MiB         |
| 30,000+            | 512 MiB (cap)    |

`num_data_packs` is computed at `open()` by counting distinct `pack_id` values in the ChunkIndex (zero extra I/O).

### Pack Files vs Individual Files

| Aspect | Pack files | Individual files |
|--------|-----------|------------------|
| File count | Very low (1 file per ~32 MiB) | Very high (1 file per chunk) |
| S3/cloud cost | Fewer PUT/GET ops, much cheaper | Many small PUTs, expensive at scale |
| Local FS perf | Efficient, fewer inodes | Can hit inode limits, slow listings |
| Random access | Range read from pack | Direct file read, simpler |
| GC/compaction | Need `compact` to reclaim space | Simple file deletion |
| Write speed | Faster (batched writes) | Slower (many small writes) |

The `compact` command reclaims space from orphaned blobs left behind by `delete` and `prune`. See the [Compact Command](#compact-command) section below.

---

## Compact Command

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

```text
vger compact [--threshold 10] [--max-repack-size 2G] [-n/--dry-run]
```

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
| **delete command** | Remove individual snapshots, decrement refcounts |
| **prune command** | Apply retention policies, remove expired snapshots |
| **check command** | Structural integrity + optional `--verify-data` for full content verification |
| **Type-safe PackId** | Newtype for pack file identifiers with `storage_key()` |
| **compact command** | Rewrite packs to reclaim space from orphaned blobs after delete/prune |
| **REST server** | axum-based backup server with auth, append-only, quotas, freshness tracking, lock TTL, server-side compaction |
| **REST backend** | `StorageBackend` over HTTP with range-read support (behind `backend-rest` feature) |
| **Parallel pipeline** | `rayon` for chunk compress/encrypt pipeline |

### Planned / Not Yet Implemented

| Feature | Description | Priority |
|---------|-------------|----------|
| **File-level cache** | inode/mtime skip for unchanged files (incremental speedup) | High |
| **Type-safe IDs** | Newtypes for `SnapshotId`, `ManifestId` | Medium |
| **Hot/cold tiering** | Separate backends by access frequency | Medium |
| **Snapshot filtering** | By host, tag, path, date ranges | Medium |
| **FUSE mount** | Read-only mount of repository | Medium |
| **Async I/O** | Non-blocking storage operations | Medium |
| **Rate limiting** | Tower middleware for requests/sec and bytes/sec on the server | Medium |
| **Warm-up commands** | Prepare cold storage before access | Low |
| **Metrics** | Prometheus/OpenTelemetry | Low |
