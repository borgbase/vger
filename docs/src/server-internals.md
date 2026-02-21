# Server Internals

Technical reference for vger-server's internal architecture: crate layout, REST API surface, authentication, policy enforcement, and server-side operations.

*For deployment and configuration, see [Setup](server-setup.md).*

---

## Crate Layout

| Component | Location | Purpose |
|-----------|----------|---------|
| **vger-server** | `crates/vger-server/` | axum HTTP server with all server-side features |
| **vger-protocol** | `crates/vger-protocol/` | Shared wire-format types and pack/protocol version constants (no I/O or crypto deps) |
| **RestBackend** | `crates/vger-core/src/storage/rest_backend.rs` | `StorageBackend` impl over HTTP |

## REST API

Storage endpoints map 1:1 to the `StorageBackend` trait:

| Method | Path | Maps to | Notes |
|--------|------|---------|-------|
| `GET` | `/{repo}/{*path}` | `get(key)` | `200` + body or `404`. With `Range` header → `get_range` (returns `206`). |
| `HEAD` | `/{repo}/{*path}` | `exists(key)` | `200` (with Content-Length) or `404` |
| `PUT` | `/{repo}/{*path}` | `put(key, data)` | Raw bytes body. `201`/`204`. Rejected if over quota. Pack uploads require `X-Content-BLAKE2b` header (BLAKE2b-256 hex); server verifies during streaming write. Non-pack objects: header optional. |
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
| `POST` | `/{repo}?verify-packs` | Server-side pack integrity verification (BLAKE2b hash, header magic/version, blob boundaries) |
| `GET` | `/{repo}?verify-structure` | Structural integrity check (pack magic, shard naming) |
| `GET` | `/` | List all repos |
| `GET` | `/health` | Uptime, disk space, version (unauthenticated) |

Lock endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{repo}/locks/{id}` | Acquire lock (body: `{"hostname": "...", "pid": 123}`) |
| `DELETE` | `/{repo}/locks/{id}` | Release lock |
| `GET` | `/{repo}/locks` | List active locks |

## Authentication

Single shared bearer token, constant-time compared via the `subtle` crate. Set via the `VGER_TOKEN` environment variable.

`GET /health` is the only unauthenticated endpoint.

## Append-Only Enforcement

When `append_only = true`:
- `DELETE` on any path → `403 Forbidden`
- `PUT` to existing `packs/**` keys → `403` (no overwriting pack files)
- `PUT` to `manifest`, `index` → allowed (updated every backup)
- `batch-delete` → `403`
- `repack` with `delete_after: true` → `403`

This prevents a compromised client from destroying backup history.

## Quota Enforcement

Per-repo storage quota (`quota_bytes` in config). Server tracks total bytes per repo (initialized by scanning `data_dir` on startup, updated on PUT/DELETE). When a PUT would exceed the limit → `413 Payload Too Large`.

## Backup Freshness Monitoring

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

## Lock Management with TTL

Server-managed locks replace advisory JSON lock files:
- Locks are held in memory with a configurable TTL (default 1 hour)
- A background task (tokio interval, every 60 seconds) removes expired locks
- Prevents orphaned locks from crashed clients

## Server-Side Compaction (Repack)

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
6. Client updates `ChunkIndex` mappings and calls `save_state`

For packs with `keep_blobs: []`, the server simply deletes the pack.

## Structural Integrity Check

`GET /{repo}?verify-structure` checks (no encryption key needed):
- Required files exist (`config`, `manifest`, `index`, `keys/repokey`)
- Pack files follow `<2-char-hex>/<64-char-hex>` shard pattern
- No zero-byte packs (minimum valid = magic 8 bytes + version 1 byte = 9 bytes)
- Pack files start with `VGERPACK` magic bytes
- Reports stale lock count, total size, and pack counts

Full content verification (decrypt + recompute chunk IDs) can be done client- or server-side

- Server-side `vger check --verify-data`: This won't download the whole repo, but instruct the server to checksum the actual data.
- Client-side `vger check --verify-data --distrust-server`: This will download all the data and verify it client-side.

## Server Configuration

All settings are passed as CLI flags. The authentication token is read from the `VGER_TOKEN` environment variable.

```bash
export VGER_TOKEN="some-secret-token"
vger-server --listen 127.0.0.1:8585 --data-dir /var/lib/vger --append-only --log-format json --quota 10G --max-blocking-threads 6
```

See `vger-server --help` for the full list of flags and defaults.

## RestBackend (Client Side)

`crates/vger-core/src/storage/rest_backend.rs` implements `StorageBackend` using `ureq` (sync HTTP client). Connection-pooled. Maps each trait method to the corresponding HTTP verb. `get_range` sends a `Range: bytes=<start>-<end>` header and expects `206 Partial Content`. Also exposes extra methods beyond the trait: `batch_delete()`, `repack()`, `verify_packs()`, `acquire_lock()`, `release_lock()`, `stats()`.

Client config:
```yaml
repositories:
  - url: https://backup.example.com/myrepo
    label: server
    rest_token: "secret-token-here"
```

---

*Related: [Setup](server-setup.md), [Architecture](architecture.md)*
