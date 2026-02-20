# CLAUDE.md — vger

## What this project is

A fast, encrypted, deduplicated backup tool written in Rust. YAML config inspired by Borgmatic. Storage abstracted via Apache OpenDAL. Uses its own on-disk format.

## Build & test

```bash
cargo build --release        # binary at target/release/vger
cargo check                  # fast type-check
cargo test                   # run unit + integration tests
make fmt                     # apply rustfmt across workspace
make pre-commit              # local CI gate: fmt-check + clippy -D warnings + tests
```

Minimum Rust version: 1.88 (some deps require it). Tested on macOS (aarch64).

## Project structure

```
Cargo.toml                              # workspace root
crates/
  vger-core/                            # library crate — all backup logic
    src/
      lib.rs                            # module re-exports
      error.rs                          # VgerError enum (thiserror)
      config.rs                         # YAML config structs (serde)
      storage/
        mod.rs                          # StorageBackend trait (get/put/delete/exists/list/get_range/create_dir)
        local_backend.rs                # Native std::fs local filesystem backend
        opendal_backend.rs              # OpenDAL adapter (S3 only)
      crypto/
        mod.rs                          # CryptoEngine trait + PlaintextEngine
        aes_gcm.rs                      # AES-256-GCM implementation
        key.rs                          # MasterKey, EncryptedKey, Argon2id KDF
        chunk_id.rs                     # ChunkId — keyed BLAKE2b-256 MAC
        pack_id.rs                      # PackId — unkeyed BLAKE2b-256 of pack contents
      compress/mod.rs                   # LZ4 / ZSTD / None with 1-byte tag prefix
      chunker/mod.rs                    # FastCDC wrapper
      index/mod.rs                      # ChunkIndex — HashMap<ChunkId, ChunkIndexEntry>
      repo/
        mod.rs                          # Repository struct — init, open, store_chunk, read_chunk, save_state
        file_cache.rs                   # FileCache — inode/mtime skip for unchanged files
        format.rs                       # RepoObj envelope — pack_object / unpack_object
        pack.rs                         # PackWriter, PackType, pack read/write helpers
        manifest.rs                     # Manifest — snapshot list
        lock.rs                         # Advisory JSON lock files
      snapshot/
        mod.rs                          # SnapshotMeta, SnapshotStats
        item.rs                         # Item, ItemType, ChunkRef
      commands/
        mod.rs
        init.rs                         # vger init
        backup.rs                       # vger backup (walk + chunk + dedup + compress + encrypt)
        list.rs                         # vger list (snapshots or snapshot contents)
        restore.rs                      # vger restore (restore files)
        delete.rs                       # vger delete (remove snapshot, decrement refcounts)
        prune.rs                        # vger prune (retention policy)
        check.rs                        # vger check (integrity verification)
        compact.rs                      # vger compact (repack packs to reclaim space)
  vger-cli/                             # binary crate — thin CLI
    src/main.rs                         # clap CLI, passphrase handling, dispatches to vger-core commands
```

## Architecture overview

### Data flow (backup)

1. Walk source dirs (walkdir) → apply exclude patterns (globset)
2. For each file: check file cache (device, inode, mtime, ctime, size) → on hit, reuse cached `ChunkRef`s
3. On cache miss: read → FastCDC chunk → for each chunk:
   - Compute `ChunkId` = keyed BLAKE2b-256(chunk_id_key, data)
   - Check `ChunkIndex` + pending pack writers — if exists, skip (dedup hit)
   - Compress (LZ4/ZSTD) → encrypt (AES-256-GCM) → buffer into `PackWriter`
   - When pack reaches target size → flush to `packs/<shard>/<pack_id>`
3. Serialize all `Item` structs → chunk the item stream → store item-stream chunks (tree packs)
4. Build `SnapshotMeta` with `item_ptrs` → encrypt → store at `snapshots/<id>`
5. Flush remaining packs → update manifest + chunk index → encrypt → store

### Repository on-disk layout

```
<repo>/
  config              # unencrypted msgpack: RepoConfig (version, chunker params, pack size limits)
  keys/repokey        # Argon2id-wrapped master key
  manifest            # encrypted: Manifest (snapshot list)
  index               # encrypted: ChunkIndex (chunk_id → pack_id, offset, size, refcount)
  snapshots/<id>      # encrypted: SnapshotMeta per snapshot
  packs/<xx>/<id>     # pack files containing compressed+encrypted chunks (256 shard dirs)
  locks/*.json        # advisory locks
```

### RepoObj wire format

- Encrypted: `[1-byte type_tag][12-byte nonce][ciphertext + 16-byte GCM tag]`
- Plaintext: `[1-byte type_tag][plaintext]`

The type tag byte is used as AAD (authenticated additional data) in AES-GCM.

### Key types

- `StorageBackend` trait (storage/mod.rs) — get/put/delete/exists/list/get_range/create_dir
- `CryptoEngine` trait (crypto/mod.rs) — encrypt/decrypt/chunk_id_key
- `ChunkId` (crypto/chunk_id.rs) — 32-byte keyed BLAKE2b-MAC for content-addressed dedup
- `PackId` (crypto/pack_id.rs) — 32-byte unkeyed BLAKE2b-256, has `storage_key()` → `packs/<shard>/<hex>`, `from_hex()`, `from_storage_key()`
- `PackWriter` (repo/pack.rs) — buffers encrypted blobs and flushes them as pack files
- `PackType` (repo/pack.rs) — `Data` (file content) or `Tree` (item-stream metadata)
- `Repository` (repo/mod.rs) — central orchestrator, owns storage + crypto + manifest + index + pack writers
- `Item` (snapshot/item.rs) — single filesystem entry (file/dir/symlink)
- `Compression` enum (compress/mod.rs) — 1-byte tag prefix on compressed data

## Important conventions

- All serialization uses `rmp_serde` (msgpack). Structs serialize as positional arrays — do **not** use `#[serde(skip_serializing_if)]` on Item fields (breaks positional deserialization).
- OpenDAL builder methods consume `self` — must chain calls, not use `mut` + separate method calls.
- `blake2::Blake2bMac<U32>` has ambiguous trait methods — use `Mac::update(&mut hasher, data)` and `<KeyedBlake2b256 as KeyInit>::new_from_slice()` if needed.
- The `PlaintextEngine` still needs a `chunk_id_key` for deterministic dedup. For unencrypted repos, it's derived as `BLAKE2b(repo_id)`.
- `store_chunk()` requires a `PackType` argument — use `PackType::Data` for file content and `PackType::Tree` for item-stream metadata.
- `save_state()` takes `&mut self` (not `&self`) because it flushes pending pack writes before persisting manifest/index.

## Dependencies (key ones)

| Purpose | Crate |
|---------|-------|
| Encryption | `aes-gcm` 0.10 |
| Chunk IDs / Pack IDs | `blake2` 0.10 (Blake2bMac, Blake2bVar) |
| KDF | `argon2` 0.5 |
| Compression | `lz4_flex` 0.11, `zstd` 0.13 |
| Chunking | `fastcdc` 3 |
| Storage | `opendal` 0.51 (services-s3), `std::fs` for local |
| Serialization | `rmp-serde` 1, `serde_json` 1 |
| CLI | `clap` 4 (derive), `serde_yaml` 0.9 |
| Filesystem | `walkdir` 2, `globset` 0.4, `xattr` 1 |

## Release

```bash
gh workflow run release.yml                              # trigger release build
gh run watch                                             # wait for it to finish
gh run download --name linux-x86_64-unknown-linux-gnu    # download Linux binary
```

## What's not implemented yet

- `mount` command
- Async I/O
- SSH RPC protocol (use OpenDAL SFTP instead)
- Hardlinks, block/char devices, FIFOs
