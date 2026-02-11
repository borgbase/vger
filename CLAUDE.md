# CLAUDE.md — borg-rs

## What this project is

A Rust re-implementation of BorgBackup's core features. YAML config inspired by Borgmatic. Storage abstracted via Apache OpenDAL. **Not compatible with existing Borg repositories** — uses its own on-disk format.

## Build & test

```bash
cargo build --release        # binary at target/release/borg-rs
cargo check                  # fast type-check
cargo test                   # no test suite yet — verify manually
```

Minimum Rust version: 1.88 (some deps require it). Tested on macOS (aarch64).

## Project structure

```
Cargo.toml                              # workspace root
crates/
  borg-core/                            # library crate — all backup logic
    src/
      lib.rs                            # module re-exports
      error.rs                          # BorgError enum (thiserror)
      config.rs                         # YAML config structs (serde)
      storage/
        mod.rs                          # StorageBackend trait
        opendal_backend.rs              # OpenDAL adapter (local, S3)
      crypto/
        mod.rs                          # CryptoEngine trait + PlaintextEngine
        aes_gcm.rs                      # AES-256-GCM implementation
        key.rs                          # MasterKey, EncryptedKey, Argon2id KDF
        chunk_id.rs                     # ChunkId — keyed BLAKE2b-256 MAC
      compress/mod.rs                   # LZ4 / ZSTD / None with 1-byte tag prefix
      chunker/mod.rs                    # FastCDC wrapper
      index/mod.rs                      # ChunkIndex — HashMap<ChunkId, (refcount, size)>
      repo/
        mod.rs                          # Repository struct — init, open, store_chunk, read_chunk, save_state
        format.rs                       # RepoObj envelope — pack_object / unpack_object
        manifest.rs                     # Manifest — archive list
        lock.rs                         # Advisory JSON lock files
      archive/
        mod.rs                          # ArchiveMeta, ArchiveStats
        item.rs                         # Item, ItemType, ChunkRef
      commands/
        mod.rs
        init.rs                         # borg-rs init
        create.rs                       # borg-rs create (walk + chunk + dedup + compress + encrypt)
        list.rs                         # borg-rs list (archives or archive contents)
        extract.rs                      # borg-rs extract (restore files)
  borg-cli/                             # binary crate — thin CLI
    src/main.rs                         # clap CLI, passphrase handling, dispatches to borg-core commands
```

## Architecture overview

### Data flow (create)

1. Walk source dirs (walkdir) → apply exclude patterns (globset)
2. For each file: read → FastCDC chunk → for each chunk:
   - Compute `ChunkId` = keyed BLAKE2b-256(chunk_id_key, data)
   - Check `ChunkIndex` — if exists, skip (dedup hit)
   - Compress (LZ4/ZSTD) → encrypt (AES-256-GCM) → store at `data/<shard>/<id>`
3. Serialize all `Item` structs → chunk the item stream → store item-stream chunks
4. Build `ArchiveMeta` with `item_ptrs` → encrypt → store at `archives/<id>`
5. Update manifest + chunk index → encrypt → store

### Repository on-disk layout

```
<repo>/
  config              # unencrypted msgpack: RepoConfig
  keys/repokey        # Argon2id-wrapped master key
  manifest            # encrypted: Manifest (archive list)
  index               # encrypted: ChunkIndex
  archives/<id>       # encrypted: ArchiveMeta per archive
  data/<xx>/<id>      # encrypted: compressed chunk data (256 shard dirs)
  locks/*.json        # advisory locks
```

### RepoObj wire format

- Encrypted: `[1-byte type_tag][12-byte nonce][ciphertext + 16-byte GCM tag]`
- Plaintext: `[1-byte type_tag][plaintext]`

The type tag byte is used as AAD (authenticated additional data) in AES-GCM.

### Key types

- `StorageBackend` trait (storage/mod.rs) — get/put/delete/exists/list/create_dir
- `CryptoEngine` trait (crypto/mod.rs) — encrypt/decrypt/chunk_id_key
- `ChunkId` (crypto/chunk_id.rs) — 32-byte BLAKE2b-MAC, has `storage_key()` → `data/<shard>/<hex>`
- `Repository` (repo/mod.rs) — central orchestrator, owns storage + crypto + manifest + index
- `Item` (archive/item.rs) — single filesystem entry (file/dir/symlink)
- `Compression` enum (compress/mod.rs) — 1-byte tag prefix on compressed data

## Important conventions

- All serialization uses `rmp_serde` (msgpack). Structs serialize as positional arrays — do **not** use `#[serde(skip_serializing_if)]` on Item fields (breaks positional deserialization).
- OpenDAL builder methods consume `self` — must chain calls, not use `mut` + separate method calls.
- `blake2::Blake2bMac<U32>` has ambiguous trait methods — use `Mac::update(&mut hasher, data)` and `<KeyedBlake2b256 as KeyInit>::new_from_slice()` if needed.
- The `PlaintextEngine` still needs a `chunk_id_key` for deterministic dedup. For unencrypted repos, it's derived as `BLAKE2b(repo_id)`.

## Dependencies (key ones)

| Purpose | Crate |
|---------|-------|
| Encryption | `aes-gcm` 0.10 |
| Chunk IDs | `blake2` 0.10 (Blake2bMac) |
| KDF | `argon2` 0.5 |
| Compression | `lz4_flex` 0.11, `zstd` 0.13 |
| Chunking | `fastcdc` 3 |
| Storage | `opendal` 0.51 (services-fs, services-s3) |
| Serialization | `rmp-serde` 1, `serde_json` 1 |
| CLI | `clap` 4 (derive), `serde_yaml` 0.9 |
| Filesystem | `walkdir` 2, `globset` 0.4, `filetime` 0.2, `xattr` 1 |

## What's not implemented yet

- `prune` / `delete` / `check` / `compact` / `mount` commands
- File-level cache (inode/mtime skip for unchanged files)
- Async I/O
- SSH RPC protocol (use OpenDAL SFTP instead)
- Hardlinks, block/char devices, FIFOs
- Unit/integration test suite
