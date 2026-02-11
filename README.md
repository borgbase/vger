# borg-rs

A Rust re-implementation of [BorgBackup](https://www.borgbackup.org/), the deduplicating archiver. Configuration is YAML-based, inspired by [Borgmatic](https://torsion.org/borgmatic/).

This is **not compatible** with existing Borg repositories — it uses a fresh on-disk format designed for simplicity while retaining Borg's core principles: content-defined chunking, deduplication, compression, and authenticated encryption.

## Features

### Available commands

| Command | Description |
|---------|-------------|
| `borg-rs init` | Initialize a new backup repository |
| `borg-rs create` | Create a new backup archive |
| `borg-rs list` | List archives, or files within an archive |
| `borg-rs extract` | Restore files from an archive |
| `borg-rs delete` | Delete a specific archive |
| `borg-rs prune` | Prune archives according to retention policy |
| `borg-rs check` | Verify repository integrity (`--verify-data` for full content verification) |

### Core capabilities

- **Deduplication** — Content-defined chunking (FastCDC) splits files into variable-size chunks. Identical chunks are stored only once, even across archives.
- **Compression** — LZ4 (fast, default) or Zstandard (better ratio).
- **Encryption** — AES-256-GCM authenticated encryption with Argon2id key derivation. Plaintext mode also available.
- **Storage backends** — Abstracted via [Apache OpenDAL](https://opendal.apache.org/). Supports local filesystem and S3-compatible storage out of the box. SFTP can be added by enabling the opendal feature.

### What is not (yet) implemented

Mount (FUSE), compact, repair, remote SSH protocol, hardlinks, special files (devices, FIFOs), and file-level caching for incremental speedup. These are candidates for future versions.

## Quick start

### Build

```
cargo build --release
```

The binary is at `target/release/borg-rs`.

### Create a config file

Create `borg-rs.yaml`:

```yaml
repository:
  path: "/path/to/backup/repo"
  backend: "local"

encryption:
  mode: "aes256gcm"   # or "none" for unencrypted

source_directories:
  - "/home/user/documents"
  - "/home/user/photos"

exclude_patterns:
  - "*.tmp"
  - ".cache/**"
  - "node_modules/**"

compression:
  algorithm: "lz4"     # "lz4", "zstd", or "none"
```

### Run

```bash
# Initialize the repository (prompts for passphrase if encrypted)
borg-rs --config borg-rs.yaml init

# Create a backup
borg-rs --config borg-rs.yaml create --archive daily-2025-01-15

# List all archives
borg-rs --config borg-rs.yaml list

# List files inside an archive
borg-rs --config borg-rs.yaml list --archive daily-2025-01-15

# Restore to a directory
borg-rs --config borg-rs.yaml extract --archive daily-2025-01-15 --dest /tmp/restored

# Delete a specific archive
borg-rs --config borg-rs.yaml delete --archive daily-2025-01-15

# Prune old archives per retention policy
borg-rs --config borg-rs.yaml prune

# Verify repository integrity (structural check)
borg-rs --config borg-rs.yaml check

# Full data verification (reads and verifies every chunk)
borg-rs --config borg-rs.yaml check --verify-data
```

## Configuration reference

```yaml
repository:
  path: "/backup/repo"           # Local path or S3 prefix
  backend: "local"               # "local" or "s3"
  # S3-specific options:
  # s3_bucket: "my-backups"
  # s3_region: "us-east-1"
  # s3_endpoint: "http://localhost:9000"  # For MinIO etc.

encryption:
  mode: "aes256gcm"              # "aes256gcm" or "none"
  # passphrase: "inline-secret"  # Not recommended for production
  # passcommand: "pass show borg"  # Shell command that prints the passphrase

source_directories:
  - "/home/user/documents"

exclude_patterns:                # Glob patterns
  - "*.tmp"
  - ".cache/**"

chunker:                         # Optional, defaults shown
  min_size: 524288               # 512 KiB
  avg_size: 2097152              # 2 MiB
  max_size: 8388608              # 8 MiB

compression:
  algorithm: "lz4"               # "lz4", "zstd", or "none"
  zstd_level: 3                  # Only used with zstd

archive_name_format: "{hostname}-{now:%Y-%m-%dT%H:%M:%S}"
```

## Design

### Inspired by

- **[BorgBackup](https://github.com/borgbackup/borg/)** — Architecture, chunking strategy, repository concept, and the overall backup pipeline.
- **[Borgmatic](https://torsion.org/borgmatic/)** — YAML configuration approach.
- **[Rustic](https://github.com/rustic-rs/rustic)** — Using [Apache OpenDAL](https://opendal.apache.org/) for the storage backend abstraction layer.

### Key differences from Borg

| Aspect | Borg | borg-rs |
|--------|------|---------|
| Language | Python + Cython | Rust |
| Chunker | Buzhash (custom) | FastCDC |
| Encryption | AES-CTR+HMAC / AES-OCB / ChaCha20 | AES-256-GCM |
| Key derivation | PBKDF2 or Argon2id | Argon2id only |
| Serialization | msgpack | msgpack |
| Storage | Custom borgstore + SSH RPC | OpenDAL (local, S3, extensible) |
| Repo compatibility | Borg v1/v2/v3 formats | Own format (not compatible) |

### Repository layout

```
<repo>/
├── config                    # Repository metadata (unencrypted)
├── keys/repokey              # Encrypted master key
├── manifest                  # Encrypted archive list
├── index                     # Encrypted chunk index
├── archives/<id>             # Encrypted archive metadata
├── data/<xx>/<chunk-id>      # Encrypted data chunks (256 shard dirs)
└── locks/                    # Advisory lock files
```

## License

TBD
