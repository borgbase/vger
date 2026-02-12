# V'Ger

A fast, encrypted, deduplicated backup tool written in Rust. Named after [V'Ger](https://memory-alpha.fandom.com/wiki/V%27Ger) from *Star Trek: The Motion Picture* — a probe that assimilated everything it encountered and returned as something far more powerful.

Inspired by [BorgBackup](https://www.borgbackup.org/), [Borgmatic](https://torsion.org/borgmatic/), [restic](https://restic.net/), and [rustic](https://rustic.cli.rs/). Uses its own on-disk format — **not compatible** with Borg repositories.

## Features

### Available commands

| Command | Description |
|---------|-------------|
| `vger config` | Generate a starter configuration file |
| `vger init` | Initialize a new backup repository |
| `vger backup` | Back up files to a new archive |
| `vger list` | List archives, or files within an archive |
| `vger extract` | Restore files from an archive |
| `vger delete` | Delete a specific archive |
| `vger prune` | Prune archives according to retention policy |
| `vger check` | Verify repository integrity (`--verify-data` for full content verification) |
| `vger compact` | Free space by repacking pack files after delete/prune |

### Core capabilities

- **Deduplication** — Content-defined chunking (FastCDC) splits files into variable-size chunks. Identical chunks are stored only once, even across archives.
- **Compression** — LZ4 (fast, default) or Zstandard (better ratio).
- **Encryption** — AES-256-GCM authenticated encryption with Argon2id key derivation. Plaintext mode also available.
- **Storage backends** — Abstracted via [Apache OpenDAL](https://opendal.apache.org/). Supports local filesystem, S3-compatible storage, and a dedicated REST server out of the box.
- **REST server** — Purpose-built backup server (`vger-server`) with append-only enforcement, per-repo quotas, lock management with auto-expiry, backup freshness monitoring, server-side compaction, and structural integrity checks. See [Server Mode](#server-mode) below.

### Why a dedicated REST server instead of plain S3?

Dumb storage backends (S3, WebDAV, SFTP) work well for basic backups, but they can't enforce policy or do server-side work. vger-server adds capabilities that are impossible with object storage alone:

| Capability | S3 / dumb storage | vger-server |
|------------|-------------------|----------------|
| **Append-only mode** | Not enforceable — a compromised client with S3 credentials can delete anything | vger-server rejects DELETE and pack overwrites. Even a fully compromised client cannot destroy backup history. |
| **Server-side compaction** | Client must download all live blobs and re-upload — potentially GBs over the network | Server repacks locally on disk. Client sends a small JSON plan, server does the I/O. |
| **Quota enforcement** | Requires separate S3 bucket policies or IAM tricks per repo | Built-in per-repo byte quota, checked on every PUT |
| **Backup freshness monitoring** | Requires external monitoring to poll and parse repo metadata | Server tracks `last_backup_at` automatically on every manifest write |
| **Lock auto-expiry** | Advisory lock files stay forever if a client crashes | Server-managed locks with configurable TTL, background cleanup of stale locks |
| **Structural health checks** | Client must download data to verify structure | Server checks pack magic bytes, shard naming, required files — no encryption key needed |

All data remains **client-side encrypted**. The server never has the encryption key and cannot read backup contents. It is opaque storage that understands repository structure just enough to enforce policy and optimize I/O.

### What is not (yet) implemented

Mount (FUSE), repair, hardlinks, special files (devices, FIFOs), and file-level caching for incremental speedup.

## Quick start

### Build

```
cargo build --release
```

The binary is at `target/release/vger`.

### Create a config file

```bash
# Generate a starter config in the current directory
vger config

# Or write it to a specific path
vger config --dest ~/.config/vger/config.yaml
```

Edit the generated `vger.yaml` to set your repository path, source directories, and encryption mode. See [Configuration reference](#configuration-reference) for all options.

vger automatically finds config files in this order:

1. `--config <path>` flag
2. `VGER_CONFIG` environment variable
3. `./vger.yaml` (project)
4. `$XDG_CONFIG_HOME/vger/config.yaml` or `~/.config/vger/config.yaml` (user)
5. `/etc/vger/config.yaml` (system)

You can also set `VGER_PASSPHRASE` to supply the passphrase non-interactively (useful for cron jobs and scripts).

### Run

```bash
# Initialize the repository (prompts for passphrase if encrypted)
vger init

# Create a backup
vger backup --archive daily-2025-01-15

# List all archives
vger list

# List files inside an archive
vger list --archive daily-2025-01-15

# Restore to a directory
vger extract --archive daily-2025-01-15 --dest /tmp/restored

# Delete a specific archive
vger delete --archive daily-2025-01-15

# Prune old archives per retention policy
vger prune

# Verify repository integrity (structural check)
vger check

# Full data verification (reads and verifies every chunk)
vger check --verify-data

# Reclaim space from deleted/pruned archives (dry-run first)
vger compact --dry-run
vger compact
```

## Server mode

vger includes a dedicated backup server for secure, policy-enforced remote backups. TLS is handled by a reverse proxy (nginx, caddy, etc.).

### Build the server

```bash
cargo build --release -p vger-server
# Binary at target/release/vger-server
```

### Build the client with REST support

```bash
cargo build --release -p vger-cli --features vger-core/backend-rest
```

### Server configuration

Create `vger-server.toml`:

```toml
[server]
listen = "127.0.0.1:8484"
data_dir = "/var/lib/vger"
token = "some-secret-token"
append_only = false              # true = reject all deletes
log_format = "pretty"            # "json" for structured logging (systemd, etc.)

# Optional limits
# quota_bytes = 5368709120       # 5 GiB per-repo quota. 0 = unlimited.
# lock_ttl_seconds = 3600        # auto-expire locks after 1 hour (default)
```

### Start the server

```bash
vger-server --config vger-server.toml
```

### Client configuration (REST backend)

```yaml
repositories:
  - path: "https://backup.example.com/myrepo"
    backend: "rest"
    rest_token: "some-secret-token"
    # rest_token_command: "pass show borg-token"  # alternative: shell command

encryption:
  mode: "aes256gcm"

source_directories:
  - "/home/user/documents"
```

All standard commands (`init`, `backup`, `list`, `extract`, `delete`, `prune`, `check`, `compact`) work transparently over REST — no changes to the CLI workflow.

### Health check

```bash
# No auth required
curl http://localhost:8484/health
```

Returns server status, uptime, disk free space, and repo count.

## Configuration reference

```yaml
repositories:
  - path: "/backup/repo"           # Local path, S3 prefix, or REST URL
    label: "main"                  # Short name for --repo selection
    # backend: "local"             # "local", "s3", or "rest" (default: "local")
    # min_pack_size: 33554432      # Pack size floor (default 32 MiB)
    # max_pack_size: 536870912     # Pack size ceiling (default 512 MiB)
    # S3-specific options:
    # s3_bucket: "my-backups"
    # s3_region: "us-east-1"
    # s3_endpoint: "http://localhost:9000"  # For MinIO etc.
    # REST-specific options:
    # rest_token: "secret"                  # Bearer token for REST server
    # rest_token_command: "pass show borg"  # Alternative: shell command for token

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

### Multiple repositories

Add more entries to `repositories:` to back up to multiple destinations. Top-level settings serve as defaults; each entry can override `encryption`, `compression`, `retention`, and `source_directories`.

```yaml
repositories:
  - path: "/backups/local"
    label: "local"

  - path: "s3://bucket/remote"
    label: "remote"
    backend: "s3"
    s3_bucket: "my-bucket"
    encryption:
      passcommand: "pass show vger-remote"
    compression:
      algorithm: "zstd"          # Better ratio for remote
    retention:
      keep_daily: 30             # Keep more on remote
```

By default, commands operate on **all** repositories. Use `--repo` / `-R` to target a single one:

```bash
vger --repo local list
vger -R /backups/local list
```

## Design

### Inspired by

- **[BorgBackup](https://github.com/borgbackup/borg/)** — Architecture, chunking strategy, repository concept, and the overall backup pipeline.
- **[Borgmatic](https://torsion.org/borgmatic/)** — YAML configuration approach.
- **[Rustic](https://github.com/rustic-rs/rustic)** — Storage backend abstraction via [Apache OpenDAL](https://opendal.apache.org/), pack file design, and general architectural reference as a mature Rust backup tool. See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed comparison.

### Key differences from Borg

| Aspect | Borg | vger |
|--------|------|------|
| Language | Python + Cython | Rust |
| Chunker | Buzhash (custom) | FastCDC |
| Encryption | AES-CTR+HMAC / AES-OCB / ChaCha20 | AES-256-GCM |
| Key derivation | PBKDF2 or Argon2id | Argon2id only |
| Serialization | msgpack | msgpack |
| Storage | Custom borgstore + SSH RPC | OpenDAL (local, S3) + vger-server with append-only/quotas |
| Repo compatibility | Borg v1/v2/v3 formats | Own format (not compatible) |

### Repository layout

```
<repo>/
├── config                    # Repository metadata (unencrypted)
├── keys/repokey              # Encrypted master key
├── manifest                  # Encrypted archive list
├── index                     # Encrypted chunk index
├── archives/<id>             # Encrypted archive metadata
├── packs/<xx>/<pack-id>      # Pack files containing compressed+encrypted chunks (256 shard dirs)
└── locks/                    # Advisory lock files
```

## License

TBD
