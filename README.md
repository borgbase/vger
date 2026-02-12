# V'Ger

A fast, encrypted, deduplicated backup tool written in Rust. Named after [V'Ger](https://memory-alpha.fandom.com/wiki/V%27Ger) from *Star Trek: The Motion Picture* — a probe that assimilated everything it encountered and returned as something far more powerful.

Inspired by [BorgBackup](https://www.borgbackup.org/), [Borgmatic](https://torsion.org/borgmatic/), [restic](https://restic.net/), and [rustic](https://rustic.cli.rs/). Uses its own on-disk format — **not compatible** with Borg repositories.

## Features

### Available commands

| Command | Description |
|---------|-------------|
| `vger config` | Generate a starter configuration file |
| `vger init` | Initialize a new backup repository |
| `vger backup` | Back up files to a new snapshot |
| `vger list` | List snapshots, or files within a snapshot |
| `vger extract` | Restore files from a snapshot |
| `vger delete` | Delete a specific snapshot |
| `vger prune` | Prune snapshots according to retention policy |
| `vger check` | Verify repository integrity (`--verify-data` for full content verification) |
| `vger compact` | Free space by repacking pack files after delete/prune |
| `vger mount` | Browse snapshots via a local WebDAV server |

### Core capabilities

- **Deduplication** — Content-defined chunking (FastCDC) splits files into variable-size chunks. Identical chunks are stored only once, even across snapshots.
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

Repair, hardlinks, special files (devices, FIFOs), and file-level caching for incremental speedup.

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

# Create a backup (auto-generated 8-char hex ID)
vger backup

# Create a backup with a label annotation
vger backup --label before-upgrade

# Back up only one configured source
vger backup --source docs

# List all snapshots
vger list

# List the 5 most recent snapshots
vger list --last 5

# List snapshots for a specific source
vger list --source docs

# List files inside a snapshot (use the hex ID from `vger list`)
vger list --snapshot a1b2c3d4

# Browse snapshots via a local WebDAV server
vger mount

# Restore to a directory
vger extract --snapshot a1b2c3d4 --dest /tmp/restored

# Delete a specific snapshot
vger delete --snapshot a1b2c3d4

# Prune old snapshots per retention policy
vger prune

# Verify repository integrity (structural check)
vger check

# Full data verification (reads and verifies every chunk)
vger check --verify-data

# Reclaim space from deleted/pruned snapshots (dry-run first)
vger compact --dry-run
vger compact
```

### Mount

Browse snapshot contents via a local WebDAV server — open the URL in a file manager or web browser.

```bash
# Serve all snapshots (default: http://127.0.0.1:8080)
vger mount

# Serve a single snapshot
vger mount --snapshot a1b2c3d4

# Only snapshots from a specific source
vger mount --source docs

# Custom listen address
vger mount --address 127.0.0.1:9090
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
  - url: "https://backup.example.com/myrepo"
    label: "server"
    rest_token: "some-secret-token"

encryption:
  mode: "aes256gcm"

sources:
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
  - url: "/backup/repo"             # Local path, file://, s3://, sftp://, or https://
    label: "main"                  # Short name for --repo selection
    # min_pack_size: 33554432      # Pack size floor (default 32 MiB)
    # max_pack_size: 536870912     # Pack size ceiling (default 512 MiB)
    # S3-specific options (s3:// URLs):
    # region: "us-east-1"
    # access_key_id: "AKIA..."
    # secret_access_key: "..."
    # REST-specific options (https:// URLs):
    # rest_token: "secret"                  # Bearer token for REST server

encryption:
  mode: "aes256gcm"              # "aes256gcm" or "none"
  # passphrase: "inline-secret"  # Not recommended for production
  # passcommand: "pass show borg"  # Shell command that prints the passphrase

# Simple form — list of paths (auto-labeled from directory name)
sources:
  - "/home/user/documents"
  - "/home/user/photos"

# Rich form — per-source options
sources:
  - path: "/home/user/documents"
    label: "docs"
    exclude: ["*.tmp", ".cache/**"]
    repos: ["main"]               # Only back up to this repo (default: all)
    retention:
      keep_daily: 7
    hooks:
      before: "echo starting docs backup"

exclude_patterns:                # Global exclude patterns (merged with per-source)
  - "*.tmp"
  - ".cache/**"

chunker:                         # Optional, defaults shown
  min_size: 524288               # 512 KiB
  avg_size: 2097152              # 2 MiB
  max_size: 8388608              # 8 MiB

compression:
  algorithm: "lz4"               # "lz4", "zstd", or "none"
  zstd_level: 3                  # Only used with zstd

retention:                       # Global retention policy (can be overridden per-source)
  keep_last: 10
  keep_daily: 7
  keep_weekly: 4
  keep_monthly: 6
  keep_yearly: 2
  keep_within: "2d"              # Keep everything within this period (e.g. "2d", "48h", "1w")

hooks:                           # Global hooks — run for every command
  before: "echo starting"
  after: "echo done"
  # before_backup: "echo backup starting"  # Command-specific hooks
  # failed: "notify-send 'vger failed'"
  # finally: "cleanup.sh"
```

### Multiple repositories

Add more entries to `repositories:` to back up to multiple destinations. Top-level settings serve as defaults; each entry can override `encryption`, `compression`, and `retention`.

```yaml
repositories:
  - url: "/backups/local"
    label: "local"

  - url: "s3://bucket/remote"
    label: "remote"
    region: "us-east-1"
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
├── manifest                  # Encrypted snapshot list
├── index                     # Encrypted chunk index
├── snapshots/<id>            # Encrypted snapshot metadata
├── packs/<xx>/<pack-id>      # Pack files containing compressed+encrypted chunks (256 shard dirs)
└── locks/                    # Advisory lock files
```

## License

TBD
