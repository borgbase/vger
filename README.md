# V'Ger Backup

A fast, encrypted, deduplicated backup tool written in Rust.

Inspired by BorgBackup, Borgmatic, restic, and rustic. V'Ger uses its own on-disk format and is not compatible with Borg or Restic repositories.

## Features

- Deduplication via FastCDC content-defined chunking
- Compression with LZ4 (default), Zstandard, or none
- Encryption with AES-256-GCM and Argon2id key derivation
- Storage backends via Apache OpenDAL (local filesystem, S3-compatible storage, SFTP)
- Dedicated REST server (`vger-server`) for append-only enforcement, quotas, lock TTLs, and server-side compaction
- Built-in web interface (webDAV) to browse snapshots and restore files
- Basic GUI (work in progress)

## Quick start

```bash
cargo build --release

# Generate a starter config
vger config

# Initialize and run a backup
vger init
vger backup

# Inspect snapshots
vger list

# Show repository statistics
vger info

# Start desktop UI (Slint + tray + scheduler)
vger-gui
```

For full command examples, see [Quick Start](docs/src/quickstart.md).

## Desktop UI

`vger-gui` is a Slint-based desktop app that uses `vger-core` directly (it does not shell out to the CLI).

- Run backups on demand
- List snapshots and browse snapshot contents
- Extract snapshot contents
- Run in the system tray with periodic background backups
- Uses `vger.yaml` as the source of truth and auto-reloads config changes

Periodic GUI scheduling is configured in `vger.yaml` via:

```yaml
schedule:
  enabled: true
  every: "24h"
  on_startup: false
  jitter_seconds: 0
  passphrase_prompt_timeout_seconds: 300
```

## Documentation

This repository now uses mdBook for project documentation in `docs/`.

- Docs home: [docs/src/index.md](docs/src/index.md)
- Quick start: [docs/src/quickstart.md](docs/src/quickstart.md)
- Configuration reference: [docs/src/configuration.md](docs/src/configuration.md)
- Server mode: [docs/src/server-mode.md](docs/src/server-mode.md)
- Architecture notes and research: [docs/src/architecture.md](docs/src/architecture.md)
- Design notes: [docs/src/design.md](docs/src/design.md)
- Workflow placeholders: [docs/src/workflows/index.md](docs/src/workflows/index.md)

Build the docs site:

```bash
make docs-build
# or directly
mdbook build docs
```

Serve locally:

```bash
make docs-serve
# or directly
mdbook serve docs --open
```

## License

TBD
