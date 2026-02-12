# V'Ger Backup

<p align="center">
  <img src="docs/src/images/vger-color-rounded.webp" alt="V'Ger Backup Logo" width="350">
</p>

A fast, encrypted, deduplicated backup tool written in Rust.

Inspired by [BorgBackup](https://github.com/borgbackup/borg/), [Borgmatic](https://torsion.org/borgmatic/), [Restic](https://github.com/restic/restic), and [Rustic](https://github.com/rustic-rs/rustic). V'Ger uses its own on-disk format and is not compatible with Borg or Restic repositories.

## Features

- Deduplication via FastCDC content-defined chunking
- Compression with LZ4 (default), Zstandard, or none
- Encryption with AES-256-GCM and Argon2id key derivation
- Storage backends via Apache OpenDAL (local filesystem, S3-compatible storage, SFTP)
- Dedicated REST server (`vger-server`) for append-only enforcement, quotas, lock TTLs, and server-side compaction
- Built-in web interface (webDAV) to browse snapshots and restore files
- Rate limiting for CPU, disk I/O, and network bandwidth
- Hooks for monitoring, database backups, and custom scripts
- Basic GUI (work in progress)

## Quick start

Download a binary from the [releases page](https://github.com/borgbase/vger/releases), then:

```bash
# Generate a starter config and edit it
vger config

# Initialize the repository and run a backup
vger init
vger backup

# List snapshots
vger list
```

See the [full documentation](https://vger.borgbase.com) for storage backends, restore, maintenance, and more.

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

## License

TBD
