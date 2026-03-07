Vykar is a fast, encrypted, deduplicated backup tool written in Rust. It's centered around a simple YAML config format and includes a desktop GUI and webDAV server to browse snapshots. More about [design goals](goals.md).

**Do not use for production backups yet, but do test it along other backup tools.**

## Features

- **Storage backends** -- local filesystem, S3 (any compatible provider), SFTP, dedicated REST server
- **Encryption** with AES-256-GCM or ChaCha20-Poly1305 (auto-selected) and Argon2id key derivation
- **YAML-based configuration** with multiple repositories, hooks, and command dumps for monitoring and database backups
- **Deduplication** via FastCDC content-defined chunking with a memory-optimized engine (tiered dedup index plus local mmap-backed lookup caches)
- **Compression** with LZ4 or Zstandard
- **Built-in WebDAV and desktop GUI** to browse and restore snapshots
- **REST server** with append-only enforcement, quotas, and server-side compaction
- **Concurrent multi-client backups** -- multiple machines back up to the same repository simultaneously; only the brief commit phase is serialized
- **Built-in scheduling** via `vykar daemon` -- runs backup cycles on a configurable interval or cron schedule
- **Resource limits** for worker threads, backend connections, and upload/download bandwidth
- **Cross-platform** -- Linux, macOS, and Windows


## Benchmarks

Vykar is the fastest tool for both backup and restore, with the lowest CPU cost, while maintaining competitive memory usage.

[![Backup Tool Benchmark](images/benchmark.summary.png)](images/benchmark.summary.png)

<small>All benchmarks were run 5x on the same idle Intel i7-6700 CPU @ 3.40GHz machine with 2x Samsung PM981 NVMe drives, with results averaged across all runs. Compression settings were chosen to keep resulting repository sizes comparable. The sample corpus is a mix of small and large files with varying compressibility. See [detailed results](images/summary.json) or our [benchmark script](https://github.com/borgbase/vykar/tree/main/scripts) for full details.</small>


## Comparison

| Aspect | Borg | Restic | Rustic | Kopia | Vykar |
|--------|------|--------|--------|-------|------|
| Configuration | CLI (YAML via Borgmatic) | CLI (YAML via ResticProfile) | TOML config file | JSON config + CLI policies | YAML config with env-var expansion |
| Browse snapshots | FUSE mount | FUSE mount | FUSE mount | FUSE mount or WebDAV | Built-in WebDAV + web UI |
| Command dumps | Via Borgmatic (database-specific) | None | None | Native (before/after actions) | Native (generic command capture) |
| Hooks | Via Borgmatic | Via ResticProfile | Native | Native (before/after folder/snapshot) | Native (per-command before/after) |
| Compression | LZ4, Zstd, Zlib, LZMA, None | Zstd, None | Zstd, None | Gzip, Zstd, S2, LZ4, Deflate, Pgzip | LZ4, Zstd, None |
| Rate limiting | None | Upload/download bandwidth | -- | Upload/download bandwidth, request rates | Threads, backend connections, upload/download bandwidth |
| Concurrent backups | No (exclusive lock) | Lock-based | Lock-based (Restic-compat) | Via repository server | Session-based (only commit serialized) |
| Dedicated server | SSH (`borg serve`) | rest-server (append-only) | rustic_server | Built-in HTTP/gRPC server with web UI | REST server with append-only, quotas, server-side compaction |
| Desktop GUI | Vorta (third-party) | Third-party (Backrest) | None | Built-in (Electron) | Built-in |
| Scheduling | Via Borgmatic | Via ResticProfile | External (cron/systemd) | Built-in (interval, time-of-day, cron) | Built-in |
| Language | Python + Cython | Go | Rust | Go | Rust |
| Chunker | Buzhash (custom) | Rabin | Rabin (Restic-compat) | Buzhash (default), Rabin-Karp, Fixed | FastCDC |
| Encryption | AES-CTR+HMAC / AES-OCB / ChaCha20 | AES-256-CTR + Poly1305-AES | AES-256-CTR + Poly1305-AES | AES-256-GCM / ChaCha20-Poly1305 | AES-256-GCM / ChaCha20-Poly1305 (auto-select at init) |
| Key derivation | PBKDF2 or Argon2id | scrypt | scrypt | PBKDF2 or scrypt | Argon2id |
| Serialization | msgpack | JSON + Protocol Buffers | JSON + Protocol Buffers | JSON | msgpack |
| Storage | borgstore + SSH RPC | Local, S3, SFTP, REST, rclone | Local, S3, SFTP, REST | Local, S3, Azure, GCS, B2, SFTP, WebDAV, Rclone | Local, S3, SFTP, REST + vykar-server |
| Repo compatibility | Borg v1/v2/v3 | Restic format | Restic-compatible | Own format | Own format |


## Inspired by

- [BorgBackup](https://github.com/borgbackup/borg/): architecture, chunking strategy, repository concept, and overall backup pipeline.
- [Borgmatic](https://torsion.org/borgmatic/): YAML configuration approach, pipe-based database dumps.
- [Rustic](https://github.com/rustic-rs/rustic): pack file design and architectural references from a mature Rust backup tool.
- **Name**: From Latin *vicarius* ("substitute, stand-in") — because a backup is literally a substitute for lost data.


## Get Started

Follow the **[Quick Start guide](quickstart.md)** to install Vykar, create a config, and run your first backup in under 5 minutes.

Once you're up and running:

- [Configure storage backends](backends.md) -- connect S3, SFTP, or the REST server
- [Set up hooks and command dumps](configuration.md#hooks) -- run scripts before/after backups, capture database dumps
- [Browse and restore snapshots](restore.md) -- list, search, and restore files
- [Maintain your repository](maintenance.md) -- prune old snapshots, check integrity, compact packs
- [Explore backup recipes](recipes.md) -- common patterns for databases, containers, and filesystems
