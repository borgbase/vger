<p align="center">
  <img src="images/vger-color-rounded.webp" alt="V'Ger Backup Logo" width="380">
</p>

V'Ger is a fast, encrypted, deduplicated backup tool written in Rust. It's centered around a simple YAML config format and includes a desktop GUI and webDAV server to browse snapshots. More about [design goals](goals.md).

**⚠️ Don't use for production backups yet, but do test it along other backup tools.**

## Features

- **Storage backends** — local filesystem, S3-compatible storage, SFTP
- **Encryption** with AES-256-GCM or ChaCha20-Poly1305 (auto-selected) and Argon2id key derivation
- **YAML-based configuration** with multiple repositories, hooks, and command dumps for monitoring and database backups
- **Deduplication** via FastCDC content-defined chunking with a memory-optimized engine (tiered dedup index + mmap-backed pack assembly)
- **Compression** with LZ4 or Zstandard
- **Built-in WebDAV and desktop GUI** to browse and restore snapshots
- **REST server** with append-only enforcement, quotas, and server-side compaction
- **Built-in scheduling** via `vger daemon` — runs backup cycles on a configurable interval (no cron needed)
- **Rate limiting** for CPU, disk I/O, and network bandwidth


## Benchmarks

V'Ger leads in both speed and CPU efficiency, while maintaining competitive memory usage.

![Backup Tool Benchmark](images/benchmark.summary.png)

<small>All benchmarks were run on the same idle Intel i7-6700 CPU @ 3.40GHz machine with 2x Samsung PM981 NVMe drives. Compression settings were chosen to keep resulting repository sizes comparable. The sample corpus is a mix of small and large files with varying compressibility. See our [benchmark script](https://github.com/borgbase/vger/tree/main/scripts) for full details.</small>


## Comparison

| Aspect | Borg | Restic | Rustic | V'Ger |
|--------|------|--------|--------|------|
| Configuration | CLI (YAML via Borgmatic) | CLI (YAML via ResticProfile) | TOML config file | YAML config with env-var expansion |
| Browse snapshots | FUSE mount | FUSE mount | FUSE mount | Built-in WebDAV + web UI |
| Command dumps | Via Borgmatic (database-specific) | None | None | Native (generic command capture) |
| Hooks | Via Borgmatic | Via ResticProfile | Native | Native (per-command before/after) |
| Rate limiting | None | Upload/download bandwidth | — | CPU, disk I/O, and network bandwidth |
| Dedicated server | SSH (`borg serve`) | rest-server (append-only) | rustic_server | REST server with append-only, quotas, server-side compaction |
| Desktop GUI | Vorta (third-party) | Third-party (Backrest) | None | Built-in |
| Scheduling | Via Borgmatic | Via ResticProfile | External (cron/systemd) | Built-in |
| Language | Python + Cython | Go | Rust | Rust |
| Chunker | Buzhash (custom) | Rabin | Rabin (Restic-compat) | FastCDC |
| Encryption | AES-CTR+HMAC / AES-OCB / ChaCha20 | AES-256-CTR + Poly1305-AES | AES-256-CTR + Poly1305-AES | AES-256-GCM / ChaCha20-Poly1305 (auto-select at init) |
| Key derivation | PBKDF2 or Argon2id | scrypt | scrypt | Argon2id |
| Serialization | msgpack | JSON + Protocol Buffers | JSON + Protocol Buffers | msgpack |
| Storage | borgstore + SSH RPC | Local, S3, SFTP, REST, rclone | OpenDAL (local, S3, many more) | OpenDAL (local, S3, SFTP) + vger-server |
| Repo compatibility | Borg v1/v2/v3 | Restic format | Restic-compatible | Own format |


## Inspired by

- [BorgBackup](https://github.com/borgbackup/borg/): architecture, chunking strategy, repository concept, and overall backup pipeline.
- [Borgmatic](https://torsion.org/borgmatic/): YAML configuration approach, pipe-based database dumps.
- [Rustic](https://github.com/rustic-rs/rustic): storage backend abstraction via Apache OpenDAL, pack file design, and architectural references from a mature Rust backup tool.
- [V'Ger](https://memory-alpha.fandom.com/wiki/V%27Ger) from *Star Trek: The Motion Picture* — a probe that assimilated everything it encountered and returned as something far more powerful.


## Get Started

Follow the **[Quick Start guide](quickstart.md)** to install V'Ger, create a config, and run your first backup in under 5 minutes.

Once you're up and running:

- [Configure storage backends](backends.md) — connect S3, SFTP, or the REST server
- [Set up hooks and command dumps](configuration.md#hooks) — run scripts before/after backups, capture database dumps
- [Browse and restore snapshots](restore.md) — list, search, and restore files
- [Maintain your repository](maintenance.md) — prune old snapshots, check integrity, compact packs
- [Explore backup recipes](recipes.md) — common patterns for databases, containers, and filesystems
