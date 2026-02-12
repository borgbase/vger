# V'Ger Backup Documentation

V'Ger is a fast, encrypted, deduplicated backup tool written in Rust.

## Inspired by

- [BorgBackup](https://github.com/borgbackup/borg/): architecture, chunking strategy, repository concept, and overall backup pipeline.
- [Borgmatic](https://torsion.org/borgmatic/): YAML configuration approach.
- [Rustic](https://github.com/rustic-rs/rustic): storage backend abstraction via Apache OpenDAL, pack file design, and architectural references from a mature Rust backup tool.

## Key differences from Borg

| Aspect | Borg | vger |
|--------|------|------|
| Language | Python + Cython | Rust |
| Chunker | Buzhash (custom) | FastCDC |
| Encryption | AES-CTR+HMAC / AES-OCB / ChaCha20 | AES-256-GCM |
| Key derivation | PBKDF2 or Argon2id | Argon2id only |
| Serialization | msgpack | msgpack |
| Storage | Custom borgstore + SSH RPC | OpenDAL (local, S3) + vger-server with append-only and quotas |
| Repo compatibility | Borg v1/v2/v3 formats | Own format (not compatible) |

## Start here

- [Quick Start](quickstart.md)
- [Configuration Reference](configuration.md)
- [Server Mode](server-mode.md)

## Reference

- [Architecture](architecture.md)

## Workflow guides

- [Installing](workflows/install.md)
- [Initialize and Set Up a Repository](workflows/init-setup.md)
- [Make a Backup](workflows/backup.md)
- [Restore a Backup](workflows/restore.md)
- [Maintenance](workflows/maintenance.md)
