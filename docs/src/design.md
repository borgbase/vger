# Design Notes

## Inspired by

- [BorgBackup](https://github.com/borgbackup/borg/): architecture, chunking strategy, repository concept, and overall backup pipeline.
- [Borgmatic](https://torsion.org/borgmatic/): YAML configuration approach.
- [Rustic](https://github.com/rustic-rs/rustic): storage backend abstraction via Apache OpenDAL, pack file design, and architectural references from a mature Rust backup tool.

See [Architecture Notes and Research](architecture.md) for deeper comparison and implementation details.

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

## Repository layout

```text
<repo>/
|- config                    # Repository metadata (unencrypted)
|- keys/repokey              # Encrypted master key
|- manifest                  # Encrypted snapshot list
|- index                     # Encrypted chunk index
|- snapshots/<id>            # Encrypted snapshot metadata
|- packs/<xx>/<pack-id>      # Pack files containing compressed+encrypted chunks (256 shard dirs)
`- locks/                    # Advisory lock files
```
