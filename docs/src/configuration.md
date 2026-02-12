# Configuration Reference

## Repositories

```yaml
repositories:
  - url: "/backup/repo"             # Local path, file://, s3://, sftp://, or https://
    label: "main"                    # Short name for --repo selection
    # min_pack_size: 33554432        # Pack size floor (default 32 MiB)
    # max_pack_size: 536870912       # Pack size ceiling (default 512 MiB)
    # S3-specific options (s3:// URLs):
    # region: "us-east-1"
    # access_key_id: "AKIA..."
    # secret_access_key: "..."
    # REST-specific options (https:// URLs):
    # rest_token: "secret"           # Bearer token for REST server
```

## Encryption

```yaml
encryption:
  mode: "aes256gcm"                  # "aes256gcm" or "none"
  # passphrase: "inline-secret"      # Not recommended for production
  # passcommand: "pass show borg"    # Shell command that prints the passphrase
```

## Sources

Sources can be a simple list of paths (auto-labeled from directory name) or rich entries with per-source options.

**Simple form:**

```yaml
sources:
  - "/home/user/documents"
  - "/home/user/photos"
```

**Rich form:**

```yaml
sources:
  - path: "/home/user/documents"
    label: "docs"
    exclude: ["*.tmp", ".cache/**"]
    # exclude_if_present: [".nobackup", "CACHEDIR.TAG"]
    # one_file_system: true
    # git_ignore: false
    repos: ["main"]                  # Only back up to this repo (default: all)
    retention:
      keep_daily: 7
    hooks:
      before: "echo starting docs backup"
```

## Exclude Patterns

```yaml
exclude_patterns:                    # Global gitignore-style patterns (merged with per-source)
  - "*.tmp"
  - ".cache/**"
exclude_if_present:                  # Skip dirs containing any marker file
  - ".nobackup"
  - "CACHEDIR.TAG"
one_file_system: true                # Do not cross filesystem/mount boundaries (default true)
git_ignore: false                    # Respect .gitignore files (default false)
```

## Chunker

```yaml
chunker:                             # Optional, defaults shown
  min_size: 524288                   # 512 KiB
  avg_size: 2097152                  # 2 MiB
  max_size: 8388608                  # 8 MiB
```

## Compression

```yaml
compression:
  algorithm: "lz4"                   # "lz4", "zstd", or "none"
  zstd_level: 3                      # Only used with zstd
```

## Limits

```yaml
limits:                              # Optional backup resource limits
  cpu:
    max_threads: 0                   # 0 = default rayon behavior
    nice: 0                          # Unix niceness target (-20..19), 0 = unchanged
  io:
    read_mib_per_sec: 0              # Source file reads during backup
    write_mib_per_sec: 0             # Local repository writes during backup
  network:
    read_mib_per_sec: 0              # Remote backend reads during backup
    write_mib_per_sec: 0             # Remote backend writes during backup
```

## Retention

```yaml
retention:                           # Global retention policy (can be overridden per-source)
  keep_last: 10
  keep_daily: 7
  keep_weekly: 4
  keep_monthly: 6
  keep_yearly: 2
  keep_within: "2d"                  # Keep everything within this period (e.g. "2d", "48h", "1w")
```

## Hooks

```yaml
hooks:                               # Global hooks: run for every command
  before: "echo starting"
  after: "echo done"
  # before_backup: "echo backup starting"  # Command-specific hooks
  # failed: "notify-send 'vger failed'"
  # finally: "cleanup.sh"
```

## Multiple sources

Each source entry in rich form can override global settings. This lets you tailor backup behavior per directory:

```yaml
sources:
  - path: "/home/user/documents"
    label: "docs"
    exclude: ["*.tmp"]
    repos: ["local"]                 # Only back up to the "local" repo
    retention:
      keep_daily: 7
      keep_weekly: 4

  - path: "/home/user/photos"
    label: "photos"
    repos: ["local", "remote"]       # Back up to both repos
    retention:
      keep_daily: 30
      keep_monthly: 12
    hooks:
      after: "echo photos backed up"
```

Per-source fields that override globals: `exclude`, `exclude_if_present`, `one_file_system`, `git_ignore`, `repos`, `retention`, `hooks`.

## Multiple repositories

Add more entries to `repositories:` to back up to multiple destinations. Top-level settings serve as defaults; each entry can override `encryption`, `compression`, `retention`, and `limits`.

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
      algorithm: "zstd"             # Better ratio for remote
    retention:
      keep_daily: 30                 # Keep more on remote
    limits:
      cpu:
        max_threads: 2
      network:
        write_mib_per_sec: 25
```

When `limits` is set on a repository entry, it replaces top-level `limits` for that repository.

By default, commands operate on all repositories. Use `--repo` / `-R` to target a single one:

```bash
vger --repo local list
vger -R /backups/local list
```
