# Configuration Reference

```yaml
repositories:
  - url: "/backup/repo"             # Local path, file://, s3://, sftp://, or https://
    label: "main"                  # Short name for --repo selection
    # min_pack_size: 33554432       # Pack size floor (default 32 MiB)
    # max_pack_size: 536870912      # Pack size ceiling (default 512 MiB)
    # S3-specific options (s3:// URLs):
    # region: "us-east-1"
    # access_key_id: "AKIA..."
    # secret_access_key: "..."
    # REST-specific options (https:// URLs):
    # rest_token: "secret"         # Bearer token for REST server

encryption:
  mode: "aes256gcm"                # "aes256gcm" or "none"
  # passphrase: "inline-secret"    # Not recommended for production
  # passcommand: "pass show borg"  # Shell command that prints the passphrase

# Simple form: list of paths (auto-labeled from directory name)
sources:
  - "/home/user/documents"
  - "/home/user/photos"

# Rich form: per-source options
sources:
  - path: "/home/user/documents"
    label: "docs"
    exclude: ["*.tmp", ".cache/**"]
    # Optional per-source overrides:
    # exclude_if_present: [".nobackup", "CACHEDIR.TAG"]
    # one_file_system: true
    # git_ignore: false
    repos: ["main"]                # Only back up to this repo (default: all)
    retention:
      keep_daily: 7
    hooks:
      before: "echo starting docs backup"

exclude_patterns:                   # Global gitignore-style patterns (merged with per-source)
  - "*.tmp"
  - ".cache/**"
exclude_if_present:                 # Skip dirs containing any marker file
  - ".nobackup"
  - "CACHEDIR.TAG"
one_file_system: true               # Do not cross filesystem/mount boundaries (default true)
git_ignore: false                   # Respect .gitignore files (default false)

chunker:                            # Optional, defaults shown
  min_size: 524288                  # 512 KiB
  avg_size: 2097152                 # 2 MiB
  max_size: 8388608                 # 8 MiB

compression:
  algorithm: "lz4"                 # "lz4", "zstd", or "none"
  zstd_level: 3                     # Only used with zstd

retention:                          # Global retention policy (can be overridden per-source)
  keep_last: 10
  keep_daily: 7
  keep_weekly: 4
  keep_monthly: 6
  keep_yearly: 2
  keep_within: "2d"                # Keep everything within this period (for example "2d", "48h", "1w")

hooks:                              # Global hooks: run for every command
  before: "echo starting"
  after: "echo done"
  # before_backup: "echo backup starting"  # Command-specific hooks
  # failed: "notify-send 'vger failed'"
  # finally: "cleanup.sh"
```

## Multiple repositories

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
      algorithm: "zstd"           # Better ratio for remote
    retention:
      keep_daily: 30               # Keep more on remote
```

By default, commands operate on all repositories. Use `--repo` / `-R` to target a single one:

```bash
vger --repo local list
vger -R /backups/local list
```
