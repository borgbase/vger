# Configuration

V'Ger is driven by a YAML configuration file. Generate a starter config with:

```bash
vger config
```

## Config file locations

V'Ger automatically finds config files in this order:

1. `--config <path>` flag
2. `VGER_CONFIG` environment variable
3. `./vger.yaml` (project)
4. `$XDG_CONFIG_HOME/vger/config.yaml` or `~/.config/vger/config.yaml` (user)
5. `/etc/vger/config.yaml` (system)

You can also set `VGER_PASSPHRASE` to supply the passphrase non-interactively.

## Minimal example

A complete but minimal working config. Encryption defaults to `auto` (init benchmarks AES-256-GCM vs ChaCha20-Poly1305 and pins the repo), so you only need repositories and sources:

```yaml
repositories:
  - url: "/backup/repo"

sources:
  - "/home/user/documents"
```

## Repositories

**Local:**

```yaml
repositories:
  - url: "/backups/repo"
    label: "local"
```

**S3:**

```yaml
repositories:
  - url: "s3://my-bucket/vger"
    label: "s3"
    region: "us-east-1"
```

Each entry accepts an optional `label` for CLI targeting (`vger --repo local list`) and optional pack size tuning (`min_pack_size`, `max_pack_size`). See [Storage Backends](backends.md) for all backend-specific options.

## Sources

Sources can be a simple list of paths (auto-labeled from directory name) or rich entries with per-source options.

**Simple form:**

```yaml
sources:
  - "/home/user/documents"
  - "/home/user/photos"
```

**Rich form (single path):**

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

**Rich form (multiple paths):**

Use `paths` (plural) to group several directories into a single source. An explicit `label` is required:

```yaml
sources:
  - paths:
      - "/home/user/documents"
      - "/home/user/notes"
    label: "writing"
    exclude: ["*.tmp"]
```

These directories are backed up together as one snapshot. You cannot use both `path` and `paths` on the same entry.

## Encryption

Encryption is enabled by default (`auto` mode with Argon2id key derivation). You only need an `encryption` section to supply a passcommand, force a specific algorithm, or disable encryption:

```yaml
encryption:
  # mode: "auto"                     # Default — benchmark at init and persist chosen mode
  # mode: "aes256gcm"                # Force AES-256-GCM
  # mode: "chacha20poly1305"         # Force ChaCha20-Poly1305
  # mode: "none"                     # Disable encryption
  # passphrase: "inline-secret"      # Not recommended for production
  # passcommand: "pass show borg"    # Shell command that prints the passphrase
```

## Compression

```yaml
compression:
  algorithm: "lz4"                   # "lz4", "zstd", or "none"
  zstd_level: 3                      # Only used with zstd
```

## Chunker

```yaml
chunker:                             # Optional, defaults shown
  min_size: 524288                   # 512 KiB
  avg_size: 2097152                  # 2 MiB
  max_size: 8388608                  # 8 MiB
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
xattrs:                              # Extended attribute handling
  enabled: true                      # Preserve xattrs on backup/restore (default true)
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

## Hooks

Shell commands that run at specific points in the vger command lifecycle. Hooks can be defined at three levels: global (top-level `hooks:`), per-repository, and per-source.

```yaml
hooks:                               # Global hooks: run for every command
  before: "echo starting"
  after: "echo done"
  # before_backup: "echo backup starting"  # Command-specific hooks
  # failed: "notify-send 'vger failed'"
  # finally: "cleanup.sh"
```

### Hook types

| Hook                | Runs when                        | Failure behavior       |
|---------------------|----------------------------------|------------------------|
| `before` / `before_<cmd>`   | Before the command      | Aborts the command     |
| `after` / `after_<cmd>`     | After success only      | Logged, doesn't affect result |
| `failed` / `failed_<cmd>`   | After failure only      | Logged, doesn't affect result |
| `finally` / `finally_<cmd>` | Always, regardless of outcome | Logged, doesn't affect result |

Use the bare form (`before`, `after`, etc.) to match all commands, or append a command name (`before_backup`, `failed_prune`, etc.) to match only that command.

### Execution order

1. `before` hooks run: global bare → repo bare → global specific → repo specific
2. The vger command runs (skipped if a `before` hook fails)
3. On success: `after` hooks run (repo specific → global specific → repo bare → global bare)
   On failure: `failed` hooks run (same order)
4. `finally` hooks always run last (same order)

If a `before` hook fails, the command is skipped and both `failed` and `finally` hooks still run.

### Variable substitution

Hook commands support `{variable}` placeholders that are replaced before execution. Values are automatically shell-escaped.

| Variable         | Description                                  |
|------------------|----------------------------------------------|
| `{command}`      | The vger command name (e.g. `backup`, `prune`) |
| `{repository}`   | Repository URL                               |
| `{label}`        | Repository label (empty if unset)            |
| `{error}`        | Error message (empty if no error)            |
| `{source_label}` | Source label (empty if unset)                |
| `{source_path}`  | Source path(s), colon-separated if multiple  |

The same values are also exported as environment variables: `VGER_COMMAND`, `VGER_REPOSITORY`, `VGER_LABEL`, `VGER_ERROR`, `VGER_SOURCE_LABEL`, `VGER_SOURCE_PATH`.

```yaml
hooks:
  failed:
    - 'notify-send "vger {command} failed: {error}"'
  after_backup:
    - 'echo "Backed up {source_label} to {repository}"'
```

### Notifications with Apprise

[Apprise](https://github.com/caronc/apprise) lets you send notifications to 100+ services (Gotify, Slack, Discord, Telegram, ntfy, email, and more) from the command line. Since vger hooks run arbitrary shell commands, you can use the `apprise` CLI directly — no built-in integration needed.

Install it with:

```bash
pip install apprise
```

Then add hooks that call `apprise` with the service URLs you want:

```yaml
hooks:
  after_backup:
    - >-
      apprise -t "Backup complete"
      -b "vger {command} finished for {repository}"
      "gotify://hostname/token"
      "slack://tokenA/tokenB/tokenC"
  failed:
    - >-
      apprise -t "Backup failed"
      -b "vger {command} failed for {repository}: {error}"
      "gotify://hostname/token"
```

Common service URL examples:

| Service  | URL format                                          |
|----------|-----------------------------------------------------|
| Gotify   | `gotify://hostname/token`                           |
| Slack    | `slack://tokenA/tokenB/tokenC`                      |
| Discord  | `discord://webhook_id/webhook_token`                |
| Telegram | `tgram://bot_token/chat_id`                         |
| ntfy     | `ntfy://topic`                                      |
| Email    | `mailto://user:pass@gmail.com`                      |

You can pass multiple URLs in a single command to notify several services at once. See the [Apprise wiki](https://github.com/caronc/apprise/wiki) for the full list of supported services and URL formats.

## Environment Variable Expansion

Config files support environment variable placeholders in values:

```yaml
repositories:
  - url: "${VGER_REPO_URL:-/backup/repo}"
    # rest_token: "${VGER_REST_TOKEN}"
```

Supported syntax:

- `${VAR}`: requires `VAR` to be set (hard error if missing)
- `${VAR:-default}`: uses `default` when `VAR` is unset or empty

Notes:

- Expansion runs on raw config text before YAML parsing.
- Variable names must match `[A-Za-z_][A-Za-z0-9_]*`.
- Malformed placeholders fail config loading.
- No escape syntax is supported for literal `${...}`.

## Multiple sources

Each source entry in rich form can override global settings. This lets you tailor backup behavior per directory:

```yaml
sources:
  - path: "/home/user/documents"
    label: "docs"
    exclude: ["*.tmp"]
    xattrs:
      enabled: false                 # Override top-level xattrs setting for this source
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

Per-source fields that override globals: `exclude`, `exclude_if_present`, `one_file_system`, `git_ignore`, `repos`, `retention`, `hooks`, `command_dumps`.

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
