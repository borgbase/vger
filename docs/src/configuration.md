# Configuration

Vykar is driven by a YAML configuration file. Generate a starter config with:

```bash
vykar config
```

## Config file locations

Vykar automatically finds config files in this order:

1. `--config <path>` flag
2. `VYKAR_CONFIG` environment variable
3. `./vykar.yaml` (project)
4. User config dir + `vykar/config.yaml`:
   - Unix: `$XDG_CONFIG_HOME/vykar/config.yaml` or `~/.config/vykar/config.yaml`
   - Windows: `%APPDATA%\\vykar\\config.yaml`
5. System config:
   - Unix: `/etc/vykar/config.yaml`
   - Windows: `%PROGRAMDATA%\\vykar\\config.yaml`

You can also set `VYKAR_PASSPHRASE` to supply the passphrase non-interactively.

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
  - label: "local"
    url: "/backups/repo"
```

**S3:**

```yaml
repositories:
  - label: "s3"
    url: "s3://s3.us-east-1.amazonaws.com/my-bucket/vykar"
    region: "us-east-1"
    access_key_id: "AKIA..."
    secret_access_key: "..."
```

Each entry accepts an optional `label` for CLI targeting (`vykar list --repo local`) and optional pack size tuning (`min_pack_size`, `max_pack_size`). Defaults are `min_pack_size = 32 MiB` and `max_pack_size = 192 MiB`; `max_pack_size` has a hard ceiling of `512 MiB`. See [Storage Backends](backends.md) for all backend-specific options.

For remote repositories, transport is HTTPS-first by default. To intentionally use plaintext HTTP (for local/dev setups), set:

```yaml
repositories:
  - url: "http://localhost:8484"
    allow_insecure_http: true
```

For S3-compatible HTTP endpoints, use `s3+http://...` URLs with `allow_insecure_http: true`.

### Multiple repositories

Add more entries to `repositories:` to back up to multiple destinations. Top-level settings serve as defaults; each entry can override `encryption`, `compression`, `retention`, and `limits`.

```yaml
repositories:
  - label: "local"
    url: "/backups/local"

  - label: "remote"
    url: "s3://s3.us-east-1.amazonaws.com/bucket/remote"
    region: "us-east-1"
    access_key_id: "AKIA..."
    secret_access_key: "..."
    encryption:
      passcommand: "pass show vykar-remote"
    compression:
      algorithm: "zstd"             # Better ratio for remote
    retention:
      keep_daily: 30                 # Keep more on remote
    limits:
      connections: 2
      upload_mib_per_sec: 25
```

When `limits` is set on a repository entry, it replaces top-level `limits` for that repository.

By default, commands operate on all repositories. Use `--repo` / `-R` to target a single one:

```bash
vykar list --repo local
vykar list -R /backups/local
```

### 3-2-1 backup strategy

> **Tip:** Configuring both a local and a remote repository gives you a [3-2-1 backup](https://en.wikipedia.org/wiki/Backup#3-2-1_rule) setup: three copies of your data (the original files, the local backup, and the remote backup), on two different media types, with one copy offsite. The example above already achieves this.

## Sources

Sources can be a simple list of paths or rich entries with per-source options. Each source entry produces one snapshot per backup run.

**Simple form:**

```yaml
sources:
  - "/home/user/documents"
  - "/home/user/photos"
```

Simple entries are grouped into one source. With one simple path, the source label is derived from the directory name. With multiple simple paths, the grouped source label becomes `default`. Use rich entries if you want separate source labels or one snapshot per path.

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

Each `path:` entry produces its own snapshot. To group multiple directories into a single snapshot, use `paths:` (plural) instead — see below.

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

### Per-source overrides

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

## Command Dumps

Capture the stdout of shell commands directly into your backup. Useful for database dumps, API exports, or any generated data that doesn't live as a regular file on disk.

```yaml
sources:
  - path: /var/www/myapp
    label: myapp
    command_dumps:
      - name: postgres.sql
        command: pg_dump -U myuser mydb
      - name: redis.rdb
        command: redis-cli --rdb -
```

Each entry has two required fields:

| Field     | Description                                           |
|-----------|-------------------------------------------------------|
| `name`    | Virtual filename (e.g. `mydb.sql`). Must not contain `/` or `\`. No duplicates within a source. |
| `command` | Shell command whose stdout is captured (run via `sh -c`). |

Output is stored as virtual files under `.vykar-dumps/` in the snapshot. On restore they appear as regular files (e.g. `.vykar-dumps/postgres.sql`).

You can also create **dump-only sources** with no filesystem paths — an explicit `label` is required:

```yaml
sources:
  - label: databases
    command_dumps:
      - name: all-databases.sql
        command: pg_dumpall -U postgres
```

If a dump command exits with non-zero status, the backup is aborted. Any chunks already uploaded to packs remain on disk but are not added to the index; they are reclaimed on the next `vykar compact` run.

See [Backup — Command dumps](backup.md#command-dumps) for more details and [Recipes](recipes.md) for PostgreSQL, MySQL, MongoDB, and Docker examples.

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

`none` mode requires no passphrase and creates no key file. Data is still checksummed via keyed BLAKE2b-256 chunk IDs to detect storage corruption, but is not authenticated against tampering. See [Architecture — Plaintext Mode](architecture.md#plaintext-mode-none) for details.

`passcommand` runs through the platform shell:

- Unix: `sh -c`
- Windows: `powershell -NoProfile -NonInteractive -Command`

For `vykar daemon`, encrypted repositories must have a non-interactive passphrase source available (`passcommand`, `passphrase`, or `VYKAR_PASSPHRASE`).

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

Vykar uses [gitignore-style](https://git-scm.com/docs/gitignore#_pattern_format) patterns for file exclusion. Patterns can be set globally (`exclude_patterns`) or per-source (`exclude`); both lists are merged at runtime.

> **Important:** Patterns are matched against paths **relative to each source directory**, not against absolute filesystem paths. An absolute path like `/home/user/videos/TV` will not work — it is interpreted as the gitignore-anchored glob `home/user/videos/TV` relative to the source root, which matches nothing.

### Pattern syntax quick reference

| Pattern | Matches | Notes |
|---------|---------|-------|
| `*.tmp` | Any `.tmp` file at any depth | No `/` in pattern, so `**/` is implied |
| `*.log` | Any `.log` file at any depth | Same floating match behavior |
| `.cache/` | Any directory named `.cache` (at any depth) | Trailing `/` restricts to directories |
| `.cache/**` | All files and dirs inside `.cache` | Does not exclude `.cache` dir itself |
| `/Downloads` | `Downloads` only at the source root | Leading `/` anchors to the source root |
| `/build/output` | `build/output` only at the source root | Anchored multi-component path |
| `TV` | Any file or directory named `TV` at any depth | Floating match (no `/` in pattern) |
| `logs/debug` | `logs/debug` only at the source root | Contains `/`, so anchored (same as `/logs/debug`) |
| `!important.log` | Re-include `important.log` (override earlier exclude) | Negation (order-dependent) |

Key rules:

- **No slash in the pattern** (e.g., `*.tmp`, `TV`): matches at any depth, as if prefixed with `**/`.
- **Slash in the pattern** (e.g., `logs/debug`, `/Downloads`): anchored to the source root. A leading `/` is optional — `logs/debug` and `/logs/debug` behave the same way. This is **not** a filesystem absolute path.
- **Trailing `/`** (e.g., `.cache/`): only matches directories.
- **`!` prefix**: negates a previous exclude (re-includes the file).

### Examples

**Global excludes** apply to every source directory:

```yaml
exclude_patterns:
  - "*.tmp"
  - "*.log"
  - ".cache/"
  - "__pycache__/"
  - ".DS_Store"
  - "Thumbs.db"
```

**Per-source excludes** let you target specific subdirectories within a source:

```yaml
sources:
  - path: "/home/user/videos"
    exclude:
      - "/TV"                          # Excludes <source>/TV
  - path: "/home/user/photos"
    exclude:
      - "/thumbnails"                  # Excludes <source>/thumbnails
      - "/My Albums"                   # Spaces in paths work fine
```

Per-source `exclude` patterns are added after global `exclude_patterns`. Both lists use the same matching rules.

### Common mistakes

**Absolute filesystem paths do not work as exclude patterns:**

```yaml
# WRONG — these will silently exclude nothing
exclude_patterns:
  - "/home/user/videos/TV"
  - "/home/user/photos/thumbnails"
```

Instead, use per-source excludes with paths relative to the source root:

```yaml
# CORRECT — use per-source excludes anchored to the source root
sources:
  - path: "/home/user/videos"
    exclude:
      - "/TV"
  - path: "/home/user/photos"
    exclude:
      - "/thumbnails"
```

Or, if you want a global pattern that matches in any source (e.g., skip all directories named `TV` regardless of which source they are in):

```yaml
# Matches "TV" directory inside any source
exclude_patterns:
  - "TV/"
```

### Marker-based and filesystem exclusion

```yaml
exclude_if_present:                  # Skip dirs containing any marker file
  - ".nobackup"
  - "CACHEDIR.TAG"
one_file_system: false               # Do not cross filesystem/mount boundaries (default false)
git_ignore: false                    # Respect .gitignore files (default false)
xattrs:                              # Extended attribute handling
  enabled: true                      # Preserve xattrs on backup/restore (default true, Unix-only)
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

## Compact

```yaml
compact:
  threshold: 20                      # Minimum % unused space to trigger repack (default 20)
```

## Limits

```yaml
limits:                              # Optional backup resource limits
  connections: 2                     # Parallel backend ops (1-16); also drives upload/restore concurrency
  threads: 0                         # Backup transform workers (0 = auto: min(cores,12), 1 = mostly sequential)
  nice: 0                            # Unix niceness target (-20..19), ignored on Windows
  upload_mib_per_sec: 0              # Upload bandwidth cap (MiB/s, 0 = unlimited)
  download_mib_per_sec: 0            # Download bandwidth cap (MiB/s, 0 = unlimited)
```

`limits.connections` also controls SFTP connection pool size, backup in-flight uploads, and restore reader concurrency. Internal pipeline knobs are now derived automatically from `connections` and `threads`.

## Hooks

Shell commands that run at specific points in the vykar command lifecycle. Hooks can be defined at three levels: global (top-level `hooks:`), per-repository, and per-source.

**Global / per-repository hooks** support both bare prefixes and command-specific variants:

```yaml
hooks:                               # Global hooks: run for backup/prune/check/compact
  before: "echo starting"
  after: "echo done"
  # before_backup: "echo backup starting"  # Command-specific hooks
  # failed: "notify-send 'vykar failed'"
  # finally: "cleanup.sh"
```

**Per-source hooks** only support bare prefixes (`before`, `after`, `failed`, `finally`) — command-specific variants like `before_backup` are not valid at the source level. Source hooks always run for `backup` since that is the only command that processes sources.

```yaml
sources:
  - path: /raid1/immich/db-backups
    label: immich
    hooks:
      before: '/raid1/immich/backup_db.sh'  # Correct
      # before_backup: '...'               # NOT valid here — use 'before' instead
```

### Hook types

| Hook       | Command-specific (global/repo only) | Runs when                        | Failure behavior       |
|------------|--------------------------------------|----------------------------------|------------------------|
| `before`   | `before_<cmd>`                       | Before the command               | Aborts the command     |
| `after`    | `after_<cmd>`                        | After success only               | Logged, doesn't affect result |
| `failed`   | `failed_<cmd>`                       | After failure only               | Logged, doesn't affect result |
| `finally`  | `finally_<cmd>`                      | Always, regardless of outcome    | Logged, doesn't affect result |

Hooks only run for `backup`, `prune`, `check`, and `compact`. The bare form (`before`, `after`, etc.) fires for all four commands. The command-specific form (`before_backup`, `failed_prune`, etc.) fires only for that command and is only available at the global and per-repository levels — **not** in per-source hooks.

### Execution order

1. `before` hooks run: global bare → repo bare → global specific → repo specific
2. The vykar command runs (skipped if a `before` hook fails)
3. On success: `after` hooks run (repo specific → global specific → repo bare → global bare)
   On failure: `failed` hooks run (same order)
4. `finally` hooks always run last (same order)

If a `before` hook fails, the command is skipped and both `failed` and `finally` hooks still run.

### Variable substitution

Hook commands support `{variable}` placeholders that are replaced before execution. Values are automatically shell-escaped.

| Variable         | Description                                  |
|------------------|----------------------------------------------|
| `{command}`      | The vykar command name (e.g. `backup`, `prune`) |
| `{repository}`   | Repository URL                               |
| `{label}`        | Repository label (empty if unset)            |
| `{error}`        | Error message (empty if no error)            |
| `{source_label}` | Source label (empty if unset)                |
| `{source_path}`  | Source path list (Unix `:`, Windows `;`)     |

The same values are also exported as environment variables: `VYKAR_COMMAND`, `VYKAR_REPOSITORY`, `VYKAR_LABEL`, `VYKAR_ERROR`, `VYKAR_SOURCE_LABEL`, `VYKAR_SOURCE_PATH`.

`{source_path}` / `VYKAR_SOURCE_PATH` joins multiple paths with `:` on Unix and `;` on Windows.

```yaml
hooks:
  failed:
    - 'notify-send "vykar {command} failed: {error}"'
  after_backup:
    - 'echo "Backed up {source_label} to {repository}"'
```

### Notifications with Apprise

[Apprise](https://github.com/caronc/apprise) lets you send notifications to 100+ services (Gotify, Slack, Discord, Telegram, ntfy, email, and more) from the command line. Since vykar hooks run arbitrary shell commands, you can use the `apprise` CLI directly — no built-in integration needed.

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
      -b "vykar {command} finished for {repository}"
      "gotify://hostname/token"
      "slack://tokenA/tokenB/tokenC"
  failed:
    - >-
      apprise -t "Backup failed"
      -b "vykar {command} failed for {repository}: {error}"
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

## Schedule

Configure the built-in daemon scheduler for automatic periodic backups. Used with `vykar daemon`.

```yaml
schedule:
  enabled: true                        # Enable scheduled backups (default false)
  every: "6h"                          # Interval between runs: "30m", "6h", "2d", or integer days (default "24h")
  # cron: "0 3 * * *"                 # OR 5-field cron expression (mutually exclusive with every)
  on_startup: false                    # Run a backup immediately when the daemon starts (default false)
  jitter_seconds: 0                    # Random delay 0–N seconds added to each run (default 0)
  passphrase_prompt_timeout_seconds: 300  # Timeout for interactive passphrase prompts (default 300)
```

### Interval mode

The `every` field accepts `m` (minutes), `h` (hours), or `d` (days) suffixes; a plain integer is treated as days. If neither `every` nor `cron` is set, the default interval is `24h`.

### Cron mode

The `cron` field accepts a standard 5-field cron expression (`minute hour dom month dow`). Six-field (with seconds) and seven-field expressions are rejected.

```yaml
schedule:
  enabled: true
  cron: "0 3 * * *"          # daily at 3:00 AM
  jitter_seconds: 60
```

Common cron examples:
- `"0 3 * * *"` — daily at 3:00 AM
- `"30 2 * * 1-5"` — weekdays at 2:30 AM
- `"0 */6 * * *"` — every 6 hours on the hour
- `"0 0 * * 0"` — weekly on Sunday at midnight

`every` and `cron` are **mutually exclusive** — setting both is a configuration error.

Jitter (`jitter_seconds`) applies in both modes. In cron mode, jitter is added after the computed cron tick. Keep jitter small relative to the cron cadence to avoid skipping slots.

When multiple repositories are configured, schedule values are merged: `enabled` and `on_startup` are OR'd across repos, `jitter_seconds` and `passphrase_prompt_timeout_seconds` take the maximum, and `every` uses the shortest interval.

## Environment Variable Expansion

Config files support environment variable placeholders in values:

```yaml
repositories:
  - url: "${VYKAR_REPO_URL:-/backup/repo}"
    # access_token: "${VYKAR_ACCESS_TOKEN}"
```

Supported syntax:

- `${VAR}`: requires `VAR` to be set (hard error if missing)
- `${VAR:-default}`: uses `default` when `VAR` is unset or empty

Notes:

- Expansion runs on raw config text before YAML parsing.
- Variable names must match `[A-Za-z_][A-Za-z0-9_]*`.
- Malformed placeholders fail config loading.
- No escape syntax is supported for literal `${...}`.
