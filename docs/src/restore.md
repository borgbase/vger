# Restore a Backup

## Locate snapshots

```bash
# List all snapshots
vger list

# List the 5 most recent snapshots
vger list --last 5

# List snapshots for a specific source
vger list --source docs
```

## Inspect snapshot contents

```bash
# List files inside a snapshot
vger snapshot list a1b2c3d4

# List with details (type, permissions, size, mtime)
vger snapshot list a1b2c3d4 --long

# Limit listing to a subtree
vger snapshot list a1b2c3d4 --path src

# Sort listing by size (name, size, mtime)
vger snapshot list a1b2c3d4 --sort size
```

## Inspect snapshot metadata

```bash
vger snapshot info a1b2c3d4
```

## Find files across snapshots

Use `snapshot find` to locate files before choosing which snapshot to restore from.

```bash
# Find PDFs modified in the last 14 days
vger snapshot find --name '*.pdf' --since 14d

# Limit search to one source and recent snapshots
vger snapshot find --source docs --last 10 --name '*.docx'

# Search under a subtree with case-insensitive name matching
vger snapshot find sub --iname 'report*' --since 7d

# Combine type and size filters
vger snapshot find --type f --larger 1M --smaller 20M --since 30d
```

- `--last` must be `>= 1`.
- `--since` accepts positive spans with suffix `h`, `d`, or `w` (for example: `24h`, `7d`, `2w`).
- `--larger` means at least this size, and `--smaller` means at most this size.

## Restore to a directory

```bash
# Restore all files from a snapshot
vger restore a1b2c3d4 /tmp/restored

# Restore the most recent snapshot
vger restore latest /tmp/restored
```

Restore applies extended attributes (`xattrs`) by default. Control this with the top-level `xattrs.enabled` config setting.

## Browse via WebDAV (mount)

Browse snapshot contents via a local WebDAV server.

```bash
# Serve all snapshots (default: http://127.0.0.1:8080)
vger mount

# Serve a single snapshot
vger mount --snapshot a1b2c3d4

# Only snapshots from a specific source
vger mount --source docs

# Custom listen address
vger mount --address 127.0.0.1:9090
```

## Related pages

- [Quick Start](quickstart.md)
- [Make a Backup](backup.md)
