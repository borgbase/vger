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
vger list --snapshot a1b2c3d4
```

## Extract to a directory

```bash
# Restore all files from a snapshot
vger extract --snapshot a1b2c3d4 --dest /tmp/restored
```

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

- [Quick Start](../quickstart.md)
- [Make a Backup](backup.md)
