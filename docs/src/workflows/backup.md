# Make a Backup

## Run a backup

```bash
# Create a backup of all configured sources
vger backup
```

## Label backups

```bash
# Annotate a backup with a label (e.g. before a system change)
vger backup --label before-upgrade
```

## Back up a single source

```bash
# Back up only the source labeled "docs" in your config
vger backup --source docs
```

## List and verify snapshots

```bash
# List all snapshots
vger list

# List the 5 most recent snapshots
vger list --last 5

# List snapshots for a specific source
vger list --source docs

# List files inside a snapshot
vger list --snapshot a1b2c3d4
```

## Related pages

- [Quick Start](../quickstart.md)
- [Configuration Reference](../configuration.md)
- [Restore a Backup](restore.md)
