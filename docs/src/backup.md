# Make a Backup

## Run a backup

Back up all configured sources to all configured repositories:

```bash
vger backup
```

## Sources and labels

Each source in your config produces its own snapshot. When you use the rich source form, the `label` field gives each source a short name you can reference from the CLI:

```yaml
sources:
  - path: "/home/user/documents"
    label: "docs"
  - path: "/home/user/photos"
    label: "photos"
```

For simple string sources (e.g. `- "/home/user/documents"`), the label is derived automatically from the directory name (`documents`).

Back up only a specific source by label:

```bash
vger backup --source docs
```

When targeting a specific repository, use `--repo`:

```bash
vger --repo local backup --source docs
```

## Label backups

Annotate a snapshot with a label, for example before a system change:

```bash
vger backup --label before-upgrade
```

This is separate from source labels — it tags the resulting snapshot so you can identify it later in `vger list` output.

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

- [Quick Start](quickstart.md)
- [Configuration](configuration.md)
- [Restore a Backup](restore.md)
