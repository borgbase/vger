# Make a Backup

## Run a backup

Back up all configured sources to all configured repositories:

```bash
vger backup
```

By default, V'Ger preserves filesystem extended attributes (`xattrs`). Configure this globally with `xattrs.enabled`, and override per source in rich `sources` entries.

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
vger backup --repo local --source docs
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
vger snapshot list a1b2c3d4

# Find recent SQL dumps across recent snapshots
vger snapshot find --last 5 --name '*.sql'

# Find logs from one source changed in the last week
vger snapshot find --source myapp --since 7d --iname '*.log'
```

## Command dumps

You can capture the stdout of shell commands directly into your backup using `command_dumps`. This is useful for database dumps, API exports, or any generated data that doesn't live as a regular file on disk:

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

Each command runs via `sh -c` and the captured output is stored as a virtual file under `.vger-dumps/` in the snapshot. On extract, these appear as regular files:

```text
.vger-dumps/postgres.sql
.vger-dumps/redis.rdb
```

You can also create dump-only sources with no filesystem paths:

```yaml
sources:
  - label: databases
    command_dumps:
      - name: all-databases.sql
        command: pg_dumpall -U postgres
```

Dump-only sources require an explicit `label`. If any command exits with a non-zero status, the backup is aborted.

## Related pages

- [Quick Start](quickstart.md)
- [Configuration](configuration.md)
- [Restore a Backup](restore.md)
