# Quick Start

## Build

```bash
cargo build --release
```

The binary is at `target/release/vger`.

## Create a config file

```bash
# Generate a starter config in the current directory
vger config

# Or write it to a specific path
vger config --dest ~/.config/vger/config.yaml
```

Edit the generated `vger.yaml` to set your repository path, source directories, and encryption mode.

V'Ger automatically finds config files in this order:

1. `--config <path>` flag
2. `VGER_CONFIG` environment variable
3. `./vger.yaml` (project)
4. `$XDG_CONFIG_HOME/vger/config.yaml` or `~/.config/vger/config.yaml` (user)
5. `/etc/vger/config.yaml` (system)

You can also set `VGER_PASSPHRASE` to supply the passphrase non-interactively.

## Run

```bash
# Initialize the repository (prompts for passphrase if encrypted)
vger init

# Create a backup (auto-generated 8-char hex ID)
vger backup

# Create a backup with a label annotation
vger backup --label before-upgrade

# Back up only one configured source
vger backup --source docs

# List all snapshots
vger list

# List the 5 most recent snapshots
vger list --last 5

# List snapshots for a specific source
vger list --source docs

# List files inside a snapshot (use the hex ID from `vger list`)
vger list --snapshot a1b2c3d4

# Browse snapshots via a local WebDAV server
vger mount

# Restore to a directory
vger extract --snapshot a1b2c3d4 --dest /tmp/restored

# Delete a specific snapshot
vger delete --snapshot a1b2c3d4

# Prune old snapshots per retention policy
vger prune

# Verify repository integrity (structural check)
vger check

# Full data verification (reads and verifies every chunk)
vger check --verify-data

# Reclaim space from deleted/pruned snapshots (dry-run first)
vger compact --dry-run
vger compact
```

## Mount

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

## Available commands

| Command | Description |
|---------|-------------|
| `vger config` | Generate a starter configuration file |
| `vger init` | Initialize a new backup repository |
| `vger backup` | Back up files to a new snapshot |
| `vger list` | List snapshots, or files within a snapshot |
| `vger extract` | Restore files from a snapshot |
| `vger delete` | Delete a specific snapshot |
| `vger prune` | Prune snapshots according to retention policy |
| `vger check` | Verify repository integrity (`--verify-data` for full content verification) |
| `vger compact` | Free space by repacking pack files after delete/prune |
| `vger mount` | Browse snapshots via a local WebDAV server |
