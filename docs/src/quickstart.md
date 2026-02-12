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

# Create a backup
vger backup

# List all snapshots
vger list

# Show repository statistics
vger info

# List files inside a snapshot (use the hex ID from `vger list`)
vger list --snapshot a1b2c3d4

# Restore to a directory
vger extract --snapshot a1b2c3d4 --dest /tmp/restored
```

For backup options, snapshot browsing, and maintenance tasks, see the [workflow guides](workflows/index.md).

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
| `vger info` | Show repository statistics (snapshot counts and size totals) |
| `vger compact` | Free space by repacking pack files after delete/prune |
| `vger mount` | Browse snapshots via a local WebDAV server |
