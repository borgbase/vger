# Quick Start

## Install

Download a pre-built binary from the [releases page](https://github.com/borgbase/vger/releases), or build from source:

```bash
cargo build --release
```

The binary is at `target/release/vger`. See [Installing](install.md) for more details.

## Create a config file

Generate a starter configuration in the current directory:

```bash
vger config
```

Or write it to a specific path:

```bash
vger config --dest ~/.config/vger/config.yaml
```

Edit the generated `vger.yaml` to set your repository path and source directories. Encryption is enabled by default. See [Configuration](configuration.md) for a full reference.

## Initialize and back up

Initialize the repository (prompts for passphrase if encrypted):

```bash
vger init
```

Create a backup of all configured sources:

```bash
vger backup
```

## Inspect snapshots

List all snapshots:

```bash
vger list
```

Show repository statistics:

```bash
vger info
```

List files inside a snapshot (use the hex ID from `vger list`):

```bash
vger list --snapshot a1b2c3d4
```

## Restore

Restore files from a snapshot to a directory:

```bash
vger restore --snapshot a1b2c3d4 --dest /tmp/restored
```

For backup options, snapshot browsing, and maintenance tasks, see the [workflow guides](backup.md).
