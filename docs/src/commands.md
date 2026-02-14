# Command Reference

| Command | Description |
|---------|-------------|
| `vger` | Run full backup process: `backup`, `prune`, `compact`, `check`. This is useful for automation. |
| `vger config` | Generate a starter configuration file |
| `vger init` | Initialize a new backup repository |
| `vger backup` | Back up files to a new snapshot |
| `vger list` | List snapshots |
| `vger snapshot list` | Show files and directories inside a snapshot |
| `vger snapshot info` | Show metadata for a snapshot |
| `vger snapshot find` | Find matching files across snapshots and show change timeline (`added`, `modified`, `unchanged`) |
| `vger restore` | Restore files from a snapshot |
| `vger delete` | Delete a specific snapshot |
| `vger prune` | Prune snapshots according to retention policy |
| `vger check` | Verify repository integrity (`--verify-data` for full content verification) |
| `vger info` | Show repository statistics (snapshot counts and size totals) |
| `vger compact` | Free space by repacking pack files after delete/prune |
| `vger mount` | Browse snapshots via a local WebDAV server |
