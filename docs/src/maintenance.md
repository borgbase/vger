# Maintenance

## Delete a snapshot

```bash
# Delete a specific snapshot by ID
vger snapshot delete a1b2c3d4
```

## Delete a repository

Permanently delete an entire repository and all its snapshots.

```bash
# Interactive confirmation (prompts you to type "delete")
vger delete

# Non-interactive (for scripting)
vger delete --yes-delete-this-repo
```

## Prune old snapshots

Apply the retention policy defined in your configuration to remove expired snapshots. Optionally `compact` the repository after pruning.

```bash
vger prune --compact
```

## Verify repository integrity

```bash
# Structural integrity check
vger check

# Full data verification (reads and verifies every chunk)
vger check --verify-data
```

## Compact (reclaim space)

After `delete` or `prune`, blob data remains in pack files. Run `compact` to rewrite packs and reclaim disk space.

```bash
# Preview what would be repacked
vger compact --dry-run

# Repack to reclaim space
vger compact
```

## Related pages

- [Quick Start](quickstart.md)
- [Server Mode](server-mode.md) (server-side compaction)
- [Architecture](architecture.md) (compact algorithm details)
