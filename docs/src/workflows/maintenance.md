# Maintenance

## Delete a snapshot

```bash
# Delete a specific snapshot by ID
vger delete --snapshot a1b2c3d4
```

## Prune old snapshots

Apply the retention policy defined in your configuration to remove expired snapshots.

```bash
vger prune
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

- [Quick Start](../quickstart.md)
- [Server Mode](../server-mode.md) (server-side compaction)
- [Architecture](../architecture.md) (compact algorithm details)
