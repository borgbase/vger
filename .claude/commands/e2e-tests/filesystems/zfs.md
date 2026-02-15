---
name: zfs
description: "Validate ZFS snapshot hooks pattern from vger recipes"
---

# ZFS Filesystem Snapshots

## Goal

Validate vger's filesystem snapshot backup pattern for ZFS using hooks:
1. Create a dataset snapshot before backup
2. Back up the `.zfs/snapshot/...` path
3. Destroy the snapshot after backup

Recipe reference: https://vger.pages.dev/recipes#filesystem-snapshots (ZFS section)

## Safety Gate (REQUIRED)

Before touching any disk, confirm the test partition is safe:
1. Verify system/root disk is NOT the test partition:
   ```bash
   findmnt -no SOURCE /
   lsblk -f
   ```
2. Confirm the test partition holds only disposable test data
3. **Stop immediately** if any non-test data exists on the target partition

## Prerequisite Install

If ZFS packages are missing:
```bash
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y linux-headers-$(uname -r) zfs-dkms zfsutils-linux
sudo DEBIAN_FRONTEND=noninteractive dpkg --configure -a
sudo modprobe zfs
```

Use `DEBIAN_FRONTEND=noninteractive` to avoid `zfs-dkms` TTY prompt blocking automation. DKMS module build can take several minutes.

## Test Partition Setup

Use the dedicated test partition. If previously formatted for another FS (e.g., Btrfs), unmount first:
```bash
sudo zpool create -f vgerpool /dev/<test_partition>
sudo zfs create vgerpool/data
sudo zfs set snapdir=visible vgerpool/data   # CRITICAL — without this, .zfs/snapshot/ is inaccessible
```

Default mountpoint is typically `/vgerpool/data`.

## Test Data

Seed representative files (200 files) under the dataset mountpoint:
```bash
sudo bash -c 'for i in $(seq 1 200); do echo "zfs-file-$i $(date -u +%s)" > /vgerpool/data/file-$i.txt; done'
```

## Source Definition

Configure in vger config:
```yaml
sources:
  - path: /vgerpool/data/.zfs/snapshot/vger-tmp
    label: zfs-data
    hooks:
      before: zfs snapshot vgerpool/data@vger-tmp
      after: zfs destroy vgerpool/data@vger-tmp
```

Use `sudo vger` since the source path is root-owned.

## Run Matrix

1. `local` repository first
2. `s3` second
3. `sftp` optional and last (use `timeout` wrappers, skip if unstable)

## Validation

1. `vger backup` exits 0
2. `vger list` shows new snapshot for `zfs-data`
3. `vger snapshot -R <repo> list <id>` includes seeded files
4. Hook cleanup verified:
   ```bash
   sudo zfs list -t snapshot | grep 'vgerpool/data@vger-tmp'
   # Should return no match
   ```
5. Restore to temp dir and verify file count matches

## Failure Cases to Explicitly Test

- Snapshot name collision (`vgerpool/data@vger-tmp` already exists)
- `snapdir` hidden (path inaccessible at `.zfs/snapshot/...`)
- `hooks.after` failure leaves snapshot present
- Source data churn during backup (verify snapshot consistency)

## Common Issues

- **Always** verify `snapdir=visible` before running backups — without it, the snapshot path is invisible
- Keep explicit snapshot existence checks after each run
- Isolate SFTP failures from local/s3 results
- After `zpool destroy`, partition may still show `zfs_member` — next FS test can safely overwrite

## Cleanup

1. Remove temp restore directories
2. Destroy dataset and pool:
   ```bash
   sudo zfs destroy -r vgerpool/data || true
   sudo zpool destroy vgerpool || true
   ```
3. Clean remote paths with `rclone delete --rmdirs` between reruns
