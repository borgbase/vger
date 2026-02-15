---
name: sftp
description: "Validate vger SFTP backend with timeout handling for connectivity instability"
---

# SFTP Backend — Corpus Backup & Restore

## Goal

Validate vger backup and restore correctness on the SFTP backend while handling known connectivity instability.

## Scope

- **Backend**: `sftp` (credentials and host from `~/vger.sample.yaml`)
- **Source dataset**: `~/corpus-remote`
- **Verification**: restored tree matches source tree exactly

## Prerequisites

1. Create config from `~/vger.sample.yaml` with SFTP repo definition
2. `export VGER_PASSPHRASE=123`
3. Confirm SSH key auth works from host
4. Ensure `rclone` is configured for SFTP cleanup

## Timeout Strategy

SFTP connectivity can be intermittent even when rclone works against the same server. Wrap all vger commands with `timeout` to prevent indefinite hangs:
- Init/list/check: `timeout 120s`
- Backup/restore: `timeout 3600s`

Exit code 124 means timeout — mark test as **BLOCKED**.

## Remote Cleanup (before each run)

```bash
timeout 180 rclone delete <sftp_remote:path> --rmdirs
```
- If path is absent, treat `directory not found` as non-fatal

## Test Procedure

1. Initialize repo:
   ```bash
   timeout 120 vger init -c <config> -R sftp
   ```
2. Run backup:
   ```bash
   timeout 3600 vger backup -c <config> -R sftp -l remote-corpus ~/corpus-remote
   ```
3. Confirm snapshot:
   ```bash
   timeout 120 vger list -c <config> -R sftp --last 3
   ```
4. Capture latest snapshot ID.
5. Restore to empty temp directory:
   ```bash
   timeout 3600 vger extract -c <config> -R sftp --dest <restore_dir> <snapshot_id>
   ```
6. Integrity check:
   ```bash
   timeout 300 vger check -c <config> -R sftp
   ```

## Validation

1. Snapshot exists for label `remote-corpus`
2. Restore exits 0 (not timeout 124)
3. `diff -qr ~/corpus-remote <restore_dir>` reports no differences
4. Optional: SHA256 manifest comparison

## Failure Cases to Record

- Init/list/backup timeouts
- Connection retries that never converge
- Restore mismatch vs source
- Repository check failures
- False lock detection (locks/ directory empty but lock error reported)

## Cleanup

1. Remove restore temp directory
2. Clean SFTP path with `rclone delete --rmdirs` between reruns
3. Preserve timeout logs for troubleshooting
4. Ensure no stuck `vger` process remains after aborted steps
