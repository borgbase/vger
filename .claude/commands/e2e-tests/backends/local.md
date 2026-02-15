---
name: local
description: "Validate vger local backend with full corpus backup and restore"
---

# Local Backend — Corpus Backup & Restore

## Goal

Validate vger filesystem backup and restore correctness on the local backend using the large corpus dataset.

## Scope

- **Backend**: `local`
- **Source dataset**: `~/corpus-local`
- **Verification**: restored tree matches source tree exactly

## Prerequisites

1. Create config from `~/vger.sample.yaml` with local repo path set to a writable location (e.g., `~/runtime/repos/local`)
2. `export VGER_PASSPHRASE=123`
3. Ensure enough free disk space for repository + restore directory

## Test Procedure

1. Clean local repo path:
   ```bash
   rm -rf ~/runtime/repos/local
   ```
2. Initialize repo:
   ```bash
   vger init -c <config> -R local
   ```
3. Run backup:
   ```bash
   vger backup -c <config> -R local -l local-corpus ~/corpus-local
   ```
4. Confirm snapshot:
   ```bash
   vger list -c <config> -R local --last 3
   ```
5. Capture latest snapshot ID from output.
6. Restore into empty temp directory:
   ```bash
   vger extract -c <config> -R local --dest <restore_dir> <snapshot_id>
   ```
7. Integrity check:
   ```bash
   vger check -c <config> -R local
   ```

## Validation

1. Snapshot exists for label `local-corpus`
2. Restore command exits 0
3. `diff -qr ~/corpus-local <restore_dir>` produces no differences
4. Optional: compare sorted SHA256 manifests from both trees

## Failure Cases to Record

- Local repo path permission errors
- Insufficient disk space during backup or restore
- Restore completes but diff reports missing or changed files
- `vger check` reports repository issues

## Cleanup

1. Remove temporary restore directory
2. Keep logs under `~/runtime/logs/`
