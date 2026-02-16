---
name: rest
description: "Validate vger REST backend against a local vger-server instance"
---

# REST Backend (Local Server) — Corpus Backup & Restore

## Goal

Validate vger backup and restore correctness over the REST backend using a local `vger-server` instance.

## Scope

- **Backend**: `rest` (URL and token from `~/vger.sample.yaml`)
- **Source dataset**: `~/corpus-remote` (default), optionally `~/corpus-local` for stress
- **Verification**: restored tree matches source tree exactly

## Prerequisites

1. Ensure user service is running:
   ```bash
   systemctl --user enable --now vger-server.service
   systemctl --user is-active vger-server.service
   curl -fsS http://127.0.0.1:8484/health
   ```
2. Create config from `~/vger.sample.yaml` with REST repo definition:
   - `url: "http://127.0.0.1:8484/<repo-name>"`
   - `label: "rest"`
   - `rest_token: "<token>"`
3. `export VGER_PASSPHRASE=123`

## Local REST Cleanup (before each run)

Use a unique REST repo name per run (recommended), or delete previous server-side repo data directory before reruns.

## Test Procedure

1. Delete REST repo from previous runs (best effort):
   ```bash
   vger -c <config> delete -R rest --yes-delete-this-repo || true
   ```
2. Initialize REST repo:
   ```bash
   vger -c <config> init -R rest
   ```
3. Run backup:
   ```bash
   vger -c <config> backup -R rest -l rest-corpus ~/corpus-remote
   ```
4. Confirm snapshot:
   ```bash
   vger -c <config> list -R rest
   ```
5. Capture latest snapshot ID.
6. Restore to empty temp directory:
   ```bash
   vger -c <config> restore -R rest --dest <restore_dir> <snapshot_id>
   ```
7. Integrity check:
   ```bash
   vger -c <config> check -R rest
   ```
8. Delete the tested snapshot:
   ```bash
   vger -c <config> snapshot -R rest delete <snapshot_id>
   ```
9. Compact repository packs:
   ```bash
   vger -c <config> compact -R rest
   ```

## Validation

1. Snapshot exists for label `rest-corpus`
2. Restore completes successfully
3. `diff -qr ~/corpus-remote <restore_dir>` reports no differences
4. `vger snapshot ... delete <snapshot_id>` exits 0
5. `vger compact` exits 0
6. Optional: SHA256 manifest comparison

## Failure Cases to Record

- REST auth or token mismatch (`401`)
- Request body limit errors (`413`) on larger uploads
- Server-side connection resets (`broken pipe`) during pack uploads
- Restore mismatch vs source
- `vger check` failures
- `vger snapshot delete` or `vger compact` failures

## Cleanup

1. Remove restore temp directory
2. Keep logs under `~/runtime/logs/`
3. Keep report under `~/runtime/reports/`
