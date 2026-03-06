# E2E Test Results (MongoDB Skipped)

## Summary
- Timestamp (UTC): 2026-03-06T22:40:53Z
- Passed tests: 11
- Failed tests: 6

## Per-test Status

| Backend | Test | Pass/Fail | Notes |
|---|---|---|---|
| local | backends | PASS | ok |
| rest | backends | FAIL | init |
| s3 | backends | PASS | ok |
| sftp | backends | PASS | ok |
| local | interrupt-minimal | PASS | interrupt_rc=130 resume_rc=0 snapshot_id=a22ee5f7 |
| rest | interrupt-minimal | FAIL | init |
| s3 | interrupt-minimal | PASS | interrupt_rc=130 resume_rc=0 snapshot_id=0a9f13d8 |
| sftp | interrupt-minimal | PASS | interrupt_rc=137 resume_rc=0 snapshot_id=a598e486 |
| local | databases-postgres | PASS | ok |
| local | databases-mariadb | PASS | ok |
| local | containers-docker | PASS | ok |
| local | containers-podman | FAIL | rc=125 |
| local | filesystems-btrfs | FAIL | rc=2 |
| local | filesystems-zfs | FAIL | rc=1 |
| local | filesystems-vm-image | PASS | ok |
| local | stress | PASS | ok |
| local | benchmarks | FAIL | canceled by user request before completion |

## Failure Details

### rest / backends — FAIL
- `vykar init` failed against REST endpoint.
- Error: `REST HEAD config: http://127.0.0.1:8585/config: status code 400`.
- Source log: `/home/manu/runtime/logs/backend_rest_init.log`.

### rest / interrupt-minimal — FAIL
- Could not start interrupt flow because REST init failed with the same 400 on `HEAD /config`.
- Source log: `/home/manu/runtime/logs/interrupt_rest_init.log`.

### local / containers-podman — FAIL
- Rootless Podman failed: `newuidmap` not found.
- Backup then failed with `source directory does not exist` due invalid source path setup.
- Source log: `/home/manu/runtime/logs/containers-podman.log`.

### local / filesystems-btrfs — FAIL
- Btrfs setup reached format/mount stage, then test script hit shell syntax error: `syntax error near unexpected token `2'`.
- Source log: `/home/manu/runtime/logs/filesystems-btrfs.log`.

### local / filesystems-zfs — FAIL
- ZFS pool creation failed: `/dev/nvme0n1p2` reported busy / already referenced.
- Source log: `/home/manu/runtime/logs/filesystems-zfs.log`.

### local / benchmarks — FAIL
- Benchmark canceled by user request before completion.
- Run had progressed through at least vykar/restic/rustic and into borg phases.
- Source log: `/home/manu/runtime/logs/benchmarks.log`.
