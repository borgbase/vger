---
name: postgres
description: "Test PostgreSQL backups using hooks and command_dumps patterns"
---

# PostgreSQL Integration

## Goal

Test PostgreSQL backups using both recipe patterns from [vger docs](https://vger.pages.dev/recipes#databases):
1. **Hooks** that write dump files to disk
2. **command_dumps** that stream stdout directly

## Test Data Setup

1. Start a Postgres container:
   ```bash
   sudo docker run -d --name vger-pg -e POSTGRES_PASSWORD=testpass -p 5432:5432 postgres:16
   ```
2. Create test database `vger_pg_test`
3. Seed dummy data:
   - Table `users` — 10 rows
   - Table `orders` — 50 rows
4. Verify seeded counts before backup

## Variant A: Hooks Dump to Temporary Directory

Configure source in vger config:
- `label: pg-hooks`
- `path: <temp_dump_dir>`
- `hooks.before`: create dir + `pg_dump -U postgres -Fc vger_pg_test > <temp_dump_dir>/vger_pg_test.dump`
- `hooks.after`: remove temp dir

Run backup and validate snapshot contains `vger_pg_test.dump`.

## Variant B: command_dumps

Configure source in vger config:
- `label: pg-cmd`
- `command_dumps`:
  - `name: vger_pg_test.dump`
  - `command: pg_dump -U postgres -h 127.0.0.1 -Fc vger_pg_test`

Run backup and validate artifact exists under `.vger-dumps/` in snapshot listing.

## Run Matrix

Run both variants against each backend:
1. `local` first
2. `s3` second
3. `sftp` last (with timeouts, mark BLOCKED on failure)

Clean remote storage with `rclone delete --rmdirs` between backend runs.

## Integrity Check

1. Restore dump artifact from snapshot into temp directory
2. Create fresh database `vger_pg_restore_test`
3. `pg_restore` the dump into the fresh database
4. Verify row counts match: `users=10`, `orders=50`
5. Ensure Postgres client/server major versions are compatible

## Common Issues

- `vger snapshot` usage: `-R <repo>` belongs to `snapshot`, not `list` subcommand
- Command dump artifacts appear under `.vger-dumps/` in snapshot listings
- Client/server version mismatches can cause `pg_restore` config parameter errors
- Use `sudo docker` if user lacks Docker socket access

## Cleanup

1. Drop test databases
2. Stop and remove Postgres container: `sudo docker rm -f vger-pg`
3. Clean remote storage paths with `rclone` before next scenario
