---
name: mariadb
description: "Test MariaDB backups with hooks and command_dumps patterns"
---

# MariaDB Integration

## Goal

Test MariaDB backups using both recipe patterns:
1. **Hooks** that write SQL dump files to disk
2. **command_dumps** that stream `mariadb-dump` stdout

## Test Data Setup

1. Pre-pull image: `sudo docker pull mariadb:11`
2. Start a MariaDB container:
   ```bash
   sudo docker run -d --name vger-maria -e MYSQL_ROOT_PASSWORD=testpass -p 3306:3306 mariadb:11
   ```
3. Create test database `vger_maria_test`
4. Seed dummy data:
   - Table `customers` — 10 rows
   - Table `invoices` — 50 rows
5. Verify seeded counts before backup

## Variant A: Hooks Dump to Temporary Directory

Configure source in vger config:
- `label: maria-hooks`
- `path: <temp_dump_dir>`
- `hooks.before`: create dir + `mariadb-dump -h 127.0.0.1 -P 3306 -u root -p"$MYSQL_ROOT_PASSWORD" vger_maria_test > <temp_dump_dir>/vger_maria_test.sql`
- `hooks.after`: remove temp dir

Run backup and validate snapshot contains SQL dump file.

## Variant B: command_dumps

Configure source in vger config:
- `label: maria-cmd`
- `command_dumps`:
  - `name: vger_maria_test.sql`
  - `command: mariadb-dump -h 127.0.0.1 -P 3306 -u root -p"$MYSQL_ROOT_PASSWORD" vger_maria_test`

Run backup and validate artifact exists and is non-empty.

## Run Matrix

1. Initialize local repo, run both variants + restore check
2. Clean S3 with `rclone delete --rmdirs`, run both variants
3. Run SFTP variants last with timeouts — mark BLOCKED on timeout
4. Do NOT rerun full plan for SFTP-only failures

## Integrity Check

1. Restore SQL artifact from snapshot
2. Import into fresh database `vger_maria_restore_test`
3. Verify row counts: `customers=10`, `invoices=50`

## Common Issues

- Modern MariaDB images use `mariadb`, `mariadb-dump`, `mariadb-admin` — not the legacy `mysql*` names
- Command dump artifacts appear under `.vger-dumps/` in snapshot listings
- Use `sudo docker` if user lacks Docker socket access

## Cleanup

1. Drop test databases
2. Stop and remove MariaDB container: `sudo docker rm -f vger-maria`
3. Clean remote storage paths with `rclone` between runs
4. Ensure no stuck `vger` process remains after aborted SFTP steps
