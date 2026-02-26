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
   sudo docker run -d --name vger-maria -e MARIADB_ROOT_PASSWORD=testpass -p 3306:3306 mariadb:11
   ```
3. Create test database `vger_maria_test`
4. Create dedicated dump user (recommended for stable auth inside container):
   - user: `vger`
   - host: `localhost`
   - password: `vgerpass`
   - grants: full privileges on `vger_maria_test.*`
5. Seed dummy data:
   - Table `customers` — 10 rows
   - Table `invoices` — 50 rows
6. Verify seeded counts before backup

## Variant A: Hooks Dump to Temporary Directory

Configure source in vger config:
- `label: maria-hooks`
- `path: <temp_dump_dir>`
- `hooks.before`: create dir + `mariadb-dump --protocol=socket --socket=/run/mysqld/mysqld.sock -u vger -pvgerpass vger_maria_test > <temp_dump_dir>/vger_maria_test.sql`
- `hooks.after`: remove temp dir

Run backup and validate snapshot contains SQL dump file.

## Variant B: command_dumps

Configure source in vger config:
- `label: maria-cmd`
- `command_dumps`:
  - `name: vger_maria_test.sql`
  - `command: sh -lc 'for i in 1 2 3 4 5; do mariadb-dump --protocol=socket --socket=/run/mysqld/mysqld.sock -u vger -pvgerpass vger_maria_test && exit 0; sleep 1; done; exit 1'`

Run backup and validate artifact exists and is non-empty.

## Run Matrix

1. Initialize local repo, run both variants + restore check
2. Run REST backend variants second (local `vger-server`)
3. Clean S3 with `rclone delete --rmdirs`, run both variants
4. Run SFTP variants last with timeouts — mark BLOCKED on timeout
5. Do NOT rerun full plan for SFTP-only failures

## Integrity Check

1. Restore SQL artifact from snapshot
2. Import into fresh database `vger_maria_restore_test`
3. Verify row counts: `customers=10`, `invoices=50`

## Large-Volume Variant (about 30 GiB)

Use this only when stress-testing `command_dumps` behavior:

1. Seed a `large_payload` table in batches until `information_schema.tables` reports at least `30 * 1024^3` bytes
   - Preferred helper for high-entropy rows:
     - `scripts/mariadb-generate-random-data.sh --container <maria_container> --target-gib 30`
   - Default behavior truncates the target table before refill
2. Keep the dump command socket-based with retries:
   - `mariadb-dump --quick --ssl=0 --protocol=socket --socket=/run/mysqld/mysqld.sock -u vger -pvgerpass vger_maria_test`
3. Enforce generous backup/restore timeouts (`timeout 10800`)
4. Validate restored SQL artifact size (expect tens of GiB)

## Common Issues

- Modern MariaDB images use `mariadb`, `mariadb-dump`, `mariadb-admin` — not the legacy `mysql*` names
- `mariadb:11` images may not include a `mysql` binary at all; use `mariadb` client commands for probes/imports
- Root authentication mode can vary across images; a dedicated dump user is more reliable than root for `command_dumps`
- Some runs intermittently fail with socket/auth errors (`2002`/`1045`); add bounded retry around dump/backup when validating stability
- In this sandbox, host TCP to a mapped port can work while in-container `--protocol=tcp -h 127.0.0.1` fails intermittently; prefer in-container socket dumps for `docker exec` workflows
- Large `command_dumps` (for example 30+ GiB SQL) can drive very high `vger` RSS during capture; if memory pressure appears, prefer hook-based dump-to-file workflows for realistic large-data tests
- Synthetic payloads like `REPEAT('x', ...)` are useful for volume-path validation but compress unrealistically well; use higher-entropy sample generation when benchmarking storage efficiency
- Command dump artifacts appear under `.vger-dumps/` in snapshot listings
- Use `sudo docker` if user lacks Docker socket access

## Cleanup

1. Drop test databases
2. Stop and remove MariaDB container: `sudo docker rm -f vger-maria`
3. Clean remote storage paths with `rclone` between runs
4. Ensure no stuck `vger` process remains after aborted SFTP steps
