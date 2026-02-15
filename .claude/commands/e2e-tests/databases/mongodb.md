---
name: mongodb
description: "Test MongoDB backups using command_dumps with mongodump --archive"
---

# MongoDB Integration

## Goal

Test MongoDB backup workflow using `command_dumps` with `mongodump --archive --gzip`.

## Test Data Setup

1. Pre-pull image: `sudo docker pull mongo:7`
2. Start a MongoDB container:
   ```bash
   sudo docker run -d --name vger-mongo -p 27017:27017 mongo:7
   ```
3. Create test database `vger_mongo_test`
4. Insert dummy data:
   - Collection `profiles` — 10 documents
   - Collection `events` — 100 documents
5. Verify seeded counts before backup

## Backup Variant: command_dumps

Configure source in vger config:
- `label: mongodb`
- `command_dumps`:
  - `name: vger_mongo_test.archive.gz`
  - `command: mongodump --archive --gzip --db vger_mongo_test`

If host lacks `mongodump`, use the Docker exec fallback:
```yaml
command: sudo docker exec vger-mongo sh -lc 'mongodump --archive --gzip --db vger_mongo_test'
```

## Optional: Full-Instance Variant

Additional run capturing all databases:
- `name: all.archive.gz`
- `command: mongodump --archive --gzip` (no `--db` flag)

## Run Matrix

1. `local` first with full restore validation
2. Clean S3 with `rclone delete --rmdirs`, run `s3` second
3. `sftp` last (or skip when investigating unrelated scenarios)

## Integrity Check

1. Restore archive artifact from snapshot into temp directory
2. Restore into fresh database `vger_mongo_restore_test`:
   ```bash
   mongorestore --archive=<artifact> --gzip --nsFrom='vger_mongo_test.*' --nsTo='vger_mongo_restore_test.*'
   ```
3. Verify document counts: `profiles=10`, `events=100`

If host lacks `mongorestore`, use `docker exec` approach.

## Common Issues

- Host may not have MongoDB client tools — `docker exec` is a reliable fallback
- Command dump artifacts appear under `.vger-dumps/` in snapshot listings

## Cleanup

1. Drop test databases
2. Stop and remove MongoDB container: `sudo docker rm -f vger-mongo`
3. Clean remote storage paths with `rclone` between runs
