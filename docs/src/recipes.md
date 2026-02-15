# Backup Recipes

V'Ger provides hooks, command dumps, and source directories as universal building blocks. Rather than adding dedicated flags for each database or container runtime, the same patterns work for any application.

These recipes are starting points — adapt the commands to your setup.


## Databases

Databases should never be backed up by copying their data files while running. Use the database's own dump tool to produce a consistent export.


### Using hooks

Hooks run shell commands before and after a backup. For databases, dump to a temporary directory, back it up, then clean up. This approach works with any tool and gives you full control.

**PostgreSQL:**

```yaml
sources:
  - path: /var/backups/postgres
    label: postgres
    hooks:
      before: >
        mkdir -p /var/backups/postgres &&
        pg_dump -U myuser -Fc mydb > /var/backups/postgres/mydb.dump
      after: "rm -rf /var/backups/postgres"
```

**MySQL / MariaDB:**

```yaml
sources:
  - path: /var/backups/mysql
    label: mysql
    hooks:
      before: >
        mkdir -p /var/backups/mysql &&
        mysqldump -u root -p"$MYSQL_ROOT_PASSWORD" --all-databases
        > /var/backups/mysql/all.sql
      after: "rm -rf /var/backups/mysql"
```


### Using command dumps

Command dumps stream a command's stdout directly into the backup without writing temporary files to disk. This is simpler and more efficient for any tool that supports dumping to stdout.

**PostgreSQL:**

```yaml
sources:
  - label: postgres
    command_dumps:
      - name: mydb.dump
        command: "pg_dump -U myuser -Fc mydb"
```

**PostgreSQL (all databases):**

```yaml
sources:
  - label: postgres
    command_dumps:
      - name: all.sql
        command: "pg_dumpall -U postgres"
```

**MySQL / MariaDB:**

```yaml
sources:
  - label: mysql
    command_dumps:
      - name: all.sql
        command: "mysqldump -u root -p\"$MYSQL_ROOT_PASSWORD\" --all-databases"
```

**MongoDB:**

```yaml
sources:
  - label: mongodb
    command_dumps:
      - name: mydb.archive.gz
        command: "mongodump --archive --gzip --db mydb"
```

For all MongoDB databases, omit `--db`:

```yaml
sources:
  - label: mongodb
    command_dumps:
      - name: all.archive.gz
        command: "mongodump --archive --gzip"
```

**SQLite:**

SQLite can't stream to stdout, so use a hook instead. Copying the database file directly risks corruption if a process holds a write lock.

```yaml
sources:
  - path: /var/backups/sqlite
    label: app-database
    hooks:
      before: >
        mkdir -p /var/backups/sqlite &&
        sqlite3 /var/lib/myapp/app.db ".backup '/var/backups/sqlite/app.db'"
      after: "rm -rf /var/backups/sqlite"
```

**Redis:**

```yaml
sources:
  - path: /var/backups/redis
    label: redis
    hooks:
      before: >
        mkdir -p /var/backups/redis &&
        redis-cli BGSAVE &&
        sleep 2 &&
        cp /var/lib/redis/dump.rdb /var/backups/redis/dump.rdb
      after: "rm -rf /var/backups/redis"
```

The `sleep` gives Redis time to finish the background save. For large datasets, check `redis-cli LASTSAVE` in a loop instead.


## Docker and Containers

The same patterns work for containerized applications. Use `docker exec` for command dumps and hooks, or back up Docker volumes directly from the host.

These examples use Docker, but the same approach works with Podman or any other container runtime.


### Docker volumes (static data)

For volumes that hold files not actively written to by a running process — configuration, uploaded media, static assets — back up the host path directly.

```yaml
sources:
  - path: /var/lib/docker/volumes/myapp_data/_data
    label: myapp
```

> **Note:** The default volume path `/var/lib/docker/volumes/` applies to standard Docker installs on Linux. It differs for Docker Desktop on macOS/Windows, rootless Docker, and custom `data-root` configurations. Run `docker volume inspect <n>` to find the actual path.


### Docker volumes with brief downtime

For applications that write to the volume but can tolerate a short stop, stop the container during backup.

```yaml
sources:
  - path: /var/lib/docker/volumes/wiki_data/_data
    label: wiki
    hooks:
      before: "docker stop wiki"
      after:  "docker start wiki"
```


### Database containers

Use command dumps with `docker exec` to stream database exports directly from a container.

**PostgreSQL in Docker:**

```yaml
sources:
  - label: app-database
    command_dumps:
      - name: mydb.dump
        command: "docker exec my-postgres pg_dump -U myuser -Fc mydb"
```

**MySQL / MariaDB in Docker:**

```yaml
sources:
  - label: app-database
    command_dumps:
      - name: mydb.sql
        command: "docker exec my-mysql mysqldump -u root -p\"$MYSQL_ROOT_PASSWORD\" mydb"
```

**MongoDB in Docker:**

```yaml
sources:
  - label: app-database
    command_dumps:
      - name: mydb.archive.gz
        command: "docker exec my-mongo mongodump --archive --gzip --db mydb"
```


### Multiple containers

Use separate source entries so each service gets its own label, retention policy, and hooks.

```yaml
sources:
  - path: /var/lib/docker/volumes/nginx_config/_data
    label: nginx
    retention:
      keep_daily: 7

  - label: app-database
    command_dumps:
      - name: mydb.dump
        command: "docker exec my-postgres pg_dump -U myuser -Fc mydb"
    retention:
      keep_daily: 30

  - path: /var/lib/docker/volumes/uploads/_data
    label: uploads
```


## Filesystem Snapshots

For filesystems that support snapshots, the safest approach is to snapshot first, back up the snapshot, then delete it. This gives you a consistent point-in-time view without stopping any services.


### Btrfs

```yaml
sources:
  - path: /mnt/.snapshots/data-backup
    label: data
    hooks:
      before: "btrfs subvolume snapshot -r /mnt/data /mnt/.snapshots/data-backup"
      after:  "btrfs subvolume delete /mnt/.snapshots/data-backup"
```


### ZFS

```yaml
sources:
  - path: /tank/data/.zfs/snapshot/vger-tmp
    label: data
    hooks:
      before: "zfs snapshot tank/data@vger-tmp"
      after:  "zfs destroy tank/data@vger-tmp"
```

The `.zfs/snapshot` directory is automatically available on ZFS datasets without mounting.


### LVM

```yaml
sources:
  - path: /mnt/lvm-snapshot
    label: data
    hooks:
      before: >
        lvcreate -s -n vger-snap -L 5G /dev/vg0/data &&
        mkdir -p /mnt/lvm-snapshot &&
        mount -o ro /dev/vg0/vger-snap /mnt/lvm-snapshot
      after: >
        umount /mnt/lvm-snapshot &&
        lvremove -f /dev/vg0/vger-snap
```

Set the snapshot size (`-L 5G`) large enough to hold changes during the backup.


## Monitoring

V'Ger hooks can notify monitoring services on success or failure. A `curl` in an `after` hook replaces the need for dedicated integrations.


### Healthchecks

[Healthchecks](https://healthchecks.io/) alerts you when backups stop arriving. Ping the check URL after each successful backup.

```yaml
hooks:
  after: "curl -fsS -m 10 --retry 5 https://hc-ping.com/your-uuid-here"
```

To report failures too, use separate success and failure URLs:

```yaml
hooks:
  after: "curl -fsS -m 10 --retry 5 https://hc-ping.com/your-uuid-here"
  failed: "curl -fsS -m 10 --retry 5 https://hc-ping.com/your-uuid-here/fail"
```


### ntfy

[ntfy](https://ntfy.sh/) sends push notifications to your phone. Useful for immediate failure alerts.

```yaml
hooks:
  failed: >
    curl -fsS -m 10
    -H "Title: Backup failed"
    -H "Priority: high"
    -H "Tags: warning"
    -d "vger backup failed on $(hostname)"
    https://ntfy.sh/my-backup-alerts
```


### Uptime Kuma

[Uptime Kuma](https://github.com/louislam/uptime-kuma) is a self-hosted monitoring tool. Use a push monitor to track backup runs.

```yaml
hooks:
  after: "curl -fsS -m 10 http://your-kuma-instance:3001/api/push/your-token?status=up"
```


### Generic webhook

Any service that accepts HTTP requests works the same way.

```yaml
hooks:
  after: >
    curl -fsS -m 10 -X POST
    -H "Content-Type: application/json"
    -d '{"text": "Backup completed on $(hostname)"}'
    https://hooks.slack.com/services/your/webhook/url
```
