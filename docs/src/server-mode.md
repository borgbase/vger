# Server Mode

V'Ger includes a dedicated backup server for secure, policy-enforced remote backups. TLS is handled by a reverse proxy (nginx, caddy, and similar tools).

## Why a dedicated REST server instead of plain S3

Dumb storage backends (S3, WebDAV, SFTP) work well for basic backups, but they cannot enforce policy or do server-side work. `vger-server` adds capabilities that object storage alone cannot provide.

| Capability | S3 / dumb storage | vger-server |
|------------|-------------------|-------------|
| Append-only mode | Not enforceable; a compromised client with S3 credentials can delete anything | Rejects delete and pack overwrite operations |
| Server-side compaction | Client must download and re-upload all live blobs | Server repacks locally on disk from a compact plan |
| Quota enforcement | Requires external bucket policy/IAM setup | Built-in per-repo byte quota checks on writes |
| Backup freshness monitoring | Requires external polling and parsing | Tracks `last_backup_at` on manifest writes |
| Lock auto-expiry | Advisory locks can remain after crashes | TTL-based lock cleanup in the server |
| Structural health checks | Client has to fetch data to verify structure | Server validates repository shape directly |

All data remains client-side encrypted. The server never has the encryption key and cannot read backup contents.

## Build the server

```bash
cargo build --release -p vger-server
# Binary at target/release/vger-server
```

## Build the client with REST support

```bash
cargo build --release -p vger-cli --features vger-core/backend-rest
```

## Server configuration

Create `vger-server.toml`:

```toml
[server]
listen = "127.0.0.1:8484"
data_dir = "/var/lib/vger"
token = "some-secret-token"
append_only = false              # true = reject all deletes
log_format = "pretty"           # "json" for structured logging

# Optional limits
# quota_bytes = 5368709120       # 5 GiB per-repo quota. 0 = unlimited.
# lock_ttl_seconds = 3600        # auto-expire locks after 1 hour (default)
```

## Start the server

```bash
vger-server --config vger-server.toml
```

## Client configuration (REST backend)

```yaml
repositories:
  - url: "https://backup.example.com/myrepo"
    label: "server"
    rest_token: "some-secret-token"

encryption:
  mode: "aes256gcm"

sources:
  - "/home/user/documents"
```

All standard commands (`init`, `backup`, `list`, `info`, `restore`, `delete`, `prune`, `check`, `compact`) work over REST without CLI workflow changes.

## Health check

```bash
# No auth required
curl http://localhost:8484/health
```

Returns server status, uptime, disk free space, and repository count.
