# Setup

V'Ger includes a dedicated backup server for secure, policy-enforced remote backups. TLS is handled by a reverse proxy (nginx, caddy, and similar tools).

## Why a dedicated REST server instead of plain S3

Dumb storage backends (S3, WebDAV, SFTP) work well for basic backups, but they cannot enforce policy or do server-side work. `vger-server` adds capabilities that object storage alone cannot provide.

| Capability | S3 / dumb storage | vger-server |
|------------|-------------------|-------------|
| Append-only mode | Not enforceable; a compromised client with S3 credentials can delete anything | Rejects delete and pack overwrite operations |
| Server-side compaction | Client must download and re-upload all live blobs | Server repacks locally on disk from a compact plan |
| Quota enforcement | Requires external bucket policy/IAM setup | Built-in byte quota checks on writes |
| Backup freshness monitoring | Requires external polling and parsing | Tracks `last_backup_at` on manifest writes |
| Lock auto-expiry | Advisory locks can remain after crashes | TTL-based lock cleanup in the server |
| Upload integrity | Relies on S3 Content-MD5 | Uses existing BLAKE2b checksum |
| Structural health checks | Client has to fetch data to verify structure | Server validates repository shape directly |

All data remains client-side encrypted. The server never has the encryption key and cannot read backup contents.

## Install

Download a binary for your platform from the [releases page](https://github.com/borgbase/vger/releases).

## Server configuration

All settings are passed as CLI flags. The authentication token is read from the `VGER_TOKEN` environment variable (kept out of process arguments to avoid exposure in `ps` output).

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `-l, --listen` | `localhost:8585` | Address to listen on |
| `-d, --data-dir` | `/var/lib/vger` | Root directory for the repository |
| `--append-only` | `false` | Reject DELETE and overwrite operations on pack files |
| `--log-format` | `pretty` | Log output format: `json` or `pretty` |
| `--quota` | `0` | Storage quota (`500M`, `10G`, plain bytes). 0 = unlimited |
| `--lock-ttl-seconds` | `3600` | Auto-expire locks after this many seconds |
| `--network-threads` | `4` | Async threads for handling network connections |
| `--io-threads` | `6` | Threads for blocking disk I/O (reads, writes, hashing) |

### Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `VGER_TOKEN` | Yes | Shared bearer token for authentication |

## Start the server

```bash
export VGER_TOKEN="some-secret-token"
vger-server --data-dir /var/lib/vger
```

## Run as a systemd service

Create an environment file at `/etc/vger/vger-server.env` with restricted permissions:

```bash
sudo mkdir -p /etc/vger
echo 'VGER_TOKEN=some-secret-token' | sudo tee /etc/vger/vger-server.env
sudo chmod 600 /etc/vger/vger-server.env
sudo chown vger:vger /etc/vger/vger-server.env
```

Create `/etc/systemd/system/vger-server.service`:

```ini
[Unit]
Description=V'Ger backup REST server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=vger
Group=vger
EnvironmentFile=/etc/vger/vger-server.env
ExecStart=/usr/local/bin/vger-server --data-dir /var/lib/vger
Restart=on-failure
RestartSec=2
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/vger

[Install]
WantedBy=multi-user.target
```

Then reload and enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vger-server.service
sudo systemctl status vger-server.service
```

## Reverse proxy

`vger-server` listens on HTTP and expects a reverse proxy to handle TLS. Pack uploads can be up to 512 MiB, so the proxy must allow large request bodies.

### Nginx

```nginx
server {
    listen 443 ssl http2;
    server_name backup.example.com;

    ssl_certificate     /etc/letsencrypt/live/backup.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/backup.example.com/privkey.pem;

    client_max_body_size    600m;              # vger-server allows up to 512 MiB
    proxy_request_buffering off;               # stream uploads directly to backend

    location / {
        proxy_pass http://127.0.0.1:8585;
    }
}
```

### Caddy

```caddyfile
backup.example.com {
    request_body {
        max_size 600MB
    }
    reverse_proxy 127.0.0.1:8585
}
```

## Client configuration (REST backend)

```yaml
repositories:
  - label: "server"
    url: "https://backup.example.com"
    access_token: "some-secret-token"

encryption:
  mode: "auto"

sources:
  - "/home/user/documents"
```

All standard commands (`init`, `backup`, `list`, `info`, `restore`, `delete`, `prune`, `check`, `compact`) work over REST without CLI workflow changes.

## Health check

```bash
# No auth required
curl http://localhost:8585/health
```

Returns `{"status": "ok", "version": "..."}`. No authentication required.
