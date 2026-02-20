# Storage Backends

The repository URL in your config determines which backend is used. S3 storage is implemented via [Apache OpenDAL](https://opendal.apache.org/), while SFTP uses a native [russh](https://github.com/Eugeny/russh) implementation. OpenDAL could be used to add more backends in the future.

| Backend | URL example |
|---------|-------------|
| Local filesystem | `/backups/repo` |
| S3 / S3-compatible | `s3://bucket/prefix` |
| SFTP | `sftp://host/path` |
| REST (vger-server) | `https://host/repo` |

## Transport security

HTTP transport is blocked by default for remote backends.

- `https://...` is accepted by default.
- `http://...` requires explicit opt-in with `allow_insecure_http: true`.

```yaml
repositories:
  - url: "http://localhost:8484/myrepo"  # or endpoint: "http://minio.local:9000"
    label: "dev-only"
    allow_insecure_http: true
```

Use plaintext HTTP only on trusted local/dev networks.

## Local filesystem

Store backups on a local or mounted disk. No extra configuration needed.

```yaml
repositories:
  - url: "/backups/repo"
    label: "local"
```

Accepted URL formats: absolute paths (`/backups/repo`), relative paths (`./repo`), or `file:///backups/repo`.

## S3 / S3-compatible

Store backups in Amazon S3 or any S3-compatible service (MinIO, Wasabi, Backblaze B2, etc.).

**AWS S3:**

```yaml
repositories:
  - url: "s3://my-bucket/vger"
    label: "s3"
    region: "us-east-1"                    # Default if omitted
    # access_key_id: "AKIA..."            # Optional; uses AWS SDK defaults if omitted
    # secret_access_key: "..."
```

**S3-compatible (custom endpoint):**

When the URL host contains a dot or a port, it's treated as a custom endpoint and the first path segment is the bucket:

```yaml
repositories:
  - url: "s3://minio.local:9000/my-bucket/vger"
    label: "minio"
    region: "us-east-1"
    access_key_id: "minioadmin"
    secret_access_key: "minioadmin"
```

### S3 configuration options

| Field | Description |
|-------|-------------|
| `region` | AWS region (default: `us-east-1`) |
| `access_key_id` | AWS access key (falls back to AWS SDK defaults) |
| `secret_access_key` | AWS secret key |
| `endpoint` | Override the endpoint derived from the URL |
| `allow_insecure_http` | Permit `http://` repository URL or endpoint (unsafe; default: `false`) |

## SFTP

Store backups on a remote server via SFTP. Uses a native [russh](https://github.com/Eugeny/russh) implementation (pure Rust SSH/SFTP) — no system `ssh` binary required. Works on all platforms including Windows.

Host keys are verified with an OpenSSH `known_hosts` file. Unknown hosts use TOFU (trust-on-first-use): the first key is stored, and later key changes fail connection.

```yaml
repositories:
  - url: "sftp://backup@nas.local/backups/vger"
    label: "nas"
    # sftp_key: "/home/user/.ssh/id_rsa"  # Path to private key (optional)
    # sftp_known_hosts: "/home/user/.ssh/known_hosts"  # Optional known_hosts path
    # sftp_max_connections: 4  # Optional concurrency limit (1..=32)
```

URL format: `sftp://[user@]host[:port]/path`. Default port is 22.

### SFTP configuration options

| Field | Description |
|-------|-------------|
| `sftp_key` | Path to SSH private key (auto-detects `~/.ssh/id_ed25519`, `id_rsa`, `id_ecdsa`) |
| `sftp_known_hosts` | Path to OpenSSH `known_hosts` file (default: `~/.ssh/known_hosts`) |
| `sftp_max_connections` | Max concurrent SFTP connections (default: `4`, clamped to `1..=32`) |

## REST (vger-server)

Store backups on a dedicated [vger-server](server-setup.md) instance via HTTP/HTTPS. The server provides append-only enforcement, quotas, lock management, and server-side compaction.

```yaml
repositories:
  - url: "https://backup.example.com/myrepo"
    label: "server"
    rest_token: "my-secret-token"          # Bearer token for authentication
```

### REST configuration options

| Field | Description |
|-------|-------------|
| `rest_token` | Bearer token sent as `Authorization: Bearer <token>` |
| `allow_insecure_http` | Permit `http://` REST URLs (unsafe; default: `false`) |

See [Server Setup](server-setup.md) for how to set up and configure the server.

All backends are included in the default build and in pre-built binaries from the [releases page](https://github.com/borgbase/vger/releases).
