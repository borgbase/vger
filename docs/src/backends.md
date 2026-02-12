# Storage Backends

V'Ger uses [Apache OpenDAL](https://opendal.apache.org/) for storage abstraction. The repository URL in your config determines which backend is used.

| Backend | URL example | Feature flag |
|---------|-------------|--------------|
| Local filesystem | `/backups/repo` | — (always available) |
| S3 / S3-compatible | `s3://bucket/prefix` | — (always available) |
| SFTP | `sftp://host/path` | `backend-sftp` |
| REST (vger-server) | `https://host/repo` | `backend-rest` |

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

## SFTP

Store backups on a remote server via SFTP.

> Requires building with the `backend-sftp` feature flag (see [Building with optional backends](#building-with-optional-backends) below).

```yaml
repositories:
  - url: "sftp://backup@nas.local/backups/vger"
    label: "nas"
    # sftp_key: "/home/user/.ssh/id_rsa"  # Path to private key (optional)
```

URL format: `sftp://[user@]host[:port]/path`. Default port is 22.

### SFTP configuration options

| Field | Description |
|-------|-------------|
| `sftp_key` | Path to SSH private key (defaults to `~/.ssh/id_rsa`) |

## REST (vger-server)

Store backups on a dedicated [vger-server](server-mode.md) instance via HTTP/HTTPS. The server provides append-only enforcement, quotas, lock management, and server-side compaction.

> Requires building with the `backend-rest` feature flag (see [Building with optional backends](#building-with-optional-backends) below).

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

See [Server Mode](server-mode.md) for how to set up and configure the server.

## Building with optional backends

Local and S3 backends are always available. SFTP and REST require feature flags at build time:

```bash
# All backends
cargo build --release --features backend-sftp,backend-rest

# Just SFTP
cargo build --release --features backend-sftp

# Just REST
cargo build --release --features backend-rest
```

Pre-built binaries from the [releases page](https://github.com/borgbase/vger/releases) include all backends.
