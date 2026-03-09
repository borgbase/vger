# Installing

## Quick install

```bash
curl -fsSL https://vykar.borgbase.com/install.sh | sh
```

Or download the latest release for your platform from the [releases page](https://github.com/borgbase/vykar/releases).


## Docker

Available as `ghcr.io/borgbase/vykar` on GitHub Container Registry.

### Config file

Create a `vykar.yaml` for Docker. Source paths must reference `/data/...` (the container mount point):

    repositories:
      - url: s3://my-bucket/backups
        access_key_id: "..."
        secret_access_key: "..."

    sources:
      - /data/documents
      - /data/photos

    encryption:
      passphrase: "change-me"

    retention:
      keep_daily: 7
      keep_weekly: 4

    schedule:
      enabled: true
      every: "24h"
      on_startup: true

For a local repository backend, use `/repo` as the repo path and mount a host directory there.

### Run as daemon

    docker run -d \
      --name vykar-daemon \
      --hostname my-server \
      -v /path/to/vykar.yaml:/etc/vykar/config.yaml:ro \
      -v /home/user/documents:/data/documents:ro \
      -v /home/user/photos:/data/photos:ro \
      -v vykar-cache:/cache \
      ghcr.io/borgbase/vykar

### Run ad-hoc commands

With a new container (uses the entrypoint, no need to repeat `vykar`):

    docker run --rm \
      -v /path/to/vykar.yaml:/etc/vykar/config.yaml:ro \
      -v vykar-cache:/cache \
      ghcr.io/borgbase/vykar list

Or exec into a running daemon container:

    docker exec vykar-daemon vykar list

### Docker Compose

    services:
      vykar:
        image: ghcr.io/borgbase/vykar:latest
        hostname: my-server
        restart: unless-stopped
        environment:
          - VYKAR_PASSPHRASE
          - TZ=UTC
        volumes:
          - ./vykar.yaml:/etc/vykar/config.yaml:ro
          - /home/user/documents:/data/documents:ro
          - vykar-cache:/cache
    volumes:
      vykar-cache:

### Reloading configuration

Send `SIGHUP` to the daemon container to reload the config file without restarting:

    docker kill --signal=HUP vykar-daemon

With Docker Compose:

    docker compose kill -s HUP vykar

The daemon logs whether the reload succeeded or was rejected (invalid config).

### Notes
- Use `-it` with `docker run` for interactive commands to get progress bar output (e.g. `docker run --rm -it ...`)
- Set `--hostname` to a stable name — Docker assigns random hostnames that appear in snapshot metadata
- Mount source directories under `/data/` and reference them as `/data/...` in the config
- For encryption, use `VYKAR_PASSPHRASE` env var or Docker secrets via `passcommand: "cat /run/secrets/vykar_passphrase"`
- Use a named volume for `/cache` to persist the snapshot cache across restarts
- Available for `linux/amd64` and `linux/arm64`


## Pre-built binaries

Extract the archive and place the `vykar` binary somewhere on your `PATH`:

```bash
# Example for Linux/macOS
tar xzf vykar-*.tar.gz
sudo cp vykar /usr/local/bin/
```

For Windows CLI releases:

```powershell
Expand-Archive vykar-*.zip -DestinationPath .
Move-Item .\vykar.exe "$env:USERPROFILE\\bin\\vykar.exe"
```

Add your chosen directory (for example, `%USERPROFILE%\bin`) to `PATH` if needed.


## Build from source

Requires Rust 1.88 or later.

```bash
git clone https://github.com/borgbase/vykar.git
cd vykar
cargo build --release
```

The binary is at `target/release/vykar`. Copy it to a directory on your `PATH`:

```bash
cp target/release/vykar /usr/local/bin/
```

## Verify installation

```bash
vykar --version
```

## Next steps

- [Initialize and Set Up a Repository](init-setup.md)
