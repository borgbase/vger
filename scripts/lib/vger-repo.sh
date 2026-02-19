#!/usr/bin/env bash
# scripts/lib/vger-repo.sh — vger repository lifecycle helpers
#
# Requires common.sh and defaults.sh to be sourced first.

[[ -n "${_VGER_REPO_LOADED:-}" ]] && return 0
_VGER_REPO_LOADED=1

# Delete a vger repo. Falls back to filesystem cleanup for REST repos on localhost.
#   vger_repo_delete <vger_bin> <config_path> <repo_label> [repo_url]
vger_repo_delete() {
  local vger_bin="$1" config="$2" label="$3" repo_url="${4:-}"

  if "$vger_bin" --config "$config" delete -R "$label" --yes-delete-this-repo 2>/dev/null; then
    return 0
  fi

  # Fallback: wipe REST repo dir on localhost
  if [[ -n "$repo_url" && "$repo_url" == http://127.0.0.1:*/* || "$repo_url" == http://localhost:*/* ]]; then
    local without_scheme="${repo_url#*://}"
    local path="${without_scheme#*/}"
    local repo_name="${path%%/*}"
    if [[ -n "$repo_name" && -d "$REST_DATA_DIR/$repo_name" ]]; then
      rm -rf "$REST_DATA_DIR/$repo_name"
      log "Force-reset REST repo via filesystem: $REST_DATA_DIR/$repo_name"
      return 0
    fi
  fi

  log "Repository delete skipped (not present or not initialized)"
}

# Initialize a vger repo.
#   vger_repo_init <vger_bin> <config_path> <repo_label>
vger_repo_init() {
  local vger_bin="$1" config="$2" label="$3"
  "$vger_bin" --config "$config" init -R "$label"
}

# Run a seed backup (untimed, for setup purposes).
#   vger_seed_backup <vger_bin> <config_path> <repo_label> <snapshot_label> <source_path>
vger_seed_backup() {
  local vger_bin="$1" config="$2" label="$3" snap_label="$4" src="$5"
  "$vger_bin" --config "$config" backup -R "$label" -l "$snap_label" "$src"
}

# Generate a vger YAML config file.
#   write_vger_config <out_path> <repo_label> <repo_url> <backend> <source_path> [source_label]
#
# Uses defaults from defaults.sh for credentials, encryption, compression.
write_vger_config() {
  local out="$1" label="$2" url="$3" backend="$4" src="$5" src_label="${6:-corpus}"

  local url_q src_q
  url_q="$(yaml_escape "$url")"
  src_q="$(yaml_escape "$src")"

  cat >"$out" <<CFG
repositories:
  - label: "$label"
    url: "$url_q"
CFG

  if [[ "$backend" == "rest" ]]; then
    cat >>"$out" <<CFG
    rest_token: "$(yaml_escape "$REST_TOKEN")"
CFG
  fi

  if [[ "$backend" == "s3" ]]; then
    cat >>"$out" <<CFG
    region: "$(yaml_escape "$S3_REGION")"
    access_key_id: "$(yaml_escape "$S3_ACCESS_KEY")"
    secret_access_key: "$(yaml_escape "$S3_SECRET_KEY")"
CFG
  fi

  cat >>"$out" <<CFG
encryption:
  mode: auto
  passphrase: "stress-test"
compression:
  algorithm: zstd
  zstd_level: 3
retention:
  keep_last: 1
git_ignore: false
xattrs:
  enabled: false
sources:
  - path: "$src_q"
    label: $src_label
CFG
}

# Reset MinIO service and data dir, then wait for health.
#   reset_minio() — uses globals from defaults.sh
reset_minio() {
  need systemctl
  need curl

  log "Resetting MinIO service '$MINIO_SERVICE' and data dir '$MINIO_DATA_DIR'"
  systemctl --user stop "$MINIO_SERVICE"
  rm -rf "$MINIO_DATA_DIR"
  mkdir -p "$MINIO_DATA_DIR"
  systemctl --user start "$MINIO_SERVICE"

  local attempt=0
  until curl -fsS "$MINIO_HEALTH_URL" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if (( attempt >= 30 )); then
      die "MinIO did not become healthy at $MINIO_HEALTH_URL after reset"
    fi
    sleep 1
  done
  log "MinIO reset complete"
}

# Ensure an S3 bucket exists via rclone (for MinIO endpoints).
#   ensure_s3_bucket <repo_url>
ensure_s3_bucket() {
  local url="$1"
  local without_scheme="${url#s3://}"
  local host_and_path="${without_scheme%%\?*}"
  local host="${host_and_path%%/*}"
  local path="${host_and_path#*/}"
  local bucket=""

  if [[ "$host" == "$host_and_path" ]]; then
    return 0
  fi

  if [[ "$host" == *.* || "$host" == *:* ]]; then
    bucket="${path%%/*}"
  else
    return 0
  fi

  [[ -n "$bucket" ]] || die "unable to parse S3 bucket from URL: $url"
  need rclone

  local rclone_env=(
    RCLONE_CONFIG_VGERSTRESS_TYPE=s3
    RCLONE_CONFIG_VGERSTRESS_PROVIDER=Minio
    RCLONE_CONFIG_VGERSTRESS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
    RCLONE_CONFIG_VGERSTRESS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
    RCLONE_CONFIG_VGERSTRESS_REGION="$S3_REGION"
    RCLONE_CONFIG_VGERSTRESS_ENDPOINT="http://$host"
  )

  if env "${rclone_env[@]}" rclone lsd "vgerstress:$bucket" >/dev/null 2>&1; then
    return 0
  fi
  env "${rclone_env[@]}" rclone mkdir "vgerstress:$bucket" >/dev/null
  log "Ensured S3 bucket exists: $bucket (endpoint $host)"
}
