#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Run an autonomous vger stress test against a corpus dataset.
Default setup:
  vger binary:   discovered on PATH (or set --vger-bin)
  corpus:        \$HOME/corpus-local (or set --corpus-dir)
  backend:       local (or set --backend local|rest|s3)
  repo label:    stress-<backend> (or set --repo-label)
  rest url:      http://127.0.0.1:8484/<repo-label> (if --backend rest)
  runtime:       \$HOME/runtime/stress/<backend> (or set STRESS_ROOT)

The script creates repository/restore working directories while running, then
deletes them (plus temp artifacts) on success. On failure, artifacts are
preserved for debugging.

Options:
  --iterations N         Loop count (default: 1000)
  --check-every N        Run 'check' every N iterations; 0 disables (default: 50)
  --verify-data-every N  Run 'check --verify-data' every N iterations; 0 disables (default: 0)
  --vger-bin PATH        vger binary path (default: vger from PATH)
  --corpus-dir PATH      corpus path (default: \$HOME/corpus-local)
  --backend NAME         storage backend: local|rest|s3 (default: local)
  --repo-label LABEL     repository label (default: stress-<backend>)
  --repo-url URL         repository URL override (backend-specific default if omitted)
  --rest-token TOKEN     REST bearer token (default: \$VGER_REST_TOKEN or vger-e2e-local-token)
  --s3-region REGION     S3 region (default: us-east-1)
  --s3-access-key ID     S3 access key id (default: \$AWS_ACCESS_KEY_ID or minioadmin)
  --s3-secret-key KEY    S3 secret access key (default: \$AWS_SECRET_ACCESS_KEY or minioadmin)
  --drop-caches          Attempt to drop OS file caches before backup and restore (default: off)
  --no-drop-caches       Disable cache drop (overrides STRESS_DROP_CACHES=1)
  --time-v               Capture /usr/bin/time -v per vger step into logs/*.timev (default: off)
  --no-time-v            Disable per-step /usr/bin/time -v capture
  --help                 Show help
USAGE
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

abs_path() {
  local p="$1"
  if [[ "$p" = /* ]]; then
    printf '%s\n' "$p"
  else
    printf '%s/%s\n' "$PWD" "$p"
  fi
}

yaml_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

ITERATIONS=1000
CHECK_EVERY=50
VERIFY_DATA_EVERY=0

VGER_BIN="$(command -v vger || true)"
CORPUS_DIR="$HOME/corpus-local"
BACKEND="local"
REPO_LABEL=""
REPO_URL=""
REST_TOKEN="${VGER_REST_TOKEN:-vger-e2e-local-token}"
REST_DATA_DIR="${STRESS_REST_DATA_DIR:-/mnt/repos/bench-vger/vger-server-data}"
S3_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
S3_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
S3_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
S3_AUTOCREATE_BUCKET="${STRESS_S3_AUTOCREATE_BUCKET:-1}"
DROP_CACHES="${STRESS_DROP_CACHES:-0}"
TIME_V="${STRESS_TIME_V:-0}"
MINIO_SERVICE="${STRESS_MINIO_SERVICE:-minio.service}"
MINIO_DATA_DIR="${STRESS_MINIO_DATA_DIR:-/mnt/repos/bench-vger/minio-data}"
MINIO_HEALTH_URL="${STRESS_MINIO_HEALTH_URL:-http://127.0.0.1:9000/minio/health/live}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iterations)
      ITERATIONS="${2:-}"
      shift 2
      ;;
    --check-every)
      CHECK_EVERY="${2:-}"
      shift 2
      ;;
    --verify-data-every)
      VERIFY_DATA_EVERY="${2:-}"
      shift 2
      ;;
    --vger-bin)
      VGER_BIN="${2:-}"
      shift 2
      ;;
    --corpus-dir)
      CORPUS_DIR="${2:-}"
      shift 2
      ;;
    --backend)
      BACKEND="${2:-}"
      shift 2
      ;;
    --repo-label)
      REPO_LABEL="${2:-}"
      shift 2
      ;;
    --repo-url)
      REPO_URL="${2:-}"
      shift 2
      ;;
    --rest-token)
      REST_TOKEN="${2:-}"
      shift 2
      ;;
    --s3-region)
      S3_REGION="${2:-}"
      shift 2
      ;;
    --s3-access-key)
      S3_ACCESS_KEY_ID="${2:-}"
      shift 2
      ;;
    --s3-secret-key)
      S3_SECRET_ACCESS_KEY="${2:-}"
      shift 2
      ;;
    --drop-caches)
      DROP_CACHES="1"
      shift
      ;;
    --no-drop-caches)
      DROP_CACHES="0"
      shift
      ;;
    --time-v)
      TIME_V="1"
      shift
      ;;
    --no-time-v)
      TIME_V="0"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

[[ "$ITERATIONS" =~ ^[0-9]+$ ]] || die "--iterations must be a non-negative integer"
[[ "$CHECK_EVERY" =~ ^[0-9]+$ ]] || die "--check-every must be a non-negative integer"
[[ "$VERIFY_DATA_EVERY" =~ ^[0-9]+$ ]] || die "--verify-data-every must be a non-negative integer"
[[ -n "$VGER_BIN" ]] || die "vger binary not found on PATH (or set --vger-bin)"
[[ "$BACKEND" =~ ^(local|rest|s3)$ ]] || die "--backend must be one of: local, rest, s3"
[[ "$DROP_CACHES" =~ ^(0|1)$ ]] || die "STRESS_DROP_CACHES must be 0 or 1"
[[ "$TIME_V" =~ ^(0|1)$ ]] || die "STRESS_TIME_V must be 0 or 1"

TIME_CMD="/usr/bin/time"
if [[ "$TIME_V" == "1" ]]; then
  [[ -x "$TIME_CMD" ]] || die "/usr/bin/time is required when --time-v is enabled"
fi

if [[ -z "$REPO_LABEL" ]]; then
  REPO_LABEL="stress-$BACKEND"
fi

VGER_BIN="$(abs_path "$VGER_BIN")"
CORPUS_DIR="$(abs_path "$CORPUS_DIR")"

STRESS_ROOT="${STRESS_ROOT:-$HOME/runtime/stress/$BACKEND}"
WORK_DIR="$STRESS_ROOT/work"
REPO_DIR="$WORK_DIR/repository"
RESTORE_DIR="$WORK_DIR/restore"
RUNTIME_DIR="$WORK_DIR/runtime"
CONFIG_PATH="$WORK_DIR/vger.stress.yaml"
LOG_DIR="$WORK_DIR/logs"
HOME_DIR="$RUNTIME_DIR/home"
XDG_CACHE_DIR="$RUNTIME_DIR/xdg-cache"

RUN_OK=0
CURRENT_ITER=0
CURRENT_STEP="startup"
CURRENT_SNAPSHOT=""
LAST_BACKUP_LOG=""
LAST_INIT_LOG=""
LAST_RESET_LOG=""
LAST_LIST_LOG=""
LAST_RESTORE_LOG=""
LAST_DELETE_LOG=""
LAST_COMPACT_LOG=""
LAST_PRUNE_LOG=""
LAST_BREAK_LOCK_LOG=""
LAST_CHECK_LOG=""
LAST_VERIFY_CHECK_LOG=""
LAST_VERIFY_DIFF=""
REPO_URL_RESOLVED=""

resolve_repo_url() {
  if [[ -n "$REPO_URL" ]]; then
    REPO_URL_RESOLVED="$REPO_URL"
    return
  fi

  case "$BACKEND" in
    local)
      REPO_URL_RESOLVED="$REPO_DIR"
      ;;
    rest)
      REPO_URL_RESOLVED="http://127.0.0.1:8484/$REPO_LABEL"
      ;;
    s3)
      REPO_URL_RESOLVED="s3://127.0.0.1:9000/vger-stress/$REPO_LABEL"
      ;;
  esac
}

rest_repo_name_from_url() {
  local url="$1"
  local without_scheme host_and_path path repo

  without_scheme="${url#*://}"
  host_and_path="${without_scheme%%\?*}"
  path="${host_and_path#*/}"
  repo="${path%%/*}"
  printf '%s\n' "$repo"
}

maybe_force_reset_rest_repo() {
  local repo_name repo_path

  [[ "$BACKEND" == "rest" ]] || return 1
  [[ "$REPO_URL_RESOLVED" == http://127.0.0.1:*/* || "$REPO_URL_RESOLVED" == http://localhost:*/* ]] || return 1
  repo_name="$(rest_repo_name_from_url "$REPO_URL_RESOLVED")"
  [[ -n "$repo_name" && "$repo_name" != "$REPO_URL_RESOLVED" ]] || return 1

  repo_path="$REST_DATA_DIR/$repo_name"
  if [[ -d "$repo_path" ]]; then
    rm -rf "$repo_path"
    log "Force-reset REST repo via filesystem: $repo_path"
  fi
  return 0
}

s3_parse_url() {
  local url="$1"
  local without_scheme host_and_path host path

  S3_PARSED_HOST=""
  S3_PARSED_BUCKET=""
  S3_PARSED_PREFIX=""

  without_scheme="${url#s3://}"
  host_and_path="${without_scheme%%\?*}"
  host="${host_and_path%%/*}"
  path="${host_and_path#*/}"

  if [[ "$host" == "$host_and_path" ]]; then
    S3_PARSED_HOST="$host"
    S3_PARSED_BUCKET="$host"
    return 0
  fi

  if [[ "$host" == *.* || "$host" == *:* ]]; then
    S3_PARSED_HOST="$host"
    S3_PARSED_BUCKET="${path%%/*}"
    if [[ "$path" == */* ]]; then
      S3_PARSED_PREFIX="${path#*/}"
    fi
  else
    S3_PARSED_HOST="$host"
    S3_PARSED_BUCKET="$host"
    S3_PARSED_PREFIX="$path"
  fi
}

rclone_s3() {
  local cmd="$1"
  local target="$2"
  HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
    RCLONE_CONFIG_VGERSTRESS_TYPE=s3 \
    RCLONE_CONFIG_VGERSTRESS_PROVIDER=Minio \
    RCLONE_CONFIG_VGERSTRESS_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID" \
    RCLONE_CONFIG_VGERSTRESS_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY" \
    RCLONE_CONFIG_VGERSTRESS_REGION="$S3_REGION" \
    RCLONE_CONFIG_VGERSTRESS_ENDPOINT="http://$S3_PARSED_HOST" \
    rclone "$cmd" "$target"
}

ensure_s3_bucket() {
  [[ "$BACKEND" == "s3" ]] || return 0
  [[ "$S3_AUTOCREATE_BUCKET" == "1" ]] || return 0

  s3_parse_url "$REPO_URL_RESOLVED"
  [[ -n "$S3_PARSED_BUCKET" ]] || die "unable to parse S3 bucket from URL: $REPO_URL_RESOLVED"
  [[ "$S3_PARSED_HOST" == *:* || "$S3_PARSED_HOST" == *.* ]] || return 0

  command -v rclone >/dev/null 2>&1 || die "rclone is required for S3 bucket bootstrap on custom endpoint"
  if rclone_s3 lsd "vgerstress:$S3_PARSED_BUCKET" >/dev/null 2>&1; then
    return 0
  fi
  rclone_s3 mkdir "vgerstress:$S3_PARSED_BUCKET" >/dev/null
  log "Ensured S3 bucket exists: $S3_PARSED_BUCKET (endpoint $S3_PARSED_HOST)"
}

reset_minio_for_s3() {
  [[ "$BACKEND" == "s3" ]] || return 0

  command -v systemctl >/dev/null 2>&1 || die "systemctl is required for S3 MinIO reset"
  command -v curl >/dev/null 2>&1 || die "curl is required for S3 MinIO reset"

  log "Resetting MinIO service '$MINIO_SERVICE' and data dir '$MINIO_DATA_DIR'"
  systemctl --user stop "$MINIO_SERVICE"
  rm -rf "$MINIO_DATA_DIR"
  mkdir -p "$MINIO_DATA_DIR"
  systemctl --user start "$MINIO_SERVICE"

  local attempt=0
  local max_attempts=30
  until curl -fsS "$MINIO_HEALTH_URL" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if (( attempt >= max_attempts )); then
      die "MinIO did not become healthy at $MINIO_HEALTH_URL after reset"
    fi
    sleep 1
  done
  log "MinIO reset complete"
}

drop_os_caches() {
  [[ "$DROP_CACHES" == "1" ]] || return 0

  sync || true
  if sudo -n true 2>/dev/null; then
    if ! echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null 2>&1; then
      log "drop caches requested but write to /proc/sys/vm/drop_caches failed"
    fi
  else
    log "drop caches requested but passwordless sudo is unavailable; skipping"
  fi
}

cleanup() {
  if [[ "$BACKEND" == "local" ]]; then
    rm -rf "$REPO_DIR"
  fi
  rm -rf "$RESTORE_DIR" "$RUNTIME_DIR" "$CONFIG_PATH"
  if [[ "$TIME_V" == "1" ]]; then
    if [[ -d "$LOG_DIR" ]]; then
      log "Preserving logs (--time-v enabled): $LOG_DIR"
    fi
  else
    rm -rf "$LOG_DIR"
  fi
}

print_failure_context() {
  local exit_code="$1"

  printf 'Failure context:\n' >&2
  printf '  exit_code:           %s\n' "$exit_code" >&2
  printf '  iteration:           %s\n' "$CURRENT_ITER" >&2
  printf '  step:                %s\n' "$CURRENT_STEP" >&2
  printf '  snapshot:            %s\n' "${CURRENT_SNAPSHOT:-<none>}" >&2
  printf '  repository_url:      %s\n' "$REPO_URL_RESOLVED" >&2
  [[ "$BACKEND" == "local" ]] && printf '  repository_dir:      %s\n' "$REPO_DIR" >&2
  printf '  restore_dir:         %s\n' "$RESTORE_DIR" >&2
  printf '  runtime_dir:         %s\n' "$RUNTIME_DIR" >&2
  [[ -n "$LAST_VERIFY_DIFF" ]] && printf '  verify_diff:         %s\n' "$LAST_VERIFY_DIFF" >&2
  [[ -n "$LAST_RESET_LOG" ]] && printf '  reset_log:           %s\n' "$LAST_RESET_LOG" >&2
  [[ -n "$LAST_BACKUP_LOG" ]] && printf '  backup_log:          %s\n' "$LAST_BACKUP_LOG" >&2
  [[ -n "$LAST_INIT_LOG" ]] && printf '  init_log:            %s\n' "$LAST_INIT_LOG" >&2
  [[ -n "$LAST_LIST_LOG" ]] && printf '  list_log:            %s\n' "$LAST_LIST_LOG" >&2
  [[ -n "$LAST_RESTORE_LOG" ]] && printf '  restore_log:         %s\n' "$LAST_RESTORE_LOG" >&2
  [[ -n "$LAST_DELETE_LOG" ]] && printf '  delete_log:          %s\n' "$LAST_DELETE_LOG" >&2
  [[ -n "$LAST_COMPACT_LOG" ]] && printf '  compact_log:         %s\n' "$LAST_COMPACT_LOG" >&2
  [[ -n "$LAST_PRUNE_LOG" ]] && printf '  prune_log:           %s\n' "$LAST_PRUNE_LOG" >&2
  [[ -n "$LAST_BREAK_LOCK_LOG" ]] && printf '  break_lock_log:      %s\n' "$LAST_BREAK_LOCK_LOG" >&2
  [[ -n "$LAST_CHECK_LOG" ]] && printf '  check_log:           %s\n' "$LAST_CHECK_LOG" >&2
  [[ -n "$LAST_VERIFY_CHECK_LOG" ]] && printf '  check_verify_log:    %s\n' "$LAST_VERIFY_CHECK_LOG" >&2
}

on_exit() {
  local exit_code="$?"

  if [[ "$RUN_OK" == "1" ]]; then
    cleanup
    return
  fi

  print_failure_context "$exit_code"
  log "Run failed; preserving artifacts for debugging"
}

trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Start from clean ephemeral state every run.
cleanup
mkdir -p "$RESTORE_DIR" "$LOG_DIR" "$HOME_DIR" "$XDG_CACHE_DIR"
if [[ "$BACKEND" == "local" ]]; then
  mkdir -p "$REPO_DIR"
fi

[[ -x "$VGER_BIN" ]] || die "vger binary not found or not executable: $VGER_BIN"
[[ -d "$CORPUS_DIR" ]] || die "corpus directory not found: $CORPUS_DIR"
find "$CORPUS_DIR" -mindepth 1 -print -quit | grep -q . || die "corpus is empty: $CORPUS_DIR"
resolve_repo_url
reset_minio_for_s3
ensure_s3_bucket

run_vger() {
  local iter="$1"
  local name="$2"
  shift 2

  local log_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-$name.log"
  local time_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-$name.timev"
  local rc=0

  if [[ "$TIME_V" == "1" ]]; then
    HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
      "$TIME_CMD" -v -o "$time_file" "$VGER_BIN" --config "$CONFIG_PATH" "$@" >"$log_file" 2>&1 || rc=$?
  else
    HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
      "$VGER_BIN" --config "$CONFIG_PATH" "$@" >"$log_file" 2>&1 || rc=$?
  fi

  if [[ "$rc" -ne 0 ]]; then
    printf 'FAILED iteration=%s step=%s snapshot=%s command=%s log=%s\n' \
      "$iter" "$name" "${CURRENT_SNAPSHOT:-<none>}" "$*" "$log_file" >&2
    if [[ "$TIME_V" == "1" ]]; then
      printf 'TIMEV iteration=%s step=%s file=%s\n' "$iter" "$name" "$time_file" >&2
    fi
    tail -n 120 "$log_file" >&2 || true
    return 1
  fi
  printf '%s\n' "$log_file"
}

write_config() {
  local repo_q corpus_q rest_token_q s3_region_q s3_access_key_q s3_secret_key_q
  repo_q="$(yaml_escape "$REPO_URL_RESOLVED")"
  corpus_q="$(yaml_escape "$CORPUS_DIR")"
  rest_token_q="$(yaml_escape "$REST_TOKEN")"
  s3_region_q="$(yaml_escape "$S3_REGION")"
  s3_access_key_q="$(yaml_escape "$S3_ACCESS_KEY_ID")"
  s3_secret_key_q="$(yaml_escape "$S3_SECRET_ACCESS_KEY")"

  cat >"$CONFIG_PATH" <<CFG
repositories:
  - label: "$REPO_LABEL"
    url: "$repo_q"
CFG

  if [[ "$BACKEND" == "rest" ]]; then
    cat >>"$CONFIG_PATH" <<CFG
    rest_token: "$rest_token_q"
CFG
  fi

  if [[ "$BACKEND" == "s3" ]]; then
    cat >>"$CONFIG_PATH" <<CFG
    region: "$s3_region_q"
    access_key_id: "$s3_access_key_q"
    secret_access_key: "$s3_secret_key_q"
CFG
  fi

  cat >>"$CONFIG_PATH" <<CFG
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
  - path: "$corpus_q"
    label: corpus
CFG
}

extract_snapshot_id() {
  local log_file="$1"
  awk '/^Snapshot created: / { id = $3 } END { if (id != "") print id; else exit 1 }' "$log_file"
}

check_locks_clear() {
  local locks_dir="$REPO_DIR/locks"
  [[ -d "$locks_dir" ]] || return 0

  local count
  count="$(find "$locks_dir" -type f -name '*.json' | wc -l | tr -d ' ')"
  [[ "$count" == "0" ]] || die "stale lock file(s) detected in $locks_dir"
}

verify_restore_matches() {
  local iter="$1"
  local restored="$2"
  local diff_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-verify.diff"
  LAST_VERIFY_DIFF="$diff_file"

  if diff -qr "$CORPUS_DIR" "$restored" >"$diff_file"; then
    return 0
  fi

  printf 'VERIFY MISMATCH iteration=%s snapshot=%s\n' "$iter" "${CURRENT_SNAPSHOT:-<none>}" >&2
  printf '  source_dir:       %s\n' "$CORPUS_DIR" >&2
  printf '  restored_dir:     %s\n' "$restored" >&2
  printf '  diff_file:        %s\n' "$diff_file" >&2

  printf '  diff preview (first 80 lines):\n' >&2
  sed -n '1,80p' "$diff_file" >&2 || true

  return 1
}

main() {
  write_config
  log "Stress backend=$BACKEND repo_url=$REPO_URL_RESOLVED"
  log "Deleting repository before init"
  CURRENT_STEP="delete-repo"
  LAST_RESET_LOG="$LOG_DIR/iter-000000-delete-repo.log"
  if ! HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
    "$VGER_BIN" --config "$CONFIG_PATH" delete -R "$REPO_LABEL" --yes-delete-this-repo >"$LAST_RESET_LOG" 2>&1; then
    if maybe_force_reset_rest_repo; then
      :
    else
      log "Repository delete skipped (not present or not initialized)"
    fi
  fi

  log "Initializing repository"
  CURRENT_STEP="init"
  LAST_INIT_LOG="$(run_vger 0 init init -R "$REPO_LABEL")"

  log "Starting stress run iterations=$ITERATIONS"

  local i=0
  local snapshot=""
  local backup_log=""

  local backups=0
  local lists=0
  local restores=0
  local deletes=0
  local compacts=0
  local prunes=0
  local break_locks=0
  local checks=0
  local verify_checks=0

  local start_ts
  start_ts="$(date +%s)"

  for (( i=1; i<=ITERATIONS; i++ )); do
    CURRENT_ITER="$i"
    CURRENT_SNAPSHOT=""

    if [[ "$DROP_CACHES" == "1" ]]; then
      log "[$i/$ITERATIONS] drop caches (pre-backup)"
      CURRENT_STEP="drop-caches-pre-backup"
      drop_os_caches
    fi

    if [[ "$BACKEND" == "rest" || "$BACKEND" == "s3" ]]; then
      log "[$i/$ITERATIONS] break-lock"
      CURRENT_STEP="break-lock"
      LAST_BREAK_LOCK_LOG="$(run_vger "$i" break-lock break-lock -R "$REPO_LABEL")"
      break_locks=$((break_locks + 1))
    fi

    log "[$i/$ITERATIONS] backup"
    CURRENT_STEP="backup"
    backup_log="$(run_vger "$i" backup backup -R "$REPO_LABEL")"
    LAST_BACKUP_LOG="$backup_log"
    snapshot="$(extract_snapshot_id "$backup_log")" || die "failed to parse snapshot ID"
    CURRENT_SNAPSHOT="$snapshot"
    backups=$((backups + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] list (snapshot $snapshot)"
    CURRENT_STEP="list"
    local list_log
    list_log="$(run_vger "$i" list list -R "$REPO_LABEL" --last 20)"
    LAST_LIST_LOG="$list_log"
    grep -q "$snapshot" "$list_log" || die "snapshot '$snapshot' missing from list output"
    lists=$((lists + 1))

    local restore_target="$RESTORE_DIR/current"
    rm -rf "$restore_target"
    mkdir -p "$restore_target"

    if [[ "$DROP_CACHES" == "1" ]]; then
      log "[$i/$ITERATIONS] drop caches (pre-restore)"
      CURRENT_STEP="drop-caches-pre-restore"
      drop_os_caches
    fi

    log "[$i/$ITERATIONS] restore"
    CURRENT_STEP="restore"
    LAST_RESTORE_LOG="$(run_vger "$i" restore restore -R "$REPO_LABEL" "$snapshot" "$restore_target")"

    log "[$i/$ITERATIONS] verify"
    CURRENT_STEP="verify"
    verify_restore_matches "$i" "$restore_target" || die "restore verification failed (iteration $i)"
    restores=$((restores + 1))

    log "[$i/$ITERATIONS] delete"
    CURRENT_STEP="delete"
    LAST_DELETE_LOG="$(run_vger "$i" delete snapshot -R "$REPO_LABEL" delete "$snapshot")"
    deletes=$((deletes + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] compact"
    CURRENT_STEP="compact"
    LAST_COMPACT_LOG="$(run_vger "$i" compact compact -R "$REPO_LABEL" --threshold 0)"
    compacts=$((compacts + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] prune"
    CURRENT_STEP="prune"
    LAST_PRUNE_LOG="$(run_vger "$i" prune prune -R "$REPO_LABEL")"
    prunes=$((prunes + 1))
    check_locks_clear

    if (( CHECK_EVERY > 0 && i % CHECK_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check"
      CURRENT_STEP="check"
      LAST_CHECK_LOG="$(run_vger "$i" check check -R "$REPO_LABEL")"
      checks=$((checks + 1))
    fi

    if (( VERIFY_DATA_EVERY > 0 && i % VERIFY_DATA_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check --verify-data"
      CURRENT_STEP="check-verify-data"
      LAST_VERIFY_CHECK_LOG="$(run_vger "$i" check-data check -R "$REPO_LABEL" --verify-data)"
      verify_checks=$((verify_checks + 1))
    fi

    local iter_elapsed=$(( $(date +%s) - start_ts ))
    log "[$i/$ITERATIONS] done (${iter_elapsed}s elapsed)"
  done

  local elapsed
  elapsed="$(( $(date +%s) - start_ts ))"

  log "Stress run complete"
  printf 'Summary:\n'
  printf '  iterations:           %s\n' "$ITERATIONS"
  printf '  backups:              %s\n' "$backups"
  printf '  lists:                %s\n' "$lists"
  printf '  restores:             %s\n' "$restores"
  printf '  deletes:              %s\n' "$deletes"
  printf '  compacts:             %s\n' "$compacts"
  printf '  prunes:               %s\n' "$prunes"
  printf '  break-lock:           %s\n' "$break_locks"
  printf '  check:                %s\n' "$checks"
  printf '  check --verify-data:  %s\n' "$verify_checks"
  printf '  elapsed_sec:          %s\n' "$elapsed"

  CURRENT_STEP="complete"
  RUN_OK=1
}

main
