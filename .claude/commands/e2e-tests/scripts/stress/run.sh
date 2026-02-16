#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Run an autonomous vger stress test against a local corpus dataset.
Default setup:
  vger binary:   discovered on PATH (or set --vger-bin)
  corpus:        \$HOME/corpus-local (or set --corpus-dir)
  runtime:       \$HOME/runtime/stress (or set STRESS_ROOT)

The script creates repository/restore working directories while running, then
deletes them (plus temp artifacts) on success. On failure, artifacts are
preserved for debugging.

Options:
  --iterations N         Loop count (default: 1000)
  --check-every N        Run 'check' every N iterations; 0 disables (default: 50)
  --verify-data-every N  Run 'check --verify-data' every N iterations; 0 disables (default: 0)
  --vger-bin PATH        vger binary path (default: vger from PATH)
  --corpus-dir PATH      corpus path (default: \$HOME/corpus-local)
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

VGER_BIN="$(abs_path "$VGER_BIN")"
CORPUS_DIR="$(abs_path "$CORPUS_DIR")"

STRESS_ROOT="${STRESS_ROOT:-$HOME/runtime/stress}"
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
LAST_LIST_LOG=""
LAST_RESTORE_LOG=""
LAST_DELETE_LOG=""
LAST_COMPACT_LOG=""
LAST_PRUNE_LOG=""
LAST_CHECK_LOG=""
LAST_VERIFY_CHECK_LOG=""
LAST_VERIFY_DIFF=""

cleanup() {
  rm -rf "$REPO_DIR" "$RESTORE_DIR" "$RUNTIME_DIR" "$LOG_DIR" "$CONFIG_PATH"
}

print_failure_context() {
  local exit_code="$1"

  printf 'Failure context:\n' >&2
  printf '  exit_code:           %s\n' "$exit_code" >&2
  printf '  iteration:           %s\n' "$CURRENT_ITER" >&2
  printf '  step:                %s\n' "$CURRENT_STEP" >&2
  printf '  snapshot:            %s\n' "${CURRENT_SNAPSHOT:-<none>}" >&2
  printf '  repository_dir:      %s\n' "$REPO_DIR" >&2
  printf '  restore_dir:         %s\n' "$RESTORE_DIR" >&2
  printf '  runtime_dir:         %s\n' "$RUNTIME_DIR" >&2
  [[ -n "$LAST_VERIFY_DIFF" ]] && printf '  verify_diff:         %s\n' "$LAST_VERIFY_DIFF" >&2
  [[ -n "$LAST_BACKUP_LOG" ]] && printf '  backup_log:          %s\n' "$LAST_BACKUP_LOG" >&2
  [[ -n "$LAST_LIST_LOG" ]] && printf '  list_log:            %s\n' "$LAST_LIST_LOG" >&2
  [[ -n "$LAST_RESTORE_LOG" ]] && printf '  restore_log:         %s\n' "$LAST_RESTORE_LOG" >&2
  [[ -n "$LAST_DELETE_LOG" ]] && printf '  delete_log:          %s\n' "$LAST_DELETE_LOG" >&2
  [[ -n "$LAST_COMPACT_LOG" ]] && printf '  compact_log:         %s\n' "$LAST_COMPACT_LOG" >&2
  [[ -n "$LAST_PRUNE_LOG" ]] && printf '  prune_log:           %s\n' "$LAST_PRUNE_LOG" >&2
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
mkdir -p "$REPO_DIR" "$RESTORE_DIR" "$LOG_DIR" "$HOME_DIR" "$XDG_CACHE_DIR"

[[ -x "$VGER_BIN" ]] || die "vger binary not found or not executable: $VGER_BIN"
[[ -d "$CORPUS_DIR" ]] || die "corpus directory not found: $CORPUS_DIR"
find "$CORPUS_DIR" -mindepth 1 -print -quit | grep -q . || die "corpus is empty: $CORPUS_DIR"

run_vger() {
  local iter="$1"
  local name="$2"
  shift 2

  local log_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-$name.log"
  if ! HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
    "$VGER_BIN" --config "$CONFIG_PATH" "$@" >"$log_file" 2>&1; then
    printf 'FAILED iteration=%s step=%s snapshot=%s command=%s log=%s\n' \
      "$iter" "$name" "${CURRENT_SNAPSHOT:-<none>}" "$*" "$log_file" >&2
    tail -n 120 "$log_file" >&2 || true
    return 1
  fi
  printf '%s\n' "$log_file"
}

write_config() {
  local repo_q corpus_q
  repo_q="$(yaml_escape "$REPO_DIR")"
  corpus_q="$(yaml_escape "$CORPUS_DIR")"

  cat >"$CONFIG_PATH" <<CFG
repositories:
  - label: "stress"
    url: "$repo_q"
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
  log "Initializing repository"
  CURRENT_STEP="init"
  run_vger 0 init init >/dev/null

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
  local checks=0
  local verify_checks=0

  local start_ts
  start_ts="$(date +%s)"

  for (( i=1; i<=ITERATIONS; i++ )); do
    CURRENT_ITER="$i"
    CURRENT_SNAPSHOT=""

    log "[$i/$ITERATIONS] backup"
    CURRENT_STEP="backup"
    backup_log="$(run_vger "$i" backup backup)"
    LAST_BACKUP_LOG="$backup_log"
    snapshot="$(extract_snapshot_id "$backup_log")" || die "failed to parse snapshot ID"
    CURRENT_SNAPSHOT="$snapshot"
    backups=$((backups + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] list (snapshot $snapshot)"
    CURRENT_STEP="list"
    local list_log
    list_log="$(run_vger "$i" list list --last 20)"
    LAST_LIST_LOG="$list_log"
    grep -q "$snapshot" "$list_log" || die "snapshot '$snapshot' missing from list output"
    lists=$((lists + 1))

    local restore_target="$RESTORE_DIR/current"
    rm -rf "$restore_target"
    mkdir -p "$restore_target"

    log "[$i/$ITERATIONS] restore"
    CURRENT_STEP="restore"
    LAST_RESTORE_LOG="$(run_vger "$i" restore restore "$snapshot" "$restore_target")"

    log "[$i/$ITERATIONS] verify"
    CURRENT_STEP="verify"
    verify_restore_matches "$i" "$restore_target" || die "restore verification failed (iteration $i)"
    restores=$((restores + 1))

    log "[$i/$ITERATIONS] delete"
    CURRENT_STEP="delete"
    LAST_DELETE_LOG="$(run_vger "$i" delete snapshot delete "$snapshot")"
    deletes=$((deletes + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] compact"
    CURRENT_STEP="compact"
    LAST_COMPACT_LOG="$(run_vger "$i" compact compact --threshold 0)"
    compacts=$((compacts + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] prune"
    CURRENT_STEP="prune"
    LAST_PRUNE_LOG="$(run_vger "$i" prune prune)"
    prunes=$((prunes + 1))
    check_locks_clear

    if (( CHECK_EVERY > 0 && i % CHECK_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check"
      CURRENT_STEP="check"
      LAST_CHECK_LOG="$(run_vger "$i" check check)"
      checks=$((checks + 1))
    fi

    if (( VERIFY_DATA_EVERY > 0 && i % VERIFY_DATA_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check --verify-data"
      CURRENT_STEP="check-verify-data"
      LAST_VERIFY_CHECK_LOG="$(run_vger "$i" check-data check --verify-data)"
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
  printf '  check:                %s\n' "$checks"
  printf '  check --verify-data:  %s\n' "$verify_checks"
  printf '  elapsed_sec:          %s\n' "$elapsed"

  CURRENT_STEP="complete"
  RUN_OK=1
}

main
