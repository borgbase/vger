#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
SCRIPT_NAME="$(basename "$SCRIPT_PATH")"

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Run an autonomous vger stress test from a single folder layout:
  ./corpus/      sample data (input)
  ./vger         vger CLI binary (or set --vger-bin)
  ./stress-cli.sh

The script creates ./repository and ./restore while running, then always deletes
both (plus all temp artifacts) before exit. Only corpus, script, and CLI remain.

Options:
  --iterations N           Loop count (default: 1000)
  --check-every N          Run 'check' every N iterations; 0 disables (default: 50)
  --verify-data-every N    Run 'check --verify-data' every N iterations; 0 disables (default: 0)
  --delete-every N         Run delete every N iterations (default: 1)
  --compact-threshold PCT  Compact threshold (default: 0)
  --compression ALG        lz4|zstd|none (default: zstd)
  --zstd-level N           zstd level (default: 6)
  --keep-last N            retention.keep_last (default: 1)
  --repo-label LABEL       repository label in config (default: stress)
  --vger-bin PATH          vger binary path (default: ./vger next to script)
  --corpus-dir PATH        corpus path (default: ./corpus next to script)
  --help                   Show help
USAGE
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

find_sha_tool() {
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s\n' "sha256sum"
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    printf '%s\n' "shasum"
    return 0
  fi
  if command -v openssl >/dev/null 2>&1; then
    printf '%s\n' "openssl"
    return 0
  fi
  return 1
}

sha256_file() {
  local file="$1"
  case "$SHA_TOOL" in
    sha256sum)
      sha256sum "$file" | awk '{print $1}'
      ;;
    shasum)
      shasum -a 256 "$file" | awk '{print $1}'
      ;;
    openssl)
      # output format: SHA2-256(file)= <hex>
      openssl dgst -sha256 "$file" | awk '{print $NF}'
      ;;
    *)
      die "internal error: unsupported hash tool '$SHA_TOOL'"
      ;;
  esac
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

stat_detect() {
  if stat -c '%a' . >/dev/null 2>&1; then
    printf '%s\n' "gnu"
  else
    printf '%s\n' "bsd"
  fi
}

perm_of() {
  local p="$1"
  if [[ "$STAT_STYLE" == "gnu" ]]; then
    stat -c '%a' "$p"
  else
    stat -f '%Lp' "$p"
  fi
}

# Print "mode\tsize" in one fork (halves per-file stat overhead).
stat_mode_size() {
  if [[ "$STAT_STYLE" == "gnu" ]]; then
    stat -c '%a\t%s' "$1"
  else
    stat -f '%Lp\t%z' "$1"
  fi
}

# Read NUL-delimited paths on stdin, emit "hash  path" lines (sha256sum format).
sha256_bulk() {
  case "$SHA_TOOL" in
    sha256sum) xargs -0 sha256sum ;;
    shasum)    xargs -0 shasum -a 256 ;;
    openssl)
      xargs -0 openssl dgst -sha256 \
        | sed -E 's/^SHA2-256\((.+)\)= (.+)$/\2  \1/'
      ;;
  esac
}

ITERATIONS=1000
CHECK_EVERY=50
VERIFY_DATA_EVERY=0
DELETE_EVERY=1
COMPACT_THRESHOLD=0
COMPRESSION="zstd"
ZSTD_LEVEL=6
KEEP_LAST=1
REPO_LABEL="stress"

DEFAULT_VGER="$SCRIPT_DIR/vger"
if [[ -x "$DEFAULT_VGER" ]]; then
  VGER_BIN="$DEFAULT_VGER"
else
  VGER_BIN="$SCRIPT_DIR/vger-cli"
fi

CORPUS_DIR="$SCRIPT_DIR/corpus"

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
    --delete-every)
      DELETE_EVERY="${2:-}"
      shift 2
      ;;
    --compact-threshold)
      COMPACT_THRESHOLD="${2:-}"
      shift 2
      ;;
    --compression)
      COMPRESSION="${2:-}"
      shift 2
      ;;
    --zstd-level)
      ZSTD_LEVEL="${2:-}"
      shift 2
      ;;
    --keep-last)
      KEEP_LAST="${2:-}"
      shift 2
      ;;
    --repo-label)
      REPO_LABEL="${2:-}"
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
[[ "$DELETE_EVERY" =~ ^[0-9]+$ ]] || die "--delete-every must be a non-negative integer"
[[ "$KEEP_LAST" =~ ^[0-9]+$ ]] || die "--keep-last must be a non-negative integer"
[[ "$ZSTD_LEVEL" =~ ^-?[0-9]+$ ]] || die "--zstd-level must be an integer"
(( DELETE_EVERY > 0 )) || die "--delete-every must be > 0"

case "$COMPRESSION" in
  lz4|zstd|none) ;;
  *) die "--compression must be one of: lz4, zstd, none" ;;
esac

VGER_BIN="$(abs_path "$VGER_BIN")"
CORPUS_DIR="$(abs_path "$CORPUS_DIR")"

REPO_DIR="$SCRIPT_DIR/repository"
RESTORE_DIR="$SCRIPT_DIR/restore"
RUNTIME_DIR="$SCRIPT_DIR/.stress-runtime"
CONFIG_PATH="$RUNTIME_DIR/vger.stress.yaml"
LOG_DIR="$RUNTIME_DIR/logs"
HOME_DIR="$RUNTIME_DIR/home"
XDG_CACHE_DIR="$RUNTIME_DIR/xdg-cache"

cleanup() {
  rm -rf "$REPO_DIR" "$RESTORE_DIR" "$RUNTIME_DIR"
}

trap cleanup EXIT INT TERM

# Start from clean ephemeral state every run.
cleanup
mkdir -p "$REPO_DIR" "$RESTORE_DIR" "$LOG_DIR" "$HOME_DIR" "$XDG_CACHE_DIR"

require_cmd awk
require_cmd sed
require_cmd find
require_cmd sort
require_cmd wc
require_cmd diff
require_cmd stat
require_cmd readlink

SHA_TOOL="$(find_sha_tool || true)"
[[ -n "$SHA_TOOL" ]] || die "need one of: sha256sum, shasum, or openssl"

STAT_STYLE="$(stat_detect)"

[[ -x "$VGER_BIN" ]] || die "vger binary not found or not executable: $VGER_BIN"
[[ -d "$CORPUS_DIR" ]] || die "corpus directory not found: $CORPUS_DIR"
find "$CORPUS_DIR" -mindepth 1 -print -quit | grep -q . || die "corpus is empty: $CORPUS_DIR"

run_vger() {
  local iter="$1"
  local name="$2"
  shift 2

  local log_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-$name.log"
  if ! HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
    "$VGER_BIN" --config "$CONFIG_PATH" --repo "$REPO_LABEL" "$@" >"$log_file" 2>&1; then
    printf 'FAILED iteration=%s command=%s\n' "$iter" "$*" >&2
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
  - label: "$REPO_LABEL"
    url: "$repo_q"
encryption:
  mode: none
compression:
  algorithm: $COMPRESSION
  zstd_level: $ZSTD_LEVEL
retention:
  keep_last: $KEEP_LAST
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

fingerprint_tree() {
  local root="$1"
  local out_file="$2"

  (
    cd "$root"

    # Directories — just path + perms (cheap)
    LC_ALL=C find . -mindepth 1 -type d -print0 | LC_ALL=C sort -z | while IFS= read -r -d '' rel; do
      printf 'D\t%s\t%s\n' "$rel" "$(perm_of "$rel")"
    done

    # Symlinks — path + target (cheap)
    LC_ALL=C find . -mindepth 1 -type l -print0 | LC_ALL=C sort -z | while IFS= read -r -d '' rel; do
      printf 'L\t%s\t%s\n' "$rel" "$(readlink "$rel")"
    done

    # Regular files — bulk hash via xargs (one process per batch, not per file)
    LC_ALL=C find . -mindepth 1 -type f -print0 | LC_ALL=C sort -z | sha256_bulk | \
    while IFS= read -r line; do
      local hash="${line:0:64}"
      local rel="${line:66}"
      printf 'F\t%s\t%s\t%s\n' "$rel" "$(stat_mode_size "$rel")" "$hash"
    done
  ) >"$out_file"
}

verify_restore_matches() {
  local iter="$1"
  local restored="$2"
  local src_fp="$3"

  local dst_fp="$RUNTIME_DIR/dst-$iter.fp"
  fingerprint_tree "$restored" "$dst_fp"
  diff -u "$src_fp" "$dst_fp" >/dev/null
}

main() {
  write_config
  log "Initializing repository"
  run_vger 0 init init >/dev/null

  local corpus_fp="$RUNTIME_DIR/corpus.fp"
  log "Computing corpus fingerprint (once)"
  fingerprint_tree "$CORPUS_DIR" "$corpus_fp"
  log "Corpus fingerprint ready"

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
    log "[$i/$ITERATIONS] backup"
    backup_log="$(run_vger "$i" backup backup --label "stress-$i")"
    snapshot="$(extract_snapshot_id "$backup_log")" || die "failed to parse snapshot ID"
    backups=$((backups + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] list (snapshot $snapshot)"
    local list_log
    list_log="$(run_vger "$i" list list --last 20)"
    grep -q "$snapshot" "$list_log" || die "snapshot '$snapshot' missing from list output"
    lists=$((lists + 1))

    local restore_target="$RESTORE_DIR/current"
    rm -rf "$restore_target"
    mkdir -p "$restore_target"

    log "[$i/$ITERATIONS] restore"
    run_vger "$i" restore restore --snapshot "$snapshot" --dest "$restore_target" >/dev/null

    log "[$i/$ITERATIONS] verify"
    verify_restore_matches "$i" "$restore_target" "$corpus_fp" || die "restore verification failed (iteration $i)"
    restores=$((restores + 1))

    if (( i % DELETE_EVERY == 0 )); then
      log "[$i/$ITERATIONS] delete"
      run_vger "$i" delete delete "$snapshot" >/dev/null
      deletes=$((deletes + 1))
      check_locks_clear
    fi

    log "[$i/$ITERATIONS] compact"
    run_vger "$i" compact compact --threshold "$COMPACT_THRESHOLD" >/dev/null
    compacts=$((compacts + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] prune"
    run_vger "$i" prune prune >/dev/null
    prunes=$((prunes + 1))
    check_locks_clear

    if (( CHECK_EVERY > 0 && i % CHECK_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check"
      run_vger "$i" check check >/dev/null
      checks=$((checks + 1))
    fi

    if (( VERIFY_DATA_EVERY > 0 && i % VERIFY_DATA_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check --verify-data"
      run_vger "$i" check-data check --verify-data >/dev/null
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
}

main
