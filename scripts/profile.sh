#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Build vger-cli with the profiling profile, prepare repo state for a selected
mode, profile the selected command via heaptrack and/or perf, and generate
reports.

Options:
  --mode MODE            profiling mode:
                         backup|restore|compact|prune|check (required)
  --config PATH          vger config path
                         (default: \$HOME/runtime/config.backends-20260216T112422Z.yaml)
  --repo LABEL           repository label/path for -R (default: local)
  --source PATH          source path to back up (default: \$HOME/corpus-local)
  --out-root DIR         output root directory (default: \$HOME/runtime/heaptrack/reports)
  --profiler NAME        profiler to run: heaptrack|perf|both (default: heaptrack)
  --cost-type TYPE       flamegraph cost type: allocations|temporary|leaked|peak (default: peak)
  --perf-events LIST     perf events list for perf stat -e (optional)
  --perf-repeat N        perf stat repeat count -r (default: 1)
  --perf-record          run perf record (sampled call graph) after perf stat (default: enabled)
  --perf-record-freq N   perf record sample frequency -F (default: 99)
  --skip-build           skip cargo build and use existing target/profiling/vger
  --no-drop-caches       do not call drop_caches before timed profiling run

Backup/compact setup options:
  --label LABEL          snapshot label for setup backups (default: corpus-local-heaptrack-clean)
  --seed-source PATH     seed source path for setup seed backup (default: SOURCE/snapshot-1)

Restore options:
  --restore-snapshot S   snapshot to restore (default: latest)
  --restore-dest PATH    destination directory for restore (default: RUN_DIR/restore-target)
  --restore-setup MODE   restore setup mode: full|benchmark (default: full)

Compact options:
  --compact-threshold N  compact threshold percentage (default: 10)
  --compact-max-repack-size SIZE
                         compact max repack size (e.g. 500M, 2G)
  --dry-run              enable dry-run for compact/prune

Prune options:
  --prune-list           include --list for prune
  --prune-source LABEL   source label filter for prune (repeatable)

Check options:
  --verify-data          include --verify-data for check
  --help                 show this help

Examples:
  $SCRIPT_NAME --mode backup
  $SCRIPT_NAME --mode restore --restore-snapshot latest
  $SCRIPT_NAME --mode compact --compact-threshold 15
  $SCRIPT_NAME --mode prune --dry-run --prune-list --prune-source local
  $SCRIPT_NAME --mode check --verify-data
USAGE
}

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 2
  }
}

drop_caches() {
  sync
  if sudo -n true >/dev/null 2>&1; then
    echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true
  else
    echo "warning: sudo -n unavailable; skipping drop_caches" >&2
  fi
}

MODE=""
CONFIG_PATH="$HOME/runtime/config.backends-20260216T112422Z.yaml"
REPO_LABEL="local"
SNAPSHOT_LABEL="corpus-local-heaptrack-clean"
SEED_SOURCE_PATH=""
SOURCE_PATH="$HOME/corpus-local"
OUT_ROOT="$HOME/runtime/heaptrack/reports"
COST_TYPE="peak"
PROFILER="heaptrack"
PERF_EVENTS=""
PERF_REPEAT="1"
PERF_RECORD=1
PERF_RECORD_FREQ="99"
SKIP_BUILD=0
NO_DROP_CACHES=0
RESTORE_SNAPSHOT="latest"
RESTORE_DEST=""
RESTORE_SETUP="full"
COMPACT_THRESHOLD="10"
COMPACT_MAX_REPACK_SIZE=""
DRY_RUN=0
PRUNE_LIST=0
PRUNE_SOURCES=()
VERIFY_DATA=0

HAS_LABEL=0
HAS_SEED_SOURCE=0
HAS_RESTORE_SNAPSHOT=0
HAS_RESTORE_DEST=0
HAS_RESTORE_SETUP=0
HAS_COMPACT_THRESHOLD=0
HAS_COMPACT_MAX_REPACK=0
HAS_DRY_RUN=0
HAS_PRUNE_LIST=0
HAS_PRUNE_SOURCE=0
HAS_VERIFY_DATA=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --repo)
      REPO_LABEL="${2:-}"
      shift 2
      ;;
    --label)
      SNAPSHOT_LABEL="${2:-}"
      HAS_LABEL=1
      shift 2
      ;;
    --seed-source)
      SEED_SOURCE_PATH="${2:-}"
      HAS_SEED_SOURCE=1
      shift 2
      ;;
    --source)
      SOURCE_PATH="${2:-}"
      shift 2
      ;;
    --out-root)
      OUT_ROOT="${2:-}"
      shift 2
      ;;
    --cost-type)
      COST_TYPE="${2:-}"
      shift 2
      ;;
    --profiler)
      PROFILER="${2:-}"
      shift 2
      ;;
    --perf-events)
      PERF_EVENTS="${2:-}"
      shift 2
      ;;
    --perf-repeat)
      PERF_REPEAT="${2:-}"
      shift 2
      ;;
    --perf-record)
      PERF_RECORD=1
      shift
      ;;
    --perf-record-freq)
      PERF_RECORD_FREQ="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --no-drop-caches)
      NO_DROP_CACHES=1
      shift
      ;;
    --restore-snapshot)
      RESTORE_SNAPSHOT="${2:-}"
      HAS_RESTORE_SNAPSHOT=1
      shift 2
      ;;
    --restore-dest)
      RESTORE_DEST="${2:-}"
      HAS_RESTORE_DEST=1
      shift 2
      ;;
    --restore-setup)
      RESTORE_SETUP="${2:-}"
      HAS_RESTORE_SETUP=1
      shift 2
      ;;
    --compact-threshold)
      COMPACT_THRESHOLD="${2:-}"
      HAS_COMPACT_THRESHOLD=1
      shift 2
      ;;
    --compact-max-repack-size)
      COMPACT_MAX_REPACK_SIZE="${2:-}"
      HAS_COMPACT_MAX_REPACK=1
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      HAS_DRY_RUN=1
      shift
      ;;
    --prune-list)
      PRUNE_LIST=1
      HAS_PRUNE_LIST=1
      shift
      ;;
    --prune-source)
      PRUNE_SOURCES+=("${2:-}")
      HAS_PRUNE_SOURCE=1
      shift 2
      ;;
    --verify-data)
      VERIFY_DATA=1
      HAS_VERIFY_DATA=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$MODE" in
  backup|restore|compact|prune|check) ;;
  *)
    echo "invalid or missing --mode: $MODE" >&2
    usage >&2
    exit 2
    ;;
esac

case "$COST_TYPE" in
  allocations|temporary|leaked|peak) ;;
  *)
    echo "invalid --cost-type: $COST_TYPE" >&2
    exit 2
    ;;
esac

case "$PROFILER" in
  heaptrack|perf|both) ;;
  *)
    echo "invalid --profiler: $PROFILER (expected: heaptrack|perf|both)" >&2
    exit 2
    ;;
esac

if ! [[ "$PERF_REPEAT" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid --perf-repeat: $PERF_REPEAT (expected positive integer)" >&2
  exit 2
fi
if ! [[ "$PERF_RECORD_FREQ" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid --perf-record-freq: $PERF_RECORD_FREQ (expected positive integer)" >&2
  exit 2
fi

case "$RESTORE_SETUP" in
  full|benchmark) ;;
  *)
    echo "invalid --restore-setup: $RESTORE_SETUP (expected: full|benchmark)" >&2
    exit 2
    ;;
esac

[[ -f "$CONFIG_PATH" ]] || {
  echo "config not found: $CONFIG_PATH" >&2
  exit 2
}
[[ -d "$SOURCE_PATH" ]] || {
  echo "source not found: $SOURCE_PATH" >&2
  exit 2
}

if [[ -z "$SEED_SOURCE_PATH" ]]; then
  SEED_SOURCE_PATH="$SOURCE_PATH/snapshot-1"
fi

if [[ "$MODE" == "backup" || "$MODE" == "compact" ]]; then
  [[ -d "$SEED_SOURCE_PATH" ]] || {
    echo "seed source not found: $SEED_SOURCE_PATH" >&2
    exit 2
  }
fi

case "$MODE" in
  backup)
    if (( HAS_RESTORE_SNAPSHOT || HAS_RESTORE_DEST || HAS_COMPACT_THRESHOLD || HAS_COMPACT_MAX_REPACK || HAS_DRY_RUN || HAS_PRUNE_LIST || HAS_PRUNE_SOURCE || HAS_VERIFY_DATA )); then
      echo "mode backup does not accept restore/compact/prune/check options" >&2
      exit 2
    fi
    ;;
  restore)
    if (( HAS_COMPACT_THRESHOLD || HAS_COMPACT_MAX_REPACK || HAS_DRY_RUN || HAS_PRUNE_LIST || HAS_PRUNE_SOURCE || HAS_VERIFY_DATA )); then
      echo "mode restore does not accept compact/prune/check options" >&2
      exit 2
    fi
    if [[ "$RESTORE_SETUP" == "full" ]] && (( HAS_LABEL || HAS_SEED_SOURCE )); then
      echo "mode restore with --restore-setup full does not use --label/--seed-source" >&2
      exit 2
    fi
    ;;
  compact)
    if (( HAS_RESTORE_SNAPSHOT || HAS_RESTORE_DEST || HAS_PRUNE_LIST || HAS_PRUNE_SOURCE || HAS_VERIFY_DATA )); then
      echo "mode compact does not accept restore/prune/check options" >&2
      exit 2
    fi
    ;;
  prune)
    if (( HAS_LABEL || HAS_SEED_SOURCE || HAS_RESTORE_SNAPSHOT || HAS_RESTORE_DEST || HAS_COMPACT_THRESHOLD || HAS_COMPACT_MAX_REPACK || HAS_VERIFY_DATA )); then
      echo "mode prune does not accept backup/restore/compact/check options" >&2
      exit 2
    fi
    ;;
  check)
    if (( HAS_LABEL || HAS_SEED_SOURCE || HAS_RESTORE_SNAPSHOT || HAS_RESTORE_DEST || HAS_COMPACT_THRESHOLD || HAS_COMPACT_MAX_REPACK || HAS_DRY_RUN || HAS_PRUNE_LIST || HAS_PRUNE_SOURCE )); then
      echo "mode check does not accept backup/restore/compact/prune options" >&2
      exit 2
    fi
    ;;
esac

need cargo
need perl
need git
if [[ "$PROFILER" == "heaptrack" || "$PROFILER" == "both" ]]; then
  need heaptrack
  need heaptrack_print
fi
if [[ "$PROFILER" == "perf" || "$PROFILER" == "both" ]]; then
  need perf
fi
if (( NO_DROP_CACHES == 0 )); then
  need sudo
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="$OUT_ROOT/$STAMP"
RUN_DIR="$RUN_ROOT/$MODE"
mkdir -p "$RUN_DIR"

HEAPTRACK_DATA="$RUN_DIR/heaptrack.vger.$STAMP.%p.gz"
ANALYSIS_TXT="$RUN_DIR/heaptrack.analysis.txt"
STACKS_TXT="$RUN_DIR/heaptrack.stacks.txt"
FLAMEGRAPH_SVG="$RUN_DIR/heaptrack.flamegraph.svg"
PERF_STAT_TXT="$RUN_DIR/perf.stat.txt"
PERF_STDOUT_TXT="$RUN_DIR/perf.stdout.txt"
PERF_LOG="$RUN_DIR/perf.log"
PERF_DATA="$RUN_DIR/perf.data"
PERF_RECORD_TXT="$RUN_DIR/perf.record.txt"
PERF_REPORT_TXT="$RUN_DIR/perf.report.txt"
PROFILE_LOG="$RUN_DIR/profile.log"
SETUP_LOG="$RUN_DIR/setup.log"
DROP_CACHES_LOG="$RUN_DIR/drop-caches.log"
META_TXT="$RUN_DIR/meta.txt"
HEAPTRACK_FILE=""
AUTO_RESTORE_DEST=0
CLEANUP_DIRS=()

VGER_BIN="$REPO_ROOT/target/profiling/vger"
PROFILE_CMD=()
SETUP_STEPS=()

add_setup_step() {
  SETUP_STEPS+=("$1")
}

register_cleanup_dir() {
  local d="$1"
  [[ -n "$d" ]] || return 0
  CLEANUP_DIRS+=("$d")
}

cleanup_restored_data() {
  local d=""
  for d in "${CLEANUP_DIRS[@]}"; do
    [[ -n "$d" ]] || continue
    rm -rf "$d" || true
  done
}

trap cleanup_restored_data EXIT

maybe_drop_caches() {
  if (( NO_DROP_CACHES == 1 )); then
    echo "skipped: drop_caches (--no-drop-caches)" | tee "$DROP_CACHES_LOG" >/dev/null
    return 0
  fi
  drop_caches 2>&1 | tee "$DROP_CACHES_LOG"
}

build_profiling_bin() {
  echo "[1/6] Building vger-cli with profiling profile..."
  (
    cd "$REPO_ROOT"
    cargo build -p vger-cli --profile profiling
  )
}

run_repo_reset_and_init() {
  echo "[setup] Cleaning and initializing target repo..." | tee -a "$SETUP_LOG"
  add_setup_step "delete+init repo ($REPO_LABEL)"
  {
    "$VGER_BIN" --config "$CONFIG_PATH" delete -R "$REPO_LABEL" --yes-delete-this-repo || true
    "$VGER_BIN" --config "$CONFIG_PATH" init -R "$REPO_LABEL"
  } 2>&1 | tee -a "$SETUP_LOG"
}

run_seed_backup() {
  local label="$1"
  local src="$2"
  echo "[setup] Seed backup: $src (label: $label)" | tee -a "$SETUP_LOG"
  add_setup_step "seed backup ($src, label=$label)"
  "$VGER_BIN" --config "$CONFIG_PATH" backup -R "$REPO_LABEL" -l "$label" "$src" 2>&1 | tee -a "$SETUP_LOG"
}

run_full_backup() {
  local label="$1"
  local src="$2"
  echo "[setup] Full backup: $src (label: $label)" | tee -a "$SETUP_LOG"
  add_setup_step "full backup ($src, label=$label)"
  "$VGER_BIN" --config "$CONFIG_PATH" backup -R "$REPO_LABEL" -l "$label" "$src" 2>&1 | tee -a "$SETUP_LOG"
}

resolve_heaptrack_data_file() {
  HEAPTRACK_FILE="$(ls -1t "$RUN_DIR"/heaptrack.vger."$STAMP".*.gz.zst "$RUN_DIR"/heaptrack.vger."$STAMP".*.gz 2>/dev/null | head -n 1 || true)"
  [[ -n "$HEAPTRACK_FILE" ]] || {
    echo "could not find heaptrack output in: $RUN_DIR" >&2
    exit 2
  }
}

write_meta() {
  {
    echo "mode=$MODE"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "repo_root=$REPO_ROOT"
    echo "vger_bin=$VGER_BIN"
    echo "config=$CONFIG_PATH"
    echo "repo=$REPO_LABEL"
    echo "source=$SOURCE_PATH"
    echo "out_dir=$RUN_DIR"
    echo "cost_type=$COST_TYPE"
    echo "profiler=$PROFILER"
    echo "perf_repeat=$PERF_REPEAT"
    echo "perf_events=$PERF_EVENTS"
    echo "perf_record=$PERF_RECORD"
    echo "perf_record_freq=$PERF_RECORD_FREQ"
    if [[ "$PROFILER" == "heaptrack" || "$PROFILER" == "both" ]]; then
      echo "heaptrack_enabled=1"
    else
      echo "heaptrack_enabled=0"
    fi
    if [[ "$PROFILER" == "perf" || "$PROFILER" == "both" ]]; then
      echo "perf_enabled=1"
    else
      echo "perf_enabled=0"
    fi
    echo "skip_build=$SKIP_BUILD"
    echo "drop_caches=$((1 - NO_DROP_CACHES))"
    if [[ "$MODE" == "backup" || "$MODE" == "compact" ]]; then
      echo "seed_source=$SEED_SOURCE_PATH"
      echo "snapshot_label=$SNAPSHOT_LABEL"
    fi
    if [[ "$MODE" == "restore" ]]; then
      echo "restore_snapshot=$RESTORE_SNAPSHOT"
      echo "restore_dest=$RESTORE_DEST"
      echo "auto_restore_dest=$AUTO_RESTORE_DEST"
      echo "restore_dest_cleanup=1"
      echo "restore_setup=$RESTORE_SETUP"
      if [[ "$RESTORE_SETUP" == "benchmark" ]]; then
        echo "snapshot_label=$SNAPSHOT_LABEL"
        echo "seed_source=$SEED_SOURCE_PATH"
      fi
    fi
    if [[ "$MODE" == "compact" ]]; then
      echo "compact_threshold=$COMPACT_THRESHOLD"
      echo "compact_max_repack_size=$COMPACT_MAX_REPACK_SIZE"
      echo "dry_run=$DRY_RUN"
    fi
    if [[ "$MODE" == "prune" ]]; then
      echo "dry_run=$DRY_RUN"
      echo "prune_list=$PRUNE_LIST"
      if [[ "${#PRUNE_SOURCES[@]}" -gt 0 ]]; then
        printf "prune_sources=%s\n" "${PRUNE_SOURCES[*]}"
      fi
    fi
    if [[ "$MODE" == "check" ]]; then
      echo "verify_data=$VERIFY_DATA"
    fi
    printf "profile_cmd="
    printf "%q " "${PROFILE_CMD[@]}"
    echo
    echo "setup_steps<<EOF"
    if [[ "${#SETUP_STEPS[@]}" -gt 0 ]]; then
      printf '%s\n' "${SETUP_STEPS[@]}"
    fi
    echo "EOF"
    if [[ -n "$HEAPTRACK_FILE" ]]; then
      echo "heaptrack_data=$HEAPTRACK_FILE"
      echo "analysis_txt=$ANALYSIS_TXT"
      echo "flamegraph_svg=$FLAMEGRAPH_SVG"
      echo "profile_log=$PROFILE_LOG"
    fi
    if [[ "$PROFILER" == "perf" || "$PROFILER" == "both" ]]; then
      echo "perf_stat_txt=$PERF_STAT_TXT"
      echo "perf_stdout_txt=$PERF_STDOUT_TXT"
      echo "perf_log=$PERF_LOG"
      if (( PERF_RECORD == 1 )); then
        echo "perf_data=$PERF_DATA"
        echo "perf_record_txt=$PERF_RECORD_TXT"
        echo "perf_report_txt=$PERF_REPORT_TXT"
      fi
    fi
    echo "setup_log=$SETUP_LOG"
    echo "drop_caches_log=$DROP_CACHES_LOG"
  } >"$META_TXT"
}

run_heaptrack_profile() {
  echo "[4/6] Running heaptrack for mode: $MODE"
  set -o pipefail
  heaptrack -o "$HEAPTRACK_DATA" "${PROFILE_CMD[@]}" 2>&1 | tee "$PROFILE_LOG"
  set +o pipefail
}

run_perf_profile() {
  echo "[4/6] Running perf stat for mode: $MODE"
  {
    echo "perf command: perf stat -d -r $PERF_REPEAT ${PERF_EVENTS:+-e $PERF_EVENTS} -- ${PROFILE_CMD[*]}"
    if [[ -n "$PERF_EVENTS" ]]; then
      perf stat -d -r "$PERF_REPEAT" -e "$PERF_EVENTS" -- "${PROFILE_CMD[@]}" >"$PERF_STDOUT_TXT" 2>"$PERF_STAT_TXT"
    else
      perf stat -d -r "$PERF_REPEAT" -- "${PROFILE_CMD[@]}" >"$PERF_STDOUT_TXT" 2>"$PERF_STAT_TXT"
    fi
    echo "perf rc=$?"
    if (( PERF_RECORD == 1 )); then
      echo "perf record command: perf record -F $PERF_RECORD_FREQ -g --output $PERF_DATA -- ${PROFILE_CMD[*]}"
      perf record -F "$PERF_RECORD_FREQ" -g --output "$PERF_DATA" -- "${PROFILE_CMD[@]}" >"$PERF_RECORD_TXT" 2>&1
      echo "perf record rc=$?"
      perf report --stdio --input "$PERF_DATA" >"$PERF_REPORT_TXT" 2>&1
      echo "perf report rc=$?"
    fi
  } | tee "$PERF_LOG"
}

render_heaptrack_reports() {
  echo "[5/6] Generating text analysis and stacks..."
  heaptrack_print -f "$HEAPTRACK_FILE" > "$ANALYSIS_TXT"
  heaptrack_print -f "$HEAPTRACK_FILE" --flamegraph-cost-type "$COST_TYPE" -F "$STACKS_TXT" > /dev/null

  FLAMEGRAPH_PL="$(command -v flamegraph.pl || true)"
  if [[ -z "$FLAMEGRAPH_PL" ]]; then
    FG_DIR="/tmp/FlameGraph"
    if [[ ! -x "$FG_DIR/flamegraph.pl" ]]; then
      rm -rf "$FG_DIR"
      git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "$FG_DIR" >/dev/null 2>&1
    fi
    FLAMEGRAPH_PL="$FG_DIR/flamegraph.pl"
  fi

  echo "[6/6] Rendering flamegraph SVG..."
  perl "$FLAMEGRAPH_PL" \
    --title "heaptrack: $COST_TYPE (vger-cli profiling build, mode=$MODE)" \
    --colors mem \
    --countname "$COST_TYPE" \
    < "$STACKS_TXT" > "$FLAMEGRAPH_SVG"
}

if (( SKIP_BUILD == 0 )); then
  build_profiling_bin
else
  echo "[1/6] Skipping build (--skip-build)"
fi

[[ -x "$VGER_BIN" ]] || {
  echo "built binary not found or not executable: $VGER_BIN" >&2
  exit 2
}

echo "[2/6] Running setup for mode: $MODE"
: >"$SETUP_LOG"

case "$MODE" in
  backup)
    run_repo_reset_and_init
    run_seed_backup "$SNAPSHOT_LABEL" "$SEED_SOURCE_PATH"
    PROFILE_CMD=( "$VGER_BIN" --config "$CONFIG_PATH" backup -R "$REPO_LABEL" -l "$SNAPSHOT_LABEL" "$SOURCE_PATH" )
    ;;
  restore)
    run_repo_reset_and_init
    if [[ "$RESTORE_SETUP" == "benchmark" ]]; then
      RESTORE_SEED_LABEL="${SNAPSHOT_LABEL}-seed"
      run_seed_backup "$RESTORE_SEED_LABEL" "$SEED_SOURCE_PATH"
      run_full_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    else
      run_full_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    fi
    if [[ -z "$RESTORE_DEST" ]]; then
      RESTORE_DEST="$RUN_DIR/restore-target"
      AUTO_RESTORE_DEST=1
      rm -rf "$RESTORE_DEST"
      mkdir -p "$RESTORE_DEST"
    else
      rm -rf "$RESTORE_DEST"
      mkdir -p "$RESTORE_DEST"
    fi
    register_cleanup_dir "$RESTORE_DEST"
    add_setup_step "prepare restore destination ($RESTORE_DEST)"
    add_setup_step "cleanup restore destination on exit ($RESTORE_DEST)"
    PROFILE_CMD=( "$VGER_BIN" --config "$CONFIG_PATH" restore -R "$REPO_LABEL" "$RESTORE_SNAPSHOT" "$RESTORE_DEST" )
    ;;
  compact)
    SEED_LABEL="${SNAPSHOT_LABEL}-seed"
    run_repo_reset_and_init
    run_seed_backup "$SEED_LABEL" "$SEED_SOURCE_PATH"
    run_full_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    SEED_SNAPSHOT="$("$VGER_BIN" --config "$CONFIG_PATH" list -R "$REPO_LABEL" -S "$SEED_LABEL" --last 1 | awk 'NR==2{print $1}')"
    [[ -n "$SEED_SNAPSHOT" ]] || {
      echo "could not resolve seed snapshot for compact setup (label: $SEED_LABEL)" >&2
      exit 2
    }
    echo "[setup] Deleting seed snapshot to create reclaimable data: $SEED_SNAPSHOT" | tee -a "$SETUP_LOG"
    add_setup_step "delete seed snapshot ($SEED_SNAPSHOT)"
    "$VGER_BIN" --config "$CONFIG_PATH" snapshot -R "$REPO_LABEL" delete "$SEED_SNAPSHOT" 2>&1 | tee -a "$SETUP_LOG"
    PROFILE_CMD=( "$VGER_BIN" --config "$CONFIG_PATH" compact -R "$REPO_LABEL" --threshold "$COMPACT_THRESHOLD" )
    if [[ -n "$COMPACT_MAX_REPACK_SIZE" ]]; then
      PROFILE_CMD+=( --max-repack-size "$COMPACT_MAX_REPACK_SIZE" )
    fi
    if (( DRY_RUN == 1 )); then
      PROFILE_CMD+=( -n )
    fi
    ;;
  prune)
    run_repo_reset_and_init
    run_full_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    PROFILE_CMD=( "$VGER_BIN" --config "$CONFIG_PATH" prune -R "$REPO_LABEL" )
    if (( DRY_RUN == 1 )); then
      PROFILE_CMD+=( -n )
    fi
    if (( PRUNE_LIST == 1 )); then
      PROFILE_CMD+=( --list )
    fi
    if [[ "${#PRUNE_SOURCES[@]}" -gt 0 ]]; then
      for src in "${PRUNE_SOURCES[@]}"; do
        PROFILE_CMD+=( -S "$src" )
      done
    fi
    ;;
  check)
    run_repo_reset_and_init
    run_full_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    PROFILE_CMD=( "$VGER_BIN" --config "$CONFIG_PATH" check -R "$REPO_LABEL" )
    if (( VERIFY_DATA == 1 )); then
      PROFILE_CMD+=( --verify-data )
    fi
    ;;
esac

echo "[3/6] Dropping caches before measured command..."
add_setup_step "drop_caches before profile"
maybe_drop_caches

if [[ "$PROFILER" == "heaptrack" ]]; then
  run_heaptrack_profile
  resolve_heaptrack_data_file
  render_heaptrack_reports
elif [[ "$PROFILER" == "perf" ]]; then
  run_perf_profile
else
  run_heaptrack_profile
  resolve_heaptrack_data_file
  render_heaptrack_reports
  if (( NO_DROP_CACHES == 0 )); then
    echo "[7/7] Dropping caches before perf run..."
    add_setup_step "drop_caches before perf profile"
    maybe_drop_caches
  fi
  run_perf_profile
fi

write_meta

echo
echo "Run complete. Outputs:"
echo "  mode:          $MODE"
echo "  profiler:      $PROFILER"
echo "  run_dir:       $RUN_DIR"
if [[ -n "$HEAPTRACK_FILE" ]]; then
  echo "  heaptrack_data: $HEAPTRACK_FILE"
  echo "  analysis_txt:   $ANALYSIS_TXT"
  echo "  flamegraph_svg: $FLAMEGRAPH_SVG"
  echo "  profile_log:    $PROFILE_LOG"
fi
if [[ "$PROFILER" == "perf" || "$PROFILER" == "both" ]]; then
  echo "  perf_stat_txt:  $PERF_STAT_TXT"
  echo "  perf_stdout:    $PERF_STDOUT_TXT"
  echo "  perf_log:       $PERF_LOG"
  if (( PERF_RECORD == 1 )); then
    echo "  perf_data:      $PERF_DATA"
    echo "  perf_record:    $PERF_RECORD_TXT"
    echo "  perf_report:    $PERF_REPORT_TXT"
  fi
fi
echo "  setup_log:      $SETUP_LOG"
echo "  drop_caches_log:$DROP_CACHES_LOG"
echo "  meta_txt:       $META_TXT"
