#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Build vger-cli with the profiling profile, clean+init the selected repo,
run a seed backup, drop caches, run a heaptrack incremental backup,
and generate analysis + flamegraph reports.

Options:
  --config PATH          vger config path
                         (default: \$HOME/runtime/config.backends-20260216T112422Z.yaml)
  --repo LABEL           repository label/path for -R (default: local)
  --label LABEL          snapshot label for -l (default: corpus-local-heaptrack-clean)
  --seed-source PATH     seed source path for initial partial backup
                         (default: SOURCE/snapshot-1)
  --source PATH          source path to back up (default: \$HOME/corpus-local)
  --out-root DIR         output root directory (default: \$HOME/runtime/heaptrack/reports)
  --cost-type TYPE       flamegraph cost type: allocations|temporary|leaked|peak (default: peak)
  --help                 show this help
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

CONFIG_PATH="$HOME/runtime/config.backends-20260216T112422Z.yaml"
REPO_LABEL="local"
SNAPSHOT_LABEL="corpus-local-heaptrack-clean"
SEED_SOURCE_PATH=""
SOURCE_PATH="$HOME/corpus-local"
OUT_ROOT="$HOME/runtime/heaptrack/reports"
COST_TYPE="peak"

while [[ $# -gt 0 ]]; do
  case "$1" in
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
      shift 2
      ;;
    --seed-source)
      SEED_SOURCE_PATH="${2:-}"
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

case "$COST_TYPE" in
  allocations|temporary|leaked|peak) ;;
  *)
    echo "invalid --cost-type: $COST_TYPE" >&2
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
[[ -d "$SEED_SOURCE_PATH" ]] || {
  echo "seed source not found: $SEED_SOURCE_PATH" >&2
  exit 2
}

need cargo
need heaptrack
need heaptrack_print
need perl
need git
need sudo

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$OUT_ROOT/$STAMP"
mkdir -p "$RUN_DIR"

HEAPTRACK_DATA="$RUN_DIR/heaptrack.vger.$STAMP.%p.gz"
ANALYSIS_TXT="$RUN_DIR/heaptrack.analysis.txt"
STACKS_TXT="$RUN_DIR/heaptrack.stacks.txt"
FLAMEGRAPH_SVG="$RUN_DIR/heaptrack.flamegraph.svg"
BACKUP_LOG="$RUN_DIR/backup.log"
SEED_LOG="$RUN_DIR/seed-backup.log"
REPO_CLEAN_LOG="$RUN_DIR/repo-clean.log"
DROP_CACHES_LOG="$RUN_DIR/drop-caches.log"

echo "[1/7] Building vger-cli with profiling profile..."
(
  cd "$REPO_ROOT"
  cargo build -p vger-cli --profile profiling
)

VGER_BIN="$REPO_ROOT/target/profiling/vger"
[[ -x "$VGER_BIN" ]] || {
  echo "built binary not found or not executable: $VGER_BIN" >&2
  exit 2
}

echo "[2/7] Cleaning and initializing target repo..."
{
  "$VGER_BIN" --config "$CONFIG_PATH" delete -R "$REPO_LABEL" --yes-delete-this-repo || true
  "$VGER_BIN" --config "$CONFIG_PATH" init -R "$REPO_LABEL"
} 2>&1 | tee "$REPO_CLEAN_LOG"

echo "[3/7] Running seed backup (partial snapshot)..."
"$VGER_BIN" --config "$CONFIG_PATH" backup -R "$REPO_LABEL" -l "$SNAPSHOT_LABEL" "$SEED_SOURCE_PATH" \
  2>&1 | tee "$SEED_LOG"

echo "[4/7] Dropping caches before measured incremental backup..."
drop_caches 2>&1 | tee "$DROP_CACHES_LOG"

echo "[5/7] Running heaptrack incremental backup (full corpus)..."
set -o pipefail
heaptrack -o "$HEAPTRACK_DATA" \
  "$VGER_BIN" --config "$CONFIG_PATH" backup -R "$REPO_LABEL" -l "$SNAPSHOT_LABEL" "$SOURCE_PATH" \
  2>&1 | tee "$BACKUP_LOG"
set +o pipefail

DATA_FILE="$(ls -1t "$RUN_DIR"/heaptrack.vger."$STAMP".*.gz.zst "$RUN_DIR"/heaptrack.vger."$STAMP".*.gz 2>/dev/null | head -n 1 || true)"
[[ -n "$DATA_FILE" ]] || {
  echo "could not find heaptrack output in: $RUN_DIR" >&2
  exit 2
}

echo "[6/7] Generating text analysis and stacks..."
heaptrack_print -f "$DATA_FILE" > "$ANALYSIS_TXT"
heaptrack_print -f "$DATA_FILE" --flamegraph-cost-type "$COST_TYPE" -F "$STACKS_TXT" > /dev/null

FLAMEGRAPH_PL="$(command -v flamegraph.pl || true)"
if [[ -z "$FLAMEGRAPH_PL" ]]; then
  FG_DIR="/tmp/FlameGraph"
  if [[ ! -x "$FG_DIR/flamegraph.pl" ]]; then
    rm -rf "$FG_DIR"
    git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "$FG_DIR" >/dev/null 2>&1
  fi
  FLAMEGRAPH_PL="$FG_DIR/flamegraph.pl"
fi

echo "[7/7] Rendering flamegraph SVG..."
perl "$FLAMEGRAPH_PL" \
  --title "heaptrack: $COST_TYPE (vger-cli profiling build)" \
  --colors mem \
  --countname "$COST_TYPE" \
  < "$STACKS_TXT" > "$FLAMEGRAPH_SVG"

echo
echo "Run complete. Outputs:"
echo "  heaptrack_data: $DATA_FILE"
echo "  analysis_txt:   $ANALYSIS_TXT"
echo "  flamegraph_svg: $FLAMEGRAPH_SVG"
echo "  repo_clean_log: $REPO_CLEAN_LOG"
echo "  drop_caches_log:$DROP_CACHES_LOG"
echo "  seed_log:       $SEED_LOG"
echo "  backup_log:     $BACKUP_LOG"
