#!/usr/bin/env bash
set -euo pipefail

# Reproducible benchmark harness for vger vs restic vs rustic vs borg on a local repo.
#
# Per measured run:
# - drop caches and reset repo for the tool
# - take an untimed seed backup of snapshot-1
# - benchmark top-level dataset backup (includes snapshot-1 + snapshot-2)
#   or restore-from-top-level backup (restore phase)
#
# Outputs to ~/runtime/benchmarks/<UTC_STAMP>/ with:
# - repeated /usr/bin/time -v runs for timing/resource distributions
# - optional: perf stat + strace -c summaries (enable via env vars)

DATASET_DIR=${1:-"$HOME/corpus-remote"}
DATASET_SNAPSHOT1="$DATASET_DIR/snapshot-1"
DATASET_SNAPSHOT2="$DATASET_DIR/snapshot-2"
DATASET_BENCHMARK="$DATASET_DIR"
RUNS=${RUNS:-3}
WARMUP_RUNS=${WARMUP_RUNS:-1}
PASSPHRASE=${PASSPHRASE:-123}
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ ! -d "$DATASET_DIR" ]]; then
  echo "dataset dir not found: $DATASET_DIR" >&2
  exit 2
fi
if [[ ! -d "$DATASET_SNAPSHOT1" ]]; then
  echo "missing required seed folder: $DATASET_SNAPSHOT1" >&2
  exit 2
fi
if [[ ! -d "$DATASET_SNAPSHOT2" ]]; then
  echo "missing required benchmark folder: $DATASET_SNAPSHOT2" >&2
  exit 2
fi

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 2; }; }
need vger
need restic
need rustic
need borg
need /usr/bin/time
command -v perf >/dev/null 2>&1 && HAVE_PERF=1 || HAVE_PERF=0
command -v strace >/dev/null 2>&1 && HAVE_STRACE=1 || HAVE_STRACE=0

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
  echo "RUNS must be a positive integer; got: $RUNS" >&2
  exit 2
fi
if ! [[ "$WARMUP_RUNS" =~ ^[0-9]+$ ]]; then
  echo "WARMUP_RUNS must be a non-negative integer; got: $WARMUP_RUNS" >&2
  exit 2
fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT_ROOT="$HOME/runtime/benchmarks/$STAMP"
LOGS="$OUT_ROOT/logs"
mkdir -p "$LOGS"

cleanup_restore_dirs() {
  # Always remove restore artifacts from runtime after run completion/failure.
  # Keep benchmark metrics/logs/reports intact.
  local dirs=("${RESTORE_VGER:-}" "${RESTORE_RESTIC:-}" "${RESTORE_RUSTIC:-}" "${RESTORE_BORG:-}")
  local d=""
  for d in "${dirs[@]}"; do
    [[ -n "$d" ]] && rm -rf "$d"
  done
}
trap cleanup_restore_dirs EXIT

# Repos on fast local mount. Keep paths stable for easier diffing across runs.
# Override with REPO_ROOT env var if /mnt/repos is not available.
REPO_ROOT=${REPO_ROOT:-/mnt/repos}
VGER_REPO="$REPO_ROOT/bench-vger"
RESTIC_REPO="$REPO_ROOT/bench-restic"
RUSTIC_REPO="$REPO_ROOT/bench-rustic"
BORG_REPO="$REPO_ROOT/bench-borg"

sudo -n mkdir -p "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO"
sudo -n chown -R "$USER:$USER" "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO"

# vger config (local path repo)
VGER_CFG="$OUT_ROOT/vger.bench.yaml"
cat >"$VGER_CFG" <<YAML
repositories:
  - url: "$VGER_REPO"
    label: bench
compression:
  algorithm: zstd
YAML
chmod 600 "$VGER_CFG"

export VGER_CONFIG="$VGER_CFG"
export VGER_PASSPHRASE="$PASSPHRASE"  # passphrase via env, not config file

export RESTIC_REPOSITORY="$RESTIC_REPO"
export RESTIC_PASSWORD="$PASSPHRASE"

export RUSTIC_REPOSITORY="$RUSTIC_REPO"
export RUSTIC_PASSWORD="$PASSPHRASE"

export BORG_REPO="$BORG_REPO"
export BORG_PASSPHRASE="$PASSPHRASE"

DROP_CACHES_CMD="sync; if sudo -n true 2>/dev/null; then echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true; fi"

RESTORE_VGER="$OUT_ROOT/restore-vger"
RESTORE_RESTIC="$OUT_ROOT/restore-restic"
RESTORE_RUSTIC="$OUT_ROOT/restore-rustic"
RESTORE_BORG="$OUT_ROOT/restore-borg"
mkdir -p "$RESTORE_VGER" "$RESTORE_RESTIC" "$RESTORE_RUSTIC" "$RESTORE_BORG"

OPS=(
  vger_backup
  vger_restore
  restic_backup
  restic_restore
  rustic_backup
  rustic_restore
  borg_backup
  borg_restore
)

tool_from_op() {
  local op="$1"
  echo "${op%%_*}"
}

phase_from_op() {
  local op="$1"
  echo "${op##*_}"
}

drop_caches() {
  bash -lc "$DROP_CACHES_CMD"
}

reset_repo_for_tool() {
  local tool="$1"
  local repo=""
  case "$tool" in
    vger) repo="$VGER_REPO" ;;
    restic) repo="$RESTIC_REPO" ;;
    rustic) repo="$RUSTIC_REPO" ;;
    borg) repo="$BORG_REPO" ;;
    *) echo "unknown tool: $tool" >&2; return 2 ;;
  esac

  sudo -n rm -rf "$repo"
  sudo -n mkdir -p "$repo"
  sudo -n chown -R "$USER:$USER" "$repo"
}

init_repo_for_tool() {
  local tool="$1"
  case "$tool" in
    vger) vger init -R bench ;;
    restic) restic init ;;
    rustic) rustic init ;;
    borg) borg init --encryption=repokey-blake2 ;;
    *) echo "unknown tool: $tool" >&2; return 2 ;;
  esac
}

backup_adhoc_for_tool() {
  local tool="$1"
  local src="$2"
  case "$tool" in
    vger) vger backup -R bench -l bench "$src" ;;
    restic) restic backup "$src" ;;
    rustic) rustic backup "$src" ;;
    borg)
      local arch=""
      arch="bench-$(date -u +%Y%m%dT%H%M%S)-$RANDOM"
      borg create --compression zstd,3 --stats "::$arch" "$src"
      ;;
    *) echo "unknown tool: $tool" >&2; return 2 ;;
  esac
}

measured_cmd_for_op() {
  local op="$1"
  case "$op" in
    vger_backup)
      echo "$DROP_CACHES_CMD; vger backup -R bench -l bench '$DATASET_BENCHMARK'"
      ;;
    vger_restore)
      echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_VGER'/*; SNAP=\$(vger list -R bench --last 1 | awk 'NR==2{print \$1}'); vger restore -R bench \"\$SNAP\" '$RESTORE_VGER'"
      ;;
    restic_backup)
      echo "$DROP_CACHES_CMD; restic backup '$DATASET_BENCHMARK'"
      ;;
    restic_restore)
      echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_RESTIC'/*; restic restore latest --target '$RESTORE_RESTIC'"
      ;;
    rustic_backup)
      echo "$DROP_CACHES_CMD; rustic backup '$DATASET_BENCHMARK'"
      ;;
    rustic_restore)
      echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_RUSTIC'/*; rustic restore latest '$RESTORE_RUSTIC'"
      ;;
    borg_backup)
      echo "$DROP_CACHES_CMD; ARCH=bench-\$(date -u +%Y%m%dT%H%M%S)-\$RANDOM; borg create --compression zstd,3 --stats \"::\$ARCH\" '$DATASET_BENCHMARK'"
      ;;
    borg_restore)
      echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_BORG'/*; ARCH=\$(borg list --short | tail -n1); (cd '$RESTORE_BORG' && borg extract \"::\$ARCH\")"
      ;;
    *)
      echo "unknown operation: $op" >&2
      return 2
      ;;
  esac
}

prepare_op_run() {
  local op="$1"
  local prep_log="$2"
  local tool=""
  local phase=""

  tool=$(tool_from_op "$op")
  phase=$(phase_from_op "$op")

  {
    echo "[prepare] op=$op"
    echo "[prepare] drop caches"
  } >>"$prep_log"
  drop_caches >>"$prep_log" 2>&1

  echo "[prepare] reset repo" >>"$prep_log"
  reset_repo_for_tool "$tool" >>"$prep_log" 2>&1

  echo "[prepare] init repo" >>"$prep_log"
  init_repo_for_tool "$tool" >>"$prep_log" 2>&1

  echo "[prepare] seed backup snapshot-1 (untimed)" >>"$prep_log"
  backup_adhoc_for_tool "$tool" "$DATASET_SNAPSHOT1" >>"$prep_log" 2>&1

  if [[ "$phase" == "restore" ]]; then
    echo "[prepare] benchmark backup top-level dataset (untimed, for restore source)" >>"$prep_log"
    backup_adhoc_for_tool "$tool" "$DATASET_BENCHMARK" >>"$prep_log" 2>&1
  fi
}

run_one() {
  local op="$1"
  local cmd=""
  cmd=$(measured_cmd_for_op "$op")

  local out="$OUT_ROOT/profile.$op"
  local runs_dir="$out/runs"
  mkdir -p "$out"
  mkdir -p "$runs_dir"

  {
    echo "name=$op"
    echo "dataset=$DATASET_DIR"
    echo "dataset_snapshot_1=$DATASET_SNAPSHOT1"
    echo "dataset_snapshot_2=$DATASET_SNAPSHOT2"
    echo "dataset_benchmark=$DATASET_BENCHMARK"
    echo "cmd=$cmd"
    echo "runs=$RUNS"
    echo "warmup_runs=$WARMUP_RUNS"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$out/meta.txt"

  local i=0
  local rc=0
  local failed_runs=0
  local failed_warmups=0
  local run_label=""
  local run_stdout=""
  local run_timev=""
  local run_rc_file=""

  if [[ "$WARMUP_RUNS" -gt 0 ]]; then
    for ((i = 1; i <= WARMUP_RUNS; i++)); do
      run_label=$(printf "%03d" "$i")
      echo "[warmup] $op $i/$WARMUP_RUNS"
      if ! prepare_op_run "$op" "$out/warmup-$run_label.prep.log"; then
        failed_warmups=$((failed_warmups + 1))
        continue
      fi
      if ! bash -lc "$cmd" >"$out/warmup-$run_label.stdout.txt" 2>"$out/warmup-$run_label.stderr.txt"; then
        failed_warmups=$((failed_warmups + 1))
      fi
    done
  fi

  for ((i = 1; i <= RUNS; i++)); do
    run_label=$(printf "%03d" "$i")
    run_stdout="$runs_dir/run-$run_label.stdout.txt"
    run_timev="$runs_dir/run-$run_label.timev.txt"
    run_rc_file="$runs_dir/run-$run_label.rc"

    echo "[run] $op $i/$RUNS"

    if ! prepare_op_run "$op" "$runs_dir/run-$run_label.prep.log"; then
      rc=1
      failed_runs=$((failed_runs + 1))
      : >"$run_timev"
      echo "$rc" >"$run_rc_file"
      continue
    fi

    if /usr/bin/time -v bash -lc "$cmd" >"$run_stdout" 2>"$run_timev"; then
      rc=0
    else
      rc=$?
      failed_runs=$((failed_runs + 1))
    fi
    echo "$rc" >"$run_rc_file"
  done

  if [[ "$failed_runs" -eq 0 ]]; then
    echo "OK (time -v runs=$RUNS warmups=$WARMUP_RUNS failed_warmups=$failed_warmups)" >"$out/status.txt"
  else
    echo "FAILED (time -v failed_runs=$failed_runs/$RUNS failed_warmups=$failed_warmups/$WARMUP_RUNS)" >"$out/status.txt"
  fi

  if [[ "${PROFILE_PERF:-0}" == "1" && "$HAVE_PERF" == "1" ]]; then
    if prepare_op_run "$op" "$out/perf.prep.log"; then
      perf stat -d -r 1 -- bash -lc "$cmd" >"$out/perf.stdout.txt" 2>"$out/perf.stat.txt" || true
    fi
  fi

  if [[ "${PROFILE_STRACE:-0}" == "1" && "$HAVE_STRACE" == "1" ]]; then
    if prepare_op_run "$op" "$out/strace.prep.log"; then
      strace -c -f -qq -o "$out/strace.summary.txt" bash -lc "$cmd" >/dev/null 2>&1 || true
    fi
  fi
}

cd "$OUT_ROOT"
{
  echo "workflow: per warmup/run => drop caches + reset repo + init + untimed backup snapshot-1"
  echo "restore workflow: adds untimed backup of top-level dataset before timed restore"
  for op in "${OPS[@]}"; do
    echo "$op: $(measured_cmd_for_op "$op")"
  done
} >"$OUT_ROOT/commands.txt"

echo "[config] dataset=$DATASET_DIR runs=$RUNS warmup_runs=$WARMUP_RUNS"

echo "[dataset] seed=$DATASET_SNAPSHOT1"
echo "[dataset] benchmark=$DATASET_BENCHMARK"

for op in "${OPS[@]}"; do
  run_one "$op"
done

# Repo size stats
# Repos reflect the final state from the last run per tool.
du -sh "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO" >"$OUT_ROOT/repo-sizes.txt"

# Tool-specific repo stats (helps explain performance/space deltas).
{
  echo "== vger info =="
  vger info -R bench || true
} >"$OUT_ROOT/vger.info.txt" 2>&1

{
  echo "== restic snapshots =="
  restic snapshots || true
  echo
  echo "== restic stats (raw-data) =="
  restic stats --mode raw-data || true
} >"$OUT_ROOT/restic.stats.txt" 2>&1

{
  echo "== rustic snapshots =="
  rustic snapshots || true
  echo
  echo "== rustic stats =="
  rustic stats || true
} >"$OUT_ROOT/rustic.stats.txt" 2>&1

{
  echo "== borg info =="
  borg info || true
  echo
  echo "== borg list =="
  borg list || true
} >"$OUT_ROOT/borg.stats.txt" 2>&1

cat >"$OUT_ROOT/README.txt" <<EOF2
Benchmark run: $STAMP
Dataset root: $DATASET_DIR
Seed snapshot (untimed): $DATASET_SNAPSHOT1
Benchmark dataset (timed): $DATASET_BENCHMARK
Runs per benchmark: $RUNS
Warmup runs per benchmark: $WARMUP_RUNS

Workflow per run:
1) drop cache + reset/init tool repo
2) untimed backup of snapshot-1
3) timed benchmark step:
   - backup ops: backup top-level dataset (snapshot-1 + snapshot-2)
   - restore ops: untimed backup top-level dataset, then timed restore of latest

Outputs:
- commands.txt
- repo-sizes.txt
- vger.info.txt / restic.stats.txt / rustic.stats.txt / borg.stats.txt
- profile.<op>/runs/run-*.timev.txt (+ stdout/rc/prep logs)
- reports/summary.tsv / reports/summary.md / reports/summary.json
- reports/benchmark.summary.png
EOF2

# Post-processing outputs (summary + chart)
python3 "$SCRIPT_DIR/bench_report.py" all "$OUT_ROOT" --out-dir "$OUT_ROOT/reports"

echo "OK: results in $OUT_ROOT"
