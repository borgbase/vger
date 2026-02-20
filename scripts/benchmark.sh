#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/defaults.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Reproducible benchmark harness for vger vs restic vs rustic vs borg vs kopia.

Per measured run: drop caches, reset repo, init, untimed seed backup of snapshot-1,
then timed benchmark of full dataset backup or restore.

Options:
  --dataset PATH   Dataset directory (default: \$CORPUS_REMOTE)
  --tool NAME      Single tool: vger|restic|rustic|borg|kopia (default: all)
  --runs N         Timed runs per operation (default: 3)
  --warmups N      Warmup runs per operation (default: 0)
  --perf           Run perf stat summary after timed runs
  --strace         Run strace -c summary after timed runs
  --help           Show help

Environment overrides: CORPUS_REMOTE, REPO_ROOT, PASSPHRASE
USAGE
}

# --- Parse args ---

DATASET_DIR="$CORPUS_REMOTE"
TOOL=""
RUNS=3
WARMUP_RUNS=0
PROFILE_PERF=0
PROFILE_STRACE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset)  DATASET_DIR="${2:-}"; shift 2 ;;
    --tool)     TOOL="${2:-}"; shift 2 ;;
    --runs)     RUNS="${2:-}"; shift 2 ;;
    --warmups)  WARMUP_RUNS="${2:-}"; shift 2 ;;
    --perf)     PROFILE_PERF=1; shift ;;
    --strace)   PROFILE_STRACE=1; shift ;;
    --help|-h)  usage; exit 0 ;;
    *)          die "unknown option: $1" ;;
  esac
done

DATASET_SNAPSHOT1="$DATASET_DIR/snapshot-1"
DATASET_SNAPSHOT2="$DATASET_DIR/snapshot-2"
DATASET_BENCHMARK="$DATASET_DIR"

[[ -d "$DATASET_DIR" ]] || die "dataset dir not found: $DATASET_DIR"
[[ -d "$DATASET_SNAPSHOT1" ]] || die "missing required seed folder: $DATASET_SNAPSHOT1"
[[ -d "$DATASET_SNAPSHOT2" ]] || die "missing required benchmark folder: $DATASET_SNAPSHOT2"
[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || die "--runs must be a positive integer"
[[ "$WARMUP_RUNS" =~ ^[0-9]+$ ]] || die "--warmups must be a non-negative integer"

need /usr/bin/time
command -v perf >/dev/null 2>&1 && HAVE_PERF=1 || HAVE_PERF=0
command -v strace >/dev/null 2>&1 && HAVE_STRACE=1 || HAVE_STRACE=0

case "$TOOL" in
  "")
    need vger; need restic; need rustic; need borg; need kopia ;;
  vger|restic|rustic|borg|kopia)
    need "$TOOL" ;;
  *)
    die "TOOL must be one of: vger, restic, rustic, borg, kopia (or empty for all); got: $TOOL" ;;
esac

# --- Output layout ---

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT_ROOT="$RUNTIME_ROOT/benchmarks/$STAMP"
LOGS="$OUT_ROOT/logs"
mkdir -p "$LOGS"

# --- Repos ---

VGER_REPO="$REPO_ROOT/bench-vger"
RESTIC_REPO="$REPO_ROOT/bench-restic"
RUSTIC_REPO="$REPO_ROOT/bench-rustic"
BORG_REPO="$REPO_ROOT/bench-borg"
KOPIA_REPO="$REPO_ROOT/bench-kopia"
KOPIA_CONFIG="$OUT_ROOT/kopia.repository.config"
KOPIA_CACHE="$OUT_ROOT/kopia-cache"

sudo -n mkdir -p "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO" "$KOPIA_REPO"
sudo -n chown -R "$USER:$USER" "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO" "$KOPIA_REPO"

# --- Tool config ---

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
export VGER_PASSPHRASE="$PASSPHRASE"
export RESTIC_REPOSITORY="$RESTIC_REPO"
export RESTIC_PASSWORD="$PASSPHRASE"
export RUSTIC_REPOSITORY="$RUSTIC_REPO"
export RUSTIC_PASSWORD="$PASSPHRASE"
export BORG_REPO="$BORG_REPO"
export BORG_PASSPHRASE="$PASSPHRASE"
export KOPIA_PASSWORD="$PASSPHRASE"

DROP_CACHES_CMD="sync; if sudo -n true 2>/dev/null; then echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true; fi"

# --- Restore dirs ---

RESTORE_VGER="$OUT_ROOT/restore-vger"
RESTORE_RESTIC="$OUT_ROOT/restore-restic"
RESTORE_RUSTIC="$OUT_ROOT/restore-rustic"
RESTORE_BORG="$OUT_ROOT/restore-borg"
RESTORE_KOPIA="$OUT_ROOT/restore-kopia"
mkdir -p "$RESTORE_VGER" "$RESTORE_RESTIC" "$RESTORE_RUSTIC" "$RESTORE_BORG" "$RESTORE_KOPIA"

cleanup_restore_dirs() {
  rm -rf "$RESTORE_VGER" "$RESTORE_RESTIC" "$RESTORE_RUSTIC" "$RESTORE_BORG" "$RESTORE_KOPIA"
  [[ -n "${KOPIA_CACHE:-}" ]] && rm -rf "$KOPIA_CACHE"
}
trap cleanup_restore_dirs EXIT

# --- Operation list ---

if [[ -n "$TOOL" ]]; then
  OPS=("${TOOL}_backup" "${TOOL}_restore")
else
  OPS=(
    vger_backup vger_restore
    restic_backup restic_restore
    rustic_backup rustic_restore
    borg_backup borg_restore
    kopia_backup kopia_restore
  )
fi

# --- Tool helpers ---

tool_from_op()  { echo "${1%%_*}"; }
phase_from_op() { echo "${1##*_}"; }

repo_dir_for_tool() {
  case "$1" in
    vger)   echo "$VGER_REPO" ;;
    restic) echo "$RESTIC_REPO" ;;
    rustic) echo "$RUSTIC_REPO" ;;
    borg)   echo "$BORG_REPO" ;;
    kopia)  echo "$KOPIA_REPO" ;;
    *) die "unknown tool: $1" ;;
  esac
}

write_repo_size_bytes_for_tool() {
  local tool="$1" out_file="$2"
  local repo bytes
  repo="$(repo_dir_for_tool "$tool")"
  bytes="NA"
  if [[ -d "$repo" ]]; then
    bytes="$(du -sb "$repo" 2>/dev/null | awk 'NR==1 { print $1 }')"
    [[ "$bytes" =~ ^[0-9]+$ ]] || bytes="NA"
  fi
  printf '%s\n' "$bytes" >"$out_file"
}

reset_repo_for_tool() {
  local tool="$1" repo=""
  repo="$(repo_dir_for_tool "$tool")"
  sudo -n rm -rf "$repo"
  sudo -n mkdir -p "$repo"
  sudo -n chown -R "$USER:$USER" "$repo"
  if [[ "$tool" == "kopia" ]]; then
    rm -f "$KOPIA_CONFIG"
    rm -rf "$KOPIA_CACHE"
    mkdir -p "$KOPIA_CACHE"
  fi
}

cleanup_repo_for_tool() {
  local tool="$1" repo=""
  repo="$(repo_dir_for_tool "$tool")"
  # Remove repository contents to free disk between tools, then recreate empty dir for final stats.
  sudo -n rm -rf "$repo"
  sudo -n mkdir -p "$repo"
  sudo -n chown -R "$USER:$USER" "$repo"
}

init_repo_for_tool() {
  case "$1" in
    vger)   vger init -R bench ;;
    restic) restic init ;;
    rustic) rustic init ;;
    borg)   borg init --encryption=repokey-blake2 ;;
    kopia)
      kopia --config-file "$KOPIA_CONFIG" repository create filesystem --path="$KOPIA_REPO" --cache-directory="$KOPIA_CACHE"
      kopia --config-file "$KOPIA_CONFIG" policy set --global --compression=zstd
      ;;
    *) die "unknown tool: $1" ;;
  esac
}

backup_adhoc_for_tool() {
  local tool="$1" src="$2"
  case "$tool" in
    vger)   vger backup -R bench -l bench "$src" ;;
    restic) restic backup "$src" ;;
    rustic) rustic backup "$src" ;;
    borg)
      local arch="bench-$(date -u +%Y%m%dT%H%M%S)-$RANDOM"
      borg create --compression zstd,3 --stats "::$arch" "$src"
      ;;
    kopia) kopia --config-file "$KOPIA_CONFIG" snapshot create "$src" ;;
    *) die "unknown tool: $tool" ;;
  esac
}

measured_cmd_for_op() {
  case "$1" in
    vger_backup)    echo "$DROP_CACHES_CMD; vger backup -R bench -l bench '$DATASET_BENCHMARK'" ;;
    vger_restore)   echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_VGER'/*; SNAP=\$(vger list -R bench --last 1 | awk 'NR==2{print \$1}'); vger restore -R bench \"\$SNAP\" '$RESTORE_VGER'" ;;
    restic_backup)  echo "$DROP_CACHES_CMD; restic backup '$DATASET_BENCHMARK'" ;;
    restic_restore) echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_RESTIC'/*; restic restore latest --target '$RESTORE_RESTIC'" ;;
    rustic_backup)  echo "$DROP_CACHES_CMD; rustic backup '$DATASET_BENCHMARK'" ;;
    rustic_restore) echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_RUSTIC'/*; rustic restore latest '$RESTORE_RUSTIC'" ;;
    borg_backup)    echo "$DROP_CACHES_CMD; ARCH=bench-\$(date -u +%Y%m%dT%H%M%S)-\$RANDOM; borg create --compression zstd,3 --stats \"::\$ARCH\" '$DATASET_BENCHMARK'" ;;
    borg_restore)   echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_BORG'/*; ARCH=\$(borg list --short | tail -n1); (cd '$RESTORE_BORG' && borg extract \"::\$ARCH\")" ;;
    kopia_backup)   echo "$DROP_CACHES_CMD; kopia --config-file '$KOPIA_CONFIG' snapshot create '$DATASET_BENCHMARK'" ;;
    kopia_restore)  echo "$DROP_CACHES_CMD; rm -rf '$RESTORE_KOPIA'/*; kopia --config-file '$KOPIA_CONFIG' snapshot restore '$DATASET_BENCHMARK' '$RESTORE_KOPIA' --snapshot-time latest" ;;
    *) die "unknown operation: $1" ;;
  esac
}

should_prepare_op_run() {
  [[ "$(phase_from_op "$1")" == "backup" ]]
}

prepare_op_run() {
  local op="$1" prep_log="$2"
  local tool
  tool=$(tool_from_op "$op")

  { echo "[prepare] op=$op"; echo "[prepare] drop caches"; } >>"$prep_log"
  drop_caches >>"$prep_log" 2>&1
  echo "[prepare] reset repo" >>"$prep_log"
  reset_repo_for_tool "$tool" >>"$prep_log" 2>&1
  echo "[prepare] init repo" >>"$prep_log"
  init_repo_for_tool "$tool" >>"$prep_log" 2>&1
  echo "[prepare] seed backup snapshot-1 (untimed)" >>"$prep_log"
  backup_adhoc_for_tool "$tool" "$DATASET_SNAPSHOT1" >>"$prep_log" 2>&1
}

# --- Backfill helper ---

list_previous_run_roots() {
  local base="$RUNTIME_ROOT/benchmarks"
  [[ -d "$base" ]] || return 0
  local stamps=()
  for d in "$base"/*; do
    [[ -d "$d" ]] || continue
    local bn
    bn=$(basename "$d")
    [[ "$bn" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || continue
    [[ "$bn" < "$STAMP" ]] || continue
    stamps+=("$bn")
  done
  if [[ "${#stamps[@]}" -gt 0 ]]; then
    printf "%s\n" "${stamps[@]}" | sort -r | while IFS= read -r bn; do
      echo "$base/$bn"
    done
  fi
}

# --- Run one operation ---

run_one() {
  local op="$1"
  local cmd phase tool
  cmd=$(measured_cmd_for_op "$op")
  phase=$(phase_from_op "$op")
  tool=$(tool_from_op "$op")

  local out="$OUT_ROOT/profile.$op"
  local runs_dir="$out/runs"
  mkdir -p "$runs_dir"

  # Meta
  {
    echo "name=$op"
    echo "dataset=$DATASET_DIR"
    echo "dataset_snapshot_1=$DATASET_SNAPSHOT1"
    echo "dataset_snapshot_2=$DATASET_SNAPSHOT2"
    echo "dataset_benchmark=$DATASET_BENCHMARK"
    echo "timed_cmd=$cmd"
    echo "runs=$RUNS"
    echo "warmup_runs=$WARMUP_RUNS"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$out/meta.txt"

  local i rc=0 failed_runs=0 failed_warmups=0

  # Warmup
  for ((i = 1; i <= WARMUP_RUNS; i++)); do
    local run_label
    run_label=$(printf "%03d" "$i")
    echo "[warmup] $op $i/$WARMUP_RUNS"
    if should_prepare_op_run "$op"; then
      if ! prepare_op_run "$op" "$out/warmup-$run_label.prep.log"; then
        failed_warmups=$((failed_warmups + 1))
        continue
      fi
    fi
    if ! bash -lc "$cmd" >"$out/warmup-$run_label.stdout.txt" 2>"$out/warmup-$run_label.stderr.txt"; then
      failed_warmups=$((failed_warmups + 1))
    fi
  done

  # Timed runs
  for ((i = 1; i <= RUNS; i++)); do
    local run_label run_stdout run_timev run_rc_file run_repo_size_file
    run_label=$(printf "%03d" "$i")
    run_stdout="$runs_dir/run-$run_label.stdout.txt"
    run_timev="$runs_dir/run-$run_label.timev.txt"
    run_rc_file="$runs_dir/run-$run_label.rc"
    run_repo_size_file="$runs_dir/run-$run_label.repo-size-bytes.txt"

    if [[ "$phase" == "restore" ]]; then
      echo "[run] $op $i/$RUNS (no prep; uses preceding timed backup state)"
    else
      echo "[run] $op $i/$RUNS"
    fi

    if should_prepare_op_run "$op"; then
      if ! prepare_op_run "$op" "$runs_dir/run-$run_label.prep.log"; then
        rc=1; failed_runs=$((failed_runs + 1))
        : >"$run_timev"
        echo "$rc" >"$run_rc_file"
        write_repo_size_bytes_for_tool "$tool" "$run_repo_size_file"
        continue
      fi
    fi

    if /usr/bin/time -v bash -lc "$cmd" >"$run_stdout" 2>"$run_timev"; then
      rc=0
    else
      rc=$?; failed_runs=$((failed_runs + 1))
    fi
    echo "$rc" >"$run_rc_file"
    write_repo_size_bytes_for_tool "$tool" "$run_repo_size_file"
  done

  if [[ "$failed_runs" -eq 0 ]]; then
    echo "OK (time -v runs=$RUNS warmups=$WARMUP_RUNS failed_warmups=$failed_warmups)" >"$out/status.txt"
  else
    echo "FAILED (time -v failed_runs=$failed_runs/$RUNS failed_warmups=$failed_warmups/$WARMUP_RUNS)" >"$out/status.txt"
  fi

  # Optional perf stat
  if [[ "$PROFILE_PERF" == "1" && "$HAVE_PERF" == "1" ]]; then
    if should_prepare_op_run "$op"; then
      prepare_op_run "$op" "$out/perf.prep.log" && \
        perf stat -d -r 1 -- bash -lc "$cmd" >"$out/perf.stdout.txt" 2>"$out/perf.stat.txt" || true
    else
      perf stat -d -r 1 -- bash -lc "$cmd" >"$out/perf.stdout.txt" 2>"$out/perf.stat.txt" || true
    fi
  fi

  # Optional strace
  if [[ "$PROFILE_STRACE" == "1" && "$HAVE_STRACE" == "1" ]]; then
    if should_prepare_op_run "$op"; then
      prepare_op_run "$op" "$out/strace.prep.log" && \
        strace -c -f -qq -o "$out/strace.summary.txt" bash -lc "$cmd" >/dev/null 2>&1 || true
    else
      strace -c -f -qq -o "$out/strace.summary.txt" bash -lc "$cmd" >/dev/null 2>&1 || true
    fi
  fi
}

# --- Main ---

cd "$OUT_ROOT"

# Write commands manifest
{
  echo "workflow: per warmup/run => drop caches + reset repo + init + untimed backup snapshot-1"
  echo "restore workflow: no repo prep; restore uses state from preceding timed <tool>_backup op"
  for op in "${OPS[@]}"; do
    echo "$op: $(measured_cmd_for_op "$op")"
  done
} >"$OUT_ROOT/commands.txt"

echo "[config] dataset=$DATASET_DIR runs=$RUNS tool=${TOOL:-all}"
echo "[dataset] seed=$DATASET_SNAPSHOT1"
echo "[dataset] benchmark=$DATASET_BENCHMARK"

for op in "${OPS[@]}"; do
  run_one "$op"
  if [[ "$(phase_from_op "$op")" == "restore" ]]; then
    cleanup_repo_for_tool "$(tool_from_op "$op")"
  fi
done

# Repo size stats
du -sh "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO" "$KOPIA_REPO" >"$OUT_ROOT/repo-sizes.txt"

# Tool-specific repo stats
{ echo "== vger info =="; vger info -R bench || true; } >"$OUT_ROOT/vger.info.txt" 2>&1
{ echo "== restic snapshots =="; restic snapshots || true; echo; echo "== restic stats (raw-data) =="; restic stats --mode raw-data || true; } >"$OUT_ROOT/restic.stats.txt" 2>&1
{ echo "== rustic snapshots =="; rustic snapshots || true; echo; echo "== rustic stats =="; rustic stats || true; } >"$OUT_ROOT/rustic.stats.txt" 2>&1
{ echo "== borg info =="; borg info || true; echo; echo "== borg list =="; borg list || true; } >"$OUT_ROOT/borg.stats.txt" 2>&1
{ echo "== kopia repository status =="; kopia --config-file "$KOPIA_CONFIG" repository status || true; echo; echo "== kopia snapshots =="; kopia --config-file "$KOPIA_CONFIG" snapshot list || true; echo; echo "== kopia content stats =="; kopia --config-file "$KOPIA_CONFIG" content stats || true; } >"$OUT_ROOT/kopia.stats.txt" 2>&1

cat >"$OUT_ROOT/README.txt" <<EOF
Benchmark run: $STAMP
Dataset root: $DATASET_DIR
Seed snapshot (untimed): $DATASET_SNAPSHOT1
Benchmark dataset (timed): $DATASET_BENCHMARK
Runs per benchmark: $RUNS
Selected tool: ${TOOL:-all}

Workflow per run:
1) drop cache + reset/init tool repo
2) untimed backup of snapshot-1
3) timed benchmark step:
   - backup ops: backup top-level dataset (snapshot-1 + snapshot-2)
   - restore ops: timed restore of latest from preceding timed backup op state

Outputs:
- commands.txt / repo-sizes.txt
- profile.<op>/runs/run-*.timev.txt
- profile.<op>/runs/run-*.repo-size-bytes.txt
- reports/summary.{tsv,md,json}
- reports/benchmark.summary.png
EOF

# Summary + chart report
REPORT_ARGS=()
if [[ -n "$TOOL" ]]; then
  mapfile -t PREV_RUN_ROOTS < <(list_previous_run_roots || true)
  if [[ "${#PREV_RUN_ROOTS[@]}" -gt 0 ]]; then
    echo "[report] backfill roots (${#PREV_RUN_ROOTS[@]}):"
    for prev in "${PREV_RUN_ROOTS[@]}"; do
      echo "  - $prev"
      REPORT_ARGS+=(--backfill-root "$prev")
    done
    REPORT_ARGS+=(--backfill-mode nonselected --selected-tool "$TOOL")
  else
    echo "[report] no previous run found for backfill"
  fi
fi

python3 "$SCRIPT_DIR/benchmark_report.py" all "$OUT_ROOT" --out-dir "$OUT_ROOT/reports" "${REPORT_ARGS[@]}"

echo "OK: results in $OUT_ROOT"
