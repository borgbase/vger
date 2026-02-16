#!/usr/bin/env bash
set -euo pipefail

# Reproducible benchmark harness for vger vs restic vs rustic vs borg on a local repo.
#
# Outputs to ~/runtime/benchmarks/<UTC_STAMP>/ with:
# - hyperfine JSON for timing stability (wall time distributions)
# - /usr/bin/time -v for resource usage (peak RSS, user/sys CPU, IO, cswitches)
# - optional: perf stat + strace -c summaries (enable via env vars)

DATASET_DIR=${1:-"$HOME/corpus-remote"}
RUNS=${RUNS:-3}
PASSPHRASE=${PASSPHRASE:-123}

if [[ ! -d "$DATASET_DIR" ]]; then
  echo "dataset dir not found: $DATASET_DIR" >&2
  exit 2
fi

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 2; }; }
need vger
need restic
need rustic
need borg
need hyperfine
need /usr/bin/time
command -v perf >/dev/null 2>&1 && HAVE_PERF=1 || HAVE_PERF=0
command -v strace >/dev/null 2>&1 && HAVE_STRACE=1 || HAVE_STRACE=0

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT_ROOT="$HOME/runtime/benchmarks/$STAMP"
LOGS="$OUT_ROOT/logs"
mkdir -p "$LOGS"

# Repos on fast local mount. Keep paths stable for easier diffing across runs.
# Override with REPO_ROOT env var if /mnt/repos is not available.
REPO_ROOT=${REPO_ROOT:-/mnt/repos}
VGER_REPO="$REPO_ROOT/bench-vger"
RESTIC_REPO="$REPO_ROOT/bench-restic"
RUSTIC_REPO="$REPO_ROOT/bench-rustic"
BORG_REPO="$REPO_ROOT/bench-borg"

sudo -n mkdir -p "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO"
sudo -n chown -R "$USER:$USER" "$VGER_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO"

rm -rf "$VGER_REPO"/* "$RESTIC_REPO"/* "$RUSTIC_REPO"/* "$BORG_REPO"/*

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

# Optional: try to reduce noise between runs (best-effort). Inline snippet so it
# works inside hyperfine/profile shells too.
DROP_CACHES_CMD="sync; if sudo -n true 2>/dev/null; then echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true; fi"

# Init all repos once
echo "[init] vger"
vger init -R bench >"$LOGS/vger.init.log" 2>&1

echo "[init] restic"
restic init >"$LOGS/restic.init.log" 2>&1

echo "[init] rustic"
rustic init >"$LOGS/rustic.init.log" 2>&1

echo "[init] borg"
borg init --encryption=repokey-blake2 >"$LOGS/borg.init.log" 2>&1

# Benchmark commands: backup + restore to temp dirs
RESTORE_VGER="$OUT_ROOT/restore-vger"
RESTORE_RESTIC="$OUT_ROOT/restore-restic"
RESTORE_RUSTIC="$OUT_ROOT/restore-rustic"
RESTORE_BORG="$OUT_ROOT/restore-borg"
mkdir -p "$RESTORE_VGER" "$RESTORE_RESTIC" "$RESTORE_RUSTIC" "$RESTORE_BORG"

# vger: create one snapshot per run, then restore the most recent snapshot
vger_backup_cmd="$DROP_CACHES_CMD; vger backup -R bench -l bench '$DATASET_DIR'"
# NB: 'vger list' uses comfy_table NOTHING preset — when stdout is not a TTY
# (piped to awk), output is plain space-padded columns with a header row.
vger_restore_cmd="$DROP_CACHES_CMD; rm -rf '$RESTORE_VGER'/*; SNAP=\$(vger list -R bench --last 1 | awk 'NR==2{print \$1}'); vger restore -R bench --dest '$RESTORE_VGER' \"\$SNAP\""

# restic: backup into repo, then restore latest snapshot
restic_backup_cmd="$DROP_CACHES_CMD; restic backup '$DATASET_DIR'"
restic_restore_cmd="$DROP_CACHES_CMD; rm -rf '$RESTORE_RESTIC'/*; restic restore latest --target '$RESTORE_RESTIC'"

# rustic: backup, then restore latest snapshot
rustic_backup_cmd="$DROP_CACHES_CMD; rustic backup '$DATASET_DIR'"
rustic_restore_cmd="$DROP_CACHES_CMD; rm -rf '$RESTORE_RUSTIC'/*; rustic restore latest '$RESTORE_RUSTIC'"

# borg: backup into archive, then extract latest archive
borg_backup_cmd="$DROP_CACHES_CMD; ARCH=bench-\$(date -u +%Y%m%dT%H%M%S)-\$RANDOM; borg create --compression zstd,3 --stats \"::\$ARCH\" '$DATASET_DIR'"
borg_restore_cmd="$DROP_CACHES_CMD; rm -rf '$RESTORE_BORG'/*; ARCH=\$(borg list --short | tail -n1); (cd '$RESTORE_BORG' && borg extract \"::\$ARCH\")"

# Run hyperfine with per-run JSON output for easy comparisons
cd "$OUT_ROOT"

cat >"$OUT_ROOT/commands.txt" <<EOF
vger_backup: $vger_backup_cmd
vger_restore: $vger_restore_cmd
restic_backup: $restic_backup_cmd
restic_restore: $restic_restore_cmd
rustic_backup: $rustic_backup_cmd
rustic_restore: $rustic_restore_cmd
borg_backup: $borg_backup_cmd
borg_restore: $borg_restore_cmd
EOF

bench_one() {
  local name="$1" cmd="$2" json="$3" log="$4"
  hyperfine \
    --runs "$RUNS" \
    --warmup 1 \
    --export-json "$json" \
    --shell=bash \
    --command-name "$name" \
    "$cmd" \
    >"$log" 2>&1
}

profile_one() {
  # Collect metrics that explain *why* something is faster/slower.
  local name="$1" cmd="$2"
  local out="$OUT_ROOT/profile.$name"
  mkdir -p "$out"

  {
    echo "name=$name"
    echo "dataset=$DATASET_DIR"
    echo "cmd=$cmd"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$out/meta.txt"

  /usr/bin/time -v bash -lc "$cmd" >"$out/stdout.txt" 2>"$out/timev.txt" || {
    echo "FAILED (time -v) rc=$?" >"$out/status.txt"
    return 0
  }
  echo "OK (time -v)" >"$out/status.txt"

  if [[ "${PROFILE_PERF:-0}" == "1" && "$HAVE_PERF" == "1" ]]; then
    perf stat -d -r 1 -- bash -lc "$cmd" >"$out/perf.stdout.txt" 2>"$out/perf.stat.txt" || true
  fi

  if [[ "${PROFILE_STRACE:-0}" == "1" && "$HAVE_STRACE" == "1" ]]; then
    strace -c -f -qq -o "$out/strace.summary.txt" bash -lc "$cmd" >/dev/null 2>&1 || true
  fi
}

bench_one vger_backup    "$vger_backup_cmd"    "$OUT_ROOT/vger.backup.json"    "$LOGS/vger.backup.hyperfine.log"
bench_one vger_restore   "$vger_restore_cmd"   "$OUT_ROOT/vger.restore.json"   "$LOGS/vger.restore.hyperfine.log"
bench_one restic_backup  "$restic_backup_cmd"  "$OUT_ROOT/restic.backup.json"  "$LOGS/restic.backup.hyperfine.log"
bench_one restic_restore "$restic_restore_cmd" "$OUT_ROOT/restic.restore.json" "$LOGS/restic.restore.hyperfine.log"
bench_one rustic_backup  "$rustic_backup_cmd"  "$OUT_ROOT/rustic.backup.json"  "$LOGS/rustic.backup.hyperfine.log"
bench_one rustic_restore "$rustic_restore_cmd" "$OUT_ROOT/rustic.restore.json" "$LOGS/rustic.restore.hyperfine.log"
bench_one borg_backup    "$borg_backup_cmd"    "$OUT_ROOT/borg.backup.json"    "$LOGS/borg.backup.hyperfine.log"
bench_one borg_restore   "$borg_restore_cmd"   "$OUT_ROOT/borg.restore.json"   "$LOGS/borg.restore.hyperfine.log"

# One-shot profiling runs (always enabled).
profile_one vger_backup "$vger_backup_cmd"
profile_one vger_restore "$vger_restore_cmd"
profile_one restic_backup "$restic_backup_cmd"
profile_one restic_restore "$restic_restore_cmd"
profile_one rustic_backup "$rustic_backup_cmd"
profile_one rustic_restore "$rustic_restore_cmd"
profile_one borg_backup "$borg_backup_cmd"
profile_one borg_restore "$borg_restore_cmd"

# Repo size stats
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

cat >"$OUT_ROOT/README.txt" <<EOF
Benchmark run: $STAMP
Dataset: $DATASET_DIR
Runs per benchmark: $RUNS

Outputs:
- commands.txt
- *.json (hyperfine results)
- repo-sizes.txt
- vger.info.txt / restic.stats.txt / rustic.stats.txt / borg.stats.txt
- logs/ (hyperfine stdout/stderr)
- profile.<op>/
EOF

echo "OK: results in $OUT_ROOT"
