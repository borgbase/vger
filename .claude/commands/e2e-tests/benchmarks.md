---
name: benchmarks
description: "Compare vger performance against restic, rustic, and borg on local backend"
---

# Performance Benchmarks

## Goal

Compare vger against three established backup tools — restic, rustic, and borg — on a local backend. Measure wall-clock time, peak memory usage, and CPU usage across common backup operations.

## Scope

- **Backend**: local only (eliminates network variability)
- **Source dataset**: default `~/corpus-remote` (smaller, faster iteration). Optionally `~/corpus-local` for stress.
- **Tools under test**: `vger`, `restic`, `rustic`, `borg`

## Prerequisites

1. Install required tools if not present:
   ```bash
   sudo apt-get update -y
   sudo apt-get install -y restic hyperfine time strace linux-perf
   # rustic is typically preinstalled in this sandbox; otherwise install it.
   ```
2. Verify all tools are available:
   ```bash
   vger --version
   restic version
   rustic --version
   borg --version
   ```
3. Ensure `/usr/bin/time` exists (not the shell builtin `time`).

## Quick Start (Scripted)

Use the bundled harness (preferred; produces comparable outputs every run).

Scripts live under `scripts/benchmarks/` relative to this skill directory.

```bash
SKILL_DIR="$(dirname "$(readlink -f "$0")")"  # or set manually

# Basic (includes profiling): timing + repo size + tool stats + /usr/bin/time -v profiles
RUNS=5 "$SKILL_DIR/scripts/benchmarks/run.sh" ~/corpus-remote

# Add strace/perf on top of default profiling (if allowed on host)
PROFILE_STRACE=1 PROFILE_PERF=1 RUNS=3 "$SKILL_DIR/scripts/benchmarks/run.sh" ~/corpus-remote
```

The harness writes to `~/runtime/benchmarks/<UTC_STAMP>/`.

Post-processing (summary table + chart):
```bash
python3 "$SKILL_DIR/scripts/benchmarks/bench_report.py" all ~/runtime/benchmarks/<UTC_STAMP>
```

`run.sh` now calls the reporter automatically at the end of each run and writes:
- `~/runtime/benchmarks/<UTC_STAMP>/reports/summary.tsv`
- `~/runtime/benchmarks/<UTC_STAMP>/reports/summary.md`
- `~/runtime/benchmarks/<UTC_STAMP>/reports/summary.json`
- `~/runtime/benchmarks/<UTC_STAMP>/reports/benchmark.summary.png`

## What To Look At (Actionable Signals)

From `reports/summary.tsv`:
- `maxrss_kb`: memory spikes (often restore path)
- `user` vs `sys`: CPU vs kernel/IO bound work
- `fs_out`: write amplification during restore

From `strace.summary.txt` (when `PROFILE_STRACE=1`):
- Heavy `futex` can indicate contention/over-threading.
- Heavy `statx/newfstatat/llistxattr/getdents64` indicates metadata-walk cost (tree scan).

From tool stats:
- vger: `vger.info.txt`
- restic: `restic.stats.txt` (includes `restic stats --mode raw-data`)
- rustic: `rustic.stats.txt`
- borg: `borg.stats.txt`

## Full vs Incremental

Decide what you are measuring:
- **Incremental**: run backups repeatedly against an existing repo (measures “unchanged tree” behavior).
- **Full**: wipe/re-init the repo before each backup run (measures ingest/pack performance).

The harness defaults to an “incremental-like” loop once initialized, but you can rerun it multiple times and compare across stamps. For full-ingest benchmarks, patch the harness to re-init repos per run or wrap each command with repo wipe + init.

## Benchmark Phases

Test each tool through four phases:

### Phase 1: Repository Init
```bash
/usr/bin/time -v <tool> init ...
```

### Phase 2: First Backup (cold — no dedup)
```bash
/usr/bin/time -v <tool> backup ~/corpus-local ...
```

### Phase 3: Second Backup (warm — full dedup, no changes)
```bash
/usr/bin/time -v <tool> backup ~/corpus-local ...
```

### Phase 4: Full Restore
```bash
/usr/bin/time -v <tool> restore ... --dest <restore_dir>
```

## Tool Setup

### vger
```bash
export VGER_PASSPHRASE=123
vger init -c <config> -R local
vger backup -c <config> -R local -l bench ~/corpus-local
vger restore -c <config> -R local --dest <restore_dir> <snapshot_id>
```

### restic
```bash
export RESTIC_PASSWORD=123
restic init --repo ~/runtime/repos/restic-bench
restic backup --repo ~/runtime/repos/restic-bench ~/corpus-local
restic restore latest --repo ~/runtime/repos/restic-bench --target <restore_dir>
```

### rustic
```bash
export RUSTIC_PASSWORD=123
rustic init --repo ~/runtime/repos/rustic-bench
rustic backup --repo ~/runtime/repos/rustic-bench ~/corpus-local
rustic restore latest --repo ~/runtime/repos/rustic-bench <restore_dir>
```

### borg
```bash
export BORG_PASSPHRASE=123
export BORG_REPO=~/runtime/repos/borg-bench
borg init --encryption=repokey-blake2
borg create --compression zstd,3 ::bench-1 ~/corpus-local
mkdir -p <restore_dir>
(cd <restore_dir> && borg extract ::bench-1)
```

Adjust command syntax as needed — consult each tool's `--help` for exact flags.

## Metrics to Capture

From `/usr/bin/time -v` output:
- **Wall clock time** (`Elapsed (wall clock) time`)
- **Peak RSS** (`Maximum resident set size`)
- **CPU usage** (`Percent of CPU this job got`)
- **User time** (`User time`)
- **System time** (`System time`)
- **Context switches** (`Voluntary/Involuntary context switches`)
- **Page faults** (`Minor/Major page faults`)
- **FS IO** (`File system inputs/outputs`)

From `perf stat` (optional):
- Often blocked on locked-down hosts (`perf_event_paranoid=3`). Treat as best-effort.

## Output Format

Produce a comparison table:

| Phase | Metric | vger | restic | rustic | borg |
|-------|--------|------|--------|--------|------|
| Init | Wall time | ... | ... | ... | ... |
| First backup | Wall time | ... | ... | ... | ... |
| First backup | Peak RSS | ... | ... | ... | ... |
| Second backup | Wall time | ... | ... | ... | ... |
| Restore | Wall time | ... | ... | ... | ... |
| Restore | Peak RSS | ... | ... | ... | ... |

Also note:
- Repository size on disk after first backup
- Repository size after second backup (dedup efficiency)
- Compression ratio if measurable

## Cleanup

1. Remove all benchmark repositories:
   ```bash
   rm -rf ~/runtime/repos/restic-bench ~/runtime/repos/rustic-bench ~/runtime/repos/borg-bench
   ```
2. Remove restore directories
3. Keep timing logs under `~/runtime/logs/`
