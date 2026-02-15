---
name: benchmarks
description: "Compare vger performance against restic and rustic on local backend"
---

# Performance Benchmarks

## Goal

Compare vger against two established backup tools — restic and rustic — on a local backend. Measure wall-clock time, peak memory usage, and CPU usage across common backup operations.

## Scope

- **Backend**: local only (eliminates network variability)
- **Source dataset**: `~/corpus-local`
- **Tools under test**: `vger`, `restic`, `rustic`

## Prerequisites

1. Install restic and rustic if not present:
   ```bash
   sudo apt-get install -y restic
   # For rustic, download from GitHub releases or use cargo install
   ```
2. Verify all three tools are available:
   ```bash
   vger --version
   restic version
   rustic --version
   ```
3. Ensure `/usr/bin/time` is available (not shell builtin — use full path)
4. Optional: install `hyperfine` for more rigorous timing

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
vger extract -c <config> -R local --dest <restore_dir> <snapshot_id>
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

Adjust command syntax as needed — consult each tool's `--help` for exact flags.

## Metrics to Capture

From `/usr/bin/time -v` output:
- **Wall clock time** (`Elapsed (wall clock) time`)
- **Peak RSS** (`Maximum resident set size`)
- **CPU usage** (`Percent of CPU this job got`)
- **User time** (`User time`)
- **System time** (`System time`)

## Output Format

Produce a comparison table:

| Phase | Metric | vger | restic | rustic |
|-------|--------|------|--------|--------|
| Init | Wall time | ... | ... | ... |
| First backup | Wall time | ... | ... | ... |
| First backup | Peak RSS | ... | ... | ... |
| Second backup | Wall time | ... | ... | ... |
| Restore | Wall time | ... | ... | ... |
| Restore | Peak RSS | ... | ... | ... |

Also note:
- Repository size on disk after first backup
- Repository size after second backup (dedup efficiency)
- Compression ratio if measurable

## Cleanup

1. Remove all benchmark repositories:
   ```bash
   rm -rf ~/runtime/repos/restic-bench ~/runtime/repos/rustic-bench
   ```
2. Remove restore directories
3. Keep timing logs under `~/runtime/logs/`
