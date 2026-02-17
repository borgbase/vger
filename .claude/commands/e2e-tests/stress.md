---
name: stress
description: "Run long-loop local backend stress testing with backup/restore/verify/delete lifecycle"
---

# Stress Testing (Local Corpus)

## Goal

Continuously exercise the local backend lifecycle to catch correctness, lock handling, and repository maintenance regressions under repeated operations.

## Scope

- **Backend**: local
- **Source dataset**: `~/corpus-local` by default
- **Lifecycle per iteration**: `backup -> list -> restore -> verify -> snapshot delete -> compact -> prune`
- **Optional periodic checks**: `check` and `check --verify-data`

## Script Location

Use the bundled harness:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
bash "$REPO_ROOT/scripts/stress.sh" --help
```

## Defaults

- `vger` binary: discovered on `PATH` (override with `--vger-bin`)
- Corpus: `~/corpus-local` (override with `--corpus-dir`)
- Runtime root: `~/runtime/stress` (override with `STRESS_ROOT`)

## Quick Runs

Smoke test:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
bash "$REPO_ROOT/scripts/stress.sh" \
  --iterations 1 \
  --check-every 1 \
  --verify-data-every 0
```

Longer stress pass:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
bash "$REPO_ROOT/scripts/stress.sh" \
  --iterations 1000 \
  --check-every 50 \
  --verify-data-every 0
```

## Outputs and Artifacts

- Working artifacts: `~/runtime/stress/work/`
- Per-command logs: `~/runtime/stress/work/logs/`

On success, transient work artifacts are removed automatically. On failure, artifacts are preserved and failure context is printed with pointers to relevant logs.

## Validation Expectations

1. `backup` succeeds every iteration.
2. Snapshot appears in `list` output.
3. `restore` completes for the newly created snapshot.
4. `diff -qr <source> <restore_dir>` reports no differences.
5. `snapshot delete`, `compact`, and `prune` complete without stale lock files.
6. Optional `check` operations pass when enabled.

## Failure Triage

When the harness fails, capture:

1. Failure context printed to stderr (iteration, step, snapshot, log paths).
2. Relevant logs under `~/runtime/stress/work/logs/`.
3. Diff output path when restore verification fails.
4. Any stale lock files under `~/runtime/stress/work/repository/locks/`.
