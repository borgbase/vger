# Scripts

This directory contains two Python sub-projects intended to be run with `uv` from the repository root.

## `benchmarks`

Benchmark harness for comparing `vykar` with `restic`, `rustic`, `borg`, and `kopia`.

Examples:

```bash
uv run --project scripts/benchmarks benchmark-runner --runs 3
uv run --project scripts/benchmarks benchmark-runner --runs 5 --tool vykar
uv run --project scripts/benchmarks benchmark-runner --runs 3 --dataset ~/corpus-remote
```

Report entrypoint:

```bash
uv run --project scripts/benchmarks benchmark-report all ~/runtime/benchmarks/<STAMP>
```

## `scenarios`

YAML-driven end-to-end scenario runner for `vykar` across multiple backends.

Examples:

```bash
uv run --project scripts/scenarios scenario-runner scripts/scenarios/scenarios/simple-backup.yaml
uv run --project scripts/scenarios scenario-runner scripts/scenarios/scenarios/5xchurn.yaml --backend local --runs 3
```
