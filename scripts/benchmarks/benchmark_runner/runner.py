"""Benchmark orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import random
import shutil
import subprocess

from . import report
from .defaults import BenchmarkConfig
from .host import append_log, build_storage_settle_targets, drop_caches, run_storage_settle
from .tools import (
    RunState,
    cleanup_repo_for_tool,
    cleanup_restore_for_tool,
    commands_manifest_entries,
    init_repo_for_tool,
    make_base_env,
    measurement_command,
    post_init_for_tool,
    reset_repo_for_tool,
    resolve_latest_borg_archive,
    resolve_latest_vykar_snapshot,
    seed_backup_for_tool,
    write_vykar_config,
)


@dataclass
class RunResult:
    failed_runs: int = 0


def _run_command(spec_argv: list[str], *, env: dict[str, str], stdout_path: Path | None = None, stderr_path: Path | None = None, cwd: Path | None = None) -> int:
    stdout = subprocess.DEVNULL
    stderr = subprocess.DEVNULL
    stdout_handle = None
    stderr_handle = None
    try:
        if stdout_path is not None:
            stdout_handle = stdout_path.open("w", encoding="utf-8")
            stdout = stdout_handle
        if stderr_path is not None:
            stderr_handle = stderr_path.open("w", encoding="utf-8")
            stderr = stderr_handle
        return subprocess.run(spec_argv, env=env, cwd=cwd, stdout=stdout, stderr=stderr, check=False).returncode
    finally:
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def _append_command(spec_argv: list[str], *, env: dict[str, str], log_path: Path, cwd: Path | None = None) -> int:
    with log_path.open("a", encoding="utf-8") as handle:
        return subprocess.run(spec_argv, env=env, cwd=cwd, stdout=handle, stderr=handle, check=False).returncode


def _ensure_out_dirs(config: BenchmarkConfig) -> None:
    config.logs_dir.mkdir(parents=True, exist_ok=True)
    config.out_root.mkdir(parents=True, exist_ok=True)
    for restore_dir in config.restore_dirs.values():
        restore_dir.mkdir(parents=True, exist_ok=True)


def _write_commands_manifest(config: BenchmarkConfig) -> None:
    lines = [
        "workflow: per run => reset repo + init + untimed backup snapshot-1 + storage settle(sync/fstrim/nvme flush + 20s) + drop caches",
        "restore workflow: no repo prep; restore uses state from preceding timed <tool>_backup op",
    ]
    lines.extend(commands_manifest_entries(config))
    (config.out_root / "commands.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_readme(config: BenchmarkConfig) -> None:
    selected_tool = config.selected_tool_arg or "all"
    (config.out_root / "README.txt").write_text(
        "\n".join(
            [
                f"Benchmark run: {config.out_root.name}",
                f"Dataset root: {config.dataset_dir}",
                f"Seed snapshot (untimed): {config.dataset_snapshot1}",
                f"Benchmark dataset (timed): {config.dataset_benchmark}",
                f"Runs per benchmark: {config.runs}",
                f"Selected tool: {selected_tool}",
                "",
                "Workflow per run:",
                "1) reset/init tool repo",
                "2) untimed backup of snapshot-1",
                "3) storage settle (sync + fstrim + nvme flush + cooldown) + drop caches",
                "4) timed benchmark step:",
                "   - backup ops: backup top-level dataset (snapshot-1 + snapshot-2)",
                "   - restore ops: timed restore of latest from preceding timed backup op state",
                "",
                "Outputs:",
                "- commands.txt / repo-sizes.txt",
                "- profile.<op>/runs/run-*.timev.txt",
                "- profile.<op>/runs/run-*.repo-size-bytes.txt",
                "- reports/summary.{tsv,md,json}",
                "- reports/benchmark.summary.png",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_meta(config: BenchmarkConfig, op_dir: Path, tool: str, phase: str, timed_argv: list[str]) -> None:
    op_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.strptime(config.out_root.name, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    meta = [
        f"name={tool}_{phase}",
        f"dataset={config.dataset_dir}",
        f"dataset_snapshot_1={config.dataset_snapshot1}",
        f"dataset_snapshot_2={config.dataset_snapshot2}",
        f"dataset_benchmark={config.dataset_benchmark}",
        f"timed_cmd={' '.join(timed_argv)}",
        f"runs={config.runs}",
        "warmup_runs=0",
        f"timestamp_utc={stamp.strftime('%Y-%m-%dT%H:%M:%SZ')}",
    ]
    (op_dir / "meta.txt").write_text("\n".join(meta) + "\n", encoding="utf-8")


def _write_repo_size_bytes(config: BenchmarkConfig, tool: str, out_file: Path) -> None:
    repo = config.repo_dir_for_tool(tool)
    value = "NA"
    if repo.exists():
        proc = subprocess.run(["du", "-sb", str(repo)], text=True, capture_output=True, check=False)
        if proc.returncode == 0:
            first = proc.stdout.split()
            if first and first[0].isdigit():
                value = first[0]
    out_file.write_text(f"{value}\n", encoding="utf-8")


def _prepare_backup(tool: str, config: BenchmarkConfig, env: dict[str, str], prep_log: Path) -> bool:
    append_log(prep_log, f"[prepare] op={tool}_backup")
    append_log(prep_log, "[prepare] reset repo")
    if reset_repo_for_tool(tool, config, prep_log) != 0:
        return False
    append_log(prep_log, "[prepare] init repo")
    if _append_command(init_repo_for_tool(tool, config).argv, env=env, log_path=prep_log) != 0:
        return False
    post_init = post_init_for_tool(tool, config)
    if post_init is not None:
        append_log(prep_log, "[prepare] post-init config")
        if _append_command(post_init.argv, env=env, log_path=prep_log) != 0:
            return False
    append_log(prep_log, "[prepare] seed backup snapshot-1 (untimed)")
    return _append_command(seed_backup_for_tool(tool, config).argv, env=env, log_path=prep_log) == 0


def _prepare_measurement(tool: str, phase: str, config: BenchmarkConfig, env: dict[str, str], log_path: Path, trim_mounts: list[str], nvme_devices: list[str], state: RunState) -> bool:
    append_log(log_path, f"[measure-prepare] op={tool}_{phase}")
    state.vykar_restore_snapshot = ""
    state.borg_backup_archive = ""
    state.borg_restore_archive = ""

    if phase == "restore":
        restore_dir = config.restore_dir_for_tool(tool)
        append_log(log_path, f"[measure-prepare] clean restore dir: {restore_dir}")
        cleanup_restore_for_tool(tool, config)
        if tool == "vykar":
            snapshot = resolve_latest_vykar_snapshot(config, env)
            if not snapshot:
                append_log(log_path, "[measure-prepare] failed to resolve latest vykar snapshot")
                return False
            state.vykar_restore_snapshot = snapshot
            append_log(log_path, f"[measure-prepare] vykar snapshot={snapshot}")
        elif tool == "borg":
            archive = resolve_latest_borg_archive(env)
            if not archive:
                append_log(log_path, "[measure-prepare] failed to resolve latest borg archive")
                return False
            state.borg_restore_archive = archive
            append_log(log_path, f"[measure-prepare] borg archive={archive}")
    elif tool == "borg":
        state.borg_backup_archive = f"bench-{config.out_root.name}-{random.randint(1000, 999999)}"
        append_log(log_path, f"[measure-prepare] borg archive={state.borg_backup_archive}")

    run_storage_settle(log_path, trim_mounts, nvme_devices)
    append_log(log_path, "[measure-prepare] drop caches")
    drop_caches(log_path)
    return True


def _timed_run(tool: str, phase: str, config: BenchmarkConfig, env: dict[str, str], op_dir: Path, run_idx: int, trim_mounts: list[str], nvme_devices: list[str]) -> int:
    run_label = f"{run_idx:03d}"
    runs_dir = op_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    prep_log = runs_dir / f"run-{run_label}.prep.log"
    measure_prep_log = runs_dir / f"run-{run_label}.measure-prep.log"
    stdout_path = runs_dir / f"run-{run_label}.stdout.txt"
    timev_path = runs_dir / f"run-{run_label}.timev.txt"
    rc_path = runs_dir / f"run-{run_label}.rc"
    repo_size_path = runs_dir / f"run-{run_label}.repo-size-bytes.txt"
    state = RunState()

    if phase == "backup" and not _prepare_backup(tool, config, env, prep_log):
        timev_path.write_text("", encoding="utf-8")
        rc_path.write_text("1\n", encoding="utf-8")
        _write_repo_size_bytes(config, tool, repo_size_path)
        return 1

    if not _prepare_measurement(tool, phase, config, env, measure_prep_log, trim_mounts, nvme_devices, state):
        timev_path.write_text("", encoding="utf-8")
        rc_path.write_text("1\n", encoding="utf-8")
        _write_repo_size_bytes(config, tool, repo_size_path)
        return 1

    command = measurement_command(tool, phase, config, state)
    argv = ["/usr/bin/time", "-v", *command.argv]
    rc = _run_command(argv, env=env, stdout_path=stdout_path, stderr_path=timev_path, cwd=command.cwd)
    rc_path.write_text(f"{rc}\n", encoding="utf-8")
    _write_repo_size_bytes(config, tool, repo_size_path)
    return rc


def _write_status(op_dir: Path, config: BenchmarkConfig, failures: int) -> None:
    status_path = op_dir / "status.txt"
    if failures == 0:
        text = f"OK (time -v runs={config.runs} warmups=0 failed_warmups=0)\n"
    else:
        text = f"FAILED (time -v failed_runs={failures}/{config.runs} failed_warmups=0/0)\n"
    status_path.write_text(text, encoding="utf-8")


def _collect_repo_sizes(config: BenchmarkConfig) -> None:
    lines: list[str] = []
    for tool in ("vykar", "restic", "rustic", "borg", "kopia"):
        repo = config.repo_dir_for_tool(tool)
        proc = subprocess.run(["du", "-sh", str(repo)], text=True, capture_output=True, check=False)
        output = proc.stdout.strip() if proc.returncode == 0 else f"NA\t{repo}"
        lines.append(output)
    (config.out_root / "repo-sizes.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_tool_stats(config: BenchmarkConfig, env: dict[str, str]) -> None:
    commands = {
        "vykar.info.txt": [["vykar", "info", "-R", "bench"]],
        "restic.stats.txt": [["restic", "snapshots"], ["restic", "stats", "--mode", "raw-data"]],
        "rustic.stats.txt": [["rustic", "snapshots"], ["rustic", "stats"]],
        "borg.stats.txt": [["borg", "info"], ["borg", "list"]],
        "kopia.stats.txt": [
            ["kopia", "--config-file", str(config.kopia_config), "repository", "status"],
            ["kopia", "--config-file", str(config.kopia_config), "snapshot", "list"],
            ["kopia", "--config-file", str(config.kopia_config), "content", "stats"],
        ],
    }
    for filename, groups in commands.items():
        out_path = config.out_root / filename
        with out_path.open("w", encoding="utf-8") as handle:
            for argv in groups:
                handle.write(f"== {' '.join(argv)} ==\n")
                proc = subprocess.run(argv, env=env, text=True, capture_output=True, check=False)
                if proc.stdout:
                    handle.write(proc.stdout)
                    if not proc.stdout.endswith("\n"):
                        handle.write("\n")
                if proc.stderr:
                    handle.write(proc.stderr)
                    if not proc.stderr.endswith("\n"):
                        handle.write("\n")
                handle.write("\n")


def list_previous_run_roots(config: BenchmarkConfig) -> list[str]:
    base = config.runtime_root / "benchmarks"
    if not base.is_dir():
        return []
    roots: list[str] = []
    for child in sorted(base.iterdir(), reverse=True):
        if not child.is_dir():
            continue
        name = child.name
        if len(name) != 16 or name[8] != "T" or not name.endswith("Z"):
            continue
        if name >= config.out_root.name:
            continue
        roots.append(str(child))
    return roots


def _run_report(config: BenchmarkConfig) -> int:
    argv: list[str] = ["all", str(config.out_root), "--out-dir", str(config.out_root / "reports")]
    if config.selected_tool_arg is not None:
        previous = list_previous_run_roots(config)
        for root in previous:
            argv.extend(["--backfill-root", root])
        if previous:
            argv.extend(["--backfill-mode", "nonselected", "--selected-tool", config.selected_tool_arg])
    return report.main(argv)


def _cleanup_transient_dirs(config: BenchmarkConfig) -> None:
    for restore_dir in config.restore_dirs.values():
        if restore_dir.exists():
            shutil.rmtree(restore_dir, ignore_errors=True)
    if config.kopia_cache.exists():
        shutil.rmtree(config.kopia_cache, ignore_errors=True)


def run_benchmarks(config: BenchmarkConfig) -> int:
    _ensure_out_dirs(config)
    write_vykar_config(config)
    env = make_base_env(config)
    trim_mounts, nvme_devices = build_storage_settle_targets([config.repo_root, config.out_root])
    _write_commands_manifest(config)
    _write_readme(config)

    print(f"[config] dataset={config.dataset_dir} runs={config.runs} tool={config.selected_tool_arg or 'all'}")
    print(f"[dataset] seed={config.dataset_snapshot1}")
    print(f"[dataset] benchmark={config.dataset_benchmark}")

    try:
        for tool in config.selected_tools:
            for phase in ("backup", "restore"):
                op_dir = config.out_root / f"profile.{tool}_{phase}"
                timed_preview = measurement_command(tool, phase, config, RunState()).argv
                _write_meta(config, op_dir, tool, phase, timed_preview)

                failures = 0
                for run_idx in range(1, config.runs + 1):
                    if phase == "restore":
                        print(f"[run] {tool}_{phase} {run_idx}/{config.runs} (no prep; uses preceding timed backup state)")
                    else:
                        print(f"[run] {tool}_{phase} {run_idx}/{config.runs}")
                    rc = _timed_run(tool, phase, config, env, op_dir, run_idx, trim_mounts, nvme_devices)
                    if rc != 0:
                        failures += 1

                _write_status(op_dir, config, failures)
                if phase == "restore":
                    cleanup_repo_for_tool(tool, config)
                    cleanup_restore_for_tool(tool, config)

        _collect_repo_sizes(config)
        _write_tool_stats(config, env)
        rc = _run_report(config)
    finally:
        _cleanup_transient_dirs(config)

    if rc == 0:
        print(f"OK: results in {config.out_root}")
    return rc
