"""Tool-specific benchmark commands."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os
import shutil
import subprocess

from .defaults import BenchmarkConfig


@dataclass
class CommandSpec:
    argv: list[str]
    cwd: Path | None = None
    env_overrides: dict[str, str] = field(default_factory=dict)


@dataclass
class RunState:
    vykar_restore_snapshot: str = ""
    borg_backup_archive: str = ""
    borg_restore_archive: str = ""


def write_vykar_config(config: BenchmarkConfig) -> None:
    config.vykar_config_path.write_text(
        "\n".join(
            [
                "repositories:",
                f'  - url: "{config.vykar_repo}"',
                "    label: bench",
                "compression:",
                "  algorithm: zstd",
                "",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(config.vykar_config_path, 0o600)


def make_base_env(config: BenchmarkConfig) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "VYKAR_CONFIG": str(config.vykar_config_path),
            "VYKAR_PASSPHRASE": config.passphrase,
            "RESTIC_REPOSITORY": str(config.restic_repo),
            "RESTIC_PASSWORD": config.passphrase,
            "RUSTIC_REPOSITORY": str(config.rustic_repo),
            "RUSTIC_PASSWORD": config.passphrase,
            "BORG_REPO": str(config.borg_repo),
            "BORG_PASSPHRASE": config.passphrase,
            "KOPIA_PASSWORD": config.passphrase,
        }
    )
    return env


def _run(argv: list[str], env: dict[str, str], log_path: Path | None = None, cwd: Path | None = None) -> int:
    stdout = stderr = subprocess.DEVNULL
    if log_path is not None:
        handle = log_path.open("a", encoding="utf-8")
        try:
            return subprocess.run(argv, env=env, cwd=cwd, stdout=handle, stderr=handle, check=False).returncode
        finally:
            handle.close()
    return subprocess.run(argv, env=env, cwd=cwd, stdout=stdout, stderr=stderr, check=False).returncode


def clear_dir_contents(path: Path, log_path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    okay = True
    for child in path.iterdir():
        try:
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        except FileNotFoundError:
            continue
        except OSError:
            okay = False
    if not okay:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[measure-prepare] failed to clean restore dir: {path}\n")
    return okay


def reset_repo_for_tool(tool: str, config: BenchmarkConfig, log_path: Path) -> int:
    repo = config.repo_dir_for_tool(tool)
    if _run(["sudo", "-n", "rm", "-rf", str(repo)], os.environ.copy(), log_path) != 0:
        return 1
    if _run(["sudo", "-n", "mkdir", "-p", str(repo)], os.environ.copy(), log_path) != 0:
        return 1
    if _run(["sudo", "-n", "chown", "-R", f"{config.user}:{config.user}", str(repo)], os.environ.copy(), log_path) != 0:
        return 1
    if tool == "kopia":
        if config.kopia_config.exists():
            config.kopia_config.unlink()
        if config.kopia_cache.exists():
            shutil.rmtree(config.kopia_cache)
        config.kopia_cache.mkdir(parents=True, exist_ok=True)
    return 0


def cleanup_repo_for_tool(tool: str, config: BenchmarkConfig, log_path: Path | None = None) -> int:
    repo = config.repo_dir_for_tool(tool)
    env = os.environ.copy()
    if _run(["sudo", "-n", "rm", "-rf", str(repo)], env, log_path) != 0:
        return 1
    if _run(["sudo", "-n", "mkdir", "-p", str(repo)], env, log_path) != 0:
        return 1
    return _run(["sudo", "-n", "chown", "-R", f"{config.user}:{config.user}", str(repo)], env, log_path)


def cleanup_restore_for_tool(tool: str, config: BenchmarkConfig) -> None:
    restore_dir = config.restore_dir_for_tool(tool)
    if restore_dir.exists():
        shutil.rmtree(restore_dir)
    restore_dir.mkdir(parents=True, exist_ok=True)


def init_repo_for_tool(tool: str, config: BenchmarkConfig) -> CommandSpec:
    if tool == "vykar":
        return CommandSpec(["vykar", "init", "-R", "bench"])
    if tool == "restic":
        return CommandSpec(["restic", "init"])
    if tool == "rustic":
        return CommandSpec(["rustic", "init"])
    if tool == "borg":
        return CommandSpec(["borg", "init", "--encryption=repokey-blake2"])
    return CommandSpec(
        [
            "kopia",
            "--config-file",
            str(config.kopia_config),
            "repository",
            "create",
            "filesystem",
            f"--path={config.kopia_repo}",
            f"--cache-directory={config.kopia_cache}",
        ]
    )


def post_init_for_tool(tool: str, config: BenchmarkConfig) -> CommandSpec | None:
    if tool != "kopia":
        return None
    return CommandSpec(
        [
            "kopia",
            "--config-file",
            str(config.kopia_config),
            "policy",
            "set",
            "--global",
            "--compression=zstd",
        ]
    )


def seed_backup_for_tool(tool: str, config: BenchmarkConfig) -> CommandSpec:
    src = str(config.dataset_snapshot1)
    if tool == "vykar":
        return CommandSpec(["vykar", "backup", "-R", "bench", "-l", "bench", src])
    if tool == "restic":
        return CommandSpec(["restic", "backup", src])
    if tool == "rustic":
        return CommandSpec(["rustic", "backup", src])
    if tool == "borg":
        return CommandSpec(["borg", "create", "--compression", "zstd,3", f"::bench-seed", src])
    return CommandSpec(["kopia", "--config-file", str(config.kopia_config), "snapshot", "create", src])


def resolve_latest_vykar_snapshot(config: BenchmarkConfig, env: dict[str, str]) -> str | None:
    proc = subprocess.run(
        ["vykar", "list", "-R", "bench", "--last", "1"],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    return lines[1].split()[0]


def resolve_latest_borg_archive(env: dict[str, str]) -> str | None:
    proc = subprocess.run(
        ["borg", "list", "--short"],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return lines[-1] if lines else None


def measurement_command(tool: str, phase: str, config: BenchmarkConfig, state: RunState) -> CommandSpec:
    dataset = str(config.dataset_benchmark)
    restore_dir = str(config.restore_dir_for_tool(tool))
    if tool == "vykar" and phase == "backup":
        return CommandSpec(["vykar", "backup", "-R", "bench", "-l", "bench", dataset])
    if tool == "vykar" and phase == "restore":
        return CommandSpec(["vykar", "restore", "-R", "bench", state.vykar_restore_snapshot, restore_dir])
    if tool == "restic" and phase == "backup":
        return CommandSpec(["restic", "backup", dataset])
    if tool == "restic" and phase == "restore":
        return CommandSpec(["restic", "restore", "latest", "--target", restore_dir])
    if tool == "rustic" and phase == "backup":
        return CommandSpec(["rustic", "backup", dataset])
    if tool == "rustic" and phase == "restore":
        return CommandSpec(["rustic", "restore", "latest", restore_dir])
    if tool == "borg" and phase == "backup":
        return CommandSpec(["borg", "create", "--compression", "zstd,3", f"::{state.borg_backup_archive}", dataset])
    if tool == "borg" and phase == "restore":
        return CommandSpec(["borg", "extract", f"::{state.borg_restore_archive}"], cwd=config.restore_dir_for_tool(tool))
    if tool == "kopia" and phase == "backup":
        return CommandSpec(["kopia", "--config-file", str(config.kopia_config), "snapshot", "create", dataset])
    return CommandSpec(
        [
            "kopia",
            "--config-file",
            str(config.kopia_config),
            "snapshot",
            "restore",
            dataset,
            restore_dir,
            "--snapshot-time",
            "latest",
        ]
    )


def commands_manifest_entries(config: BenchmarkConfig) -> list[str]:
    entries: list[str] = []
    for tool in config.selected_tools:
        for phase in ("backup", "restore"):
            op = f"{tool}_{phase}"
            entries.append(f"{op}: {' '.join(measurement_command(tool, phase, config, RunState()).argv)}")
    return entries
