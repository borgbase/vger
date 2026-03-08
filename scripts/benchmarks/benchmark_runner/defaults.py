"""Configuration defaults and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import os
import shutil


TOOLS = ("vykar", "restic", "rustic", "borg", "kopia")
PHASES = ("backup", "restore")


class ConfigError(ValueError):
    """Raised when benchmark configuration is invalid."""


@dataclass(frozen=True)
class BenchmarkConfig:
    runs: int
    dataset_dir: Path
    selected_tools: tuple[str, ...]
    selected_tool_arg: str | None
    repo_root: Path
    runtime_root: Path
    out_root: Path
    logs_dir: Path
    passphrase: str
    user: str
    vykar_repo: Path
    restic_repo: Path
    rustic_repo: Path
    borg_repo: Path
    kopia_repo: Path
    kopia_config: Path
    kopia_cache: Path
    vykar_config_path: Path
    restore_dirs: dict[str, Path] = field(default_factory=dict)

    @property
    def dataset_snapshot1(self) -> Path:
        return self.dataset_dir / "snapshot-1"

    @property
    def dataset_snapshot2(self) -> Path:
        return self.dataset_dir / "snapshot-2"

    @property
    def dataset_benchmark(self) -> Path:
        return self.dataset_dir

    def repo_dir_for_tool(self, tool: str) -> Path:
        return {
            "vykar": self.vykar_repo,
            "restic": self.restic_repo,
            "rustic": self.rustic_repo,
            "borg": self.borg_repo,
            "kopia": self.kopia_repo,
        }[tool]

    def restore_dir_for_tool(self, tool: str) -> Path:
        return self.restore_dirs[tool]


def default_dataset_dir() -> Path:
    return Path.home() / "corpus-local"


def resolve_selected_tools(tool: str | None) -> tuple[str, ...]:
    if tool is None:
        return TOOLS
    if tool not in TOOLS:
        raise ConfigError(f"--tool must be one of: {', '.join(TOOLS)}")
    return (tool,)


def ensure_required_commands(selected_tools: tuple[str, ...]) -> None:
    if not Path("/usr/bin/time").exists():
        raise ConfigError("missing required command: /usr/bin/time")
    for tool in selected_tools:
        if shutil.which(tool) is None:
            raise ConfigError(f"missing required command: {tool}")


def build_config(*, runs: int, tool: str | None, dataset: str | None) -> BenchmarkConfig:
    if runs <= 0:
        raise ConfigError("--runs must be a positive integer")

    dataset_dir = Path(dataset).expanduser() if dataset else default_dataset_dir()
    dataset_dir = dataset_dir.resolve()
    if not dataset_dir.is_dir():
        raise ConfigError(f"dataset dir not found: {dataset_dir}")
    if not (dataset_dir / "snapshot-1").is_dir():
        raise ConfigError(f"missing required seed folder: {dataset_dir / 'snapshot-1'}")
    if not (dataset_dir / "snapshot-2").is_dir():
        raise ConfigError(f"missing required benchmark folder: {dataset_dir / 'snapshot-2'}")

    selected_tools = resolve_selected_tools(tool)
    ensure_required_commands(selected_tools)

    repo_root = Path(os.environ.get("REPO_ROOT", "/mnt/repos")).expanduser().resolve()
    runtime_root = Path(os.environ.get("RUNTIME_ROOT", str(Path.home() / "runtime"))).expanduser().resolve()
    passphrase = os.environ.get("PASSPHRASE", "123")
    user = os.environ.get("USER", "unknown")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_root = runtime_root / "benchmarks" / stamp
    logs_dir = out_root / "logs"

    return BenchmarkConfig(
        runs=runs,
        dataset_dir=dataset_dir,
        selected_tools=selected_tools,
        selected_tool_arg=tool,
        repo_root=repo_root,
        runtime_root=runtime_root,
        out_root=out_root,
        logs_dir=logs_dir,
        passphrase=passphrase,
        user=user,
        vykar_repo=repo_root / "bench-vykar",
        restic_repo=repo_root / "bench-restic",
        rustic_repo=repo_root / "bench-rustic",
        borg_repo=repo_root / "bench-borg",
        kopia_repo=repo_root / "bench-kopia",
        kopia_config=out_root / "kopia.repository.config",
        kopia_cache=out_root / "kopia-cache",
        vykar_config_path=out_root / "vykar.bench.yaml",
        restore_dirs={
            "vykar": out_root / "restore-vykar",
            "restic": out_root / "restore-restic",
            "rustic": out_root / "restore-rustic",
            "borg": out_root / "restore-borg",
            "kopia": out_root / "restore-kopia",
        },
    )
