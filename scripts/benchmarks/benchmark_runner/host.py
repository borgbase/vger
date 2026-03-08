"""Host preparation helpers for benchmark runs."""

from __future__ import annotations

from pathlib import Path
import subprocess
import time


def append_log(log_path: Path, message: str) -> None:
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{message}\n")


def _capture_text(argv: list[str]) -> str:
    try:
        return subprocess.check_output(argv, text=True, stderr=subprocess.DEVNULL).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""


def mount_target_for_path(path: Path) -> str | None:
    if subprocess.run(["bash", "-lc", "command -v findmnt >/dev/null 2>&1"], check=False).returncode == 0:
        out = _capture_text(["findmnt", "-n", "-o", "TARGET", "--target", str(path)])
        return out.splitlines()[0] if out else None
    out = _capture_text(["df", "-P", str(path)])
    lines = out.splitlines()
    if len(lines) < 2:
        return None
    parts = lines[1].split()
    return parts[5] if len(parts) >= 6 else None


def mount_source_for_path(path: Path) -> str | None:
    if subprocess.run(["bash", "-lc", "command -v findmnt >/dev/null 2>&1"], check=False).returncode == 0:
        out = _capture_text(["findmnt", "-n", "-o", "SOURCE", "--target", str(path)])
        return out.splitlines()[0] if out else None
    out = _capture_text(["df", "-P", str(path)])
    lines = out.splitlines()
    if len(lines) < 2:
        return None
    parts = lines[1].split()
    return parts[0] if parts else None


def nvme_namespace_for_source(source: str | None) -> str | None:
    if not source:
        return None
    if source.startswith("/dev/nvme") and "p" in source:
        return source.rsplit("p", 1)[0]
    if source.startswith("/dev/nvme"):
        return source
    return None


def build_storage_settle_targets(paths: list[Path]) -> tuple[list[str], list[str]]:
    trim_mounts: list[str] = []
    nvme_devices: list[str] = []
    for path in paths:
        mount = mount_target_for_path(path)
        if mount and mount not in trim_mounts:
            trim_mounts.append(mount)
        source = mount_source_for_path(path)
        nvme_device = nvme_namespace_for_source(source)
        if nvme_device and nvme_device not in nvme_devices:
            nvme_devices.append(nvme_device)
    return trim_mounts, nvme_devices


def _run_logged(argv: list[str], log_path: Path) -> bool:
    with log_path.open("a", encoding="utf-8") as handle:
        rc = subprocess.run(argv, stdout=handle, stderr=handle, check=False).returncode
    return rc == 0


def run_storage_settle(log_path: Path, trim_mounts: list[str], nvme_devices: list[str]) -> None:
    append_log(log_path, "[measure-prepare] sync storage")
    _run_logged(["sync"], log_path)

    if trim_mounts:
        for mount in trim_mounts:
            if not _run_logged(["sudo", "-n", "fstrim", "-v", mount], log_path):
                append_log(log_path, f"[measure-prepare] fstrim skipped/failed mount={mount}")
    else:
        append_log(log_path, "[measure-prepare] fstrim skipped (no mount targets resolved)")

    if subprocess.run(["bash", "-lc", "command -v nvme >/dev/null 2>&1"], check=False).returncode == 0:
        if nvme_devices:
            for dev in nvme_devices:
                if not _run_logged(["sudo", "-n", "nvme", "flush", dev], log_path):
                    append_log(log_path, f"[measure-prepare] nvme flush skipped/failed device={dev}")
        else:
            append_log(log_path, "[measure-prepare] nvme flush skipped (no nvme devices resolved)")
    else:
        append_log(log_path, "[measure-prepare] nvme cli missing; skipping nvme flush")

    append_log(log_path, "[measure-prepare] cooldown sleep 20s")
    time.sleep(20)


def drop_caches(log_path: Path) -> None:
    _run_logged(["sync"], log_path)
    if not Path("/proc/sys/vm/drop_caches").exists():
        return
    if subprocess.run(["sudo", "-n", "true"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        append_log(log_path, "drop_caches: passwordless sudo unavailable; skipping")
        return
    with log_path.open("a", encoding="utf-8") as handle:
        subprocess.run(
            ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
            input="3\n",
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=handle,
            check=False,
        )
