#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import subprocess
import sys
from typing import Dict, List


OPS: List[str] = [
    "vger_backup",
    "vger_restore",
    "restic_backup",
    "restic_restore",
    "rustic_backup",
    "rustic_restore",
    "borg_backup",
    "borg_restore",
]


TIMEV_FIELDS = {
    "elapsed": "Elapsed (wall clock) time (h:mm:ss or m:ss)",
    "cpu_pct": "Percent of CPU this job got",
    "user_s": "User time (seconds)",
    "sys_s": "System time (seconds)",
    "max_rss_kb": "Maximum resident set size (kbytes)",
    "fs_in": "File system inputs",
    "fs_out": "File system outputs",
    "exit_status": "Exit status",
}


def parse_kv_file(path: pathlib.Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        if ": " not in line:
            continue
        k, v = line.split(": ", 1)
        out[k.strip()] = v.strip()
    return out


def parse_timev(path: pathlib.Path) -> Dict[str, str]:
    raw = parse_kv_file(path)
    out: Dict[str, str] = {}
    for k, label in TIMEV_FIELDS.items():
        out[k] = raw.get(label, "NA")
    return out


def parse_repo_sizes(path: pathlib.Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    tool_map = {
        "bench-vger": "vger",
        "bench-restic": "restic",
        "bench-rustic": "rustic",
        "bench-borg": "borg",
    }
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        size = parts[0]
        repo_path = parts[1]
        for suffix, tool in tool_map.items():
            if repo_path.endswith(suffix):
                out[tool] = size
                break
    return out


def parse_elapsed_seconds(s: str) -> float | None:
    # Supports m:ss(.ff) and h:mm:ss(.ff)
    if not s or s == "NA":
        return None
    parts = s.split(":")
    try:
        if len(parts) == 2:
            m = int(parts[0])
            sec = float(parts[1])
            return m * 60.0 + sec
        if len(parts) == 3:
            h = int(parts[0])
            m = int(parts[1])
            sec = float(parts[2])
            return h * 3600.0 + m * 60.0 + sec
    except ValueError:
        return None
    return None


def format_float(v: float | None, ndigits: int) -> str:
    if v is None:
        return "NA"
    return f"{v:.{ndigits}f}"


def get_dataset_bytes(root: pathlib.Path) -> int | None:
    # Read dataset path from profile metadata, then use du -sb for exact byte size.
    meta = root / "profile.vger_backup" / "meta.txt"
    if not meta.exists():
        return None
    dataset: str | None = None
    for line in meta.read_text(errors="replace").splitlines():
        if line.startswith("dataset="):
            dataset = line.split("=", 1)[1].strip()
            break
    if not dataset:
        return None
    try:
        out = subprocess.check_output(["du", "-sb", dataset], text=True).strip()
        return int(out.split()[0])
    except Exception:
        return None


def main(root: str) -> None:
    r = pathlib.Path(root).expanduser()
    if not r.is_dir():
        print(f"root dir not found: {r}", file=sys.stderr)
        raise SystemExit(2)

    repo_sizes = parse_repo_sizes(r / "repo-sizes.txt")
    dataset_bytes = get_dataset_bytes(r)
    dataset_mib = dataset_bytes / (1024.0 * 1024.0) if dataset_bytes else None

    print(f"root: {r}")
    if dataset_bytes is not None:
        print(f"dataset_bytes: {dataset_bytes}")
    print(
        "op\tduration_s\tthroughput_mib_s\tcpu%\tuser_s\tsys_s\tmaxrss_kb\tfs_in\tfs_out\trepo_size\texit"
    )

    for op in OPS:
        t = parse_timev(r / f"profile.{op}" / "timev.txt")
        tool = op.split("_", 1)[0]
        elapsed_s = parse_elapsed_seconds(t.get("elapsed", "NA"))
        throughput = None
        if dataset_mib is not None and elapsed_s and elapsed_s > 0:
            throughput = dataset_mib / elapsed_s

        row = [
            op,
            format_float(elapsed_s, 2),
            format_float(throughput, 1),
            t.get("cpu_pct", "NA"),
            t.get("user_s", "NA"),
            t.get("sys_s", "NA"),
            t.get("max_rss_kb", "NA"),
            t.get("fs_in", "NA"),
            t.get("fs_out", "NA"),
            repo_sizes.get(tool, "NA"),
            t.get("exit_status", "NA"),
        ]
        print("\t".join(row))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <benchmark_out_root>", file=sys.stderr)
        raise SystemExit(2)
    main(sys.argv[1])
