#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import re
import sys
from typing import Dict, List, Tuple


TIMEV_FIELDS = {
    "elapsed": "Elapsed (wall clock) time (h:mm:ss or m:ss)",
    "cpu_pct": "Percent of CPU this job got",
    "user_s": "User time (seconds)",
    "sys_s": "System time (seconds)",
    "max_rss_kb": "Maximum resident set size (kbytes)",
    "maj_faults": "Major (requiring I/O) page faults",
    "min_faults": "Minor (reclaiming a frame) page faults",
    "vol_csw": "Voluntary context switches",
    "invol_csw": "Involuntary context switches",
    "fs_in": "File system inputs",
    "fs_out": "File system outputs",
    "exit_status": "Exit status",
}


def parse_kv_file(path: pathlib.Path) -> Dict[str, str]:
    d: Dict[str, str] = {}
    for line in path.read_text(errors="replace").splitlines():
        # /usr/bin/time -v keys may include ':' internally (e.g. h:mm:ss),
        # so split only on the first ': '.
        if ": " not in line:
            continue
        k, v = line.split(": ", 1)
        d[k.strip()] = v.strip()
    return d


def parse_timev(path: pathlib.Path) -> Dict[str, str]:
    raw = parse_kv_file(path)
    out: Dict[str, str] = {}
    for k, label in TIMEV_FIELDS.items():
        out[k] = raw.get(label, "NA")
    return out


def parse_perf_stat(path: pathlib.Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([0-9,.\u2212-]+|<not supported>|<not counted>)\s+([A-Za-z0-9_./-]+)\b", line)
        if not m:
            continue
        val, ev = m.group(1), m.group(2)
        out[ev] = val
    return out


def parse_strace_summary(path: pathlib.Path, top_n: int = 5) -> List[Tuple[str, str, str]]:
    if not path.exists():
        return []
    rows: List[Tuple[str, str, str]] = []
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("% time") or line.startswith("------"):
            continue
        parts = re.split(r"\s+", line)
        if len(parts) < 5:
            continue
        pct, seconds = parts[0], parts[1]
        syscall = parts[-1]
        if not re.match(r"^[0-9.]+$", pct):
            continue
        rows.append((syscall, pct, seconds))
    return rows[:top_n]


def fmt(s: str) -> str:
    return s if s and s != "NA" else "NA"


def main(root: str) -> int:
    r = pathlib.Path(root).expanduser()
    if not r.is_dir():
        print(f"root dir not found: {r}", file=sys.stderr)
        return 2

    ops = [
        "vger_backup",
        "vger_restore",
        "restic_backup",
        "restic_restore",
        "rustic_backup",
        "rustic_restore",
        "borg_backup",
        "borg_restore",
    ]

    print(f"root: {r}")
    print()
    header = [
        "op",
        "wall",
        "cpu%",
        "user",
        "sys",
        "maxrss_kb",
        "majF",
        "minF",
        "cswV",
        "cswI",
        "fs_in",
        "fs_out",
        "exit",
    ]
    print("\t".join(header))

    for op in ops:
        d = r / f"profile.{op}"
        t = parse_timev(d / "timev.txt")
        row = [
            op,
            fmt(t["elapsed"]),
            fmt(t["cpu_pct"]),
            fmt(t["user_s"]),
            fmt(t["sys_s"]),
            fmt(t["max_rss_kb"]),
            fmt(t["maj_faults"]),
            fmt(t["min_faults"]),
            fmt(t["vol_csw"]),
            fmt(t["invol_csw"]),
            fmt(t["fs_in"]),
            fmt(t["fs_out"]),
            fmt(t["exit_status"]),
        ]
        print("\t".join(row))

    print()
    print("perf(stat) key events (if present): task-clock, cycles, instructions, cache-misses")
    for op in ops:
        d = r / f"profile.{op}"
        p = parse_perf_stat(d / "perf.stat.txt")
        if not p:
            continue
        pick = {k: p.get(k, "NA") for k in ["task-clock", "cycles", "instructions", "cache-misses"]}
        print(f"{op}: " + ", ".join(f"{k}={v}" for k, v in pick.items()))

    print()
    print("strace -c top syscalls by %time (very noisy/intrusive; interpret cautiously):")
    for op in ops:
        d = r / f"profile.{op}"
        top = parse_strace_summary(d / "strace.summary.txt", top_n=5)
        if not top:
            continue
        s = " ".join([f"{sc}:{pct}%" for (sc, pct, _sec) in top])
        print(f"{op}: {s}")

    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <benchmark_root>", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
