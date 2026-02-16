#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import pathlib
import shutil
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

TOOLS: List[str] = ["vger", "restic", "rustic", "borg"]

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
    for key, label in TIMEV_FIELDS.items():
        out[key] = raw.get(label, "NA")
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
    if not s or s == "NA":
        return None
    parts = s.split(":")
    try:
        if len(parts) == 2:
            mins = int(parts[0])
            secs = float(parts[1])
            return mins * 60.0 + secs
        if len(parts) == 3:
            hours = int(parts[0])
            mins = int(parts[1])
            secs = float(parts[2])
            return hours * 3600.0 + mins * 60.0 + secs
    except ValueError:
        return None
    return None


def parse_float(s: str) -> float | None:
    if not s or s == "NA":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_int(s: str) -> int | None:
    if not s or s == "NA":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_cpu_pct_value(s: str) -> float | None:
    if not s or s == "NA":
        return None
    clean = s.strip().replace("%", "")
    return parse_float(clean)


def get_dataset_bytes(root: pathlib.Path) -> int | None:
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


def build_records(root: pathlib.Path) -> tuple[List[dict], int | None]:
    repo_sizes = parse_repo_sizes(root / "repo-sizes.txt")
    dataset_bytes = get_dataset_bytes(root)
    dataset_mib = (dataset_bytes / (1024.0 * 1024.0)) if dataset_bytes else None

    records: List[dict] = []
    for op in OPS:
        tool, phase = op.split("_", 1)
        t = parse_timev(root / f"profile.{op}" / "timev.txt")
        duration_s = parse_elapsed_seconds(t.get("elapsed", "NA"))
        throughput_mib_s: float | None = None
        if dataset_mib is not None and duration_s and duration_s > 0:
            throughput_mib_s = dataset_mib / duration_s

        records.append(
            {
                "op": op,
                "tool": tool,
                "phase": phase,
                "duration_s": duration_s,
                "throughput_mib_s": throughput_mib_s,
                "cpu_pct_raw": t.get("cpu_pct", "NA"),
                "cpu_pct_value": parse_cpu_pct_value(t.get("cpu_pct", "NA")),
                "user_s": parse_float(t.get("user_s", "NA")),
                "sys_s": parse_float(t.get("sys_s", "NA")),
                "maxrss_kb": parse_int(t.get("max_rss_kb", "NA")),
                "fs_in": parse_int(t.get("fs_in", "NA")),
                "fs_out": parse_int(t.get("fs_out", "NA")),
                "repo_size": repo_sizes.get(tool, "NA"),
                "exit_status": parse_int(t.get("exit_status", "NA")),
            }
        )
    return records, dataset_bytes


def fmt_float(v: float | None, digits: int) -> str:
    if v is None:
        return "NA"
    return f"{v:.{digits}f}"


def fmt_int(v: int | None) -> str:
    if v is None:
        return "NA"
    return str(v)


def records_tsv(records: List[dict]) -> str:
    lines = [
        "op\tduration_s\tthroughput_mib_s\tcpu%\tuser_s\tsys_s\tmaxrss_kb\tfs_in\tfs_out\trepo_size\texit"
    ]
    for r in records:
        lines.append(
            "\t".join(
                [
                    r["op"],
                    fmt_float(r["duration_s"], 2),
                    fmt_float(r["throughput_mib_s"], 1),
                    r["cpu_pct_raw"],
                    fmt_float(r["user_s"], 2),
                    fmt_float(r["sys_s"], 2),
                    fmt_int(r["maxrss_kb"]),
                    fmt_int(r["fs_in"]),
                    fmt_int(r["fs_out"]),
                    r["repo_size"],
                    fmt_int(r["exit_status"]),
                ]
            )
        )
    return "\n".join(lines) + "\n"


def records_markdown(records: List[dict]) -> str:
    lines = [
        "| op | duration_s | throughput_mib_s | cpu% | user_s | sys_s | maxrss_kb | fs_in | fs_out | repo_size | exit |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in records:
        lines.append(
            "| "
            + " | ".join(
                [
                    r["op"],
                    fmt_float(r["duration_s"], 2),
                    fmt_float(r["throughput_mib_s"], 1),
                    r["cpu_pct_raw"],
                    fmt_float(r["user_s"], 2),
                    fmt_float(r["sys_s"], 2),
                    fmt_int(r["maxrss_kb"]),
                    fmt_int(r["fs_in"]),
                    fmt_int(r["fs_out"]),
                    r["repo_size"],
                    fmt_int(r["exit_status"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_summary_outputs(out_dir: pathlib.Path, records: List[dict], dataset_bytes: int | None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.tsv").write_text(records_tsv(records))
    (out_dir / "summary.md").write_text(records_markdown(records))
    payload = {
        "dataset_bytes": dataset_bytes,
        "records": records,
    }
    (out_dir / "summary.json").write_text(json.dumps(payload, indent=2))


def print_summary(root: pathlib.Path, records: List[dict], dataset_bytes: int | None) -> None:
    print(f"root: {root}")
    if dataset_bytes is not None:
        print(f"dataset_bytes: {dataset_bytes}")
    print(
        "op\tduration_s\tthroughput_mib_s\tcpu%\tuser_s\tsys_s\tmaxrss_kb\tfs_in\tfs_out\trepo_size\texit"
    )
    for r in records:
        print(
            "\t".join(
                [
                    r["op"],
                    fmt_float(r["duration_s"], 2),
                    fmt_float(r["throughput_mib_s"], 1),
                    r["cpu_pct_raw"],
                    fmt_float(r["user_s"], 2),
                    fmt_float(r["sys_s"], 2),
                    fmt_int(r["maxrss_kb"]),
                    fmt_int(r["fs_in"]),
                    fmt_int(r["fs_out"]),
                    r["repo_size"],
                    fmt_int(r["exit_status"]),
                ]
            )
        )


def _chart_values(records: List[dict], metric: str) -> dict:
    by_tool_phase: dict = {(r["tool"], r["phase"]): r for r in records}
    backup: List[float] = []
    restore: List[float] = []
    for tool in TOOLS:
        b = by_tool_phase.get((tool, "backup"))
        r = by_tool_phase.get((tool, "restore"))
        bv = b.get(metric) if b else None
        rv = r.get(metric) if r else None
        backup.append(float(bv) if bv is not None else math.nan)
        restore.append(float(rv) if rv is not None else math.nan)
    return {"backup": backup, "restore": restore}


def generate_chart_with_deps(
    root: pathlib.Path,
    out_file: pathlib.Path,
    records: List[dict],
    no_uv_bootstrap: bool = False,
) -> int:
    try:
        import matplotlib.gridspec as gridspec
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Patch
    except ModuleNotFoundError as e:
        if no_uv_bootstrap:
            print(f"chart dependencies missing: {e}", file=sys.stderr)
            return 2
        uv = shutil.which("uv")
        if not uv:
            print("chart dependencies missing and 'uv' is not installed", file=sys.stderr)
            return 2
        cmd = [
            uv,
            "run",
            "--with",
            "numpy>=2.0,<3",
            "--with",
            "matplotlib>=3.9,<4",
            "python3",
            str(pathlib.Path(__file__).resolve()),
            "chart",
            str(root),
            "--chart-file",
            str(out_file),
            "--no-uv-bootstrap",
        ]
        return subprocess.call(cmd)

    tools_display = ["V'Ger", "Restic", "Rustic", "Borg"]
    throughput = _chart_values(records, "throughput_mib_s")
    duration = _chart_values(records, "duration_s")
    cpu = _chart_values(records, "cpu_pct_value")
    memory = _chart_values(records, "maxrss_kb")
    memory["backup"] = [v / 1024.0 if not math.isnan(v) else v for v in memory["backup"]]
    memory["restore"] = [v / 1024.0 if not math.isnan(v) else v for v in memory["restore"]]

    # Match the original benchmark chart style.
    VGER = "#fb8c00"
    VGER_LIGHT = "#ffb74d"
    OTHER = "#546e7a"
    OTHER_LIGHT = "#78909c"
    BG = "#1a2327"

    plt.rcParams.update(
        {
            "font.family": "monospace",
            "figure.facecolor": BG,
            "axes.facecolor": BG,
            "text.color": "#eceff1",
            "axes.labelcolor": "#b0bec5",
            "xtick.color": "#90a4ae",
            "ytick.color": "#90a4ae",
        }
    )

    x = np.arange(len(TOOLS))
    w = 0.32

    backup_colors = [VGER if t == "V'Ger" else OTHER for t in tools_display]
    restore_colors = [VGER_LIGHT if t == "V'Ger" else OTHER_LIGHT for t in tools_display]

    def style_axis(ax, title: str, higher_is_better: bool) -> None:
        ax.set_xticks(x)
        ax.set_xticklabels(tools_display, fontsize=10)
        for label in ax.get_xticklabels():
            if "V'Ger" in label.get_text():
                label.set_color(VGER)
                label.set_fontweight("bold")
        arrow = "↑" if higher_is_better else "↓"
        qualifier = "higher is better" if higher_is_better else "lower is better"
        ax.set_title(f"{title}  ({arrow} {qualifier})", fontsize=10, color="#cfd8dc", pad=10)
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#2e3d44", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#2a2a40")
        ax.spines["bottom"].set_color("#2a2a40")

    def draw_standard_panel(ax, title: str, vals: dict, higher_is_better: bool) -> None:
        backup_vals = np.array(vals["backup"], dtype=float)
        restore_vals = np.array(vals["restore"], dtype=float)
        bars1 = ax.bar(x - w / 2, backup_vals, w, color=backup_colors, zorder=3)
        bars2 = ax.bar(x + w / 2, restore_vals, w, color=restore_colors, zorder=3)

        finite = np.concatenate([backup_vals[np.isfinite(backup_vals)], restore_vals[np.isfinite(restore_vals)]])
        ymax = float(finite.max()) if finite.size else 1.0
        ax.set_ylim(0, ymax * 1.18)

        for bars in (bars1, bars2):
            for bar in bars:
                h = float(bar.get_height())
                if not np.isfinite(h):
                    continue
                label = f"{h:.0f}" if h >= 10 else f"{h:.1f}"
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h + ymax * 0.01,
                    label,
                    ha="center",
                    va="bottom",
                    fontsize=7.5,
                    color="#b0bec5",
                    fontweight="bold",
                )

        style_axis(ax, title, higher_is_better)

    def draw_memory_broken_panel(ax_top, ax_bot, vals: dict) -> None:
        backup_vals = np.array(vals["backup"], dtype=float)
        restore_vals = np.array(vals["restore"], dtype=float)

        for ax in (ax_top, ax_bot):
            ax.bar(x - w / 2, backup_vals, w, color=backup_colors, zorder=3)
            ax.bar(x + w / 2, restore_vals, w, color=restore_colors, zorder=3)

        finite = np.concatenate([backup_vals[np.isfinite(backup_vals)], restore_vals[np.isfinite(restore_vals)]])
        max_val = float(finite.max()) if finite.size else 1.0
        lower_cap = 1100.0
        upper_min = max(lower_cap * 1.2, max_val * 0.9)
        upper_max = max(upper_min + 200.0, max_val * 1.06)

        ax_bot.set_ylim(0, lower_cap)
        ax_top.set_ylim(upper_min, upper_max)

        ax_top.spines["bottom"].set_visible(False)
        ax_bot.spines["top"].set_visible(False)
        ax_top.tick_params(bottom=False, labelbottom=False)

        zigzag_n = 12
        zx = np.linspace(-0.6, len(tools_display) - 0.4, zigzag_n * 2 + 1)
        zy_amp_b = (ax_bot.get_ylim()[1] - ax_bot.get_ylim()[0]) * 0.018
        zy_bot = [ax_bot.get_ylim()[1] + (zy_amp_b if i % 2 == 0 else -zy_amp_b) for i in range(len(zx))]
        ax_bot.plot(zx, zy_bot, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

        zy_amp_t = (ax_top.get_ylim()[1] - ax_top.get_ylim()[0]) * 0.018
        zy_top = [ax_top.get_ylim()[0] + (zy_amp_t if i % 2 == 0 else -zy_amp_t) for i in range(len(zx))]
        ax_top.plot(zx, zy_top, color="#78909c", linewidth=0.7, clip_on=False, zorder=10)

        for i in range(len(tools_display)):
            for values, x_off in ((backup_vals, -w / 2), (restore_vals, w / 2)):
                v = float(values[i])
                if not np.isfinite(v):
                    continue
                if v > lower_cap:
                    ax_top.text(
                        x[i] + x_off,
                        v + (upper_max - upper_min) * 0.03,
                        f"{v:.0f}",
                        ha="center",
                        va="bottom",
                        fontsize=7.5,
                        color="#ff6666",
                        fontweight="bold",
                    )
                else:
                    ax_bot.text(
                        x[i] + x_off,
                        v + lower_cap * 0.012,
                        f"{v:.0f}",
                        ha="center",
                        va="bottom",
                        fontsize=7.5,
                        color="#b0bec5",
                        fontweight="bold",
                    )

        ax_bot.set_xticks(x)
        ax_bot.set_xticklabels(tools_display, fontsize=10)
        for label in ax_bot.get_xticklabels():
            if "V'Ger" in label.get_text():
                label.set_color(VGER)
                label.set_fontweight("bold")
        ax_top.set_xticks([])
        ax_top.set_title("Peak Memory (MB)  (↓ lower is better)", fontsize=10, color="#cfd8dc", pad=10)

        for ax in (ax_top, ax_bot):
            ax.set_axisbelow(True)
            ax.grid(axis="y", color="#2e3d44", linewidth=0.5)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.spines["left"].set_color("#2a2a40")
            ax.spines["bottom"].set_color("#2a2a40")
        ax_top.spines["bottom"].set_visible(False)
        ax_bot.spines["top"].set_visible(False)

    dataset_bytes = get_dataset_bytes(root)
    dataset_gib = (dataset_bytes / (1024.0 ** 3)) if dataset_bytes else None
    dataset_label = f"~{dataset_gib:.0f} GiB dataset" if dataset_gib else "dataset"

    fig = plt.figure(figsize=(11, 8.5))
    fig.suptitle(
        f"Backup Tool Benchmark  ·  {dataset_label}",
        fontsize=16,
        fontweight="bold",
        color="#e8e8f0",
        y=0.97,
    )

    outer = gridspec.GridSpec(
        2, 2, figure=fig, hspace=0.38, wspace=0.28, left=0.06, right=0.97, top=0.90, bottom=0.09
    )
    draw_standard_panel(fig.add_subplot(outer[0, 0]), "Throughput (MiB/s)", throughput, True)
    draw_standard_panel(fig.add_subplot(outer[0, 1]), "Duration (s)", duration, False)
    draw_standard_panel(fig.add_subplot(outer[1, 0]), "CPU Usage (%)", cpu, False)

    inner = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=outer[1, 1], height_ratios=[1, 3], hspace=0.08)
    draw_memory_broken_panel(fig.add_subplot(inner[0]), fig.add_subplot(inner[1]), memory)

    legend_elements = [
        Patch(facecolor=VGER, label="V'Ger backup"),
        Patch(facecolor=VGER_LIGHT, label="V'Ger restore"),
        Patch(facecolor=OTHER, label="Others backup"),
        Patch(facecolor=OTHER_LIGHT, label="Others restore"),
    ]
    fig.legend(
        handles=legend_elements,
        loc="lower center",
        ncol=4,
        frameon=True,
        fontsize=9,
        fancybox=False,
        edgecolor="#4a5d66",
        facecolor="#2c3940",
        labelcolor="#c8c8d4",
        bbox_to_anchor=(0.5, 0.005),
    )

    out_file.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_file, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"chart: {out_file}")
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.root).expanduser()
    if not root.is_dir():
        print(f"root dir not found: {root}", file=sys.stderr)
        return 2
    records, dataset_bytes = build_records(root)
    print_summary(root, records, dataset_bytes)
    if args.write_files:
        out_dir = pathlib.Path(args.out_dir).expanduser()
        write_summary_outputs(out_dir, records, dataset_bytes)
        print(f"summary files: {out_dir}")
    return 0


def cmd_chart(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.root).expanduser()
    if not root.is_dir():
        print(f"root dir not found: {root}", file=sys.stderr)
        return 2
    out_file = pathlib.Path(args.chart_file).expanduser()
    records, _dataset_bytes = build_records(root)
    return generate_chart_with_deps(root, out_file, records, no_uv_bootstrap=args.no_uv_bootstrap)


def cmd_all(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.root).expanduser()
    if not root.is_dir():
        print(f"root dir not found: {root}", file=sys.stderr)
        return 2

    out_dir = pathlib.Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    chart_file = pathlib.Path(args.chart_file).expanduser()

    records, dataset_bytes = build_records(root)
    print_summary(root, records, dataset_bytes)
    write_summary_outputs(out_dir, records, dataset_bytes)
    print(f"summary files: {out_dir}")

    rc = generate_chart_with_deps(root, chart_file, records, no_uv_bootstrap=False)
    if rc != 0:
        (out_dir / "chart_error.txt").write_text("failed to generate chart\n")
        return rc
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Benchmark summary/chart reporter")
    sub = p.add_subparsers(dest="command", required=True)

    ps = sub.add_parser("summary", help="print and optionally write summary tables")
    ps.add_argument("root", help="benchmark root directory")
    ps.add_argument("--out-dir", default="", help="output dir for summary files")
    ps.add_argument("--write-files", action="store_true", help="write summary.{md,tsv,json}")
    ps.set_defaults(func=cmd_summary)

    pc = sub.add_parser("chart", help="generate chart PNG")
    pc.add_argument("root", help="benchmark root directory")
    pc.add_argument(
        "--chart-file",
        default="",
        help="output chart path (default: <root>/reports/benchmark.summary.png)",
    )
    pc.add_argument("--no-uv-bootstrap", action="store_true", help=argparse.SUPPRESS)
    pc.set_defaults(func=cmd_chart)

    pa = sub.add_parser("all", help="summary + chart")
    pa.add_argument("root", help="benchmark root directory")
    pa.add_argument("--out-dir", default="", help="output dir for report files")
    pa.add_argument(
        "--chart-file",
        default="",
        help="output chart path (default: <out-dir>/benchmark.summary.png)",
    )
    pa.set_defaults(func=cmd_all)

    return p


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    root = pathlib.Path(getattr(args, "root", ".")).expanduser()
    default_out_dir = root / "reports"
    if hasattr(args, "out_dir") and not args.out_dir:
        args.out_dir = str(default_out_dir)
    if hasattr(args, "chart_file") and not args.chart_file:
        if hasattr(args, "out_dir") and args.out_dir:
            args.chart_file = str(pathlib.Path(args.out_dir).expanduser() / "benchmark.summary.png")
        else:
            args.chart_file = str(default_out_dir / "benchmark.summary.png")

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
