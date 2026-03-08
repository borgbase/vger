"""JSON/stdout summary output."""

import json
import os
import statistics


_GIB = 1024 ** 3


def _compute_timing_stats(samples: list[float]) -> dict | None:
    if not samples:
        return None

    if len(samples) == 1:
        stdev = 0.0
    else:
        stdev = statistics.stdev(samples)

    return {
        "count": len(samples),
        "mean_sec": statistics.mean(samples),
        "stdev_sec": stdev,
        "min_sec": min(samples),
        "max_sec": max(samples),
    }


def _compute_normalized_stats(samples: list[float]) -> dict | None:
    if not samples:
        return None

    if len(samples) == 1:
        stdev = 0.0
    else:
        stdev = statistics.stdev(samples)

    mean_value = statistics.mean(samples)
    return {
        "normalized_sample_count": len(samples),
        "mean_sec_per_gib": mean_value,
        "stdev_sec_per_gib": stdev,
        "min_sec_per_gib": min(samples),
        "max_sec_per_gib": max(samples),
        "mean_gib_per_sec": 0.0 if mean_value == 0 else 1.0 / mean_value,
    }


def _phase_normalized_metric(phase: dict) -> tuple[str, float] | None:
    action = phase.get("action", "")
    duration = phase.get("duration_sec", 0.0)
    if duration <= 0:
        return None

    if action in {"backup", "verify"}:
        corpus_bytes = phase.get("corpus_bytes_at_phase_start", 0)
        if corpus_bytes <= 0:
            return None
        gib = corpus_bytes / _GIB
        return "corpus_bytes_at_phase_start", duration / gib

    if action == "churn":
        added_bytes = phase.get("stats", {}).get("added_bytes", 0)
        if added_bytes <= 0:
            return None
        gib = added_bytes / _GIB
        return "added_bytes", duration / gib

    return None


def build_summary(summaries: list[dict]) -> dict:
    """Build aggregate timing summary for runs and phases."""
    passed_runs = [s for s in summaries if s.get("passed", False)]
    run_samples = [s.get("duration_sec", 0.0) for s in passed_runs]
    run_totals = _compute_timing_stats(run_samples)
    if run_totals is None:
        run_totals = {
            "count": 0,
            "excluded_failed_runs": len(summaries),
            "mean_sec": 0.0,
            "stdev_sec": 0.0,
            "min_sec": 0.0,
            "max_sec": 0.0,
        }
    else:
        run_totals["excluded_failed_runs"] = len(summaries) - len(passed_runs)

    phase_buckets: dict[str, dict] = {}
    phase_order: list[str] = []
    for run in summaries:
        for phase in run.get("phases", []):
            action = phase.get("action", "")
            label = phase.get("label", "")
            key = action if not label else f"{action} ({label})"
            if key not in phase_buckets:
                phase_buckets[key] = {
                    "key": key,
                    "action": action,
                    "label": label,
                    "samples": [],
                    "normalized_samples": [],
                    "normalization": None,
                    "count": 0,
                    "passed_count": 0,
                    "failed_count": 0,
                }
                phase_order.append(key)

            bucket = phase_buckets[key]
            bucket["count"] += 1
            if phase.get("passed", False):
                bucket["passed_count"] += 1
                bucket["samples"].append(phase.get("duration_sec", 0.0))
                normalized = _phase_normalized_metric(phase)
                if normalized is not None:
                    normalization, value = normalized
                    bucket["normalization"] = normalization
                    bucket["normalized_samples"].append(value)
            else:
                bucket["failed_count"] += 1

    phases = []
    for key in phase_order:
        bucket = phase_buckets[key]
        stats = _compute_timing_stats(bucket.pop("samples"))
        normalized_stats = _compute_normalized_stats(bucket.pop("normalized_samples"))
        if stats is None:
            stats = {
                "mean_sec": 0.0,
                "stdev_sec": 0.0,
                "min_sec": 0.0,
                "max_sec": 0.0,
            }
        if normalized_stats is None:
            normalized_stats = {
                "normalized_sample_count": 0,
                "mean_sec_per_gib": 0.0,
                "stdev_sec_per_gib": 0.0,
                "min_sec_per_gib": 0.0,
                "max_sec_per_gib": 0.0,
                "mean_gib_per_sec": 0.0,
            }
        phases.append({
            **bucket,
            **stats,
            **normalized_stats,
        })

    return {
        "run_totals": run_totals,
        "note": "Runs reused a shared corpus; compare backup/verify/churn by normalized data size metrics.",
        "phases": phases,
    }


def write_run_summary(run_dir: str, phases: list[dict]) -> str:
    """Write summary.json for a single run. Returns path to the file."""
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, "summary.json")
    with open(path, "w") as f:
        json.dump({"phases": phases}, f, indent=2)
    return path


def write_aggregate_report(output_dir: str, summaries: list[dict]) -> str:
    """Write report.json aggregating all run summaries. Returns path."""
    path = os.path.join(output_dir, "report.json")
    total = len(summaries)
    passed = sum(1 for s in summaries if s.get("passed", False))
    summary = build_summary(summaries)
    with open(path, "w") as f:
        json.dump({
            "total_runs": total,
            "passed": passed,
            "failed": total - passed,
            "summary": summary,
            "runs": summaries,
        }, f, indent=2)
    return path


def print_summary(summaries: list[dict]) -> None:
    """Print human-readable summary table to stdout."""
    total = len(summaries)
    passed = sum(1 for s in summaries if s.get("passed", False))
    failed = total - passed

    print(f"\n{'='*60}")
    print(f"  Scenario Results: {passed}/{total} runs passed")
    print(f"{'='*60}")

    summary = build_summary(summaries)
    run_totals = summary["run_totals"]
    print("  Performance Summary")
    print(
        "  total run: "
        f"samples={run_totals['count']} "
        f"avg={run_totals['mean_sec']:.2f}s "
        f"stdev={run_totals['stdev_sec']:.2f}s "
        f"min={run_totals['min_sec']:.2f}s "
        f"max={run_totals['max_sec']:.2f}s"
    )
    print(f"  note: {summary['note']}")
    for phase in summary["phases"]:
        if phase["action"] in {"backup", "verify", "churn"} and phase["normalized_sample_count"] > 0:
            unit = "sec/GiB-added" if phase["normalization"] == "added_bytes" else "sec/GiB"
            print(
                f"  {phase['key']}: "
                f"samples={phase['normalized_sample_count']} "
                f"avg={phase['mean_sec_per_gib']:.2f} {unit} "
                f"stdev={phase['stdev_sec_per_gib']:.2f} "
                f"min={phase['min_sec_per_gib']:.2f} "
                f"max={phase['max_sec_per_gib']:.2f} "
                f"(avg {phase['mean_gib_per_sec']:.2f} GiB/s)"
            )
        else:
            print(
                f"  {phase['key']}: "
                f"samples={phase['passed_count']} "
                f"avg={phase['mean_sec']:.2f}s "
                f"stdev={phase['stdev_sec']:.2f}s "
                f"min={phase['min_sec']:.2f}s "
                f"max={phase['max_sec']:.2f}s"
            )

    failed_runs = [s for s in summaries if not s.get("passed")]
    if failed_runs:
        print("  Failed Runs")
        for s in failed_runs:
            run_id = s.get("run_id", "?")
            phases_ok = s.get("phases_passed", 0)
            phases_total = s.get("phases_total", 0)
            print(f"  Run {run_id:>3}: phases={phases_ok}/{phases_total}")
            for p in s.get("failed_phases", []):
                print(f"           -> {p}")

    print(f"{'='*60}")
    if failed:
        print(f"  FAILED: {failed} run(s)")
    else:
        print(f"  All {total} run(s) passed")
    print()
