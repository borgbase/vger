import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmark_runner.defaults import build_config
from benchmark_runner.runner import list_previous_run_roots, run_benchmarks


class RunnerTests(unittest.TestCase):
    def test_previous_run_roots_are_sorted_newest_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_root = Path(tmpdir)
            base = runtime_root / "benchmarks"
            (base / "20260101T000000Z").mkdir(parents=True)
            (base / "20260103T000000Z").mkdir(parents=True)
            (base / "20260102T000000Z").mkdir(parents=True)
            dataset = runtime_root / "dataset"
            (dataset / "snapshot-1").mkdir(parents=True)
            (dataset / "snapshot-2").mkdir(parents=True)
            with mock.patch.dict("os.environ", {"RUNTIME_ROOT": str(runtime_root), "REPO_ROOT": str(runtime_root / "repos")}):
                with mock.patch("benchmark_runner.defaults.ensure_required_commands"):
                    cfg = build_config(runs=1, tool="vykar", dataset=str(dataset))
            object.__setattr__(cfg, "out_root", base / "20260104T000000Z")
            roots = list_previous_run_roots(cfg)
        self.assertEqual(roots, [str(base / "20260103T000000Z"), str(base / "20260102T000000Z"), str(base / "20260101T000000Z")])

    def test_run_benchmarks_generates_report_arguments_for_single_tool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dataset = root / "dataset"
            (dataset / "snapshot-1").mkdir(parents=True)
            (dataset / "snapshot-2").mkdir(parents=True)
            runtime_root = root / "runtime"
            (runtime_root / "benchmarks" / "20260101T000000Z").mkdir(parents=True)
            with mock.patch.dict("os.environ", {"RUNTIME_ROOT": str(runtime_root), "REPO_ROOT": str(root / "repos")}):
                with mock.patch("benchmark_runner.defaults.ensure_required_commands"):
                    cfg = build_config(runs=1, tool="vykar", dataset=str(dataset))
            with mock.patch("benchmark_runner.runner.write_vykar_config"), \
                    mock.patch("benchmark_runner.runner.build_storage_settle_targets", return_value=([], [])), \
                    mock.patch("benchmark_runner.runner._timed_run", return_value=0), \
                    mock.patch("benchmark_runner.runner.cleanup_repo_for_tool", return_value=0), \
                    mock.patch("benchmark_runner.runner.cleanup_restore_for_tool"), \
                    mock.patch("benchmark_runner.runner._collect_repo_sizes"), \
                    mock.patch("benchmark_runner.runner._write_tool_stats"), \
                    mock.patch("benchmark_runner.runner.report.main", return_value=0) as report_main, \
                    mock.patch("benchmark_runner.runner._cleanup_transient_dirs"):
                rc = run_benchmarks(cfg)

        self.assertEqual(rc, 0)
        report_args = report_main.call_args.args[0]
        self.assertIn("--backfill-root", report_args)
        self.assertIn("--selected-tool", report_args)


if __name__ == "__main__":
    unittest.main()
