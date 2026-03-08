import subprocess
import tempfile
import unittest
from unittest import mock

from scenario_runner import corpus
from scenario_runner import runner


class RunnerTests(unittest.TestCase):
    def test_list_phase_captures_output_without_echoing_header_in_detail(self) -> None:
        list_stdout = (
            "ID        Host                       Source   Label   Date\n"
            "abc123    host                       corpus   corpus  2025-03-07\n"
        )
        completed = subprocess.CompletedProcess(
            args=["vykar", "list"],
            returncode=0,
            stdout=list_stdout,
            stderr="",
        )
        ctx = {
            "vykar_bin": "vykar",
            "config_path": "/tmp/config.yaml",
            "repo_label": "scenario-simple",
        }

        with mock.patch("scenario_runner.runner.vykar_cmd.vykar_list", return_value=completed):
            result = runner._run_phase({"action": "list"}, ctx=ctx)

        self.assertTrue(result["passed"])
        self.assertEqual(result["detail"], "captured 2 list lines")
        self.assertEqual(result["output"], list_stdout)

    def test_run_scenario_reuses_single_corpus_across_runs(self) -> None:
        scenario = {
            "name": "reuse-corpus",
            "corpus": {"size_gib": 0.1},
            "phases": [{"action": "init"}, {"action": "cleanup"}],
        }

        phase_results = [
            {"action": "init", "label": "", "passed": True, "detail": "ok", "duration_sec": 0.01},
            {"action": "cleanup", "label": "", "passed": True, "detail": "repo deleted", "duration_sec": 0.01},
        ]

        with tempfile.TemporaryDirectory() as output_dir:
            with mock.patch(
                "scenario_runner.runner.corpus.validate_corpus_mix"
            ) as validate_corpus_mix, mock.patch(
                "scenario_runner.runner.corpus.generate_corpus",
                return_value={"file_count": 1, "total_bytes": 1024},
            ) as generate_corpus, mock.patch(
                "scenario_runner.runner.cfg.write_vykar_config",
                return_value="/mnt/repos/scenario-repo",
            ) as write_config, mock.patch(
                "scenario_runner.runner.cfg.ensure_backend_ready"
            ) as ensure_backend_ready, mock.patch(
                "scenario_runner.runner.vykar_cmd.vykar_delete_repo"
            ), mock.patch(
                "scenario_runner.runner._run_phase",
                side_effect=phase_results * 2,
            ), mock.patch(
                "scenario_runner.runner.report.write_run_summary"
            ), mock.patch(
                "scenario_runner.runner.report.write_aggregate_report"
            ), mock.patch(
                "scenario_runner.runner.report.print_summary"
            ):
                passed = runner.run_scenario(
                    scenario,
                    backend="local",
                    runs=2,
                    output_dir=output_dir,
                    vykar_bin="vykar",
                    seed=123,
                )

        self.assertTrue(passed)
        validate_corpus_mix.assert_called_once_with({"size_gib": 0.1})
        self.assertEqual(generate_corpus.call_count, 1)
        self.assertEqual(write_config.call_count, 1)
        ensure_backend_ready.assert_called_once_with("local", "/mnt/repos/scenario-repo")

    def test_run_scenario_validates_corpus_before_setup(self) -> None:
        scenario = {
            "name": "broken-docx",
            "corpus": {"mix": [{"type": "docx", "weight": 1, "file_size": "1kb"}]},
            "phases": [{"action": "init"}],
        }

        with tempfile.TemporaryDirectory() as output_dir:
            with mock.patch(
                "scenario_runner.runner.corpus.validate_corpus_mix",
                side_effect=corpus.CorpusDependencyError("corpus type 'docx' is unavailable"),
            ) as validate_corpus_mix, mock.patch(
                "scenario_runner.runner.corpus.generate_corpus"
            ) as generate_corpus, mock.patch(
                "scenario_runner.runner.cfg.write_vykar_config"
            ) as write_config, mock.patch(
                "scenario_runner.runner.cfg.ensure_backend_ready"
            ) as ensure_backend_ready:
                with self.assertRaisesRegex(corpus.CorpusDependencyError, "docx"):
                    runner.run_scenario(
                        scenario,
                        backend="local",
                        runs=1,
                        output_dir=output_dir,
                        vykar_bin="vykar",
                        seed=123,
                    )

        validate_corpus_mix.assert_called_once_with(scenario["corpus"])
        generate_corpus.assert_not_called()
        write_config.assert_not_called()
        ensure_backend_ready.assert_not_called()

    def test_churn_phase_reports_cap_stats(self) -> None:
        ctx = {
            "vykar_bin": "vykar",
            "config_path": "/tmp/config.yaml",
            "repo_label": "scenario-simple",
            "corpus_dir": "/tmp/corpus",
            "work_dir": "/tmp/work",
            "scenario": {"churn": {}},
            "corpus_config": {"size_gib": 0.1},
            "initial_corpus_bytes": 1024,
            "rng": mock.Mock(),
        }

        with mock.patch("scenario_runner.runner.corpus.apply_churn", return_value={
            "added": 1,
            "deleted": 2,
            "modified": 3,
            "dirs_added": 4,
            "skipped_add_files": 5,
            "skipped_add_dirs": 6,
            "total_bytes_before": 100,
            "total_bytes_after": 200,
            "max_allowed_bytes": 400,
        }) as apply_churn:
            result = runner._run_phase({"action": "churn"}, ctx=ctx)

        self.assertTrue(result["passed"])
        self.assertIn("skipped_files=5", result["detail"])
        self.assertIn("skipped_dirs=6", result["detail"])
        self.assertEqual(result["stats"]["max_allowed_bytes"], 400)
        apply_churn.assert_called_once_with("/tmp/corpus", {"size_gib": 0.1}, {}, 1024, ctx["rng"])


if __name__ == "__main__":
    unittest.main()
