import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

from benchmark_runner import cli
from benchmark_runner import defaults


class CliTests(unittest.TestCase):
    def test_runs_is_required(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                cli.build_parser().parse_args([])
        self.assertEqual(exc.exception.code, 2)
        self.assertIn("--runs", stderr.getvalue())

    def test_default_dataset_is_corpus_local(self) -> None:
        with mock.patch("pathlib.Path.home", return_value=Path("/tmp/home")):
            self.assertEqual(defaults.default_dataset_dir(), Path("/tmp/home/corpus-local"))

    def test_main_reports_config_errors(self) -> None:
        stderr = io.StringIO()
        with mock.patch("benchmark_runner.cli.build_config", side_effect=defaults.ConfigError("bad config")), \
                mock.patch("sys.argv", ["benchmark-runner", "--runs", "1"]), \
                redirect_stderr(stderr):
            rc = cli.main()
        self.assertEqual(rc, 2)
        self.assertIn("bad config", stderr.getvalue())

    def test_build_config_validates_expected_dataset_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = Path(tmpdir)
            (dataset / "snapshot-1").mkdir()
            (dataset / "snapshot-2").mkdir()
            with mock.patch("benchmark_runner.defaults.ensure_required_commands"):
                cfg = defaults.build_config(runs=1, tool="vykar", dataset=str(dataset))
        self.assertEqual(cfg.dataset_dir, dataset.resolve())
        self.assertEqual(cfg.selected_tools, ("vykar",))


if __name__ == "__main__":
    unittest.main()
