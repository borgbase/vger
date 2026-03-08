import io
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest import mock

from scenario_runner import cli
from scenario_runner import corpus


class CliTests(unittest.TestCase):
    def _write_scenario(self, content: str) -> str:
        tmp = tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml")
        tmp.write(content)
        tmp.flush()
        tmp.close()
        self.addCleanup(lambda: __import__("os").unlink(tmp.name))
        return tmp.name

    def test_corpus_gb_overrides_yaml_value(self) -> None:
        scenario_path = self._write_scenario("name: test\ncorpus:\n  size_gib: 25\n")

        with mock.patch("scenario_runner.cli.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch("scenario_runner.cli.run_scenario", return_value=True) as run_scenario, \
                mock.patch("sys.argv", [
                    "scenario-runner",
                    scenario_path,
                    "--backend", "local",
                    "--corpus-gb", "3.5",
                ]):
            with self.assertRaises(SystemExit) as exc:
                cli.main()

        self.assertEqual(exc.exception.code, 0)
        scenario = run_scenario.call_args.args[0]
        self.assertEqual(scenario["corpus"]["size_gib"], 3.5)

    def test_corpus_gb_defaults_to_yaml_value(self) -> None:
        scenario_path = self._write_scenario("name: test\ncorpus:\n  size_gib: 25\n")

        with mock.patch("scenario_runner.cli.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch("scenario_runner.cli.run_scenario", return_value=True) as run_scenario, \
                mock.patch("sys.argv", [
                    "scenario-runner",
                    scenario_path,
                    "--backend", "local",
                ]):
            with self.assertRaises(SystemExit) as exc:
                cli.main()

        self.assertEqual(exc.exception.code, 0)
        scenario = run_scenario.call_args.args[0]
        self.assertEqual(scenario["corpus"]["size_gib"], 25)

    def test_corpus_gb_creates_corpus_section_when_missing(self) -> None:
        scenario_path = self._write_scenario("name: test\n")

        with mock.patch("scenario_runner.cli.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch("scenario_runner.cli.run_scenario", return_value=True) as run_scenario, \
                mock.patch("sys.argv", [
                    "scenario-runner",
                    scenario_path,
                    "--backend", "local",
                    "--corpus-gb", "2",
                ]):
            with self.assertRaises(SystemExit) as exc:
                cli.main()

        self.assertEqual(exc.exception.code, 0)
        scenario = run_scenario.call_args.args[0]
        self.assertEqual(scenario["corpus"]["size_gib"], 2.0)

    def test_corpus_gb_rejects_non_positive_values(self) -> None:
        scenario_path = self._write_scenario("name: test\n")

        for value in ("0", "-1"):
            with self.subTest(value=value):
                stderr = io.StringIO()
                with mock.patch("sys.argv", [
                    "scenario-runner",
                    scenario_path,
                    "--corpus-gb", value,
                ]), redirect_stderr(stderr):
                    with self.assertRaises(SystemExit) as exc:
                        cli.main()

                self.assertEqual(exc.exception.code, 2)
                self.assertIn("--corpus-gb must be greater than 0", stderr.getvalue())

    def test_corpus_dependency_error_is_reported_without_traceback(self) -> None:
        scenario_path = self._write_scenario("name: test\n")
        stderr = io.StringIO()

        with mock.patch("scenario_runner.cli.shutil.which", return_value="/usr/bin/vykar"), \
                mock.patch(
                    "scenario_runner.cli.run_scenario",
                    side_effect=corpus.CorpusDependencyError("corpus type 'docx' is unavailable"),
                ) as run_scenario, \
                mock.patch("sys.argv", ["scenario-runner", scenario_path]), \
                redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as exc:
                cli.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertEqual(run_scenario.call_count, 1)
        output = stderr.getvalue()
        self.assertIn("error: corpus type 'docx' is unavailable", output)
        self.assertNotIn("Traceback", output)


if __name__ == "__main__":
    unittest.main()
