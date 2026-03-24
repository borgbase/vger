import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from vykar_testbench import stress
from vykar_testbench import vykar


class StressRunnerTests(unittest.TestCase):
    def test_run_vykar_logged_uses_long_timeout_for_compact(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["vykar", "compact"],
            returncode=0,
            stdout="",
            stderr="",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = str(Path(tmpdir) / "compact.log")
            with mock.patch("vykar_testbench.stress.subprocess.run", return_value=completed) as run:
                rc, returned_log = stress._run_vykar_logged(
                    "vykar",
                    "/tmp/config.yaml",
                    ["compact", "-R", "scenario-simple", "--threshold", "0"],
                    env={},
                    log_file=log_file,
                )

        self.assertEqual(rc, 0)
        self.assertEqual(returned_log, log_file)
        self.assertEqual(run.call_args.kwargs["timeout"], vykar.LONG_TIMEOUT_SECONDS)

    def test_run_vykar_logged_returns_124_and_logs_timeout(self) -> None:
        timeout = subprocess.TimeoutExpired(
            cmd=["vykar", "--config", "/tmp/config.yaml", "compact", "-R", "scenario-simple"],
            timeout=vykar.LONG_TIMEOUT_SECONDS,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = str(Path(tmpdir) / "compact.log")
            with mock.patch("vykar_testbench.stress.subprocess.run", side_effect=timeout):
                rc, returned_log = stress._run_vykar_logged(
                    "vykar",
                    "/tmp/config.yaml",
                    ["compact", "-R", "scenario-simple", "--threshold", "0"],
                    env={},
                    log_file=log_file,
                )

            log_text = Path(log_file).read_text()

        self.assertEqual(rc, 124)
        self.assertEqual(returned_log, log_file)
        self.assertIn("timed out after 3600 seconds", log_text)


if __name__ == "__main__":
    unittest.main()
