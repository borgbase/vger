import subprocess
import unittest
from unittest import mock

from vykar_testbench import vykar


class VykarWrapperTests(unittest.TestCase):
    def test_timeout_policy_marks_compact_prune_and_delete_as_long_running(self) -> None:
        self.assertEqual(
            vykar.timeout_for_args(["compact", "-R", "scenario-simple"]),
            vykar.LONG_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            vykar.timeout_for_args(["prune", "-R", "scenario-simple"]),
            vykar.LONG_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            vykar.timeout_for_args(["delete", "-R", "scenario-simple", "--yes-delete-this-repo"]),
            vykar.LONG_TIMEOUT_SECONDS,
        )

    def test_compact_uses_long_running_timeout_policy(self) -> None:
        compact_result = subprocess.CompletedProcess(
            args=["vykar", "compact"],
            returncode=0,
            stdout="Compaction complete\n",
            stderr="",
        )

        with mock.patch("vykar_testbench.vykar.subprocess.run", return_value=compact_result) as run:
            result = vykar.vykar_compact("vykar", "/tmp/config.yaml", "scenario-simple")

        self.assertEqual(result.returncode, 0)
        self.assertEqual(
            run.call_args.kwargs["timeout"],
            vykar.LONG_TIMEOUT_SECONDS,
        )

    def test_delete_repo_tolerates_missing_repository(self) -> None:
        missing_repo = subprocess.CompletedProcess(
            args=["vykar", "delete"],
            returncode=1,
            stdout="",
            stderr="Error: no repository found at '/mnt/repos/scenario-repo'\n",
        )

        with mock.patch("vykar_testbench.vykar.subprocess.run", return_value=missing_repo):
            result = vykar.vykar_delete_repo("vykar", "/tmp/config.yaml", "scenario-simple")

        self.assertEqual(result.returncode, 0)

    def test_backup_ignores_runner_phase_label_for_configured_sources(self) -> None:
        backup_result = subprocess.CompletedProcess(
            args=["vykar", "backup"],
            returncode=0,
            stdout="Snapshot created: snap-123\n",
            stderr="",
        )

        with mock.patch("vykar_testbench.vykar.subprocess.run", return_value=backup_result) as run:
            result, snapshot_id = vykar.vykar_backup(
                "vykar",
                "/tmp/config.yaml",
                "scenario-simple",
                snapshot_label="baseline",
            )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(snapshot_id, "snap-123")
        self.assertEqual(
            run.call_args.args[0],
            ["vykar", "--config", "/tmp/config.yaml", "backup", "-R", "scenario-simple"],
        )

    def test_run_vykar_converts_timeouts_to_rc_124(self) -> None:
        timeout = subprocess.TimeoutExpired(
            cmd=["vykar", "--config", "/tmp/config.yaml", "compact", "-R", "scenario-simple"],
            timeout=vykar.LONG_TIMEOUT_SECONDS,
        )
        timeout.stdout = "partial stdout"
        timeout.stderr = "partial stderr"

        with mock.patch("vykar_testbench.vykar.subprocess.run", side_effect=timeout):
            result = vykar.run_vykar(
                "vykar",
                "/tmp/config.yaml",
                ["compact", "-R", "scenario-simple"],
                label="compact",
            )

        self.assertEqual(result.returncode, 124)
        self.assertEqual(result.stdout, "partial stdout")
        self.assertIn("partial stderr", result.stderr)
        self.assertIn("timed out after 3600 seconds", result.stderr)


if __name__ == "__main__":
    unittest.main()
