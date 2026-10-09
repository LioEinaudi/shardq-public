from __future__ import annotations

import unittest
from unittest.mock import call, patch

from shardq.launchers import aihc_client
from shardq.launchers.base import TransientSubmitError


class AihcClientTest(unittest.TestCase):
    def test_create_parses_one_job_id(self) -> None:
        output = (
            "Code uploaded successfully: aihc/tempfile/1234/1234.zip\n"
            "Successfully created job job-example123\n"
        )
        with patch(
            "shardq.launchers.aihc_client._run_output", return_value=output
        ) as run_output:
            handle = aihc_client.create_job(
                "worker.json", "worker.code", "pool-id", "queue-id",
                binary="/usr/local/bin/aihc", config_file="/tmp/config",
            )
        self.assertEqual(handle, "job-example123")
        run_output.assert_called_once_with(
            [
                "job", "create", "-f", "worker.json", "-p", "pool-id",
                "-q", "queue-id", "--local-code", "worker.code",
                "--code-dir", "/workspace",
            ],
            binary="/usr/local/bin/aihc", config_file="/tmp/config", timeout=180,
        )

    def test_create_idle_task_appends_flag(self) -> None:
        with patch(
            "shardq.launchers.aihc_client._run_output", return_value="Successfully created job job-idle2"
        ) as run_output:
            self.assertEqual(aihc_client.create_job("j.json", "c", "p", "q", idle_task=True), "job-idle2")
        args = run_output.call_args.args[0]
        self.assertEqual(args[-1], "--idle-task")
        self.assertNotIn("--priority", args)

    def test_create_rejects_two_different_ids(self) -> None:
        with patch(
            "shardq.launchers.aihc_client._run_output",
            return_value="created job-a1; also job-b2",
        ):
            with self.assertRaisesRegex(RuntimeError, "cannot parse one AIHC job id"):
                aihc_client.create_job("j", "c", "p", "q")

    def test_transient_error_retries_then_succeeds(self) -> None:
        error = RuntimeError("server rejected request: rate limit 429")
        with patch(
            "shardq.launchers.aihc_client._run_output",
            side_effect=[error, error, "Successfully created job job-ok123"],
        ) as run_output, patch(
            "shardq.launchers.aihc_client.time.sleep"
        ) as sleep, patch(
            "shardq.launchers.aihc_client.random.uniform", return_value=2.0
        ) as uniform:
            handle = aihc_client.create_job("j", "c", "p", "q")
        self.assertEqual(handle, "job-ok123")
        self.assertEqual(run_output.call_count, 3)
        self.assertEqual(sleep.call_args_list, [call(2.0), call(4.0)])
        self.assertEqual(uniform.call_args_list, [call(1.0, 3.0)] * 2)

    def test_transient_error_exhaustion(self) -> None:
        errors = [RuntimeError("AIHC quota exceeded") for _ in range(aihc_client.SUBMIT_RETRIES)]
        with patch(
            "shardq.launchers.aihc_client._run_output", side_effect=errors
        ) as run_output, patch("shardq.launchers.aihc_client.time.sleep") as sleep:
            with self.assertRaises(TransientSubmitError) as raised:
                aihc_client.create_job("j", "c", "p", "q")
        self.assertEqual(run_output.call_count, aihc_client.SUBMIT_RETRIES)
        self.assertEqual(sleep.call_count, aihc_client.SUBMIT_RETRIES - 1)
        self.assertIs(raised.exception.__cause__, errors[-1])

    def test_non_transient_error_is_not_retried(self) -> None:
        error = RuntimeError("permission denied")
        with patch(
            "shardq.launchers.aihc_client._run_output", side_effect=error
        ) as run_output, patch("shardq.launchers.aihc_client.time.sleep") as sleep:
            with self.assertRaises(RuntimeError) as raised:
                aihc_client.create_job("j", "c", "p", "q")
        self.assertIs(raised.exception, error)
        self.assertEqual(run_output.call_count, 1)
        sleep.assert_not_called()

    def test_get_status_selects_top_level_job_status(self) -> None:
        output = """openapigetjobresponseresult:
    jobid: job-example123
    name: smoke
    status: Succeeded
podlist:
    - pod:
            status: Failed
"""
        with patch(
            "shardq.launchers.aihc_client._run_output", return_value=output
        ) as run_output:
            status = aihc_client.get_job_status("job-example123", "pool-id", "queue-id")
        self.assertEqual(status, "Succeeded")
        run_output.assert_called_once_with(
            ["job", "get", "job-example123", "-p", "pool-id", "-q", "queue-id"],
            binary="aihc", config_file="",
        )

    def test_stop_argv(self) -> None:
        with patch("shardq.launchers.aihc_client._run_output") as run_output:
            aihc_client.stop_job("job-a1", "pool-id", "queue-id")
        run_output.assert_called_once_with(
            ["job", "stop", "job-a1", "-p", "pool-id", "-q", "queue-id"],
            binary="aihc", config_file="",
        )

    def test_normalize_job_name(self) -> None:
        self.assertEqual(
            aihc_client.normalize_job_name("  ShardQ__Pool...Worker---01  "),
            "shardq-pool-worker-01",
        )
        self.assertEqual(aihc_client.normalize_job_name("A" * 80), "a" * 60)


if __name__ == "__main__":
    unittest.main()
