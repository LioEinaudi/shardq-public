from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import call, patch

from shardq.launchers import volc_client
from shardq.launchers.base import TransientSubmitError


class VolcSubmitTest(unittest.TestCase):
    def test_flow_control_retries_then_succeeds(self) -> None:
        error = "Request was rejected: beyond the current flow control limit"
        with patch(
            "shardq.launchers.volc_client._run_output",
            side_effect=[RuntimeError(error), RuntimeError(error), "submitted task_id=t-abc123"],
        ) as run_output, patch(
            "shardq.launchers.volc_client.time.sleep"
        ) as sleep, patch(
            "shardq.launchers.volc_client.random.uniform", return_value=2.0
        ) as uniform:
            handle = volc_client.submit(Path("worker.yaml"))
        self.assertEqual(handle, "t-abc123")
        self.assertEqual(run_output.call_count, 3)
        self.assertEqual(
            run_output.call_args_list,
            [call(["submit", "--conf", "worker.yaml", "-o", "json"])] * 3,
        )
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(sleep.call_args_list, [call(2.0), call(4.0)])
        self.assertEqual(uniform.call_args_list, [call(1.0, 3.0)] * 2)

    def test_flow_control_exhaustion_is_transient(self) -> None:
        errors = [
            RuntimeError("Request was rejected: beyond the current flow control limit")
            for _ in range(volc_client.SUBMIT_RETRIES)
        ]
        with patch(
            "shardq.launchers.volc_client._run_output", side_effect=errors
        ) as run_output, patch("shardq.launchers.volc_client.time.sleep") as sleep:
            with self.assertRaises(TransientSubmitError) as raised:
                volc_client.submit(Path("worker.yaml"))
        self.assertEqual(run_output.call_count, volc_client.SUBMIT_RETRIES)
        self.assertEqual(sleep.call_count, volc_client.SUBMIT_RETRIES - 1)
        self.assertEqual(str(raised.exception), str(errors[-1]))
        self.assertIs(raised.exception.__cause__, errors[-1])
        for attempt, sleep_call in enumerate(sleep.call_args_list, start=1):
            self.assertGreaterEqual(sleep_call.args[0], 1.0 * attempt)
            self.assertLessEqual(sleep_call.args[0], 3.0 * attempt)

    def test_other_error_is_not_retried(self) -> None:
        error = RuntimeError("boom")
        with patch(
            "shardq.launchers.volc_client._run_output", side_effect=error
        ) as run_output, patch("shardq.launchers.volc_client.time.sleep") as sleep:
            with self.assertRaises(RuntimeError) as raised:
                volc_client.submit(Path("worker.yaml"))
        self.assertIs(raised.exception, error)
        self.assertEqual(run_output.call_count, 1)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
