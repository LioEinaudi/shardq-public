from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

from ..ledger import atomic_write_json
from ..spec import PoolSpec
from .lmms_reports import LmmsReports, _public_scalars
from .base import (
    Backend, Partition, RangeResult, RangeValidationError, WorkerRestartRequired,
    session_fingerprint, validate_unit_ids,
)


class _LmmsSession:
    def __init__(self, backend: "LmmsEvalBackend", lm: object, task_manager: object) -> None:
        self.backend = backend
        self.lm = lm
        self.task_manager = task_manager
        self.session_key = backend.session_key()
        # simple_evaluate() calls lm.clean() after every invocation. The base
        # implementation deletes every torch.nn.Module attribute, invalidating
        # an otherwise reusable model session before its next shard. Keep the
        # model resident for this Session and run the real cleanup once when
        # the worker closes it.
        self._model_clean = getattr(lm, "clean", None)
        if callable(self._model_clean):
            setattr(lm, "clean", lambda: None)

    def run_range(self, partition: str, start: int, count: int, out_dir: Path) -> RangeResult:
        self.backend._configure_path()
        from lmms_eval.evaluator import simple_evaluate
        from lmms_eval.loggers.evaluation_tracker import EvaluationTracker
        from lmms_eval.utils import get_datetime_str, handle_non_serializable

        out_dir.mkdir(parents=True, exist_ok=True)
        raw_dir = out_dir / "raw"
        tracker = EvaluationTracker(output_path=str(raw_dir))
        cli_args = self.backend._cli_args(partition, start, count, str(raw_dir))
        datetime_str = get_datetime_str()
        cli_args.datetime_str = datetime_str
        began = time.time()
        results = simple_evaluate(
            model=self.lm,
            model_args=self.backend.model_args,
            tasks=[partition],
            num_fewshot=None,
            batch_size=self.backend.batch_size,
            max_batch_size=None,
            device=None,
            use_cache=None,
            cache_requests=False,
            rewrite_requests_cache=False,
            delete_requests_cache=False,
            limit=count,
            offset=start,
            bootstrap_iters=self.backend.bootstrap_iters,
            check_integrity=False,
            write_out=False,
            log_samples=True,
            evaluation_tracker=tracker,
            system_instruction=None,
            apply_chat_template=False,
            fewshot_as_multiturn=False,
            gen_kwargs=self.backend.gen_kwargs,
            task_manager=self.task_manager,
            verbosity="INFO",
            predict_only=False,
            random_seed=0,
            numpy_random_seed=1234,
            torch_random_seed=1234,
            fewshot_random_seed=1234,
            datetime_str=datetime_str,
            distributed_executor_backend="accelerate",
            cli_args=cli_args,
            force_simple=self.backend.force_simple,
            repeats=1,
            baseline=None,
            max_tokens=None,
        )
        elapsed = time.time() - began
        if results is None:
            raise RuntimeError("lmms-eval returned no rank-0 results")
        samples = results.pop("samples")
        rows = samples.get(partition)
        if not isinstance(rows, list):
            raise RuntimeError(f"lmms-eval returned no samples for {partition}")
        tracker.save_results_aggregated(results=results, samples=samples, datetime_str=datetime_str)
        for task_name in sorted(samples):
            tracker.save_results_samples(task_name=task_name, samples=samples[task_name])
        raw_results = sorted(raw_dir.rglob("*_results.json"))
        if len(raw_results) != 1:
            raise RuntimeError(f"expected one raw lmms results file, found {len(raw_results)}")
        records_file = out_dir / "records.jsonl"
        with records_file.open("w", encoding="utf-8") as handle:
            for row in rows:
                record = dict(row)
                record["unit_id"] = int(record["doc_id"])
                handle.write(
                    json.dumps(record, ensure_ascii=False, default=handle_non_serializable) + "\n"
                )
        return RangeResult(
            count=len(rows),
            elapsed_seconds=elapsed,
            records_file=records_file,
            extra={"raw_results": str(raw_results[0].resolve())},
        )

    def close(self) -> None:
        if self.lm is not None and callable(self._model_clean):
            setattr(self.lm, "clean", self._model_clean)
            self._model_clean()
        self._model_clean = None
        self.lm = None
        self.task_manager = None


class LmmsEvalBackend(LmmsReports, Backend):
    name = "lmms_eval"
    _session_source: Optional[Tuple[Path, Optional[str]]] = None

    @staticmethod
    def _git(root: Path, *arguments: str, optional: bool = False) -> Optional[str]:
        proc = subprocess.run(
            ["git", "-C", str(root), *arguments],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if proc.returncode:
            if optional:
                return None
            raise RuntimeError(
                f"git {' '.join(arguments)} failed ({proc.returncode}): "
                f"{proc.stderr[-4000:]}"
            )
        return proc.stdout.strip()

    @classmethod
    def _make_pins(
        cls,
        pool: PoolSpec,
        root: Path,
        job_id: str,
        pin_mode: str,
        dry_run: bool,
    ) -> dict:
        configured = pool.extra.get("lmms_root")
        if not configured:
            raise ValueError(
                f"pool {pool.name} extra.lmms_root is required for lmms_eval"
            )
        source = Path(str(configured)).expanduser().resolve()
        commit = str(cls._git(source, "rev-parse", "HEAD"))
        branch = str(cls._git(source, "rev-parse", "--abbrev-ref", "HEAD"))
        dirty = bool(cls._git(source, "status", "--porcelain=v1"))
        remote = cls._git(
            source, "remote", "get-url", "origin", optional=True
        )
        upstream = cls._git(
            source,
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{upstream}",
            optional=True,
        )
        pinned_root = source
        if pin_mode == "worktree":
            pinned_root = Path(root).resolve() / "src" / f"lmms-eval@{commit}"
            if not dry_run:
                pinned_root.parent.mkdir(parents=True, exist_ok=True)
                if pinned_root.exists():
                    actual = cls._git(pinned_root, "rev-parse", "HEAD")
                    if actual != commit:
                        raise ValueError(
                            f"existing pinned worktree has commit {actual}, "
                            f"expected {commit}"
                        )
                else:
                    proc = subprocess.run(
                        [
                            "git",
                            "-C",
                            str(source),
                            "worktree",
                            "add",
                            "--detach",
                            str(pinned_root),
                            commit,
                        ],
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                    if proc.returncode:
                        raise RuntimeError(
                            f"git worktree add failed ({proc.returncode}): "
                            f"{proc.stderr[-4000:]}"
                        )
        elif pin_mode != "verify":
            raise ValueError(f"invalid pin mode: {pin_mode}")

        pins = {
            "lmms_source_root": str(source),
            "lmms_root": str(pinned_root),
            "lmms_commit": commit,
            "lmms_branch": branch,
            "lmms_dirty": dirty,
            "lmms_remote": remote,
            "lmms_upstream": upstream,
            "pin_mode": pin_mode,
        }
        proof_path = (
            Path(root).resolve()
            / "jobs"
            / job_id
            / "canonical_import_proof.json"
        )
        pins["canonical_source_proof_path"] = str(proof_path)
        if dry_run:
            pins["canonical_source_proof"] = None
            return pins

        python = pool.local_python or sys.executable
        if not Path(python).is_file() or not os.access(python, os.X_OK):
            raise ValueError(f"pool local_python is not executable: {python}")
        code = (
            "import json,os,sys;"
            "import lmms_eval;"
            "p=os.path.realpath(lmms_eval.__file__ or '');"
            "r=os.path.realpath(sys.argv[1]);"
            "ok=bool(p) and os.path.commonpath([p,r])==r;"
            "print(json.dumps({'status':'pass' if ok else 'fail',"
            "'lmms_eval_file':p,'python':sys.executable,"
            "'lmms_root':r,'commit':sys.argv[2]}))"
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [str(pinned_root)]
            + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
        )
        proc = subprocess.run(
            [python, "-c", code, str(pinned_root), commit],
            cwd=str(pinned_root),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        try:
            proof = json.loads(proc.stdout.splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            proof = {
                "status": "fail",
                "lmms_eval_file": None,
                "python": str(python),
                "lmms_root": str(pinned_root),
                "commit": commit,
                "error": (proc.stderr or proc.stdout)[-4000:],
            }
        atomic_write_json(proof_path, proof)
        pins["canonical_source_proof"] = proof
        if proc.returncode or proof.get("status") != "pass":
            raise RuntimeError(f"canonical import proof failed: {proof_path}")
        return pins

    @classmethod
    def pins(
        cls, pool: PoolSpec, root: Path, job_id: str, pin_mode: str
    ) -> dict:
        return cls._make_pins(pool, root, job_id, pin_mode, False)

    @classmethod
    def preview_pins(
        cls, pool: PoolSpec, root: Path, job_id: str, pin_mode: str
    ) -> dict:
        return cls._make_pins(pool, root, job_id, pin_mode, True)

    def __init__(self, backend_args: dict, pool: dict, pins: dict) -> None:
        super().__init__(backend_args, pool, pins)
        self.tasks = [str(value) for value in backend_args.get("tasks", [])]
        self.model = str(backend_args.get("model", ""))
        self.model_args = str(backend_args.get("model_args", ""))
        self.gen_kwargs = str(backend_args.get("gen_kwargs", ""))
        self.batch_size = backend_args.get("batch_size", 1)
        self.force_simple = bool(backend_args.get("force_simple", False))
        self.bootstrap_iters = int(backend_args.get("bootstrap_iters", 100000))

    def _configure_path(self) -> None:
        root = str(self.pins.get("lmms_root") or "")
        if not root:
            raise ValueError("lmms_eval backend requires pins.lmms_root")
        if not sys.path or sys.path[0] != root:
            try:
                sys.path.remove(root)
            except ValueError:
                pass
            sys.path.insert(0, root)

    def _task_outputs(self, tasks: List[str]):
        self._configure_path()
        from lmms_eval.evaluator_utils import get_task_list
        from lmms_eval.tasks import TaskManager, get_task_dict

        manager = TaskManager("INFO", model_name=self.model or None)
        matched = manager.match_tasks(tasks)
        missing = [task for task in tasks if task not in matched]
        if missing:
            raise ValueError(f"tasks not found in canonical lmms-eval: {missing}")
        outputs = get_task_list(get_task_dict(matched, task_manager=manager))
        return manager, outputs

    @classmethod
    def control_pythonpath(cls, pool: PoolSpec, pins: dict) -> Tuple[str, ...]:
        # Cached modules from another pinned revision cannot be replaced by changing sys.path.
        root = pins.get("lmms_root")
        return (str(root),) if root else ()

    def preview_describe(self) -> List[Partition]:
        if self.pins.get("pin_mode") != "worktree":
            return self.describe()
        # The future worktree may not exist. Inspect the source checkout while
        # retaining the final worktree path for this job's effective session key.
        preview = dict(self.pins, lmms_root=self.pins["lmms_source_root"])
        return type(self)(self.backend_args, self.pool, preview).describe()

    def describe(self) -> List[Partition]:
        if not self.tasks:
            raise ValueError("lmms_eval backend requires backend_args.tasks")
        _, outputs = self._task_outputs(self.tasks)
        return [
            Partition(output.task_name, len(output.task.eval_docs), None, self.traits(output.task_name))
            for output in outputs
        ]

    def session_key(self) -> str:
        return session_fingerprint(
            self.name,
            self.model,
            self.model_args,
            self.gen_kwargs,
            self.batch_size,
            self.force_simple,
            self.bootstrap_iters,
            self.pins.get("lmms_root"),
            self.pins.get("lmms_commit"),
        )

    def open_session(self) -> _LmmsSession:
        self._configure_path()
        root = Path(self.pins["lmms_root"]).resolve()
        source = (root, self.pins.get("lmms_commit"))
        # Python keeps the previously imported lmms_eval modules after Session.close().
        if LmmsEvalBackend._session_source not in (None, source):
            raise WorkerRestartRequired(f"lmms source pin changed to {source}; start a fresh worker")
        import lmms_eval

        origin = getattr(lmms_eval, "__file__", None)
        if not origin or not Path(origin).resolve().is_relative_to(root):
            raise RuntimeError(f"lmms_eval imported from {origin!r}, expected source under {root}")
        from lmms_eval import models
        from lmms_eval.tasks import TaskManager

        lm = models.get_model(self.model, self.force_simple).create_from_arg_string(
            self.model_args,
            {"batch_size": self.batch_size, "max_batch_size": None, "device": None},
        )
        manager = TaskManager("INFO", model_name=self.model)
        session = _LmmsSession(self, lm, manager)
        LmmsEvalBackend._session_source = source
        return session

    def validate_range(self, partition: str, start: int, count: int, result: RangeResult) -> None:
        validate_unit_ids(result.records_file, start, count)
        if result.count != count:
            raise RangeValidationError(f"lmms-eval returned count={result.count}, expected {count}")

    def _cli_args(self, partition: str, start: int, count: int, output_path: str) -> argparse.Namespace:
        return argparse.Namespace(
            model=self.model,
            model_args=self.model_args,
            tasks=partition,
            batch_size=self.batch_size,
            max_batch_size=None,
            device=None,
            output_path=output_path,
            limit=count,
            offset=start,
            num_fewshot=None,
            gen_kwargs=self.gen_kwargs,
            log_samples=True,
            predict_only=False,
            process_with_media=True,
            reasoning_tags=None,
            apply_chat_template=False,
            fewshot_as_multiturn=False,
            system_instruction=None,
            write_out=False,
            verbosity="INFO",
            force_simple=self.force_simple,
            datetime_str=None,
        )

    def _aggregate_rows(
        self,
        partition: Partition,
        records: List[dict],
        bootstrap_iters: Optional[int] = None,
        task_output=None,
    ) -> Tuple[dict, dict, object]:
        """Aggregate records with the task's own aggregation.

        Pass ``task_output`` from a previous call to skip rebuilding the task
        (and reloading its dataset); its metric containers are reset here.
        ``bootstrap_iters`` overrides the backend default, use 0 to skip stderr.
        """
        self._configure_path()
        import collections

        if task_output is None:
            from lmms_eval.evaluator_utils import get_task_list
            from lmms_eval.tasks import TaskManager, get_task_dict

            manager = TaskManager("INFO", model_name=self.model or None)
            outputs = get_task_list(get_task_dict([partition.name], task_manager=manager))
            if len(outputs) != 1:
                raise ValueError(f"partition {partition.name!r} resolved to {len(outputs)} leaf tasks")
            task_output = outputs[0]
        else:
            task_output.sample_metrics = collections.defaultdict(list)
            task_output.agg_metrics = collections.defaultdict(list)
        task_output.task.args = self._cli_args(partition.name, 0, len(records), "")
        aggregation = task_output.task.aggregation()
        filters = sorted({str(row.get("filter", "none")) for row in records}) or ["none"]
        for metric in aggregation:
            for filter_key in filters:
                values = [
                    row[metric]
                    for row in records
                    if str(row.get("filter", "none")) == filter_key and metric in row
                ]
                if values:
                    task_output.sample_metrics[(metric, filter_key)] = values
        task_output.calculate_aggregate_metric(
            bootstrap_iters=self.bootstrap_iters if bootstrap_iters is None else int(bootstrap_iters)
        )
        alias = task_output.task_alias or partition.name
        task_results = dict(task_output.agg_metrics)
        task_results["alias"] = alias
        payload = {
            "results": {partition.name: task_results},
            "configs": {partition.name: task_output.task_config},
            "versions": {partition.name: task_output.version},
            "n-samples": {
                partition.name: {"original": partition.size, "effective": len(records)}
            },
            "group_subtasks": {},
        }
        return payload, _public_scalars(task_results), task_output

    def merge(self, partition: Partition, records: List[dict], out_dir: Path, meta: dict) -> dict:
        self._configure_path()
        from lmms_eval.utils import handle_non_serializable

        out_dir.mkdir(parents=True, exist_ok=True)
        payload, metrics, task_output = self._aggregate_rows(partition, records)
        serializable = json.loads(json.dumps(payload, ensure_ascii=False, default=handle_non_serializable))
        atomic_write_json(out_dir / "results.json", serializable)
        mismatches = []
        checked = 0
        for shard in meta.get("shards_used", []):
            raw_path = shard.get("raw_results")
            records_path = shard.get("records_file")
            if not raw_path or not records_path or not Path(raw_path).is_file():
                continue
            with Path(records_path).open(encoding="utf-8") as stream:
                shard_rows = [json.loads(line) for line in stream if line.strip()]
            # Public scalars never include stderr, so skip the bootstrap and reuse
            # the task object: a full-size partition has hundreds of shards.
            _, shard_metrics, _ = self._aggregate_rows(
                partition, shard_rows, bootstrap_iters=0, task_output=task_output
            )
            raw = json.loads(Path(raw_path).read_text(encoding="utf-8"))
            expected = _public_scalars((raw.get("results") or {}).get(partition.name, {}))
            checked += 1
            for key, expected_value in expected.items():
                actual_value = shard_metrics.get(key)
                different = actual_value is None
                if isinstance(expected_value, (int, float)) and not isinstance(expected_value, bool) and isinstance(actual_value, (int, float)):
                    different = abs(float(expected_value) - float(actual_value)) >= 1e-9
                elif actual_value is not None:
                    different = actual_value != expected_value
                if different:
                    mismatches.append(
                        {
                            "shard_id": shard.get("shard_id"),
                            "metric": key,
                            "expected": expected_value,
                            "actual": actual_value,
                        }
                    )
        return {
            "metrics": metrics,
            "selfcheck": {"shards_checked": checked, "mismatches": mismatches},
        }
