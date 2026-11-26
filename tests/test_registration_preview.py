import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
import venv
from pathlib import Path

from shardq.backends.lmms_eval import LmmsEvalBackend


class RegistrationPreviewTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name)
        self.root = self.base / "ledger"

    def configure(self, backend, backend_args, **pool_options):
        spec = {"job_id": "job", "protocol_id": "preview", "pool": "p",
                "backend": backend, "backend_args": backend_args}
        pool = {"launcher": "local", "backend": backend, **pool_options}
        (self.base / "spec.json").write_text(json.dumps(spec))
        (self.base / "pools.json").write_text(json.dumps({"pools": {"p": pool}}))

    def register(self, *options):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join([str(self.base), env.get("PYTHONPATH", "")])
        proc = subprocess.run(
            [sys.executable, "-m", "shardq", "register", str(self.base / "spec.json"),
             "--pools", str(self.base / "pools.json"), "--root", str(self.root), *options],
            capture_output=True, text=True, env=env,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_lmms_preview_key_matches_final_worktree_and_registered_job(self):
        source = self.base / "source"
        package = source / "lmms_eval"
        package.mkdir(parents=True)
        (package / "__init__.py").touch()
        (package / "tasks.py").write_text(textwrap.dedent("""
            class TaskManager:
                def __init__(self, *args, **kwargs):
                    pass
                def match_tasks(self, tasks):
                    return tasks
            def get_task_dict(tasks, task_manager):
                return tasks
        """))
        (package / "evaluator_utils.py").write_text(textwrap.dedent("""
            from types import SimpleNamespace
            def get_task_list(tasks):
                return [SimpleNamespace(task_name=name, task=SimpleNamespace(eval_docs=[1, 2])) for name in tasks]
        """))
        git = ["git", "-C", str(source)]
        subprocess.run([*git, "init", "-q"], check=True)
        subprocess.run([*git, "add", "."], check=True)
        subprocess.run([*git, "-c", "user.name=Test", "-c", "user.email=test@example.org",
                        "-c", "commit.gpgsign=false", "commit", "-qm", "fixture"], check=True)
        worktrees = [*git, "worktree", "list", "--porcelain"]
        before = subprocess.check_output(worktrees)
        args = {"tasks": ["p"], "model": "fixture"}
        self.configure("lmms_eval", args, extra={"lmms_root": str(source)})

        previews = [self.register("--dry-run"), self.register("--dry-run", "--sizes", "p=2")]
        self.assertFalse(self.root.exists())
        self.assertEqual(subprocess.check_output(worktrees), before)
        job = self.register()
        expected = LmmsEvalBackend(args, job["pool_snapshot"], job["pins"]).session_key()
        self.assertEqual(job["session_key"], expected)
        self.assertEqual(json.loads((self.root / "jobs/job/job.json").read_text())["session_key"], expected)
        for preview in previews:
            self.assertEqual(preview["pins"]["lmms_source_root"], str(source))
            pinned = Path(preview["pins"]["lmms_root"])
            self.assertNotEqual(pinned, source)
            self.assertEqual(str(pinned), job["pins"]["lmms_root"])
            self.assertTrue(pinned.is_dir())
            self.assertEqual(preview["session_key"], expected)

    def test_backend_constructor_uses_pool_environment_with_and_without_sizes(self):
        # On POSIX venv/bin/python can point at the control interpreter while
        # importing from an entirely different site-packages directory.
        environment = self.base / "env"
        venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
        python = environment / "bin/python"
        site = subprocess.check_output(
            [str(python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"], text=True,
        ).strip()
        (Path(site) / "preview_pool_only.py").write_text("TOKEN = 'from-pool'\n")
        (self.base / "preview_backend.py").write_text(textwrap.dedent("""
            from shardq.backends.fake import FakeBackend
            class PoolBackend(FakeBackend):
                def __init__(self, *args):
                    from preview_pool_only import TOKEN
                    super().__init__(*args)
                    self.token = TOKEN
                def session_key(self):
                    return self.token
        """))
        self.configure("preview_backend:PoolBackend", {"partitions": {"p": 2}}, local_python=str(python))
        for options in (("--dry-run",), ("--dry-run", "--sizes", "p=2"), ("--sizes", "p=2")):
            with self.subTest(options=options):
                self.assertEqual(self.register(*options)["session_key"], "from-pool")
                if "--dry-run" in options:
                    self.assertFalse(self.root.exists())
