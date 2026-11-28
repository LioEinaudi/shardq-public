"""An importable launcher outside the ShardQ distribution."""

from shardq.launchers.local import LocalLauncher


class ThreadedLocal(LocalLauncher):
    """Run local workers with a pool-specific OpenMP thread limit."""

    def __init__(self, root, pool, repo_root):
        super().__init__(root, pool, repo_root)
        self.threads = pool.launcher_args.get("threads_per_worker", 1)
        if type(self.threads) is not int or self.threads < 1:
            raise ValueError("threads_per_worker must be a positive integer")

    def submit(self, worker_id, worker_argv, env):
        return super().submit(worker_id, worker_argv, {**env, "OMP_NUM_THREADS": str(self.threads)})
