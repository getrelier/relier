import asyncio
import contextlib
import os
import subprocess
import sys
import time

import pytest_asyncio


class CeleryWorkerManager:
    def __init__(self, env):
        self.env = env
        self.processes = []

    async def start_worker(self, redis_client, timeout=15):
        """Start a Celery worker and wait for it to be ready."""
        cmd = [
            sys.executable,
            "-m",
            "celery",
            "-A",
            "relier.tasks.app",
            "worker",
            "-l",
            "info",
            "-P",
            "solo",
        ]

        with open("worker.log", "w") as log_file:
            process = subprocess.Popen(
                cmd,
                env=self.env,
                stdout=log_file,
                stderr=log_file,
            )
            self.processes.append((process, log_file))

        # Wait for the worker to signal readiness by checking for its presence in Redis
        # init_worker in relier.tasks.app adds the worker to 'rl:workers'
        ready = False
        start_time = time.time()
        while time.time() - start_time < timeout:
            workers = await redis_client.smembers("rl:workers")
            if workers:
                ready = True
                break
            await asyncio.sleep(0.5)

        if not ready:
            process.kill()
            log_file.close()
            raise RuntimeError(
                "Celery worker failed to register in Redis within timeout. Check worker.log"
            )

        return (process, log_file)

    def kill_worker(self, entry):
        process, log_file = entry
        try:
            # On Windows SIGKILL is same as terminate/kill
            process.kill()
            process.wait(timeout=5)
        except Exception:
            pass
        if log_file:
            log_file.close()
        if entry in self.processes:
            self.processes.remove(entry)

    def cleanup_all(self):
        for entry in self.processes:
            process, log_file = entry
            try:
                process.terminate()
                process.wait(timeout=5)
            except Exception:
                with contextlib.suppress(Exception):
                    process.kill()
            if log_file:
                with contextlib.suppress(Exception):
                    log_file.close()
        self.processes.clear()


@pytest_asyncio.fixture
async def celery_worker_manager(setup_env, redis_client):
    """
    Provides a manager to start and stop real Celery workers dynamically.
    """
    env = os.environ.copy()

    # Ensure the src directory is in the PYTHONPATH of the worker subprocess
    if "PYTHONPATH" not in env:
        env["PYTHONPATH"] = os.path.abspath("src")
    else:
        env["PYTHONPATH"] = os.path.abspath("src") + os.pathsep + env["PYTHONPATH"]

    manager = CeleryWorkerManager(env)
    yield manager
    manager.cleanup_all()
