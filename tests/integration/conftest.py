import os
import sys
import asyncio
import subprocess
import pytest_asyncio
from relier.storage.redis import get_relier_redis


class CeleryWorkerManager:
    def __init__(self, env):
        self.env = env
        self.processes = []

    async def start_worker(self, redis_client):
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

        process = subprocess.Popen(
            cmd,
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.processes.append(process)

        # Wait for this specific worker to be ready
        ready = False
        for _ in range(40):
            workers = await redis_client.smembers("rl:workers")

            if workers:
                ready = True
                break
            await asyncio.sleep(0.5)

        if not ready:
            process.kill()
            raise RuntimeError(
                "Celery worker failed to register in Redis within timeout."
            )

        return process

    def kill_worker(self, process):
        import signal

        try:
            os.kill(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        except Exception:
            pass
        if process in self.processes:
            self.processes.remove(process)

    def cleanup_all(self):
        for process in self.processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        self.processes.clear()


@pytest_asyncio.fixture
async def celery_worker_manager(setup_env, redis_client):
    """
    Provides a manager to start and stop real Celery workers dynamically.
    """
    env = os.environ.copy()

    if "PYTHONPATH" not in env:
        env["PYTHONPATH"] = os.path.abspath("src")

    manager = CeleryWorkerManager(env)
    yield manager
    manager.cleanup_all()
