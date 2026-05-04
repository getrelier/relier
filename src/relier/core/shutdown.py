"""
Relier Core — Graceful Shutdown.

Intercepts SIGTERM / SIGINT on the Celery worker process and implements a
drain sequence that either lets active tasks complete or hands them off to
the Phoenix resurrector before the process exits.

Shutdown sequence
-----------------
1. Stop accepting new tasks (Celery ``cancel_consumer``).
2. Wait up to ``graceful_shutdown_timeout`` seconds for active tasks to finish.
3. For any tasks that did not finish: delete their heartbeat key so the
   resurrector detects them immediately and re-queues them on another worker.
4. Log the outcome and return — Celery's own exit machinery takes over.
"""

import asyncio
import logging
import signal

from relier.config import Settings, get_settings
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class GracefulShutdownHandler:
    """Manages the drain phase of the worker lifecycle.

    Args:
        worker_id: The Celery worker hostname (e.g., ``"celery@hostname"``).
    """

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self._active_tasks: set[str] = set()
        self._draining: bool = False

    @property
    def settings(self) -> Settings:
        """Lazy-load settings so we pick up testcontainer environment variables."""
        return get_settings()

    def install(self) -> None:
        """Install async signal handlers for SIGTERM and SIGINT.

        Must be called from within a running event loop (e.g., from the
        ``worker_process_init`` signal handler).
        """
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.drain()),
                )
            logger.info(
                "Graceful shutdown handlers installed.",
                extra={"worker_id": self.worker_id},
            )
        except NotImplementedError:
            # Windows does not support loop.add_signal_handler.
            logger.warning(
                "Signal handlers not supported on this platform; "
                "graceful drain will rely on Celery's built-in shutdown.",
                extra={"worker_id": self.worker_id},
            )
        except RuntimeError:
            logger.error(
                "Failed to install signal handlers: no running event loop.",
                extra={"worker_id": self.worker_id},
            )

    def track_task(self, task_id: str) -> None:
        """Register a task as currently in-flight on this worker."""
        self._active_tasks.add(task_id)

    def untrack_task(self, task_id: str) -> None:
        """Unregister a task (completed, failed, or cancelled)."""
        self._active_tasks.discard(task_id)

    async def drain(self) -> None:
        """Stop accepting new work and wait for in-flight tasks to clear.

        This is idempotent — concurrent invocations (e.g., from multiple
        signals) are collapsed into a single drain pass.
        """
        if self._draining:
            return
        self._draining = True

        logger.warning(
            "Worker entering drain mode.",
            extra={
                "worker_id": self.worker_id,
                "active_tasks": len(self._active_tasks),
            },
        )

        # Tell Celery to stop consuming from queues on this worker.
        try:
            from relier.tasks.app import celery_app

            for q in celery_app.conf.task_queues:
                celery_app.control.cancel_consumer(
                    q.name,
                    destination=[self.worker_id],
                )
            logger.info(
                "All consumers cancelled; no new tasks accepted.",
                extra={"worker_id": self.worker_id},
            )
        except Exception as exc:
            logger.error(
                "Failed to cancel consumer.",
                extra={"worker_id": self.worker_id, "error": str(exc)},
            )

        # Wait for active tasks to finish.
        timeout = self.settings.graceful_shutdown_timeout
        logger.info(
            "Waiting for tasks to finish.",
            extra={"worker_id": self.worker_id, "timeout_s": timeout},
        )

        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while self._active_tasks and loop.time() < deadline:
                await asyncio.sleep(0.5)

            if self._active_tasks:
                logger.error(
                    "Drain timeout exceeded; forcing handoff via Phoenix.",
                    extra={
                        "worker_id": self.worker_id,
                        "remaining_tasks": len(self._active_tasks),
                    },
                )
                await self._handoff_remaining()
            else:
                logger.info(
                    "All tasks completed cleanly.",
                    extra={"worker_id": self.worker_id},
                )
        finally:
            logger.info("Worker drain complete.", extra={"worker_id": self.worker_id})

    async def _handoff_remaining(self) -> None:
        """Invalidate heartbeats for tasks that didn't finish.

        Deleting the heartbeat key is the emergency signal to the Phoenix
        resurrector — it will detect the missing key on its next scan pass
        and re-queue the task on another worker within
        ``resurrection_check_interval`` seconds.
        """
        redis = await get_relier_redis()
        for task_id in list(self._active_tasks):
            hb_key = f"rl:hb:{task_id}"
            await redis.delete(hb_key)
            logger.warning(
                "Task handed off to Phoenix.",
                extra={"task_id": task_id, "worker_id": self.worker_id},
            )
