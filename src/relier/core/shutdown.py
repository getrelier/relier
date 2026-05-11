"""
Relier Core — Graceful Shutdown.

Coordinates controlled Celery worker termination during SIGTERM/SIGINT
events to minimize task loss and recovery latency.

During shutdown, the worker:
- stops consuming new tasks
- waits for active executions to finish
- transfers unfinished work to the Phoenix resurrection system
- exits only after drain orchestration completes

This enables rolling deployments and autoscaling events without silently
abandoning in-flight workloads.
"""

import asyncio
import logging
import signal

from relier.config import Settings, get_settings
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class GracefulShutdownHandler:
    """
    Coordinates worker drain behavior during controlled shutdown.

    Tracks active executions, pauses queue consumption, and hands unfinished
    workloads off to the Phoenix recovery subsystem when required.
    """

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self._active_tasks: set[str] = set()
        self._draining: bool = False

    @property
    def settings(self) -> Settings:
        """
        Resolve settings lazily at runtime.

        Avoids capturing stale configuration during module import, which is
        important for dynamically provisioned test environments.
        """
        return get_settings()

    def install(self) -> None:
        """
        Register OS signal handlers for controlled worker termination.

        Must be called from within an active asyncio event loop so signal
        callbacks can safely schedule asynchronous drain operations.
        """
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                # Schedule drain asynchronously because signal handlers cannot await.
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.drain()),
                )
            logger.info(
                "Graceful shutdown signal handlers installed.",
                extra={"worker_id": self.worker_id},
            )
        except NotImplementedError:
            # asyncio signal handlers are not implemented on Windows event loops.
            logger.warning(
                "Async signal handlers unavailable on this platform; "
                "falling back to Celery-managed shutdown behavior.",
                extra={"worker_id": self.worker_id},
            )
        except RuntimeError:
            logger.error(
                "Failed to install shutdown signal handlers because no event loop is running.",
                extra={"worker_id": self.worker_id},
            )

    def track_task(self, task_id: str) -> None:
        """Mark a task as actively executing on this worker."""
        self._active_tasks.add(task_id)

    def untrack_task(self, task_id: str) -> None:
        """Remove a task from active execution tracking."""
        self._active_tasks.discard(task_id)

    async def drain(self) -> None:
        """
        Transition the worker into drain mode and coordinate shutdown.

        Drain mode:
        - stops new task intake
        - waits for active executions to finish
        - transfers unfinished tasks to Phoenix recovery

        Concurrent drain requests are collapsed into a single execution pass.
        """
        if self._draining:
            return
        self._draining = True

        logger.warning(
            "Worker entering graceful drain mode.",
            extra={
                "worker_id": self.worker_id,
                "active_tasks": len(self._active_tasks),
            },
        )

        # Stop broker consumption so no additional work is assigned locally.
        try:
            from relier.tasks.app import celery_app

            for q in celery_app.conf.task_queues:
                celery_app.control.cancel_consumer(
                    q.name,
                    destination=[self.worker_id],
                )
            logger.info(
                "Queue consumers cancelled; worker intake paused.",
                extra={"worker_id": self.worker_id},
            )
        except Exception as exc:
            logger.error(
                "Failed to pause broker consumption during drain.",
                extra={"worker_id": self.worker_id, "error": str(exc)},
            )

        # Allow active executions to complete within the configured grace window.
        timeout = self.settings.graceful_shutdown_timeout
        logger.info(
            "Waiting for active tasks to complete before shutdown.",
            extra={"worker_id": self.worker_id, "timeout_s": timeout},
        )

        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while self._active_tasks and loop.time() < deadline:
                await asyncio.sleep(0.5)

            if self._active_tasks:
                logger.error(
                    "Drain timeout exceeded; transferring unfinished tasks to Phoenix recovery.",
                    extra={
                        "worker_id": self.worker_id,
                        "remaining_tasks": len(self._active_tasks),
                    },
                )
                await self._handoff_remaining()
            else:
                logger.info(
                    "Worker drained successfully with no unfinished tasks.",
                    extra={"worker_id": self.worker_id},
                )
        finally:
            logger.info("Graceful worker drain complete.", extra={"worker_id": self.worker_id})

    async def _handoff_remaining(self) -> None:
        """
        Force unfinished tasks into Phoenix recovery ownership.

        Heartbeat expiration acts as the replay signal for the resurrection
        coordinator, allowing unfinished work to resume on another worker.
        """
        redis = await get_relier_redis()
        for task_id in list(self._active_tasks):
            hb_key = f"rl:hb:{task_id}"
            await redis.delete(hb_key)
            logger.warning(
                "Transferred unfinished task to Phoenix recovery.",
                extra={"task_id": task_id, "worker_id": self.worker_id},
            )
