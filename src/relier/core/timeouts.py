"""
Relier Core — Timeout Enforcement.

Provides two-tier (soft and hard) timeout enforcement for asynchronous tasks.
Allows developers to inject cleanup hooks to save partial states or emit
telemetry before a worker unconditionally terminates a hanging task.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import Any

from relier.tasks.context import TaskContext

logger = logging.getLogger(__name__)


class TimeoutEnforcer:
    """Orchestrates soft and hard timeout boundaries for async tasks."""

    @classmethod
    async def run(
        cls,
        func: Callable,
        args: tuple,
        kwargs: dict,
        soft: int | None,
        hard: int | None,
        on_soft: Callable | None,
        task_id: str,
    ) -> Any:
        """Execute an async function with precise timeout boundaries.

        Args:
            func:    The async task function to execute.
            args:    Positional arguments.
            kwargs:  Keyword arguments.
            soft:    Seconds before the ``on_soft`` cleanup hook fires.
            hard:    Seconds before the task is unconditionally cancelled.
            on_soft: Async callable receiving a ``TaskContext``.
            task_id: Unique identifier for the task instance.
        """
        context = TaskContext(
            task_id=task_id,
            task_name=getattr(func, "__name__", "unknown"),
            args=args,
            kwargs=kwargs,
        )

        # The main task execution.
        task_coro = asyncio.create_task(func(*args, **kwargs))

        async def _soft_timeout_handler() -> None:
            if soft is not None:
                await asyncio.sleep(float(soft))
            if not task_coro.done():
                logger.warning(
                    "Soft timeout reached.",
                    extra={"task_id": task_id, "soft_limit": soft},
                )
                if on_soft:
                    try:
                        await on_soft(context)
                    except Exception as exc:
                        logger.error(
                            "Soft timeout cleanup hook failed.",
                            extra={"error": str(exc)},
                        )

        async def _hard_timeout_handler() -> None:
            if hard is not None:
                await asyncio.sleep(float(hard))
            if not task_coro.done():
                logger.critical(
                    "Hard timeout reached; terminating task.",
                    extra={"task_id": task_id, "hard_limit": hard},
                )
                task_coro.cancel()

        soft_watcher = None
        hard_watcher = None

        if soft:
            soft_watcher = asyncio.create_task(_soft_timeout_handler())

        if hard:
            hard_watcher = asyncio.create_task(_hard_timeout_handler())

        # Build the set of tasks to race.  We always wait for task_coro;
        # hard_watcher is included so we detect the timeout immediately.
        watchers = {task_coro}
        if hard_watcher:
            watchers.add(hard_watcher)

        try:
            done, _pending = await asyncio.wait(
                watchers,
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel whatever is still running
            for p in _pending:
                p.cancel()

            # Wait for pending tasks to finish cancelling to appease pytest-asyncio
            if _pending:
                await asyncio.gather(*_pending, return_exceptions=True)

            # If the hard watcher fired, the task was cancelled.
            if hard_watcher and hard_watcher in done and not hard_watcher.cancelled():
                # The hard watcher ran to completion → timeout was reached.
                # Wait for the cancellation to propagate to task_coro.
                if not task_coro.done():
                    with contextlib.suppress(TimeoutError, asyncio.CancelledError):
                        await asyncio.wait_for(asyncio.shield(task_coro), timeout=0.5)
                raise TimeoutError(f"Task {task_id} exceeded hard timeout of {hard}s")

            return task_coro.result()

        except asyncio.CancelledError:
            task_coro.cancel()
            raise
        finally:
            # Clean up the soft watcher cleanly if it's still floating around
            if soft_watcher and not soft_watcher.done():
                soft_watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await soft_watcher
