"""
Relier Core — Timeout Enforcement.

Implements cooperative and forced timeout boundaries for asynchronous
task execution.

Relier supports two execution deadlines:

Soft timeout
    Triggers developer-defined recovery hooks so tasks can persist
    checkpoints, release resources, or emit telemetry before termination.

Hard timeout
    Cancels the execution coroutine once the maximum runtime budget
    has been exceeded.

This separation enables graceful degradation while still protecting
worker capacity from permanently stalled workloads.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import Any

from opentelemetry import trace as _otel_trace

from relier.tasks.context import TaskContext
from relier.telemetry.metrics import timeouts_total

logger = logging.getLogger(__name__)


class TimeoutEnforcer:
    """
    Coordinates cooperative and forced execution timeout enforcement for
    asynchronous workloads.
    """

    @classmethod
    async def run(
        cls,
        func: Callable,
        args: tuple,
        kwargs: dict,
        soft: float | None,
        hard: float | None,
        on_soft: Callable | None,
        task_id: str,
    ) -> Any:
        """
        Execute a coroutine under coordinated soft and hard timeout boundaries.

        Execution lifecycle:
        - start the workload coroutine
        - optionally schedule a soft-timeout callback
        - optionally schedule a hard cancellation deadline
        - race execution against timeout watchers
        - propagate cancellation and cleanup deterministically

        Args:
            func:
                Async callable representing the workload execution.
            args:
                Positional arguments passed to the task.
            kwargs:
                Keyword arguments passed to the task.
            soft:
                Soft timeout threshold in seconds.
            hard:
                Hard cancellation deadline in seconds.
            on_soft:
                Optional async recovery hook invoked when the soft timeout fires.
            task_id:
                Unique execution identifier for observability and recovery.
        """
        context = TaskContext(
            task_id=task_id,
            task_name=getattr(func, "__name__", "unknown"),
            args=args,
            kwargs=kwargs,
        )

        # Spawn the primary workload coroutine independently from timeout watchers.
        task_coro = asyncio.create_task(func(*args, **kwargs))

        async def _soft_timeout_handler() -> None:
            """Fire cooperative timeout recovery hooks without cancelling execution."""
            if soft is not None:
                # Delay until the cooperative timeout threshold is reached.
                await asyncio.sleep(float(soft))
            if not task_coro.done():
                logger.warning(
                    "Soft execution timeout exceeded.",
                    extra={"task_id": task_id, "soft_limit": soft},
                )
                timeouts_total.add(
                    1, {"type": "soft", "rl.task.name": context.task_name}
                )
                # Record soft timeout as a span event on the active task span.
                # asyncio.create_task propagates contextvars so the active span
                # here is rl.task.execute from the parent coroutine.
                _span = _otel_trace.get_current_span()
                if _span.is_recording():
                    _span.add_event(
                        "rl.timeout.soft",
                        {
                            "rl.task.id": task_id,
                            "rl.task.name": context.task_name,
                            "rl.timeout.soft_limit": soft or 0,
                        },
                    )
                # Allow the workload to persist checkpoints or release resources before
                # forced cancellation occurs.
                if on_soft:
                    try:
                        await on_soft(context)
                    except Exception as exc:
                        logger.error(
                            "Soft-timeout recovery hook raised an exception.",
                            extra={"error": str(exc)},
                        )

        async def _hard_timeout_handler() -> None:
            """Forcefully cancel execution once the hard runtime budget is exceeded."""
            if hard is not None:
                # Wait until the absolute execution deadline expires.
                await asyncio.sleep(float(hard))
            if not task_coro.done():
                logger.critical(
                    "Hard execution timeout exceeded; cancelling workload.",
                    extra={"task_id": task_id, "hard_limit": hard},
                )
                # Cooperative asyncio cancellation propagates into the workload coroutine.
                task_coro.cancel()

        # Background timeout coordinators.
        soft_watcher = None
        hard_watcher = None

        # Cooperative recovery watcher.
        if soft:
            soft_watcher = asyncio.create_task(_soft_timeout_handler())

        # Forced cancellation watcher.
        if hard:
            hard_watcher = asyncio.create_task(_hard_timeout_handler())

        # Race workload completion against timeout coordination tasks.
        #
        # The hard-timeout watcher participates directly in the wait set so
        # forced cancellation is observed immediately.
        watchers = {task_coro}
        if hard_watcher:
            watchers.add(hard_watcher)

        try:
            # Return as soon as either execution completes or a hard timeout fires.
            done, _pending = await asyncio.wait(
                watchers,
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel any remaining coordination coroutines.
            for p in _pending:
                p.cancel()

            # Await cancellation completion to avoid orphaned background tasks during
            # shutdown and test teardown.
            if _pending:
                await asyncio.gather(*_pending, return_exceptions=True)

            # A completed hard watcher indicates the workload exceeded its absolute
            # runtime budget.
            if hard_watcher and hard_watcher in done and not hard_watcher.cancelled():
                # Allow asyncio cancellation propagation to settle before surfacing the
                # timeout failure upstream.
                if not task_coro.done():
                    with contextlib.suppress(TimeoutError, asyncio.CancelledError):
                        await asyncio.wait_for(asyncio.shield(task_coro), timeout=0.5)
                # Surface a deterministic timeout boundary to the caller.
                raise TimeoutError(f"Task {task_id} exceeded hard timeout of {hard}s")

            return task_coro.result()

        # Propagate external cancellation while ensuring the workload coroutine
        # is also terminated.
        except asyncio.CancelledError:
            task_coro.cancel()
            raise
        finally:
            # Ensure cooperative timeout watchers do not outlive the execution scope.
            if soft_watcher and not soft_watcher.done():
                soft_watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await soft_watcher
