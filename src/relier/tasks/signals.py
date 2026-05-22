"""
Relier Tasks — Celery Signal Handlers.

Hooks into Celery's lifecycle signals purely for structured logging.
All reliability logic (SLO, metrics, Phoenix) lives in the decorator.
These signals give us Celery-native completion/failure visibility.
"""

import asyncio
from typing import Any

import structlog
from celery.signals import task_failure, task_postrun, task_prerun, task_retry

import relier.tasks.app as task_app
from relier.core.slo import SLOMetrics

logger = structlog.getLogger(__name__)


@task_prerun.connect
def on_task_prerun(
    sender: Any = None,
    task_id: str = "",
    task: Any = None,
    args: Any = None,
    kwargs: Any = None,
    **extra: Any,
) -> None:
    """Log when a task begins executing in a worker child process."""
    logger.info(
        "Task started.",
        task_id=task_id,
        task_name=getattr(task, "name", "unknown"),
    )


@task_postrun.connect
def on_task_postrun(
    sender: Any = None,
    task_id: str = "",
    task: Any = None,
    state: str = "",
    retval: Any = None,
    **extra: Any,
) -> None:
    """Log when a task finishes, success or failure."""
    logger.info(
        "Task finished.",
        task_id=task_id,
        task_name=getattr(task, "name", "unknown"),
        state=state,
    )
    if state == "SUCCESS":
        if task_app.worker_loop and task_app.worker_loop.is_running():

            async def _record() -> None:
                await SLOMetrics.record_event("success")

            asyncio.run_coroutine_threadsafe(_record(), loop=task_app.worker_loop)
        else:
            logger.warning(
                "Worker loop unavailable; skipping SLO metric recording.",
                task_id=task_id,
            )


@task_failure.connect
def on_task_failure(
    sender: Any = None,
    task_id: str = "",
    exception: BaseException | None = None,
    **extra: Any,
) -> None:
    """Log unhandled task exceptions for alerting pipelines."""
    logger.error(
        "Task failed with unhandled exception.",
        extra={
            "task_id": task_id,
            "exception_type": type(exception).__name__ if exception else "unknown",
            "exception": str(exception),
        },
    )


@task_retry.connect
def on_task_retry(
    sender: Any = None,
    task_id: str = "",
    reason: Any = None,
    **extra: Any,
) -> None:
    """Log Celery-level retries (e.g. idempotency backoff)."""
    logger.warning(
        "Task retrying.",
        extra={
            "task_id": task_id,
            "task_name": getattr(sender, "name", "unknown"),
            "reason": str(reason),
        },
    )
