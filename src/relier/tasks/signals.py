"""
Relier Tasks — Celery Signal Handlers.

Hooks into Celery's built-in signal system to automatically record SLO
metrics (success/failure counts) after every task execution, without
modifying user-facing task code.
"""

import logging

from celery.signals import task_failure, task_postrun

from relier.core.slo import SLOMetrics

logger = logging.getLogger(__name__)


@task_postrun.connect
def on_task_postrun(
    sender: object = None,
    task_id: str = "",
    state: str = "",
    **kwargs: object,
) -> None:
    """Record a success or failure event in the SLO sliding window.

    Fires after every task execution regardless of outcome.
    """
    import asyncio

    import relier.tasks.app

    status = "success" if state == "SUCCESS" else "failure"

    if relier.tasks.app.worker_loop and relier.tasks.app.worker_loop.is_running():
        asyncio.run_coroutine_threadsafe(
            SLOMetrics.record_event(status), relier.tasks.app.worker_loop
        )
    else:
        logger.warning(
            "Worker loop unavailable; SLO metric not recorded.",
            extra={"task_id": task_id, "state": state},
        )


@task_failure.connect
def on_task_failure(
    sender: object = None,
    task_id: str = "",
    exception: BaseException | None = None,
    **kwargs: object,
) -> None:
    """Log unhandled task exceptions with structured context.

    This fires in addition to ``task_postrun`` on failure, giving us a
    dedicated log entry for alerting pipelines.
    """
    logger.error(
        "Task failed with unhandled exception.",
        extra={
            "task_id": task_id,
            "exception_type": type(exception).__name__ if exception else "unknown",
            "exception": str(exception),
        },
    )
