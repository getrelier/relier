import asyncio
import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")
os.environ.setdefault("RELIER_SOFT_TIMEOUT", "25")
os.environ.setdefault("RELIER_HARD_TIMEOUT", "30")
os.environ.setdefault("RELIER_GRACEFUL_SHUTDOWN_TIMEOUT", "30")

from relier import TaskContext, rl_task, task_context


async def _on_soft_timeout(ctx: TaskContext) -> None:
    # Fires 5 s before hard cancel.  Progress is already persisted via
    # set_partial inside the loop; use this hook to flush buffers, close
    # file handles, or emit a metric before the task is forcefully stopped.
    pass


@rl_task(
    queue="low_priority",
    soft_timeout=25,
    hard_timeout=30,
    on_soft_timeout=_on_soft_timeout,
)
async def bulk_export(export_id: str, total_rows: int) -> dict:
    """
    Long-running export that checkpoints every 50 rows.

    When SIGTERM arrives the worker drains in-flight tasks up to
    RELIER_GRACEFUL_SHUTDOWN_TIMEOUT seconds.  If it exceeds that window
    Phoenix re-queues this task and it resumes from the last checkpoint
    rather than row 0.
    """
    start_row = 0
    if task_context.partial_result:
        start_row = task_context.partial_result.get("rows_done", 0)

    for row in range(start_row, total_rows):
        await asyncio.sleep(0.01)  # simulate per-row work
        if row % 50 == 0:
            await task_context.set_partial({"rows_done": row})

    return {"export_id": export_id, "rows_exported": total_rows}
