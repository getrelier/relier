"""
Relier API — Admin & DLQ Routers.
"""

from datetime import datetime

from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel

from relier.core.dlq import DeadLetterQueue
from relier.storage.redis import get_relier_redis

router = APIRouter(prefix="/admin", tags=["Admin"])

# ==============================================================================
# SCHEMAS
# ==============================================================================


class QuarantinedTask(BaseModel):
    task_id: str
    task_name: str
    queue: str
    args: list
    kwargs: dict
    partial_result: dict | None = None
    reason: str
    resurrections: int
    quarantined_at: datetime


class DLQReleaseResponse(BaseModel):
    status: str = "released"
    task_id: str


class WorkerRegistry(BaseModel):
    active_workers: list[str]
    count: int


# ==============================================================================
# ROUTES
# ==============================================================================


@router.get(
    "/dlq",
    response_model=list[QuarantinedTask],
)
async def list_dlq() -> list[QuarantinedTask]:
    """List all quarantined tasks."""
    raw_tasks = await DeadLetterQueue.list_tasks()
    return [QuarantinedTask(**task) for task in raw_tasks]


@router.post(
    "/dlq/{task_id}/release",
    response_model=DLQReleaseResponse,
)
async def release_task(
    task_id: str = Path(..., min_length=1, max_length=50, pattern=r"^[a-zA-Z0-9\-_]+$"),
) -> DLQReleaseResponse:
    """Release a task from the DLQ."""
    success = await DeadLetterQueue.release(task_id)
    if not success:
        raise HTTPException(status_code=404, detail="Task not found in DLQ")
    return DLQReleaseResponse(task_id=task_id)


@router.get(
    "/workers",
    response_model=WorkerRegistry,
)
async def list_workers() -> WorkerRegistry:
    """Show the current worker registry."""
    redis = await get_relier_redis()
    workers = await redis.smembers("rl:workers")  # type: ignore[misc]
    return WorkerRegistry(active_workers=sorted(workers), count=len(workers))
