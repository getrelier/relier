"""
Relier API — Task Management Routers.
"""

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from relier.core.phoenix import PhoenixRegistry

router = APIRouter(prefix="/tasks", tags=["Tasks"])


# =============================================================================
# SCHEMAS
# =============================================================================


class TaskTriggerRequest(BaseModel):
    name: str
    args: list = []
    kwargs: dict = {}


class TaskTriggerResponse(BaseModel):
    task_id: str
    status: str = Field(..., description="The current state of the task in the queue")


class TaskStatusResponse(BaseModel):
    task_id: str
    is_managed_by_phoenix: bool
    # You could add more fields here later, like 'last_heartbeat' or 'retries'


# =============================================================================
# ROUTES
# =============================================================================


@router.post("/trigger")
async def trigger_task(req: TaskTriggerRequest) -> dict:
    """Dynamically dispatch a named task.

    Accepts either a short task name (e.g. ``checkpoint_task``) or a fully-
    qualified task name (e.g. ``relier.tasks.debug.checkpoint_task``). The
    router locates the registered Celery task and enqueues a versioned schema
    envelope so worker-side decorators can unwrap and orchestrate execution.
    """
    import asyncio
    import uuid

    from relier.core.schema import SchemaRegistry
    from relier.tasks.app import celery_app

    # Build a stable task id and wrap the payload as the worker expects.
    task_id = str(uuid.uuid4())
    envelope = SchemaRegistry.wrap(task_id, tuple(req.args), req.kwargs)

    # Resolve the requested task name against the Celery registry.
    requested = req.name
    registered = list(celery_app.tasks.keys())

    # If a fully-qualified name was provided and exists, use it directly.
    if requested in registered:
        task_name = requested
    else:
        # Otherwise, try to find a matching short-name (module suffix).
        matches = [name for name in registered if name.endswith(f".{requested}")]
        if not matches:
            raise HTTPException(status_code=404, detail=f"Task not found: {requested}")
        # Prefer exact match if present; otherwise first match.
        task_name = matches[0]

    # Enqueue using Celery so all normal routing and instrumentation run.
    # Use run_in_executor to avoid blocking the async event loop if the broker
    # client performs network I/O synchronously.
    loop = asyncio.get_running_loop()

    def _enqueue() -> Any:
        return celery_app.send_task(task_name, args=(envelope,), task_id=task_id)

    await loop.run_in_executor(None, _enqueue)
    return {"task_id": task_id, "status": "enqueued", "task_name": task_name}


@router.get("/{task_id}")
async def get_task_status(task_id: str) -> dict:
    """Fetch the current resurrection status of a task and query result backend.

    Returns both whether the task is currently tracked by the Phoenix registry
    and a snapshot of the Celery backend state (if available).
    """
    import asyncio

    from relier.tasks.app import celery_app

    is_active = await PhoenixRegistry.is_active(task_id)

    loop = asyncio.get_running_loop()

    def _query_backend() -> dict:
        try:
            ar = celery_app.AsyncResult(task_id)
            state = getattr(ar, "state", None) or getattr(ar, "status", None)
            info = {"state": state, "result": None}
            try:
                raw_result = ar.result
                if isinstance(raw_result, Exception):
                    # Convert to string to avoid Pydantic serialization errors
                    info["result"] = f"{type(raw_result).__name__}: {str(raw_result)}"
                else:
                    info["result"] = ar.result
            except Exception as e:
                info["result"] = f"Error retrieving result: {str(e)}"
            return info
        except Exception:
            return {"state": None, "result": None}

    backend = await loop.run_in_executor(None, _query_backend)

    return {
        "task_id": task_id,
        "is_managed_by_phoenix": is_active,
        "backend": backend,
    }
