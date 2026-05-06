"""
Relier Tasks — Task Context.

Defines the canonical context object passed to tasks, cleanup hooks,
and the timeout enforcer.
"""

import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

_task_context_var: ContextVar["TaskContext"] = ContextVar("task_context")


@dataclass
class TaskContext:
    """
    Contextual information about the currently executing task.
    """

    task_id: str
    task_name: str
    args: tuple
    kwargs: dict
    worker_id: str = ""

    # Storage for partial results (checkpointing)
    partial_result: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)

    @property
    def full_name(self) -> str:
        return f"{self.task_name}[{self.task_id}]"

    async def set_partial(self, data: Any) -> None:
        """Save a partial result for recovery or debugging."""
        self.partial_result = data
        from relier.core.phoenix import PhoenixRegistry

        await PhoenixRegistry.update_partial_state(self.task_id, data)


# Proxy to allow module-level access to the current TaskContext without explicit passing.
class TaskContextProxy:
    def __getattr__(self, name: str) -> Any:
        ctx = _task_context_var.get(None)
        if ctx is None:
            raise RuntimeError("TaskContext accessed outside of a running task.")
        return getattr(ctx, name)

    def __setattr__(self, name: str, value: Any) -> None:
        ctx = _task_context_var.get(None)
        if ctx is None:
            raise RuntimeError("TaskContext accessed outside of a running task.")
        return setattr(ctx, name, value)


task_context = TaskContextProxy()
