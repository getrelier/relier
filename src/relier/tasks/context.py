"""
Relier Tasks — Task Context.

Defines the canonical context object passed to tasks, cleanup hooks,
and the timeout enforcer.  The :class:`TaskContext` is injected
automatically by the :func:`~relier.tasks.decorator.rl_task` decorator
when your function declares a ``ctx: TaskContext`` parameter.

Use :data:`task_context` for module-level access without explicit
parameter passing.
"""

import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

_task_context_var: ContextVar["TaskContext"] = ContextVar("task_context")


@dataclass
class TaskContext:
    """Per-execution context injected into every ``@rl_task`` function.

    Relier creates one ``TaskContext`` per task execution and passes it as
    the ``ctx`` keyword argument when your function declares that parameter.
    The proxy :data:`task_context` gives module-level access without
    explicit passing.

    Attributes:
        task_id:        Unique Celery task identifier for this execution.
        task_name:      Fully-qualified dotted name of the task function,
                        e.g. ``"myapp.tasks.generate_report"``.
        args:           Positional arguments the task was dispatched with.
        kwargs:         Keyword arguments the task was dispatched with.
        worker_id:      Hostname of the Celery worker running this task,
                        e.g. ``"celery@prod-worker-3"``.
        partial_result: Checkpointed state saved by a previous execution
                        attempt.  ``None`` on the very first execution.
        metadata:       Arbitrary key-value annotations attached to this
                        execution.  Write anything you need for observability
                        or downstream decisions.
        started_at:     Unix timestamp (float) recording when execution began.

    Example:
        >>> @rl_task(queue="default")
        ... async def generate_report(report_id: str, ctx: TaskContext) -> dict:
        ...     if ctx.partial_result:
        ...         # Resume from where a previous crashed attempt left off.
        ...         resume_from = ctx.partial_result["processed_rows"]
        ...     await ctx.set_partial({"processed_rows": 500})
        ...     return {"report_id": report_id, "status": "complete"}
    """

    task_id: str
    """Unique Celery task identifier for this execution."""

    task_name: str
    """Fully-qualified dotted name of the task function."""

    args: tuple[Any, ...]
    """Positional arguments the task was dispatched with."""

    kwargs: dict[str, Any]
    """Keyword arguments the task was dispatched with."""

    worker_id: str = ""
    """Hostname of the Celery worker running this task."""

    partial_result: Any = None
    """Checkpointed state from a previous execution attempt, or ``None``."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Arbitrary key-value store for task-level annotations."""

    started_at: float = field(default_factory=time.time)
    """Unix timestamp recording when execution began."""

    @property
    def full_name(self) -> str:
        """Return a human-readable ``task_name[task_id]`` identifier.

        Useful for log messages and span names that need both the function
        name and the unique execution ID.

        Returns:
            A string in the form ``"myapp.tasks.process_order[abc-123]"``.

        Example:
            >>> ctx = TaskContext(
            ...     task_id="abc-123",
            ...     task_name="myapp.tasks.process_order",
            ...     args=(),
            ...     kwargs={},
            ... )
            >>> ctx.full_name
            'myapp.tasks.process_order[abc-123]'
        """
        return f"{self.task_name}[{self.task_id}]"

    async def set_partial(self, data: Any) -> None:
        """Checkpoint partial results for crash-recovery resumption.

        Persists ``data`` to the Phoenix registry so the resurrector can
        pass it back via ``ctx.partial_result`` if the worker crashes and
        the task is replayed on another process.

        Args:
            data: Any JSON-serialisable value representing the task's
                  current progress.  Common shapes: a dict of counters,
                  a cursor offset, or a list of completed item IDs.

        Example:
            >>> async def process_batch(items: list[str], ctx: TaskContext) -> int:
            ...     processed = ctx.partial_result or 0
            ...     for i, item in enumerate(items[processed:], start=processed):
            ...         await handle(item)
            ...         if i % 100 == 0:
            ...             await ctx.set_partial(i + 1)  # checkpoint every 100
            ...     return len(items)
        """
        self.partial_result = data
        from relier.core.phoenix import PhoenixRegistry

        await PhoenixRegistry.update_partial_state(self.task_id, data)


class TaskContextProxy:
    """Module-level proxy providing attribute-style access to the current context.

    :data:`task_context` is a module-level singleton of this class.  It
    delegates every attribute access and assignment to the :class:`TaskContext`
    stored in the current async context variable.

    This lets code anywhere in the call stack read task metadata without
    threading ``ctx`` through every function signature.

    Raises:
        RuntimeError: If accessed outside of an active task execution.

    Example:
        >>> # In any helper function called from within a task:
        >>> from relier.tasks.context import task_context
        >>> def log_task_progress(pct: float) -> None:
        ...     print(f"[{task_context.task_id}] {pct:.0%} complete")
    """

    def __getattr__(self, name: str) -> Any:
        ctx = _task_context_var.get(None)
        if ctx is None:
            raise RuntimeError(
                f"TaskContext attribute '{name}' accessed outside of a running "
                "task.  task_context is only available inside functions decorated "
                "with @rl_task, or in code called synchronously from them."
            )
        return getattr(ctx, name)

    def __setattr__(self, name: str, value: Any) -> None:
        ctx = _task_context_var.get(None)
        if ctx is None:
            raise RuntimeError(
                f"Cannot set TaskContext attribute '{name}' outside of a running "
                "task.  task_context is only available inside @rl_task functions."
            )
        setattr(ctx, name, value)


task_context: TaskContextProxy = TaskContextProxy()
"""Module-level proxy for the currently executing task's :class:`TaskContext`.

Access any :class:`TaskContext` attribute from anywhere in the call stack
without passing ``ctx`` through every function.  Raises :exc:`RuntimeError`
if accessed outside of an active task execution.

Example:
    >>> from relier.tasks.context import task_context
    >>> task_context.task_id          # 'abc-123-def-456'
    >>> task_context.worker_id        # 'celery@prod-worker-3'
    >>> await task_context.set_partial({"rows": 500})
"""
