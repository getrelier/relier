"""
Relier Tasks — Task Context.

Defines the canonical context object passed to tasks, cleanup hooks,
and the timeout enforcer.
"""

import time
from dataclasses import dataclass, field
from typing import Any


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

    def set_partial(self, data: Any) -> None:
        """Save a partial result for recovery or debugging."""
        self.partial_result = data
