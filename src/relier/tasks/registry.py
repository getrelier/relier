"""
Relier Tasks — Registry.

Maintains a mapping of task names to function objects for dynamic lookup.
"""

from collections.abc import Callable


class TaskRegistry:
    """
    Registry for resolving task names back to their functions.
    """

    _tasks: dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str, func: Callable) -> None:
        cls._tasks[name] = func

    @classmethod
    def get(cls, name: str) -> Callable | None:
        return cls._tasks.get(name)


# Global instance
task_registry = TaskRegistry()
