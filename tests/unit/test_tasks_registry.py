from relier.tasks.registry import TaskRegistry


def test_task_registry_operations():
    """Verify basic registration and retrieval."""
    registry = TaskRegistry()

    def my_func():
        pass

    registry.register("test_task", my_func)
    assert registry.get("test_task") == my_func
    assert registry.get("non_existent") is None
