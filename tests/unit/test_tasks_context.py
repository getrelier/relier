from relier.tasks.context import TaskContext


def test_task_context_properties():
    """Verify TaskContext methods and properties."""
    ctx = TaskContext(
        task_id="123",
        task_name="my_task",
        args=(1,),
        kwargs={"a": 2},
        worker_id="worker-1",
    )

    assert ctx.full_name == "my_task[123]"

    ctx.set_partial({"status": "working"})
    assert ctx.partial_result == {"status": "working"}
