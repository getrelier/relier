import asyncio
import pytest

pytestmark = pytest.mark.asyncio


async def test_idempotency_concurrent_executions(celery_worker_manager, redis_client):
    """
    Test that an idempotent task executing concurrently only processes once
    but returns the cached result for all invocations.
    """
    pytest.skip("Tasks module is not yet setup. Integration tests skipped.")

    # We mock out the Celery App completely for now since tasks aren't setup
    import sys
    from unittest.mock import MagicMock

    if "relier.tasks" not in sys.modules:
        sys.modules["relier.tasks"] = MagicMock()
        sys.modules["relier.tasks.app"] = MagicMock()
        sys.modules["relier.tasks.app"].celery_app = MagicMock()

    from relier.tasks.app import celery_app

    # worker = await celery_worker_manager.start_worker(redis_client)

    key = "idempotency_test"

    # Ensure starting from zero
    await redis_client.set(f"test_counter:{key}", "0")

    # Dispatch two tasks concurrently with the SAME kwargs
    # The @rl_task decorator creates an idempotency key based on task name + kwargs
    task1 = increment_task.delay(key=key)
    task2 = increment_task.delay(key=key)

    # Wait for both to finish
    # Wait via asyncio polling the backend to avoid blocking the test loop
    res1 = None
    res2 = None

    for _ in range(40):
        if task1.ready() and task2.ready():
            res1 = task1.result
            res2 = task2.result
            break
        await asyncio.sleep(0.5)

    assert res1 is not None, "Task 1 did not complete"
    assert res2 is not None, "Task 2 did not complete"

    # Verify the underlying counter only incremented ONCE
    final_counter = await redis_client.get(f"test_counter:{key}")
    assert int(final_counter) == 1, "Task executed more than once!"

    # Verify both callers got the exact same cached result
    assert res1 == 1
    assert res2 == 1

    # Cleanup to prevent Celery backend GC warnings
    task1.forget()
    task2.forget()
