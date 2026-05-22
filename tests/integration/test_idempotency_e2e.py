import asyncio

import pytest

pytestmark = pytest.mark.asyncio


async def test_idempotency_concurrent_executions(
    celery_worker_manager, redis_client
) -> None:
    """
    Test that an idempotent task executing concurrently only processes once
    but returns the cached result for all invocations.
    """
    from tests.integration.tasks import counter_task

    # Start a worker
    await celery_worker_manager.start_worker(redis_client)

    key = "idempotency_test"

    # Dispatch two tasks concurrently with the SAME kwargs
    # The @rl_task decorator creates an idempotency key based on task name + kwargs
    task1 = counter_task.delay(key=key)
    task2 = counter_task.delay(key=key)

    # Wait for both to finish
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
    final_counter = await redis_client.get(f"test:counter:{key}")
    assert int(final_counter) == 1, (
        f"Task executed more than once! Counter: {final_counter}"
    )

    # Verify both callers got the exact same cached result
    assert res1 == 1
    assert res2 == 1

    # Cleanup
    task1.forget()
    task2.forget()
