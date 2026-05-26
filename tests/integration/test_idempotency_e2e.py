import asyncio
import json

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


async def test_idempotency_lock_set_result_deduplication(redis_client) -> None:
    """idempotency_lock with set_result() commits on exit and blocks a second call."""
    from relier.core.idempotency import idempotency_lock
    from relier.core.keys import RedisKeys

    key = "e2e_webhook_event_42"

    async with idempotency_lock(key=key, ttl=60) as lock:
        assert lock.already_executed is False
        lock.set_result({"processed": True, "id": 42})

    # Verify the result was committed to Redis by __aexit__
    stored = await redis_client.get(RedisKeys.idempotency(key))
    assert json.loads(stored) == {"processed": True, "id": 42}

    # Second call must hit the cache — not re-execute
    async with idempotency_lock(key=key, ttl=60) as lock2:
        assert lock2.already_executed is True
        assert lock2.cached_result == {"processed": True, "id": 42}
