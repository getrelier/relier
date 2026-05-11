import asyncio

import pytest


@pytest.mark.asyncio
async def test_task_hard_timeout_e2e(celery_worker_manager, redis_client) -> None:
    from relier.tasks.debug import slow_task

    await celery_worker_manager.start_worker(redis_client)

    # Dispatch task
    task = slow_task.apply_async(args=(5, "key"))

    # Polling loop: Wait up to 10 seconds for the status to change from PENDING
    # Relier hard_timeout is 2s, so failure should appear around the 2-3s mark
    for _ in range(20):
        if task.status == "FAILURE":
            break
        await asyncio.sleep(0.5)

    assert task.status == "FAILURE"
