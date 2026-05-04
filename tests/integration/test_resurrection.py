import asyncio

import pytest

from relier.core.phoenix import PhoenixRegistry

pytestmark = pytest.mark.asyncio


async def test_worker_kill_resurrects_task(celery_worker_manager, redis_client):
    """
    Kill a worker mid-task. Verify the task completes by simulating the resurrector loop.
    """
    from relier.core.dlq import dead_letter_queue
    from relier.tasks.app import celery_app
    from relier.tasks.debug import resurrection_task

    # Start Worker A
    worker_a = await celery_worker_manager.start_worker(redis_client)

    # Submit the task
    marker_key = "resurrect_me_123"
    # Use a long duration so we have time to kill the worker
    task = resurrection_task.delay(duration=10, marker_key=marker_key)

    # Wait for Worker A to start processing it
    started = False
    for _ in range(40):  # up to 20 seconds
        if await redis_client.exists(f"test_marker:{marker_key}:started"):
            started = True
            break
        await asyncio.sleep(0.5)

    assert started, "Task did not start on the initial worker"

    # Verify the heartbeat exists
    assert await redis_client.exists(f"rl:hb:{task.id}") == 1

    # KILL WORKER A
    celery_worker_manager.kill_worker(worker_a)

    # Simulate heartbeat expiration (to avoid waiting 30 seconds)
    # The heartbeat key is rl:hb:{task_id}
    await redis_client.delete(f"rl:hb:{task.id}")

    # Run the resurrection scan (simulating the background beat process)
    # This should find the orphaned task and requeue it
    await PhoenixRegistry._scan_and_resurrect(
        redis_client, dead_letter_queue, celery_app
    )

    # Start Worker B
    await celery_worker_manager.start_worker(redis_client)

    # Wait for task to finish on Worker B
    finished = False
    for _ in range(40):  # up to 20 seconds
        if await redis_client.exists(f"test_marker:{marker_key}:finished"):
            finished = True
            break
        await asyncio.sleep(0.5)

    assert finished, "Task did not finish after resurrection"

    # Verify Celery AsyncResult gets the final return value
    result = task.get(timeout=10)
    assert result == "done"
