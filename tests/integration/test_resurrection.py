import asyncio
import json

import pytest

from relier.core.keys import RedisKeys
from relier.core.phoenix import PhoenixRegistry
from relier.tasks.app import celery_app

pytestmark = pytest.mark.asyncio


async def test_worker_kill_resurrects_task(celery_worker_manager, redis_client) -> None:
    """
    Kill a worker mid-task. Verify the task completes by simulating the resurrector loop.
    """
    from relier.core.dlq import DeadLetterQueue
    from tests.integration.tasks import long_running_task

    dead_letter_queue = DeadLetterQueue()

    # Start Worker A
    worker_a = await celery_worker_manager.start_worker(redis_client)

    # Submit the task
    marker_key = "resurrect_me_123"
    task = long_running_task.delay(duration=10, marker_key=marker_key)

    # Wait for Worker A to start processing it
    started = False
    for _ in range(40):
        if await redis_client.exists(f"test:longrun:{marker_key}:started"):
            started = True
            break
        await asyncio.sleep(0.5)

    assert started, "Task did not start on the initial worker"

    # Verify the heartbeat exists
    assert await redis_client.exists(RedisKeys.heartbeat(task.id)) == 1

    # KILL WORKER A
    celery_worker_manager.kill_worker(worker_a)

    # Simulate heartbeat expiration COMPLETELY
    # Delete the heartbeat key
    await redis_client.delete(RedisKeys.heartbeat(task.id))

    # Mark the task as expired in the expiry index
    # Set score to 0 (definitely in the past) so the scan finds it
    await redis_client.zadd(
        RedisKeys.phoenix_expiry_index(),
        {task.id: 0},  # Score 0 = expired
    )

    # Run the resurrection scan
    resurrected_count = await PhoenixRegistry._scan_and_resurrect(
        redis_client, dead_letter_queue, celery_app
    )

    assert resurrected_count == 1, "Expected one task to be resurrected"

    # Wait for async dispatch to complete
    await PhoenixRegistry.wait_for_resurrection(timeout=2.0)

    # Start Worker B AFTER resurrection completes
    await celery_worker_manager.start_worker(redis_client)

    # Wait for task to finish on Worker B
    finished = False
    for _ in range(40):
        if await redis_client.exists(f"test:longrun:{marker_key}:finished"):
            finished = True
            break
        await asyncio.sleep(0.5)

    assert finished, "Task did not finish after resurrection"

    # Verify Celery AsyncResult gets the final return value
    result = task.get(timeout=10)
    assert result == "done"


async def test_checkpoint_resume_real_flow(celery_worker_manager, redis_client) -> None:
    from tests.integration.tasks import checkpoint_task

    worker_a = await celery_worker_manager.start_worker(redis_client)

    marker = "checkpoint_real"
    # 30 steps × 0.1 s = ~3 s total; gives a reliable window to kill mid-flight
    task = checkpoint_task.delay(steps=30, marker=marker)

    # wait until checkpoint step 2 is reached
    checkpoint = {}
    for _ in range(100):
        data = await redis_client.hgetall(RedisKeys.phoenix(task.id))
        if data and "partial_result" in data:
            checkpoint = json.loads(data["partial_result"])
            if checkpoint.get("last_step", 0) >= 2:
                break
        await asyncio.sleep(0.1)

    assert checkpoint.get("last_step", 0) >= 2, (
        "Timed out waiting for checkpoint step 2 — task may not have started"
    )

    # kill worker mid-execution
    celery_worker_manager.kill_worker(worker_a)

    # Simulate heartbeat expiry COMPLETELY
    await redis_client.delete(RedisKeys.heartbeat(task.id))

    # Mark as expired in index
    await redis_client.zadd(RedisKeys.phoenix_expiry_index(), {task.id: 0})

    from relier.core.dlq import DeadLetterQueue

    dead_letter_queue = DeadLetterQueue()

    resurrected_count = await PhoenixRegistry._scan_and_resurrect(
        redis_client, dead_letter_queue, celery_app
    )

    assert resurrected_count == 1, "Expected one task to be resurrected"

    # Wait for dispatch
    await PhoenixRegistry.wait_for_resurrection(timeout=2.0)

    # start new worker
    await celery_worker_manager.start_worker(redis_client)

    # wait for completion
    result = task.get(timeout=15)
    assert result == "done"
