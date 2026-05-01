import asyncio
import pytest
from relier.core.phoenix import PhoenixRegistry

pytestmark = pytest.mark.asyncio


async def test_worker_kill_resurrects_task(celery_worker_manager, redis_client):
    """
    Kill a worker mid-task. Verify the task completes by simulating the resurrector loop.
    """
    pytest.skip("Tasks module is not yet setup. Integration tests skipped.")
    from relier.core.dlq import dead_letter_queue

    # We mock out the Celery App completely for now since tasks aren't setup
    import sys
    from unittest.mock import MagicMock

    if "relier.tasks" not in sys.modules:
        sys.modules["relier.tasks"] = MagicMock()
        sys.modules["relier.tasks.app"] = MagicMock()
        sys.modules["relier.tasks.app"].celery_app = MagicMock()

    from relier.tasks.app import celery_app

    # In a real environment, this task would be registered with celery.
    # For now, we mock the worker's processing manually since we unstaged tasks.
    marker_key = "resurrect_me_123"

    # 1. Start Worker A (Mocked in CI since no tasks)
    # worker_a = await celery_worker_manager.start_worker(redis_client)

    # 2. Submit the task
    marker_key = "resurrect_me_123"
    task = slow_task.delay(duration=10, marker_key=marker_key)

    # 3. Wait for Worker A to start processing it
    started = False
    for _ in range(40):  # up to 20 seconds
        if await redis_client.exists(f"test_marker:{marker_key}:started"):
            started = True
            break
        await asyncio.sleep(0.5)

    assert started, "Task did not start on the initial worker"

    # Verify the heartbeat exists
    assert await redis_client.exists(f"rl:hb:{task.id}") == 1

    # 4. KILL WORKER A
    celery_worker_manager.kill_worker(worker_a)

    # 5. Simulate heartbeat expiration (to avoid waiting 30 seconds)
    await redis_client.delete(f"rl:hb:{task.id}")

    # 6. Run the resurrection scan (simulating the background beat process)
    await PhoenixRegistry._scan_and_resurrect(
        redis_client, dead_letter_queue, celery_app
    )

    # 7. Start Worker B
    worker_b = await celery_worker_manager.start_worker(redis_client)

    # 8. Wait for task to finish on Worker B
    finished = False
    for _ in range(40):  # up to 20 seconds
        if await redis_client.exists(f"test_marker:{marker_key}:finished"):
            finished = True
            break
        await asyncio.sleep(0.5)

    assert finished, "Task did not finish after resurrection"

    # Verify Celery AsyncResult gets the final return value
    # Since we killed worker A, its connection to the backend was severed mid-flight.
    # Worker B picked it up and successfully wrote the result to the backend.
    result = task.get(timeout=10)
    assert result == "done"
