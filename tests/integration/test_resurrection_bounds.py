"""Integration tests for resurrection backpressure (bounded recovery).

Proves that when many tasks expire simultaneously, the resurrector does not
flood the re-queue broker queue beyond resurrection_max_queue_depth. This is
the "thundering herd" scenario: mass worker failure with a large backlog.

Uses a low max_queue_depth setting so the test runs quickly without needing
hundreds of real workers or a large Redis dataset.
"""

import json
from unittest.mock import patch

import pytest

from relier.config import Settings
from relier.core.keys import RedisKeys
from relier.core.phoenix import PhoenixRegistry
from relier.tasks.app import celery_app

pytestmark = pytest.mark.asyncio

_RECOVERY_QUEUE = "re-queue"


async def _seed_expired_tasks(redis_client, count: int) -> list[str]:
    """Register `count` tasks with already-expired heartbeats."""
    task_ids = [f"herd-test-{i:04d}" for i in range(count)]
    pipe = redis_client.pipeline()
    for tid in task_ids:
        pipe.hset(
            RedisKeys.phoenix(tid),
            mapping={
                "worker_id": "celery@dead-worker",
                "payload": json.dumps(
                    {
                        "task_name": "tests.integration.tasks.noop_task",
                        "args": [],
                        "kwargs": {},
                    }
                ),
            },
        )
        # No heartbeat key = already expired
        # Score 0 = definitely in the past
        pipe.zadd(RedisKeys.phoenix_expiry_index(), {tid: 0})
    await pipe.execute()
    return task_ids


async def test_resurrection_bounded_by_max_queue_depth(redis_client, caplog) -> None:
    """
    20 tasks expire simultaneously. max_queue_depth is capped at 5.

    Expected: first scan resurrects up to 5 tasks (fills the queue),
    subsequent scan defers because the queue is already at the limit,
    and the queue never exceeds the cap.
    """
    from relier.core.dlq import DeadLetterQueue

    await _seed_expired_tasks(redis_client, 20)

    low_depth_settings = Settings(resurrection_max_queue_depth=5)
    dlq = DeadLetterQueue()

    with patch("relier.core.phoenix.get_settings", return_value=low_depth_settings):
        first_pass = await PhoenixRegistry._scan_and_resurrect(
            redis_client, dlq, celery_app
        )

    # At most max_queue_depth tasks should be dispatched in one pass
    assert first_pass <= 5, (
        f"First scan dispatched {first_pass} tasks, exceeding max_queue_depth=5"
    )

    # Queue depth must not exceed the cap
    queue_depth = await redis_client.llen(_RECOVERY_QUEUE)
    assert queue_depth <= 5, (
        f"Recovery queue depth {queue_depth} exceeds resurrection_max_queue_depth=5"
    )


async def test_deferred_scan_when_queue_full(redis_client, caplog) -> None:
    """
    When re-queue is already at max_queue_depth, the scan must defer
    and log the backpressure warning: not dispatch any additional tasks.
    """
    from relier.core.dlq import DeadLetterQueue

    # Pre-fill the recovery queue to the limit
    low_depth_settings = Settings(resurrection_max_queue_depth=3)
    pipe = redis_client.pipeline()
    for _ in range(3):
        pipe.rpush(_RECOVERY_QUEUE, "placeholder")
    await pipe.execute()

    # Seed tasks that would ordinarily be resurrected
    await _seed_expired_tasks(redis_client, 5)

    dlq = DeadLetterQueue()

    import logging

    with (
        caplog.at_level(logging.WARNING, logger="relier.core.phoenix"),
        patch("relier.core.phoenix.get_settings", return_value=low_depth_settings),
    ):
        dispatched = await PhoenixRegistry._scan_and_resurrect(
            redis_client, dlq, celery_app
        )

    assert dispatched == 0, (
        f"Scan dispatched {dispatched} tasks despite queue being at max_queue_depth"
    )
    assert "Resurrection scan deferred" in caplog.text, (
        "Backpressure warning was not logged when queue was full"
    )


async def test_full_recovery_across_multiple_scans(redis_client) -> None:
    """
    With max_queue_depth=5 and 10 expired tasks, two successive scans
    should eventually resurrect all 10. Proves the backpressure is a
    rate-limiter, not a permanent block.
    """
    from relier.core.dlq import DeadLetterQueue

    await _seed_expired_tasks(redis_client, 10)
    low_depth_settings = Settings(resurrection_max_queue_depth=5)
    dlq = DeadLetterQueue()

    total_dispatched = 0
    for _ in range(4):  # generous pass limit
        # Drain the queue between scans to simulate workers consuming it
        await redis_client.delete(_RECOVERY_QUEUE)

        with patch("relier.core.phoenix.get_settings", return_value=low_depth_settings):
            n = await PhoenixRegistry._scan_and_resurrect(redis_client, dlq, celery_app)
        total_dispatched += n
        if total_dispatched >= 10:
            break

    assert total_dispatched >= 10, (
        f"Only {total_dispatched}/10 tasks were eventually resurrected "
        "across multiple scans: backpressure is permanently blocking recovery"
    )
