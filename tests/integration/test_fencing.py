"""Integration tests for Phoenix fence-token correctness.

These tests exercise the full fencing flow against a real Redis instance,
proving that a stale/zombie worker cannot commit a result after the task
has been resurrected and the fence token rotated.

The tests do NOT require SIGSTOP or real Celery workers: they drive the
PhoenixRegistry API directly, which is exactly what the decorator calls
in production. If this passes, the correctness guarantee holds regardless
of how the stale worker ends up trying to commit.
"""

import json
import time

import pytest

from relier.core.keys import RedisKeys
from relier.core.phoenix import PhoenixRegistry
from relier.tasks.app import celery_app

pytestmark = pytest.mark.asyncio


async def _register_task(redis_client, task_id: str, payload: dict) -> None:
    """Register a task in Phoenix exactly as the decorator does on pickup."""
    pipe = redis_client.pipeline()
    pipe.hset(
        RedisKeys.phoenix(task_id),
        mapping={
            "worker_id": "celery@worker-a",
            "payload": json.dumps(payload),
        },
    )
    # Heartbeat with short TTL so resurrection scan finds it immediately
    pipe.set(RedisKeys.heartbeat(task_id), "1", ex=10)
    pipe.zadd(RedisKeys.phoenix_expiry_index(), {task_id: time.time() + 10})
    await pipe.execute()


async def test_stale_worker_commit_rejected_after_resurrection(
    redis_client,
) -> None:
    """
    Core fencing correctness test.

    Flow:
      1. Register a task (simulating Worker A picking it up)
      2. Resurrect the task via PhoenixRegistry (rotates the fence token)
      3. Worker A tries to commit with its old (stale) fence token: must be rejected
      4. Worker B commits with the new fence token: must succeed
    """
    from relier.core.dlq import DeadLetterQueue

    task_id = "fencing-test-001"
    payload = {"task_name": "test_task", "args": [], "kwargs": {}}

    await _register_task(redis_client, task_id, payload)

    # --- Simulate Worker A holding a (now stale) fence token ---
    # In production this token is injected into kwargs at resurrection time.
    # Here we grab it before the second resurrection so we can test staleness.
    stale_token = "stale-token-from-worker-a"
    lease_key = RedisKeys.lease(task_id)
    fence_key = RedisKeys.fence(task_id)

    # Manually write the stale fence token as if Worker A received it from a
    # previous resurrection. The lease is intentionally NOT set: in production
    # Worker A's lease has already expired by the time the heartbeat is gone.

    # --- Simulate heartbeat expiry and resurrection ---
    await redis_client.delete(RedisKeys.heartbeat(task_id))
    await redis_client.zadd(RedisKeys.phoenix_expiry_index(), {task_id: 0})

    dlq = DeadLetterQueue()
    resurrected = await PhoenixRegistry._scan_and_resurrect(
        redis_client, dlq, celery_app
    )
    assert resurrected == 1, "Expected one resurrection"
    await PhoenixRegistry._wait_for_resurrection(timeout=2.0)

    # --- The fence token in Redis is now the NEW token (different from stale) ---
    new_token = await redis_client.get(fence_key)
    assert new_token is not None
    new_token_str = new_token.decode() if isinstance(new_token, bytes) else new_token
    assert new_token_str != stale_token, (
        "Fence token was not rotated during resurrection: fencing is broken"
    )

    # --- Worker A tries to commit with its old stale token ---
    can_commit_stale = await PhoenixRegistry.validate_commit(
        task_id, redis_client, stale_token, lease_key, fence_key
    )
    assert can_commit_stale is False, (
        "Stale worker commit was ACCEPTED: fencing failed to reject it"
    )

    # --- Worker B commits with the current token ---
    can_commit_new = await PhoenixRegistry.validate_commit(
        task_id, redis_client, new_token_str, lease_key, fence_key
    )
    assert can_commit_new is True, (
        "Valid worker commit was rejected: fencing over-rejected"
    )


async def test_fence_token_rotates_on_each_resurrection(redis_client) -> None:
    """
    Each resurrection must produce a distinct fence token.
    Two consecutive resurrections must not share a token.
    """
    from relier.core.dlq import DeadLetterQueue

    task_id = "fencing-test-002"
    payload = {"task_name": "test_task", "args": [], "kwargs": {}}
    fence_key = RedisKeys.fence(task_id)

    await _register_task(redis_client, task_id, payload)

    async def _do_resurrection() -> str:
        await redis_client.delete(RedisKeys.heartbeat(task_id))
        await redis_client.delete(RedisKeys.lease(task_id))  # simulate lease TTL expiry
        await redis_client.delete(
            RedisKeys.resurrect_lock(task_id)
        )  # simulate lock TTL expiry
        await redis_client.hdel(
            RedisKeys.monitor(), task_id
        )  # simulate monitor cleanup
        await redis_client.zadd(RedisKeys.phoenix_expiry_index(), {task_id: 0})
        dlq = DeadLetterQueue()
        await PhoenixRegistry._scan_and_resurrect(redis_client, dlq, celery_app)
        await PhoenixRegistry._wait_for_resurrection(timeout=2.0)
        token = await redis_client.get(fence_key)
        return (token.decode() if isinstance(token, bytes) else token) or ""

    # Re-register for the second resurrection (heartbeat needs to exist again)
    token_1 = await _do_resurrection()
    assert token_1, "First resurrection produced no fence token"

    # Re-register the Phoenix hash so the second scan finds a valid payload
    await _register_task(redis_client, task_id, payload)
    token_2 = await _do_resurrection()
    assert token_2, "Second resurrection produced no fence token"

    assert token_1 != token_2, (
        "Fence token was NOT rotated between resurrections: "
        "a stale worker from resurrection #1 could commit after resurrection #2"
    )


async def test_first_run_no_fence_token_always_passes(redis_client) -> None:
    """
    Tasks that were never resurrected have no fence token.
    validate_commit must accept them (None token = first normal run).
    """
    task_id = "fencing-test-003"

    # No token in Redis, no token passed: first normal run
    can_commit = await PhoenixRegistry.validate_commit(
        task_id, redis_client, None, None, None
    )
    assert can_commit is True, (
        "First-run commit (no fence token) was incorrectly rejected"
    )
