import json
from unittest.mock import patch

import pytest

from relier.core.dlq import DeadLetterQueue

pytestmark = pytest.mark.asyncio


async def test_quarantine_with_payload(mock_redis):
    payload = {
        "task_name": "test.task",
        "queue": "high-priority",
        "args": [1],
        "kwargs": {"a": 2},
    }

    await DeadLetterQueue.quarantine("123", "failure", payload)

    stored = await mock_redis.hget(DeadLetterQueue.DLQ_HASH_KEY, "123")
    entry = json.loads(stored)

    assert entry["task_id"] == "123"
    assert entry["task_name"] == "test.task"
    assert entry["queue"] == "high-priority"
    assert entry["reason"] == "failure"


async def test_quarantine_fetches_payload_from_redis(mock_redis):
    task_id = "123"
    # Manual key construction to match what DeadLetterQueue.quarantine uses
    # Usually rl:phoenix:{task_id} or rl:heartbeat:{task_id}
    key = f"rl:phoenix:{task_id}"

    payload = {
        "task_name": "redis.task",
        "args": [1, 2],
        "kwargs": {},
        "enqueued_at": "2026-05-07T00:00:00",
    }

    await mock_redis.set(key, json.dumps(payload))

    # This call should now find the key and move 'redis.task' instead of 'unknown'
    await DeadLetterQueue.quarantine(task_id, "test_error")

    stored = await mock_redis.hget(DeadLetterQueue.DLQ_HASH_KEY, task_id)
    entry = json.loads(stored)

    assert entry["task_name"] == "redis.task"
    assert entry["error"] == "test_error"


async def test_quarantine_payload_missing(mock_redis):
    await DeadLetterQueue.quarantine("123", "error")

    stored = await mock_redis.hget(DeadLetterQueue.DLQ_HASH_KEY, "123")
    entry = json.loads(stored)
    assert entry["task_name"] == "unknown"


async def test_list_tasks_sorted(mock_redis):
    await mock_redis.hset(
        DeadLetterQueue.DLQ_HASH_KEY,
        "1",
        json.dumps({"quarantined_at": "2024-01-01T00:00:00"}),
    )
    await mock_redis.hset(
        DeadLetterQueue.DLQ_HASH_KEY,
        "2",
        json.dumps({"quarantined_at": "2025-01-01T00:00:00"}),
    )

    tasks = await DeadLetterQueue.list_tasks()

    # Should be newest first
    assert tasks[0]["quarantined_at"] > tasks[1]["quarantined_at"]


async def test_list_tasks_skips_invalid_json(mock_redis):
    await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, "1", "invalid-json")

    tasks = await DeadLetterQueue.list_tasks()
    assert tasks == []


async def test_inspect_found(mock_redis):
    await mock_redis.hset(
        DeadLetterQueue.DLQ_HASH_KEY, "123", json.dumps({"task_id": "123"})
    )

    result = await DeadLetterQueue.inspect("123")
    assert result["task_id"] == "123"


async def test_inspect_not_found(mock_redis):
    result = await DeadLetterQueue.inspect("missing")
    assert result is None


async def test_release_success(mock_redis):
    entry = {
        "task_name": "test.task",
        "args": [1],
        "kwargs": {"a": 2},
        "queue": "default",
    }

    await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, "123", json.dumps(entry))

    # Mock Celery so we don't actually send a message to a broker
    from relier.tasks.app import celery_app

    with patch.object(celery_app, "send_task") as mock_send:
        result = await DeadLetterQueue.release("123")
        assert result is True
        mock_send.assert_called_once()
    assert not await mock_redis.hexists(DeadLetterQueue.DLQ_HASH_KEY, "123")


async def test_release_not_found(mock_redis):
    result = await DeadLetterQueue.release("missing")
    assert result is False


async def test_release_unknown_task(mock_redis):
    entry = {"task_name": "unknown"}
    await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, "123", json.dumps(entry))

    result = await DeadLetterQueue.release("123")
    assert result is False


async def test_purge(mock_redis):
    await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, "1", "{}")
    await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, "2", "{}")

    count = await DeadLetterQueue.purge()

    assert count == 2
    assert not await mock_redis.exists(DeadLetterQueue.DLQ_HASH_KEY)


async def test_purge_empty(mock_redis):
    count = await DeadLetterQueue.purge()
    assert count == 0
