import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relier.core.phoenix import PhoenixRegistry

pytestmark = pytest.mark.asyncio


# ==========================================
# Tests
# ==========================================


class TestPhoenixRegistry:
    async def test_registration_sets_keys_and_starts_refresh(self, mock_redis):
        """Test that task registration creates heartbeat and payload keys."""
        task_id = "phoenix_123"
        worker_id = "worker_A"
        payload = {"task_name": "test_task", "args": [1, 2], "kwargs": {}}

        await PhoenixRegistry.register(task_id, worker_id, payload)

        # Verify Heartbeat exists with correct worker_id
        hb = await mock_redis.get(f"rl:hb:{task_id}")
        assert hb.decode() == worker_id

        # Verify Payload exists
        state = await mock_redis.hgetall(f"rl:phoenix:{task_id}")
        stored_payload = state.get(b"payload") or state.get("payload")
        assert json.loads(stored_payload) == payload

        # Cleanup background task
        await PhoenixRegistry.complete(task_id)

    async def test_complete_cleans_up_all_state(self, mock_redis):
        """Test that completion wipes heartbeat, payload, and resurrection counters."""
        task_id = "complete_test"
        await PhoenixRegistry.register(task_id, "worker_B", {"data": 1})

        await PhoenixRegistry.complete(task_id)

        assert await mock_redis.exists(f"rl:hb:{task_id}") == 0
        assert await mock_redis.exists(f"rl:phoenix:{task_id}") == 0
        assert task_id not in PhoenixRegistry._active_loops

    async def test_resurrection_logic_requeues_dead_task(self, mock_redis):
        """Test that the loop detects a missing heartbeat and re-queues the task."""
        task_id = "dead_task_999"
        payload = {
            "task_name": "requeue_me",
            "args": [1, 2],
            "kwargs": {"foo": "bar"},
            "queue": "high-priority",
        }

        # Payload exists, but NO heartbeat
        await mock_redis.hset(
            f"rl:phoenix:{task_id}",
            mapping={
                "payload": json.dumps(payload),
                "worker_id": "ghost_worker",
            },
        )

        # Patch the Celery app where PhoenixRegistry looks for it
        with patch("relier.tasks.app.celery_app") as mock_celery:
            mock_dlq = AsyncMock()

            # Execute resurrection pass
            await PhoenixRegistry._scan_and_resurrect(mock_redis, mock_dlq, mock_celery)

            # Wait for the background task (_bg_send) to cycle
            for _ in range(5):
                await asyncio.sleep(0.1)
                if mock_celery.send_task.called:
                    break

            # Assert the receipt
            mock_celery.send_task.assert_called_once_with(
                "requeue_me",
                args=[1, 2],
                kwargs={"foo": "bar"},
                queue="high-priority",
                task_id=task_id,
            )

    @pytest.mark.asyncio
    async def test_resurrection_includes_checkpoints(self, mock_redis):
        task_id = "checkpoint_task"
        # Add "kwargs" to the base payload
        payload = {"task_name": "test", "kwargs": {}}
        checkpoint = {"last_index": 100}

        await mock_redis.hset(
            f"rl:phoenix:{task_id}",
            mapping={
                "payload": json.dumps(payload),
                "partial_result": json.dumps(checkpoint),
            },
        )

        with patch("relier.tasks.app.celery_app") as mock_celery:
            await PhoenixRegistry._scan_and_resurrect(
                mock_redis, AsyncMock(), mock_celery
            )
            await asyncio.sleep(0.1)

            # Verify checkpoint was merged into kwargs
            called_args = mock_celery.send_task.call_args[1]
            assert called_args["kwargs"]["checkpoint"] == checkpoint

    async def test_max_resurrections_routes_to_dlq(self, mock_redis):
        """Test that tasks that die too many times are quarantined."""
        task_id = "poison_pill"
        payload = {"task_name": "crash_loop"}

        await mock_redis.set(f"rl:phoenix:{task_id}", json.dumps(payload))
        # Set resurrection count to the limit (default 5)
        await mock_redis.set(f"rl:resurrections:{task_id}", "6")

        mock_dlq = AsyncMock()
        await PhoenixRegistry._scan_and_resurrect(mock_redis, mock_dlq, MagicMock())

        # Verify it went to DLQ instead of re-queuing
        mock_dlq.quarantine.assert_called_once_with(
            task_id, reason="max_resurrections_exceeded"
        )

        # Verify keys were cleaned up
        assert await mock_redis.exists(f"rl:phoenix:{task_id}") == 0

    async def test_monitor_transitions_state(self, mock_redis):
        """Test that the monitor tracks task progress through resurrection."""
        task_id = "monitored_task"

        # State 0: Resurrection triggered, waiting for new worker to start heartbeat
        await mock_redis.hset(PhoenixRegistry.MONITOR_KEY, task_id, "0")

        # Simulate new worker starting heartbeat
        await mock_redis.set(f"rl:hb:{task_id}", "new_worker")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Should now be in State 1 (Alive)
        state = await mock_redis.hget(PhoenixRegistry.MONITOR_KEY, task_id)
        assert state.decode() == "1"

    async def test_refresh_loop_stops_when_key_deleted(self, mock_redis):
        """Test that the heartbeat loop exits if the key is removed (task finished)."""
        task_id = "refresh_stop"
        await mock_redis.set(f"rl:hb:{task_id}", "worker")

        # Patch settings to make the loop interval very short
        with patch.object(PhoenixRegistry, "_get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(heartbeat_ttl=0.2)

            task = asyncio.create_task(PhoenixRegistry._refresh_loop(task_id, "worker"))

            # Wait for the loop to start and perform its first sleep
            await asyncio.sleep(0.1)

            # Delete the key — the NEXT loop iteration should see this and exit
            await mock_redis.delete(f"rl:hb:{task_id}")

            # Wait long enough for the loop to wake up and check Redis
            await asyncio.sleep(0.3)

            assert task.done() is True


class TestPhoenixGaps:
    async def test_monitor_cleanup_on_payload_loss(self, mock_redis):
        """Test that the monitor removes a task if its payload is gone."""
        task_id = "missing_payload_task"
        # State 1 (Alive) but the payload key was deleted (task finished)
        await mock_redis.hset(PhoenixRegistry.MONITOR_KEY, task_id, "1")
        # No payload key exists in Redis

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should detect payload loss and del the monitor key
        assert await mock_redis.hexists(PhoenixRegistry.MONITOR_KEY, task_id) == 0

    async def test_resurrection_lock_contention(self, mock_redis):
        """Test that multiple resurrectors don't duplicate tasks."""
        task_id = "contested_task"
        # Setup: Task is dead (payload exists, no heartbeat)
        await mock_redis.set(f"rl:phoenix:{task_id}", json.dumps({"task_name": "t"}))

        # Setup: Another resurrector ALREADY holds the lock
        lock_key = PhoenixRegistry.RESURRECT_LOCK.format(task_id=task_id)
        # set(nx=True) should fail if key exists
        await mock_redis.set(lock_key, "1")

        with patch("relier.tasks.app.celery_app") as mock_celery:
            await PhoenixRegistry._scan_and_resurrect(
                mock_redis, AsyncMock(), mock_celery
            )

            # Wait a beat for background tasks
            await asyncio.sleep(0.1)
            # Should NOT have called send_task because it couldn't get the lock
            assert mock_celery.send_task.called is False

    async def test_monitor_worker_redeath(self, mock_redis):
        """Test that the monitor correctly handles task resurrection if a new worker dies."""
        task_id = "redeath_task"
        # State 1: Previously resurrected and was alive
        await mock_redis.hset(PhoenixRegistry.MONITOR_KEY, task_id, "1")
        # Heartbeat is gone again (new worker died)
        await mock_redis.set(f"rl:phoenix:{task_id}", "{}")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should DEL the monitor key so the main scan can pick it up again
        assert await mock_redis.hexists(PhoenixRegistry.MONITOR_KEY, task_id) == 0

    async def test_scan_skip_active_tasks(self, mock_redis):
        """Test that the scanner ignores tasks that are healthy."""
        task_id = "healthy_task"
        await mock_redis.set(f"rl:phoenix:{task_id}", "{}")
        await mock_redis.set(f"rl:hb:{task_id}", "worker-alive")

        with patch("relier.tasks.app.celery_app") as mock_celery:
            await PhoenixRegistry._scan_and_resurrect(
                mock_redis, AsyncMock(), mock_celery
            )
            assert mock_celery.send_task.called is False


class TestPhoenixResurrectionEdgeCases:
    async def test_refresh_loop_handles_unexpected_error(self, mock_redis):
        """Test that the loop logs error but doesn't crash global registry."""
        task_id = "error_task"
        # We trigger an error by patching expire to raise an exception
        with patch.object(mock_redis, "expire", side_effect=Exception("Redis Down")):
            await PhoenixRegistry._refresh_loop(task_id, "worker-1")

    async def test_monitor_cleanup_on_task_completion(self, mock_redis):
        """Test that the monitor cleans up when payload is gone."""
        task_id = "finished_task"
        # Setup: Task is in monitor but payload is deleted (standard completion)
        await mock_redis.hset(PhoenixRegistry.MONITOR_KEY, task_id, "1")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should detect payload absence and delete the monitoring key
        exists = await mock_redis.hexists(PhoenixRegistry.MONITOR_KEY, task_id)
        assert exists == 0

    async def test_monitor_handles_redeath_loop(self, mock_redis):
        """Test that the monitor correctly handles task resurrection if a new worker dies."""
        task_id = "redeath_task"
        # State 1: Previously resurrected and alive
        await mock_redis.hset(PhoenixRegistry.MONITOR_KEY, task_id, "1")
        # Simulate new worker death (Payload remains, heartbeat gone)
        await mock_redis.set(f"rl:phoenix:{task_id}", "{}")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should DEL from monitoring so _scan_and_resurrect can find it again
        exists = await mock_redis.hexists(PhoenixRegistry.MONITOR_KEY, task_id)
        assert exists == 0

    async def test_resurrection_loop_handles_iteration_error(self, mock_redis):
        """Test that the main loop survives a single pass failure."""
        # Patch a core pass helper to raise an error
        import contextlib

        with (
            patch.object(
                PhoenixRegistry,
                "_monitor_resurrected_tasks",
                side_effect=ValueError("Simulated Pass Failure"),
            ),
            patch("asyncio.sleep", side_effect=asyncio.CancelledError),
            contextlib.suppress(asyncio.CancelledError),
        ):
            # we expect the CancelledError to propagate
            await PhoenixRegistry.resurrection_loop()
