import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relier.config import Settings
from relier.core.keys import RedisKeys
from relier.core.phoenix import PhoenixRegistry

pytestmark = pytest.mark.asyncio


# ==========================================
# Tests
# ==========================================


class TestPhoenixRegistry:
    async def test_registration_sets_keys_and_starts_refresh(self, mock_redis) -> None:
        """Test that task registration creates heartbeat and payload keys."""
        task_id = "phoenix_123"
        worker_id = "worker_A"
        payload = {"task_name": "test_task", "args": [1, 2], "kwargs": {}}

        await PhoenixRegistry.register(task_id, worker_id, payload)

        # Verify Heartbeat exists with correct worker_id
        hb = await mock_redis.get(RedisKeys.heartbeat(task_id))
        assert hb.decode() == worker_id

        # Verify Payload exists
        state = await mock_redis.hgetall(RedisKeys.phoenix(task_id))
        stored_payload = state.get(b"payload") or state.get("payload")
        assert json.loads(stored_payload) == payload

        # Cleanup background task
        await PhoenixRegistry.complete(task_id)

    async def test_complete_cleans_up_all_state(self, mock_redis) -> None:
        """Test that completion wipes heartbeat, payload, and resurrection counters."""
        task_id = "complete_test"
        await PhoenixRegistry.register(task_id, "worker_B", {"data": 1})

        await PhoenixRegistry.complete(task_id)

        assert await mock_redis.exists(RedisKeys.heartbeat(task_id)) == 0
        assert await mock_redis.exists(RedisKeys.phoenix(task_id)) == 0
        assert task_id not in PhoenixRegistry._active_loops

    async def test_resurrection_logic_requeues_dead_task(self, mock_redis) -> None:
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
            RedisKeys.phoenix(task_id),
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

            from unittest.mock import ANY

            # Assert the receipt
            mock_celery.send_task.assert_called_once_with(
                "requeue_me",
                args=[1, 2],
                kwargs={
                    "foo": "bar",
                    "_fence_token": ANY,
                    "_lease_key": RedisKeys.lease(task_id),
                    "_fence_key": RedisKeys.fence(task_id),
                },
                queue="re-queue",
                task_id=task_id,
            )

    @pytest.mark.asyncio
    async def test_resurrection_includes_checkpoints(self, mock_redis) -> None:
        task_id = "checkpoint_task"
        # Add "kwargs" to the base payload
        payload = {"task_name": "test", "kwargs": {}}
        checkpoint = {"last_index": 100}

        await mock_redis.hset(
            RedisKeys.phoenix(task_id),
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

    async def test_max_resurrections_routes_to_dlq(self, mock_redis) -> None:
        """Test that tasks that die too many times are quarantined."""
        task_id = "poison_pill"
        payload = {"task_name": "crash_loop"}
        partial = {"progress": "50%"}

        await mock_redis.hset(
            RedisKeys.phoenix(task_id),
            mapping={
                "payload": json.dumps(payload),
                "partial_result": json.dumps(partial),
            },
        )
        # Set resurrection count to the limit (default 5)
        await mock_redis.set(RedisKeys.resurrection(task_id), "6")

        mock_dlq = AsyncMock()
        await PhoenixRegistry._scan_and_resurrect(mock_redis, mock_dlq, MagicMock())

        # Verify it went to DLQ instead of re-queuing
        mock_dlq.quarantine.assert_called_once_with(
            task_id,
            reason="max_resurrections_exceeded",
            payload=payload,
        )
        # Verify keys were cleaned up
        assert await mock_redis.exists(RedisKeys.phoenix(task_id)) == 0

    async def test_monitor_transitions_state(self, mock_redis) -> None:
        """Test that the monitor tracks task progress through resurrection."""
        task_id = "monitored_task"

        # State 0: Resurrection triggered, waiting for new worker to start heartbeat
        await mock_redis.hset(RedisKeys.monitor(), task_id, "0")

        # Simulate new worker starting heartbeat
        await mock_redis.set(RedisKeys.heartbeat(task_id), "new_worker")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Should now be in State 1 (Alive)
        state = await mock_redis.hget(RedisKeys.monitor(), task_id)
        assert state.decode() == "1"

    async def test_refresh_loop_stops_when_key_deleted(self, mock_redis) -> None:
        """Test that the heartbeat loop exits if the key is removed (task finished)."""
        task_id = "refresh_stop"
        await mock_redis.set(RedisKeys.heartbeat(task_id), "worker")

        # Patch settings to make the loop interval very short
        with patch.object(PhoenixRegistry, "_get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(heartbeat_ttl=0.2)

            task = asyncio.create_task(PhoenixRegistry._refresh_loop(task_id, "worker"))

            # Wait for the loop to start and perform its first sleep
            await asyncio.sleep(0.1)

            # Delete the key, the NEXT loop iteration should see this and exit
            await mock_redis.delete(RedisKeys.heartbeat(task_id))

            # Wait long enough for the loop to wake up and check Redis
            await asyncio.sleep(0.3)

            assert task.done() is True


class TestPhoenixGaps:
    async def test_monitor_cleanup_on_payload_loss(self, mock_redis) -> None:
        """Test that the monitor removes a task if its payload is gone."""
        task_id = "missing_payload_task"
        # State 1 (Alive) but the payload key was deleted (task finished)
        await mock_redis.hset(RedisKeys.monitor(), task_id, "1")
        # No payload key exists in Redis

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should detect payload loss and del the monitor key
        assert await mock_redis.hexists(RedisKeys.monitor(), task_id) == 0

    async def test_resurrection_lock_contention(self, mock_redis) -> None:
        """Test that multiple resurrectors don't duplicate tasks."""
        task_id = "contested_task"
        # Setup: Task is dead (payload exists, no heartbeat)
        await mock_redis.set(RedisKeys.phoenix(task_id), json.dumps({"task_name": "t"}))

        # Setup: Another resurrector ALREADY holds the lock
        lock_key = RedisKeys.resurrect_lock(task_id)
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

    async def test_monitor_worker_redeath(self, mock_redis) -> None:
        """Test that the monitor correctly handles task resurrection if a new worker dies."""
        task_id = "redeath_task"
        # State 1: Previously resurrected and was alive
        await mock_redis.hset(RedisKeys.monitor(), task_id, "1")
        # Heartbeat is gone again (new worker died)
        await mock_redis.set(RedisKeys.phoenix(task_id), "{}")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should DEL the monitor key so the main scan can pick it up again
        assert await mock_redis.hexists(RedisKeys.monitor(), task_id) == 0

    async def test_scan_skip_active_tasks(self, mock_redis) -> None:
        """Test that the scanner ignores tasks that are healthy."""
        task_id = "healthy_task"
        await mock_redis.set(RedisKeys.phoenix(task_id), "{}")
        await mock_redis.set(RedisKeys.heartbeat(task_id), "worker-alive")

        with patch("relier.tasks.app.celery_app") as mock_celery:
            await PhoenixRegistry._scan_and_resurrect(
                mock_redis, AsyncMock(), mock_celery
            )
            assert mock_celery.send_task.called is False

    async def test_monitor_lost_transition(self, mock_redis) -> None:
        """state=0 + no heartbeat + payload exists re-adds task to expiry index."""
        task_id = "lost_task"
        # State 0 with an ancient timestamp (grace period already elapsed).
        await mock_redis.hset(RedisKeys.monitor(), task_id, "0:0")
        # Payload exists but NO heartbeat (worker crashed before registering)
        await mock_redis.hset(RedisKeys.phoenix(task_id), "payload", "{}")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor entry removed so the scanner can retry
        assert await mock_redis.hexists(RedisKeys.monitor(), task_id) == 0
        # Task re-added to expiry index
        members = await mock_redis.zrange(RedisKeys.phoenix_expiry_index(), 0, -1)
        assert task_id in members

    async def test_monitor_grace_period_suppresses_lost_transition(
        self, mock_redis
    ) -> None:
        """A recent resurrection within the grace period is NOT released to scan."""
        import time as _time

        task_id = "cold_start_task"
        # State 0 with a fresh timestamp — worker is still in cold-start window.
        await mock_redis.hset(RedisKeys.monitor(), task_id, f"0:{int(_time.time())}")
        await mock_redis.hset(RedisKeys.phoenix(task_id), "payload", "{}")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor entry stays — the warning must not fire yet.
        assert await mock_redis.hexists(RedisKeys.monitor(), task_id) == 1
        # And the task must NOT have been re-added to the expiry index.
        members = await mock_redis.zrange(RedisKeys.phoenix_expiry_index(), 0, -1)
        assert task_id not in members


class TestPhoenixResurrectionEdgeCases:
    async def test_refresh_loop_handles_unexpected_error(self, mock_redis) -> None:
        """Test that the loop logs error but doesn't crash global registry."""
        task_id = "error_task"
        # Use a very short heartbeat_ttl so asyncio.sleep(interval) is ~0.05s
        # instead of the default 5-15s, keeping the test fast.
        mock_pipe = AsyncMock()
        mock_pipe.expire = MagicMock(return_value=mock_pipe)
        mock_pipe.zadd = MagicMock(return_value=mock_pipe)
        mock_pipe.execute = AsyncMock(side_effect=Exception("Redis Down"))

        with (
            patch.object(PhoenixRegistry, "_get_settings") as mock_settings,
            patch.object(mock_redis, "pipeline", return_value=mock_pipe),
        ):
            mock_settings.return_value = MagicMock(
                heartbeat_ttl=0.1, resurrection_check_interval=1
            )
            await PhoenixRegistry._refresh_loop(task_id, "worker-1")

    async def test_monitor_cleanup_on_task_completion(self, mock_redis) -> None:
        """Test that the monitor cleans up when payload is gone."""
        task_id = "finished_task"
        # Setup: Task is in monitor but payload is deleted (standard completion)
        await mock_redis.hset(RedisKeys.monitor(), task_id, "1")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should detect payload absence and delete the monitoring key
        exists = await mock_redis.hexists(RedisKeys.monitor(), task_id)
        assert exists == 0

    async def test_monitor_handles_redeath_loop(self, mock_redis) -> None:
        """Test that the monitor correctly handles task resurrection if a new worker dies."""
        task_id = "redeath_task"
        # State 1: Previously resurrected and alive
        await mock_redis.hset(RedisKeys.monitor(), task_id, "1")
        # Simulate new worker death (Payload remains, heartbeat gone)
        await mock_redis.set(RedisKeys.phoenix(task_id), "{}")

        await PhoenixRegistry._monitor_resurrected_tasks(mock_redis)

        # Monitor should DEL from monitoring so _scan_and_resurrect can find it again
        exists = await mock_redis.hexists(RedisKeys.monitor(), task_id)
        assert exists == 0

    async def test_resurrection_loop_logs_pass_complete(self, mock_redis) -> None:
        """Pass-complete log fires when monitored/resurrected counts are non-zero."""
        import contextlib

        async def one_shot_sleep(_delay: float) -> None:
            raise asyncio.CancelledError()

        with (
            patch.object(
                PhoenixRegistry,
                "_monitor_resurrected_tasks",
                new=AsyncMock(return_value=1),
            ),
            patch.object(
                PhoenixRegistry,
                "_scan_and_resurrect",
                new=AsyncMock(return_value=1),
            ),
            patch("asyncio.sleep", side_effect=one_shot_sleep),
            contextlib.suppress(asyncio.CancelledError),
        ):
            await PhoenixRegistry.resurrection_loop()

    async def test_resurrection_loop_handles_iteration_error(self, mock_redis) -> None:
        """Test that the main loop survives a single pass failure."""

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


class TestPhoenixValidation:
    async def test_validate_execution_branches(self, mock_redis):
        # Duplicate lease (0) -> rejected
        mock_redis.eval = AsyncMock(return_value=0)
        res = await PhoenixRegistry.validate_execution(
            "t1", mock_redis, "token", "lease", "fence"
        )
        assert res is False

        # Stale fence (2) -> rejected and release_lease called
        mock_redis.eval = AsyncMock(return_value=2)
        called = []

        async def fake_release(redis, lease_key, token):
            called.append((lease_key, token))

        with patch.object(
            PhoenixRegistry, "release_lease", new=AsyncMock(side_effect=fake_release)
        ):
            res2 = await PhoenixRegistry.validate_execution(
                "t2", mock_redis, "token2", "lease2", "fence2"
            )
            assert res2 is False
            assert called and called[0] == ("lease2", "token2")

        # Valid (1) -> accepted
        mock_redis.eval = AsyncMock(return_value=1)
        res3 = await PhoenixRegistry.validate_execution(
            "t3", mock_redis, "token3", "lease3", "fence3"
        )
        assert res3 is True

    async def test_validate_commit_and_release(self, mock_redis):
        # Commit check fails -> False
        mock_redis.eval = AsyncMock(return_value=0)
        res = await PhoenixRegistry.validate_commit(
            "t", mock_redis, "token", "lease", "fence"
        )
        assert res is False

        # Commit check passes -> True and release_lease invoked
        mock_redis.eval = AsyncMock(return_value=1)
        called = []

        async def fake_release(redis, lease_key, token):
            called.append((lease_key, token))

        with patch.object(
            PhoenixRegistry, "release_lease", new=AsyncMock(side_effect=fake_release)
        ):
            res2 = await PhoenixRegistry.validate_commit(
                "t", mock_redis, "token", "lease", "fence"
            )
            assert res2 is True
            assert called and called[0] == ("lease", "token")

    async def test_release_lease_handles_exception(self):
        mock = MagicMock()
        mock.eval = AsyncMock(side_effect=Exception("boom"))

        await PhoenixRegistry.release_lease(mock, "lease", "token")

    async def test_update_partial_state_persists(self, mock_redis):
        task_id = "pstate"
        await PhoenixRegistry.update_partial_state(task_id, {"i": 5})
        data = await mock_redis.hgetall(RedisKeys.phoenix(task_id))
        assert b"partial_result" in data or "partial_result" in data

    async def test_safe_bg_send_and_bg_send_timeout(self, monkeypatch):
        # _safe_bg_send should swallow exceptions from _bg_send
        async def bad_bg_send(task_id, payload, app):
            raise Exception("send fail")

        with patch.object(
            PhoenixRegistry, "_bg_send", new=AsyncMock(side_effect=bad_bg_send)
        ):
            await PhoenixRegistry._safe_bg_send("t", {}, MagicMock())

        # _bg_send handles TimeoutError raised by wait_for
        async def fake_wait_for(coro, timeout):
            raise TimeoutError()

        monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)

        mock_celery = MagicMock()
        await PhoenixRegistry._bg_send("t", {"task_name": "x"}, mock_celery)

    async def test_backpressure_defers_scan_when_queue_deep(self, mock_redis) -> None:
        """A deep recovery queue makes the resurrector skip its scan entirely."""
        task_id = "would_resurrect"
        await mock_redis.hset(
            RedisKeys.phoenix(task_id),
            mapping={"payload": json.dumps({"task_name": "t", "kwargs": {}})},
        )
        await mock_redis.zadd(RedisKeys.phoenix_expiry_index(), {task_id: 1.0})

        # Fill the recovery queue past the default max-queue-depth limit.
        mock_redis.ldata["re-queue"] = ["msg"] * 10_001

        mock_celery = MagicMock()
        count = await PhoenixRegistry._scan_and_resurrect(
            mock_redis, AsyncMock(), mock_celery
        )

        assert count == 0
        mock_celery.send_task.assert_not_called()
        # The task is left in the expiry index to be retried on a later pass.
        remaining = await mock_redis.zrange(RedisKeys.phoenix_expiry_index(), 0, -1)
        assert task_id in remaining

    async def test_batch_size_caps_tasks_per_scan(self, mock_redis) -> None:
        """resurrection_batch_size bounds how many tasks one scan replays."""
        for i in range(5):
            tid = f"batch_task_{i}"
            await mock_redis.hset(
                RedisKeys.phoenix(tid),
                mapping={"payload": json.dumps({"task_name": "t", "kwargs": {}})},
            )
            await mock_redis.zadd(RedisKeys.phoenix_expiry_index(), {tid: 1.0})

        small_batch = Settings(resurrection_batch_size=2)
        with patch("relier.core.phoenix.get_settings", return_value=small_batch):
            count = await PhoenixRegistry._scan_and_resurrect(
                mock_redis, AsyncMock(), MagicMock()
            )

        # Only the batch-size's worth of tasks are replayed this pass.
        assert count == 2
        await asyncio.sleep(0.05)  # let fire-and-forget dispatch tasks settle

    async def test_wait_for_resurrection_with_active_tasks(self, mock_redis) -> None:
        """wait_for_resurrection drains pending dispatches before returning."""
        completed = asyncio.Event()

        async def _short() -> None:
            completed.set()

        bg = asyncio.create_task(_short())
        PhoenixRegistry._active_resurrections["wait_test"] = bg
        try:
            await PhoenixRegistry._wait_for_resurrection(timeout=1.0)
            assert completed.is_set()
        finally:
            PhoenixRegistry._active_resurrections.pop("wait_test", None)

    async def test_validate_execution_all_none_returns_true(self, mock_redis) -> None:
        """No fence/lease args → validate_execution returns True without Redis call."""
        mock_redis.eval = AsyncMock(side_effect=AssertionError("should not be called"))
        result = await PhoenixRegistry.validate_execution(
            "t", mock_redis, None, None, None
        )
        assert result is True
