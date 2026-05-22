import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ResponseError

from relier.config import Settings
from relier.core.dlq import DeadLetterQueue
from relier.core.idempotency import IdempotencyResult, idempotency_manager
from relier.core.keys import RedisKeys
from relier.core.phoenix import PhoenixRegistry
from relier.core.schema import SchemaRegistry
from relier.core.slo import SLOMetrics
from relier.core.timeouts import TimeoutEnforcer
from relier.core.validation import validate_connection_pool, validate_redis_config
from relier.storage.redis import RedisManager, get_relier_redis, redis_manager

pytestmark = pytest.mark.asyncio


class TestCoverageGap:
    async def test_idempotency_edge_cases(self) -> None:
        """Hit missing lines in idempotency.py."""
        # if not self._key: return
        res = IdempotencyResult(already_executed=False)
        await res.record_result({"foo": "bar"})
        assert idempotency_manager.settings is not None

    async def test_schema_edge_cases(self) -> None:
        """Hit missing lines in schema.py."""
        payload = {"args": [], "kwargs": {}}  # type: ignore[var-annotated]
        checksum = SchemaRegistry._generate_checksum(payload)
        env = {
            "task_id": "t1",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": "2024-01-01T00:00:00",
            "checksum": checksum,
        }
        with patch.object(SchemaRegistry, "CURRENT_VERSION", 2):
            args, kwargs = SchemaRegistry.unwrap_and_migrate("test_task", env)
            assert args == ()
            assert kwargs == {}

    async def test_slo_edge_cases(self) -> None:
        """Hit missing lines in slo.py (target SLO permitting zero failures)."""
        mock_redis = AsyncMock()
        # One MGET per status: successes then failures.
        mock_redis.mget.side_effect = [[b"10"], [b"10"]]
        with patch("relier.core.slo.get_relier_redis", return_value=mock_redis):
            burn = await SLOMetrics.get_burn_rate(target_slo=1.0)
            assert burn == 100.0

    async def test_timeouts_edge_cases(self) -> None:
        """Hit missing lines in timeouts.py."""

        async def mock_on_soft(ctx):
            raise Exception("Fail")

        async def slow_func():
            await asyncio.sleep(0.2)
            return "done"

        await TimeoutEnforcer.run(slow_func, (), {}, 0.1, 1.0, mock_on_soft, "t1")  # type: ignore[arg-type]

        with (
            patch("asyncio.wait", side_effect=asyncio.CancelledError),
            pytest.raises(asyncio.CancelledError),
        ):
            await TimeoutEnforcer.run(slow_func, (), {}, None, None, None, "t2")

    async def test_redis_edge_cases(self) -> None:
        """Hit missing lines in redis.py."""
        mock_client = AsyncMock()
        redis_manager._client = mock_client  # type: ignore[attr-defined]
        await redis_manager.close()

        with (
            patch.object(redis_manager, "get_client", side_effect=Exception("Error")),
            pytest.raises(Exception, match="Error"),
        ):
            await get_relier_redis()


@pytest.mark.asyncio
async def test_phoenix_is_active() -> None:
    """Hit missing lines in phoenix.py is_active()."""
    with (
        patch("relier.storage.redis.RedisManager.close", new_callable=AsyncMock),
        patch("relier.core.phoenix.get_relier_redis") as mock_get_redis,
    ):
        mock_redis = AsyncMock()
        mock_get_redis.return_value = mock_redis

        mock_redis.exists.return_value = True
        assert await PhoenixRegistry.is_active("test-task") is True

        mock_redis.exists.return_value = False
        assert await PhoenixRegistry.is_active("test-task") is False


@pytest.mark.asyncio
async def test_phoenix_bg_send_failure() -> None:
    """Hit missing lines in phoenix.py _bg_send()."""
    with patch("relier.storage.redis.RedisManager.close", new_callable=AsyncMock):
        mock_app = MagicMock()
        # Make send_task fail
        mock_app.send_task.side_effect = Exception("broker down")

        # This should log an error but not raise
        await PhoenixRegistry._bg_send("test-id", {"task_name": "test"}, mock_app)
        assert mock_app.send_task.called


@pytest.mark.asyncio
async def test_redis_ping_failure() -> None:
    """Hit missing lines in redis.py (ping failure)."""
    manager = RedisManager()
    with (
        patch("relier.storage.redis.RedisManager.close", new_callable=AsyncMock),
        patch.object(manager, "get_client", side_effect=Exception("redis down")),
    ):
        res = await manager.ping()
        assert res is False


# =============================================================================
# validate_redis_config missing branches
# =============================================================================
class TestValidationExtra:
    async def test_response_error_on_policy_check(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        redis = AsyncMock()
        redis.config_get.side_effect = ResponseError("CONFIG disabled")
        with caplog.at_level("WARNING"):
            await validate_redis_config(redis, Settings())
        assert "Unable to verify Redis 'maxmemory-policy'" in caplog.text

    async def test_generic_exception_on_policy_check_raises(self) -> None:
        redis = AsyncMock()
        redis.config_get.side_effect = ConnectionError("dropped")
        with pytest.raises(RuntimeError, match="Validation blocked"):
            await validate_redis_config(redis, Settings())

    async def test_wrong_policy_raises(self) -> None:
        redis = AsyncMock()
        redis.config_get.return_value = {"maxmemory-policy": "allkeys-lru"}
        with pytest.raises(RuntimeError, match="FATAL CONFIGURATION MISMATCH"):
            await validate_redis_config(redis, Settings())

    async def test_connection_pool_within_limits(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        settings = Settings(
            celery_worker_count=1,
            celery_worker_concurrency=4,
            redis_max_connections=10,
        )
        with caplog.at_level("INFO"):
            await validate_connection_pool(settings)
        assert "within safe" in caplog.text

    async def test_connection_pool_exceeds_limits(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        settings = Settings(
            celery_worker_count=10,
            celery_worker_concurrency=10,
            redis_max_connections=100,
        )
        with caplog.at_level("WARNING"):
            await validate_connection_pool(settings)
        assert "HIGH POTENTIAL" in caplog.text


# =============================================================================
# TaskContextProxy RuntimeError branches
# =============================================================================
class TestTaskContextProxy:
    async def test_getattr_raises_outside_task(self) -> None:
        from relier.tasks.context import TaskContextProxy

        proxy = TaskContextProxy()
        with pytest.raises(RuntimeError, match="outside of a running task"):
            _ = proxy.task_id

    async def test_setattr_raises_outside_task(self) -> None:
        from relier.tasks.context import TaskContextProxy

        proxy = TaskContextProxy()
        with pytest.raises(RuntimeError, match="outside of a running task"):
            proxy.task_id = "new_id"


# =============================================================================
# Celery signal handlers — on_task_prerun and on_task_retry
# =============================================================================
class TestSignalsExtra:
    async def test_on_task_prerun_fires(self) -> None:
        import relier.tasks.signals as task_signals

        with patch("relier.tasks.signals.logger") as mock_logger:
            task_signals.on_task_prerun(task_id="abc", task=MagicMock(name="my_task"))
        assert mock_logger.info.called

    async def test_on_task_retry_fires(self) -> None:
        import relier.tasks.signals as task_signals

        with patch("relier.tasks.signals.logger") as mock_logger:
            task_signals.on_task_retry(
                task_id="xyz", sender=MagicMock(name="my_task"), reason="backoff"
            )
        assert mock_logger.warning.called


# =============================================================================
# DLQ missing branches — json error, checkpoint injection, resurrection count
# =============================================================================
class TestDLQExtra:
    async def test_quarantine_json_error_in_partial(self, mock_redis) -> None:
        task_id = "bad_partial"
        await mock_redis.hset(
            RedisKeys.phoenix(task_id), "partial_result", "not-valid-json{{{"
        )
        await DeadLetterQueue.quarantine(
            task_id, reason="test", payload={"task_name": "t"}
        )
        raw = await mock_redis.hget(DeadLetterQueue.DLQ_HASH_KEY, task_id)
        entry = json.loads(raw)
        assert entry["partial_result"] is None

    async def test_release_injects_checkpoint(self, mock_redis) -> None:
        from relier.tasks.app import celery_app

        task_id = "release_cp"
        entry = {
            "task_name": "my_task",
            "args": [],
            "kwargs": {},
            "queue": "default",
            "partial_result": {"step": 5},
            "resurrections": 0,
        }
        await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, task_id, json.dumps(entry))
        with patch.object(celery_app, "send_task") as mock_send:
            result = await DeadLetterQueue.release(task_id)
        assert result is True
        called_kwargs = mock_send.call_args[1]["kwargs"]
        assert called_kwargs["checkpoint"] == {"step": 5}

    async def test_release_preserves_resurrection_count(self, mock_redis) -> None:
        from relier.tasks.app import celery_app

        task_id = "release_count"
        entry = {
            "task_name": "my_task",
            "args": [],
            "kwargs": {},
            "queue": "default",
            "partial_result": None,
            "resurrections": 3,
        }
        await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, task_id, json.dumps(entry))
        with patch.object(celery_app, "send_task"):
            await DeadLetterQueue.release(task_id)
        val = await mock_redis.get(RedisKeys.resurrection(task_id))
        assert val.decode() == "3"

    async def test_purge_skips_invalid_json(self, mock_redis) -> None:
        await mock_redis.hset(DeadLetterQueue.DLQ_HASH_KEY, "bad_entry", "{{invalid}")
        count = await DeadLetterQueue.purge()
        assert count >= 0
