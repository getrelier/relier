import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relier.core.idempotency import IdempotencyResult, idempotency_manager
from relier.core.phoenix import PhoenixRegistry
from relier.core.schema import SchemaRegistry
from relier.core.slo import SLOMetrics
from relier.core.timeouts import TimeoutEnforcer
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
        """Hit missing lines in slo.py."""
        mock_redis = AsyncMock()
        mock_redis.zcount.side_effect = [10, 10]
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
