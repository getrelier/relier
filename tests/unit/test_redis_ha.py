"""Unit tests for the Redis high-availability wiring.

Covers Sentinel-aware configuration parsing, Sentinel/direct client
construction, the Celery broker config builder, and the AOF validation check.
"""

from unittest.mock import AsyncMock, patch

import pytest
from redis.asyncio.client import Redis
from redis.exceptions import ResponseError

from relier.config import Settings
from relier.core.validation import validate_redis_config
from relier.storage.redis import RedisManager
from relier.tasks.app import _broker_connection_config

pytestmark = pytest.mark.asyncio


# =============================================================================
# Settings.sentinel_node_list
# =============================================================================
class TestSentinelNodeList:
    async def test_parses_host_port_pairs(self) -> None:
        settings = Settings(redis_sentinel_nodes=" s1:26379, s2:26380 ,s3:26381")
        assert settings.sentinel_node_list == [
            ("s1", 26379),
            ("s2", 26380),
            ("s3", 26381),
        ]

    async def test_empty_when_disabled_is_allowed(self) -> None:
        settings = Settings(redis_use_sentinel=False, redis_sentinel_nodes="")
        assert settings.sentinel_node_list == []

    async def test_empty_when_enabled_raises(self) -> None:
        settings = Settings(redis_use_sentinel=True, redis_sentinel_nodes="")
        with pytest.raises(ValueError, match="redis_sentinel_nodes is empty"):
            _ = settings.sentinel_node_list

    async def test_malformed_node_raises(self) -> None:
        settings = Settings(redis_sentinel_nodes="not-a-node")
        with pytest.raises(ValueError, match="Invalid Sentinel node"):
            _ = settings.sentinel_node_list


# =============================================================================
# RedisManager._create_client
# =============================================================================
class TestCreateClient:
    async def test_direct_client(self) -> None:
        settings = Settings(redis_use_sentinel=False)
        manager = RedisManager()
        with patch("relier.storage.redis.get_settings", return_value=settings):
            client = manager._create_client(1)
        assert isinstance(client, Redis)
        assert manager._sentinels == {}
        await client.aclose()

    async def test_direct_client_with_password(self) -> None:
        settings = Settings(redis_use_sentinel=False, redis_password="hunter2")
        manager = RedisManager()
        with patch("relier.storage.redis.get_settings", return_value=settings):
            client = manager._create_client(1)
        assert isinstance(client, Redis)
        await client.aclose()

    async def test_sentinel_client_registers_manager(self) -> None:
        settings = Settings(
            redis_use_sentinel=True,
            redis_sentinel_nodes="s1:26379,s2:26379,s3:26379",
            redis_sentinel_master_name="relier-master",
            redis_password="hunter2",
            redis_sentinel_password="watcher",
        )
        manager = RedisManager()
        with patch("relier.storage.redis.get_settings", return_value=settings):
            client = manager._create_client(7)
        assert isinstance(client, Redis)
        # The Sentinel manager is retained per-loop so its connections can be
        # torn down on close.
        assert 7 in manager._sentinels
        assert len(manager._sentinels[7].sentinels) == 3
        await manager._test_reset()

    async def test_safe_log_url_sentinel(self) -> None:
        settings = Settings(
            redis_use_sentinel=True,
            redis_sentinel_nodes="s1:26379,s2:26379",
            redis_sentinel_master_name="relier-master",
        )
        manager = RedisManager()
        with patch("relier.storage.redis.get_settings", return_value=settings):
            url = manager._get_safe_log_url()
        assert url == "sentinel://s1:26379,s2:26379/relier-master"


# =============================================================================
# _broker_connection_config (Celery)
# =============================================================================
class TestBrokerConnectionConfig:
    async def test_direct_broker(self) -> None:
        settings = Settings(redis_use_sentinel=False)
        with patch("relier.tasks.app.get_settings", return_value=settings):
            conf = _broker_connection_config()
        assert conf["broker_url"] == str(settings.redis_url)
        assert conf["result_backend"] == str(settings.redis_url)
        assert "broker_transport_options" not in conf

    async def test_sentinel_broker(self) -> None:
        settings = Settings(
            redis_use_sentinel=True,
            redis_sentinel_nodes="s1:26379,s2:26379,s3:26379",
            redis_sentinel_master_name="relier-master",
            redis_password="hunter2",
            redis_sentinel_password="watcher",
        )
        with patch("relier.tasks.app.get_settings", return_value=settings):
            conf = _broker_connection_config()

        assert conf["broker_url"] == (
            "sentinel://:hunter2@s1:26379;"
            "sentinel://:hunter2@s2:26379;"
            "sentinel://:hunter2@s3:26379"
        )
        assert conf["broker_url"] == conf["result_backend"]
        opts = conf["broker_transport_options"]
        assert opts["master_name"] == "relier-master"
        assert opts["sentinel_kwargs"] == {"password": "watcher"}
        assert conf["result_backend_transport_options"] is opts

    async def test_sentinel_broker_without_passwords(self) -> None:
        settings = Settings(
            redis_use_sentinel=True,
            redis_sentinel_nodes="s1:26379",
            redis_sentinel_master_name="relier-master",
        )
        with patch("relier.tasks.app.get_settings", return_value=settings):
            conf = _broker_connection_config()
        assert conf["broker_url"] == "sentinel://s1:26379"
        assert "sentinel_kwargs" not in conf["broker_transport_options"]


# =============================================================================
# validate_redis_config — AOF check
# =============================================================================
class TestAofValidation:
    @staticmethod
    def _redis(*, policy: str = "noeviction", appendonly: object = "yes") -> AsyncMock:
        redis = AsyncMock()
        responses = [{"maxmemory-policy": policy}]
        if isinstance(appendonly, Exception):
            redis.config_get.side_effect = [responses[0], appendonly]
        else:
            responses.append({"appendonly": appendonly})
            redis.config_get.side_effect = responses
        return redis

    async def test_aof_enabled_passes(self) -> None:
        settings = Settings()
        redis = self._redis(appendonly="yes")
        await validate_redis_config(redis, settings)
        assert redis.config_get.await_count == 2

    async def test_aof_disabled_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        settings = Settings()
        redis = self._redis(appendonly="no")
        with caplog.at_level("WARNING"):
            await validate_redis_config(redis, settings)
        assert "AOF persistence is DISABLED" in caplog.text

    async def test_aof_config_restricted_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        settings = Settings()
        redis = self._redis(appendonly=ResponseError("unknown command"))
        with caplog.at_level("WARNING"):
            await validate_redis_config(redis, settings)
        assert "Unable to verify Redis AOF" in caplog.text

    async def test_aof_transient_error_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        settings = Settings()
        redis = self._redis(appendonly=RuntimeError("blip"))
        with caplog.at_level("WARNING"):
            await validate_redis_config(redis, settings)
        assert "Skipped Redis AOF verification" in caplog.text
