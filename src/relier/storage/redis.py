"""
Relier Storage Layer — Redis Runtime Integration.

Provides lifecycle management for Relier's asynchronous Redis connection
pools.

Redis serves as the coordination backbone for:
- Phoenix resurrection
- idempotency
- admission control
- distributed locking
- task state persistence
- SLO telemetry

The manager implements:
- lazy pool initialization
- event-loop affinity
- fork-safe pool invalidation
- async-safe concurrency guards
- deterministic connection teardown
"""

import asyncio
import logging
from typing import Any

import redis.asyncio as redis
from redis.asyncio.client import Redis
from redis.asyncio.sentinel import Sentinel

from relier.config import Settings, get_settings

logger = logging.getLogger(__name__)


class RedisManager:
    """
    Coordinates loop-affined Redis connection pools for asynchronous runtimes.

    A separate Redis client is maintained per active asyncio event loop to
    prevent cross-loop resource corruption and inherited post-fork state.

    IMPORTANT:
    Redis clients are event-loop bound and must only be initialized from
    within an active asyncio runtime.

    Creating clients during module import or process bootstrap may bind
    the connection pool to the wrong loop, resulting in cross-loop
    execution failures such as:

        RuntimeError: Task attached to a different loop

    Relier prevents this by maintaining loop-local client caches keyed
    by ``id(asyncio.get_running_loop())``.
    """

    def __init__(self) -> None:
        # Loop-local Redis client registry.
        self._clients: dict[int, Redis] = {}
        # Loop-local Sentinel managers (only populated when Sentinel is enabled).
        # Held so their connections to the Sentinel instances can be torn down.
        self._sentinels: dict[int, Sentinel] = {}
        # Per-loop initialization locks preventing concurrent pool creation.
        self._locks: dict[int, asyncio.Lock] = {}

    @property
    def settings(self) -> Settings:
        """Lazy-load settings to avoid circular imports and ensure environment variables are read."""
        return get_settings()

    def _get_safe_log_url(self) -> str:
        """
        Return a sanitized Redis endpoint suitable for structured logging.
        """
        settings = self.settings
        if settings.redis_use_sentinel:
            nodes = ",".join(f"{h}:{p}" for h, p in settings.sentinel_node_list)
            return f"sentinel://{nodes}/{settings.redis_sentinel_master_name}"
        url_obj = settings.redis_url
        return f"redis://***@{url_obj.host}:{url_obj.port}{url_obj.path or '/0'}"

    def _create_client(self, loop_id: int) -> Redis:
        """
        Construct a Redis client for the current event loop.

        When Sentinel is enabled, the client is backed by a Sentinel-managed
        connection pool that transparently re-discovers the master after a
        failover. Otherwise a direct connection pool to ``redis_url`` is used.
        """
        settings = self.settings
        password = (
            settings.redis_password.get_secret_value()
            if settings.redis_password is not None
            else None
        )

        if settings.redis_use_sentinel:
            sentinel_kwargs: dict[str, Any] = {
                "socket_timeout": settings.redis_socket_timeout,
                "socket_connect_timeout": settings.redis_connect_timeout,
            }
            if settings.redis_sentinel_password is not None:
                # Authenticates the connections to the Sentinel instances
                # themselves, which is distinct from the data-node password.
                sentinel_kwargs["password"] = (
                    settings.redis_sentinel_password.get_secret_value()
                )

            sentinel = Sentinel(
                settings.sentinel_node_list,
                # Connection kwargs below are reused when dialing the
                # Sentinel-discovered master.
                socket_timeout=settings.redis_socket_timeout,
                socket_connect_timeout=settings.redis_connect_timeout,
                sentinel_kwargs=sentinel_kwargs,
            )
            self._sentinels[loop_id] = sentinel

            # password is forwarded explicitly: the Sentinel path has no
            # connection URL whose credentials could otherwise be clobbered.
            master: Redis = sentinel.master_for(
                settings.redis_sentinel_master_name,
                encoding="utf-8",
                decode_responses=True,
                password=password,
                health_check_interval=settings.redis_health_check_interval,
                max_connections=settings.redis_max_connections,
            )
            return master

        # Direct connection. Auth is taken from redis_url unless redis_password
        # is set explicitly, in which case it overrides the URL credentials.
        direct_kwargs: dict[str, Any] = {
            "encoding": "utf-8",
            "decode_responses": True,
            "socket_timeout": settings.redis_socket_timeout,
            "socket_connect_timeout": settings.redis_connect_timeout,
            "health_check_interval": settings.redis_health_check_interval,
            "max_connections": settings.redis_max_connections,
        }
        if password is not None:
            direct_kwargs["password"] = password
        client: Redis = redis.from_url(str(settings.redis_url), **direct_kwargs)
        return client

    async def _test_reset(self) -> None:
        """
        Testing utility that forcefully tears down all managed pools and
        clears loop-local runtime state.
        """
        for client in self._clients.values():
            await client.aclose()
        for sentinel in self._sentinels.values():
            for sentinel_client in sentinel.sentinels:
                await sentinel_client.aclose()
        self._clients.clear()
        self._sentinels.clear()
        self._locks.clear()

    async def get_client(self) -> Redis:
        """
        Return the Redis client bound to the current asyncio event loop.

        Pools are initialized lazily and cached per-loop to preserve asyncio
        resource affinity guarantees.
        """

        # Event-loop identity used for loop-local client ownership.
        loop_id = id(asyncio.get_running_loop())

        if loop_id not in self._locks:
            logger.debug(
                "Initializing loop-local Redis creation lock -> [%s].", loop_id
            )
            self._locks[loop_id] = asyncio.Lock()

        if loop_id not in self._clients:
            # Celery worker forks may inherit stale client pools from the parent
            # process. Remove all foreign loop registrations so each worker process
            # establishes its own isolated runtime connections.
            if len(self._clients) > 0:
                foreign_ids = [lid for lid in self._clients if lid != loop_id]
                for lid in foreign_ids:
                    self._clients.pop(lid, None)
                    self._sentinels.pop(lid, None)
                    self._locks.pop(lid, None)

            async with self._locks[loop_id]:
                # Double-check after acquiring the lock to prevent duplicate pool
                # initialization under concurrent startup pressure.
                if loop_id not in self._clients:
                    logger.info(
                        "Initializing loop-local Relier Redis connection pool. [%s -> %s]",
                        loop_id,
                        self._get_safe_log_url(),
                    )

                    # Create a dedicated async connection pool for this event loop.
                    self._clients[loop_id] = self._create_client(loop_id)
                    logger.debug(
                        "Redis connection pool initialized successfully. %s.", loop_id
                    )

        return self._clients[loop_id]

    async def close(self) -> None:
        """
        Gracefully tear down the Redis pool associated with the current
        asyncio event loop.
        """

        # Shutdown may occur outside an active event loop during interpreter
        # teardown or worker termination.
        try:
            loop_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return

        if loop_id in self._locks:
            async with self._locks[loop_id]:
                if loop_id in self._clients:
                    logger.info(
                        "Closing loop-local Redis connection pool.",
                        extra={"loop_id": loop_id},
                    )
                    client = self._clients.pop(loop_id)
                    await client.aclose()

                # Tear down the Sentinel manager's own connections to the
                # Sentinel instances, which the master client does not own.
                sentinel = self._sentinels.pop(loop_id, None)
                if sentinel is not None:
                    for sentinel_client in sentinel.sentinels:
                        await sentinel_client.aclose()

            self._locks.pop(loop_id, None)

    async def ping(self) -> bool:
        """
        Perform a lightweight Redis liveness check against the active pool.
        """
        try:
            client = await self.get_client()
            return await client.ping()  # type: ignore[no-any-return]
        except Exception as e:
            logger.error("Redis health check failed.", extra={"error": str(e)})
            return False


# Process-local Redis runtime manager shared across Relier subsystems.
redis_manager = RedisManager()


# Shared accessor for the loop-local Redis client used across Relier subsystems.
async def get_relier_redis() -> Redis:
    """
    Return the Redis client associated with the current execution loop.
    """
    return await redis_manager.get_client()
