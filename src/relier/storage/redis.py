"""
Relier Storage Layer — Redis Runtime Integration.

Provides lifecycle management for Relier's asynchronous Redis connection
pools.  Redis serves as the coordination backbone for Phoenix resurrection,
idempotency, admission control, distributed locking, task-state persistence,
and SLO telemetry.

The :class:`RedisManager` implements:

- Lazy pool initialisation
- Event-loop affinity (one pool per asyncio loop)
- Fork-safe pool invalidation (Celery prefork workers)
- Async-safe concurrency guards (per-loop ``asyncio.Lock``)
- Deterministic connection teardown

All low-level ``redis.exceptions`` are re-raised as :class:`~relier.core.exceptions.RedisConnectionError`
with a descriptive message that names the configured endpoint and a
one-sentence fix suggestion, so raw dependency errors never leak into
user stack traces.
"""

import asyncio
import contextlib
import logging
from typing import Any

import redis.asyncio as redis
import redis.exceptions as redis_exc
from redis.asyncio.client import Redis
from redis.asyncio.sentinel import Sentinel

from relier.config import Settings, get_settings
from relier.core.exceptions import RedisConnectionError as RelierRedisConnectionError

logger = logging.getLogger(__name__)


class RedisManager:
    """Coordinator for loop-affined Redis connection pools.

    A separate Redis client is maintained per active asyncio event loop to
    prevent cross-loop resource corruption and inherited post-fork state.

    .. important::
        Redis clients are event-loop bound and must only be initialised from
        within an active asyncio runtime.  Creating clients during module
        import or process bootstrap may bind the pool to the wrong loop,
        producing::

            RuntimeError: Task attached to a different loop

        Relier prevents this by keying the client cache on
        ``id(asyncio.get_running_loop())``.

    Example:
        >>> from relier.storage.redis import redis_manager
        >>> async def handler():
        ...     client = await redis_manager.get_client()
        ...     await client.set("key", "value")
        ...     return await client.get("key")
    """

    def __init__(self) -> None:
        # Loop-local Redis client registry.
        self._clients: dict[int, Redis] = {}
        # Loop-local Sentinel managers (only populated when Sentinel is enabled).
        self._sentinels: dict[int, Sentinel] = {}
        # Per-loop initialisation locks preventing concurrent pool creation.
        self._locks: dict[int, asyncio.Lock] = {}

    @property
    def settings(self) -> Settings:
        """Return the validated :class:`~relier.config.Settings` singleton.

        Loaded lazily to avoid circular imports and ensure environment
        variables are resolved after process bootstrap.
        """
        return get_settings()

    def _get_safe_log_url(self) -> str:
        """Return a sanitised Redis endpoint suitable for structured logging.

        Replaces credentials in the URL with ``***`` to avoid leaking secrets
        into log streams.

        Returns:
            A string like ``redis://***@host:6379/0`` or
            ``sentinel://s1:26379,s2:26379/master-name``.
        """
        settings = self.settings
        if settings.redis_use_sentinel:
            nodes = ",".join(f"{h}:{p}" for h, p in settings.sentinel_node_list)
            return f"sentinel://{nodes}/{settings.redis_sentinel_master_name}"
        url_obj = settings.redis_url
        return f"redis://***@{url_obj.host}:{url_obj.port}{url_obj.path or '/0'}"

    def _create_client(self, loop_id: int) -> Redis:
        """Construct a Redis client bound to the current event loop.

        When Sentinel is enabled, the client is backed by a
        Sentinel-managed pool that transparently re-discovers the master
        after a failover.  Otherwise a direct pool to ``redis_url`` is used.

        Args:
            loop_id: The ``id()`` of the currently running asyncio event loop.
                     Used only to register the Sentinel manager in
                     ``self._sentinels`` for clean teardown.

        Returns:
            A configured, but not yet connected, async Redis client.

        Raises:
            RedisConnectionError: If the URL or Sentinel node list is invalid
                and the client cannot be constructed.
        """
        settings = self.settings
        password = (
            settings.redis_password.get_secret_value()
            if settings.redis_password is not None
            else None
        )

        try:
            if settings.redis_use_sentinel:
                sentinel_kwargs: dict[str, Any] = {
                    "socket_timeout": settings.redis_socket_timeout,
                    "socket_connect_timeout": settings.redis_connect_timeout,
                }
                if settings.redis_sentinel_password is not None:
                    # Authenticates connections to the Sentinel instances
                    # themselves, distinct from the data-node password.
                    sentinel_kwargs["password"] = (
                        settings.redis_sentinel_password.get_secret_value()
                    )

                sentinel = Sentinel(
                    settings.sentinel_node_list,
                    socket_timeout=settings.redis_socket_timeout,
                    socket_connect_timeout=settings.redis_connect_timeout,
                    sentinel_kwargs=sentinel_kwargs,
                )
                self._sentinels[loop_id] = sentinel

                master: Redis = sentinel.master_for(
                    settings.redis_sentinel_master_name,
                    encoding="utf-8",
                    decode_responses=True,
                    password=password,
                    health_check_interval=settings.redis_health_check_interval,
                    max_connections=settings.redis_max_connections,
                )
                return master

            # Direct connection.  Auth is taken from redis_url unless
            # redis_password is set explicitly, in which case it overrides
            # the URL credentials.
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

        except (redis_exc.RedisError, ValueError, OSError) as exc:
            raise RelierRedisConnectionError(
                f"Failed to construct Redis client for endpoint "
                f"'{self._get_safe_log_url()}'.  "
                f"Verify RELIER_REDIS_URL (or Sentinel nodes) and that Redis "
                f"is reachable from this host.  "
                f"Underlying error: {type(exc).__name__}: {exc}"
            ) from exc

    async def _test_reset(self) -> None:
        """Forcefully tear down all managed pools and clear runtime state.

        This method is intended exclusively for test isolation.  It closes
        all loop-local clients without waiting for in-flight commands.
        """
        for client in list(self._clients.values()):
            with contextlib.suppress(RuntimeError):
                await client.aclose()
        for sentinel in list(self._sentinels.values()):
            for sentinel_client in sentinel.sentinels:
                with contextlib.suppress(RuntimeError):
                    await sentinel_client.aclose()
        self._clients.clear()
        self._sentinels.clear()
        self._locks.clear()

    async def get_client(self) -> Redis:
        """Return the Redis client bound to the current asyncio event loop.

        Pools are initialised lazily on first call and cached for the
        lifetime of the event loop to preserve asyncio resource affinity.

        Returns:
            A loop-local :class:`redis.asyncio.client.Redis` instance.

        Raises:
            RedisConnectionError: If the connection pool cannot be created
                (bad URL, unreachable host, or invalid credentials).

        Example:
            >>> async def example():
            ...     client = await redis_manager.get_client()
            ...     pong = await client.ping()
            ...     assert pong is True
        """
        loop_id = id(asyncio.get_running_loop())

        if loop_id not in self._locks:
            logger.debug(
                "Initialising loop-local Redis creation lock -> [%s].", loop_id
            )
            self._locks[loop_id] = asyncio.Lock()

        if loop_id not in self._clients:
            # Celery worker forks may inherit stale pools from the parent
            # process.  Remove all foreign loop registrations so each worker
            # process establishes its own isolated runtime connections.
            if len(self._clients) > 0:
                foreign_ids = [lid for lid in self._clients if lid != loop_id]
                for lid in foreign_ids:
                    self._clients.pop(lid, None)
                    self._sentinels.pop(lid, None)
                    self._locks.pop(lid, None)

            async with self._locks[loop_id]:
                # Double-check after acquiring the lock to prevent duplicate
                # pool initialisation under concurrent start-up pressure.
                if loop_id not in self._clients:
                    logger.info(
                        "Initialising loop-local Relier Redis connection pool. "
                        "[loop=%s -> %s]",
                        loop_id,
                        self._get_safe_log_url(),
                    )
                    self._clients[loop_id] = self._create_client(loop_id)
                    logger.debug("Redis connection pool initialised. loop=%s.", loop_id)

        return self._clients[loop_id]

    async def close(self) -> None:
        """Gracefully tear down the Redis pool for the current event loop.

        Safe to call during worker shutdown or at the end of an asyncio
        application's lifespan.  No-ops if the pool was already closed or
        if called outside an active event loop (e.g. during interpreter
        teardown).

        Example:
            >>> async def on_shutdown():
            ...     await redis_manager.close()
        """
        try:
            loop_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return  # Called outside an active event loop (interpreter teardown).

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
        """Perform a lightweight Redis liveness check on the active pool.

        Returns:
            ``True`` if Redis responds to ``PING``, ``False`` otherwise.
            This method never raises, connectivity failures are logged and
            swallowed so callers can use the return value for health checks.

        Example:
            >>> async def health_check():
            ...     if not await redis_manager.ping():
            ...         raise RuntimeError("Redis unavailable")
        """
        try:
            client = await self.get_client()
            return await client.ping()  # type: ignore[no-any-return]
        except Exception as exc:
            logger.error(
                "Redis health check failed.",
                extra={"error_type": type(exc).__name__, "error": str(exc)},
            )
            return False


# Process-local Redis runtime manager shared across Relier subsystems.
redis_manager = RedisManager()
"""The singleton :class:`RedisManager` for this worker process.

Relier subsystems obtain their loop-local Redis client via
:func:`get_relier_redis` rather than importing this directly.
"""


async def get_relier_redis() -> Redis:
    """Return the Redis client for the current asyncio event loop.

    Convenience shorthand for ``redis_manager.get_client()``.

    Returns:
        A loop-local :class:`redis.asyncio.client.Redis` instance.

    Raises:
        RedisConnectionError: If the connection pool cannot be initialised.

    Example:
        >>> async def my_task():
        ...     redis = await get_relier_redis()
        ...     await redis.set("hello", "world")
    """
    return await redis_manager.get_client()
