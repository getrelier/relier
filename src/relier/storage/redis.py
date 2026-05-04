"""
Relier Storage Layer — Redis Integration.

This module manages the asynchronous connection pool to Redis, serving as
the centralized state store for Relier's reliability features. It ensures
thread-safe, lazy initialization, loop-aware caching, and robust connection lifecycle management.
"""

import asyncio
import logging

import redis.asyncio as redis
from redis.asyncio.client import Redis

from relier.config import Settings, get_settings

logger = logging.getLogger(__name__)


class RedisManager:
    """
    Manages the lifecycle of the Redis connection pool.

    CRITICAL DESIGN NOTE:
    Do NOT call get_client() at module import time.
    Call it only inside a running event loop (e.g., inside a task or a FastAPI
    dependency). Calling at import time binds the pool to the wrong loop
    and causes 'Task attached to a different loop' errors.

    Relier handles this automatically via loop-aware caching (Dict[int, Redis]).

    """

    def __init__(self) -> None:
        # Dictionary mappings to tie clients and locks to specific event loops
        self._clients: dict[int, Redis] = {}
        self._locks: dict[int, asyncio.Lock] = {}

    @property
    def settings(self) -> Settings:
        """Lazy-load settings to avoid circular imports and ensure environment variables are read."""
        return get_settings()

    def _get_safe_log_url(self) -> str:
        """Returns a sanitized Redis URL for logging (masks the password)."""
        url_obj = self.settings.redis_url
        return f"redis://***@{url_obj.host}:{url_obj.port}{url_obj.path or '/0'}"

    async def _test_reset(self) -> None:
        """TESTING ONLY: Forcibly close all connections and clear state."""
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
        self._locks.clear()

    async def get_client(self) -> Redis:
        """
        Retrieve the active Redis client, initializing it if necessary.
        Uses loop-aware caching to prevent 'Task attached to a different loop' errors.
        """
        loop_id = id(asyncio.get_running_loop())

        # Ensure a lock exists for this loop to prevent concurrent initialization
        if loop_id not in self._locks:
            logger.debug("Creating new Redis lock for loop %s.", loop_id)
            self._locks[loop_id] = asyncio.Lock()

        if loop_id not in self._clients:
            async with self._locks[loop_id]:
                # Double-checked locking pattern
                if loop_id not in self._clients:
                    logger.info(
                        "Initializing Relier Redis pool on loop %s -> %s",
                        loop_id,
                        self._get_safe_log_url(),
                    )

                    self._clients[loop_id] = redis.from_url(
                        str(self.settings.redis_url),
                        encoding="utf-8",
                        decode_responses=True,
                        socket_timeout=self.settings.redis_socket_timeout,
                        socket_connect_timeout=self.settings.redis_connect_timeout,
                        health_check_interval=self.settings.redis_health_check_interval,
                        max_connections=self.settings.redis_max_connections,
                    )
                    logger.debug("Relier Redis pool initialized for loop %s.", loop_id)
        return self._clients[loop_id]

    async def close(self) -> None:
        """Gracefully close the Redis connection pool for the current event loop."""
        try:
            loop_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return

        if loop_id in self._locks:
            async with self._locks[loop_id]:
                if loop_id in self._clients:
                    logger.info(
                        "Closing Relier Redis connection pool.",
                        extra={"loop_id": loop_id},
                    )
                    client = self._clients.pop(loop_id)
                    await client.aclose()

            self._locks.pop(loop_id, None)

    async def ping(self) -> bool:
        """Perform a health check on the Redis connection."""
        try:
            client = await self.get_client()
            return await client.ping()  # type: ignore[no-any-return]
        except Exception as e:
            logger.error("Redis ping failed.", extra={"error": str(e)})
            return False


# Global instance for shared access across the library
redis_manager = RedisManager()


# Helper function to easily inject into FastAPI dependencies
async def get_relier_redis() -> Redis:
    """Dependency injection helper for FastAPI routers."""
    return await redis_manager.get_client()
