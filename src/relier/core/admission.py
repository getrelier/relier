"""
Relier Core — Admission Control.

Provides cluster-wide backpressure enforcement using an atomic Redis Lua script.

Admission checks execute before task enqueue so overloaded systems reject
traffic early rather than allowing unbounded queue growth or worker
cascade failures.

Implementation details:
- Uses Redis-side atomic execution via ``EVALSHA``.
- Implements a fixed-window counter with automatic TTL expiration.
- Avoids Python-side locking and race conditions entirely.

Failure policy:
If Redis is unavailable or script execution fails, admission control
fails open to preserve API availability during infrastructure outages.
"""

import logging
from collections.abc import Awaitable
from typing import cast

import redis.exceptions
from redis.asyncio import Redis

from relier.config import Settings, get_settings
from relier.core.keys import RedisKeys
from relier.storage.lua.scripts import ADMISSION_LUA
from relier.storage.redis import get_relier_redis
from relier.telemetry.metrics import admission_total

logger = logging.getLogger(__name__)


class AdmissionController:
    """
    Admission controller for cluster-wide capacity enforcement.

    Requests are evaluated before task enqueue so overload conditions are
    handled at the decorator but [.apush() or .push()] boundary instead of propagating downstream into
    workers and queues.
    """

    def __init__(self) -> None:
        self._script_sha: str = ""

    @property
    def settings(self) -> Settings:
        """
        Resolve settings lazily at runtime.

        Avoids capturing stale configuration during module import, which is
        important for tests that dynamically provision Redis containers.
        """
        return get_settings()

    async def _load_script(self, redis_client: Redis) -> str:
        """
        Load the admission Lua script into Redis and cache its SHA locally.

        Subsequent executions use ``EVALSHA`` to avoid repeatedly transmitting
        the script body over the network.
        """
        if not self._script_sha:
            self._script_sha = await redis_client.script_load(ADMISSION_LUA)
        return self._script_sha

    async def check_capacity(self, resource_key: str = "global") -> tuple[bool, int]:
        """
        Evaluate whether a task can enter the system.

        Admission checks are enforced using a Redis-backed fixed-window counter.
        If the configured limit is exceeded, the task is rejected and a
        retry-after duration is returned.

        Args:
            resource_key:
                Scope identifier for the admission window.

                Examples:
                - ``"global"`` for cluster-wide limits
                - tenant IDs for per-customer isolation
                - endpoint-specific keys for fine-grained throttling

        Returns:
            Tuple of ``(is_admitted, retry_after_seconds)``.

            When admitted, ``retry_after_seconds`` is always ``0``.
        """
        try:
            redis_client = await get_relier_redis()
            window_key = RedisKeys.admission(resource_key)

            limit = self.settings.admission_limit
            window_secs = self.settings.admission_window

            sha = await self._load_script(redis_client)
            result = await self._evalsha_with_fallback(
                redis_client, sha, window_key, limit, window_secs
            )

            is_admitted = bool(result[0])
            retry_after = max(0, int(result[2]))

            admission_total.add(
                1,
                {
                    "result": "admitted" if is_admitted else "rejected",
                    "resource": resource_key,
                },
            )

            if not is_admitted:
                logger.warning(
                    "Admission control rejected request.",
                    extra={
                        "resource_key": resource_key,
                        "limit": limit,
                        "retry_after": retry_after,
                    },
                )

            return is_admitted, retry_after

        except Exception as exc:
            # Fail open by design.
            # Admission control is a protective layer, not a hard dependency.
            # Preserving API availability takes priority during Redis outages.
            logger.error(
                "Admission control check failed (%s: %s) — failing open, task will proceed. "
                "Investigate if this recurs: sustained failure means rate limiting is inactive.",
                type(exc).__name__,
                exc,
                extra={"error": str(exc), "error_type": type(exc).__name__},
            )
            return True, 0

    async def _evalsha_with_fallback(
        self,
        redis_client: Redis,
        sha: str,
        window_key: str,
        limit: int,
        window_secs: int,
    ) -> list[int]:
        """
        Execute the cached Lua script with transparent NOSCRIPT recovery.

        Redis clears its script cache after restart or ``SCRIPT FLUSH``.
        When that occurs, the controller automatically reloads the script and
        retries execution once without surfacing the failure to callers.
        """
        args = [str(limit), str(window_secs)]

        try:
            result = await cast(
                Awaitable[list[int]], redis_client.evalsha(sha, 1, window_key, *args)
            )
            return result
        except redis.exceptions.NoScriptError:
            logger.warning(
                "Redis Lua script missing from cache; reloading admission script.",
                extra={"resource_key": window_key},
            )
            self._script_sha = await redis_client.script_load(ADMISSION_LUA)
            result = await cast(
                Awaitable[list[int]],
                redis_client.evalsha(self._script_sha, 1, window_key, *args),
            )
            return result


# Shared admission controller instance used across the process.
admission_control = AdmissionController()
