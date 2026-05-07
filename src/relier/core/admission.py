"""
Relier Core — Admission Control.

Enforces system-wide capacity limits using an atomic Redis Lua script.
Requests are evaluated and rejected at the API boundary — before any
FastAPI route handler, database call, or task enqueue.

The Lua script runs atomically inside Redis (no Python GIL, no race
conditions) and implements a fixed-window counter with automatic TTL.

Failure mode: if the script cannot be executed (e.g., Redis unreachable),
the controller **fails open** so that a Redis outage does not take down
the entire API.
"""

import logging
import typing

import redis.exceptions

from relier.config import Settings, get_settings
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua script — atomic fixed-window rate limiter
#
# Returns: [admitted (1|0), current_count, retry_after_seconds]
# ---------------------------------------------------------------------------
_ADMISSION_LUA = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
local limit = tonumber(ARGV[1])
if current > limit then
    return {0, current, redis.call('TTL', KEYS[1])}
end
return {1, current, 0}
"""


class AdmissionController:
    """Evaluates system capacity before admitting new requests.

    Typically invoked via ``AdmissionControlMiddleware`` on every inbound
    HTTP request, or directly in task dispatch code.
    """

    def __init__(self) -> None:
        self._script_sha: str = ""

    @property
    def settings(self) -> Settings:
        """Lazy load settings locally to pick up testcontainer URLs."""
        return get_settings()

    async def _load_script(self, redis_client: object) -> str:
        """Load the Lua script into Redis and cache the SHA.

        Uses ``EVALSHA`` on subsequent calls for maximum performance.
        """
        if not self._script_sha:
            self._script_sha = await redis_client.script_load(_ADMISSION_LUA)  # type: ignore[attr-defined]
        return self._script_sha

    async def check_capacity(self, resource_key: str = "global") -> tuple[bool, int]:
        """Evaluate whether a new request can be admitted.

        Args:
            resource_key: Scoping key for the rate-limit window.
                Use ``"global"`` for cluster-wide limits, or a tenant ID for
                per-customer limits.

        Returns:
            ``(is_admitted, retry_after_seconds)`` — if admitted, retry_after
            is always ``0``.
        """
        redis_client = await get_relier_redis()
        window_key = f"rl:admission:{resource_key}"
        limit = self.settings.admission_limit
        window_secs = self.settings.admission_window

        try:
            sha = await self._load_script(redis_client)
            result = await self._evalsha_with_fallback(
                redis_client, sha, window_key, limit, window_secs
            )

            is_admitted = bool(result[0])
            retry_after = int(result[2])

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
            # Fail open — a Redis failure must not take down the API.
            logger.error(
                "Admission control error; failing open.",
                extra={"error": str(exc)},
            )
            return True, 0

    async def _evalsha_with_fallback(
        self,
        redis_client: typing.Any,
        sha: str,
        window_key: str,
        limit: int,
        window_secs: int,
    ) -> typing.Any:
        """Execute the cached Lua script, reloading it on NOSCRIPT errors.

        Redis flushes its script cache on restart (``SCRIPT FLUSH`` or server
        restart).  This method transparently recovers by re-loading the script
        and retrying with ``EVALSHA`` once.
        """
        try:
            return await redis_client.evalsha(sha, 1, window_key, limit, window_secs)
        except redis.exceptions.NoScriptError:
            logger.warning("Redis script cache miss (NOSCRIPT); reloading.")
            self._script_sha = await redis_client.script_load(_ADMISSION_LUA)
            return await redis_client.evalsha(
                self._script_sha, 1, window_key, limit, window_secs
            )


# Module-level singleton.
admission_control = AdmissionController()
