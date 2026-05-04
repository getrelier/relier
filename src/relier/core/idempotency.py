"""
Relier Core — Idempotency Management.

Provides atomic distributed locking and result caching to ensure tasks are
executed **exactly once**, even when retried or resurrected.

Implementation
--------------
* ``ACQUIRE_LUA``: Atomically checks for an existing result.  If found,
  returns it. If not, writes an ``IN_FLIGHT:<uuid>`` sentinel and returns
  the claimed lock ID.  This prevents a race where two concurrent executions
  both see ``GET`` → ``None`` and both proceed.
* ``RELEASE_LUA``: Deletes the key only if the value matches our lock ID
  (compare-and-delete), preventing a late task from evicting another worker's
  valid result.

Usage
-----
Decorator-level (automatic)::

    @rl_task(idempotent=True, idempotency_ttl=3600)
    async def send_invoice(invoice_id: str): ...

Manual (custom key logic)::

    async with idempotency_lock(key=event_id, ttl=86400) as result:
        if result.already_executed:
            return result.cached_result
        output = await do_work()
        await result.record_result(output)
        return output
"""

import json
import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from relier.config import Settings, get_settings
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)

# =============================================================================
# Lua scripts
# =============================================================================

# Atomically check or claim the idempotency key.
_ACQUIRE_LUA = """
local existing = redis.call('GET', KEYS[1])
if existing then
    return {1, existing}
end
redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
return {0, false}
"""

# Delete the key only if we own it (compare-and-delete).
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


# ===========================================================================
# Result object
# ===========================================================================


@dataclass
class IdempotencyResult:
    """Encapsulates the outcome of an idempotency check.

    Attributes:
        already_executed: ``True`` if a result already exists for this key.
        cached_result:    The previously stored return value, or ``None``.
    """

    already_executed: bool
    cached_result: Any = None

    # Internal — not part of the public API.
    _key: str = field(default="", repr=False)
    _lock_id: str = field(default="", repr=False)
    _ttl: int = field(default=3600, repr=False)
    _recorded: bool = field(default=False, repr=False)

    async def record_result(self, result: Any) -> None:
        """Persist the task's return value for future duplicate requests.

        Overwrites the ``IN_FLIGHT`` sentinel with the actual JSON result so
        subsequent calls to ``check_or_claim`` return the cached value
        immediately without re-executing the task.
        """
        if not self._key:
            return
        redis = await get_relier_redis()
        await redis.set(self._key, json.dumps(result), ex=self._ttl)
        self._recorded = True
        logger.debug("Idempotency result cached.", extra={"key": self._key})


# ===========================================================================
# Manager
# ===========================================================================


class IdempotencyManager:
    """Low-level idempotency primitives used by the ``@rl_task`` decorator."""

    def __init__(self) -> None:
        self._prefix = "rl:idem:"

    @property
    def settings(self) -> Settings:
        """Lazy-load settings so we pick up testcontainer environment variables."""
        return get_settings()

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def check_or_claim(self, key: str, ttl: int) -> IdempotencyResult:
        """Atomically check for a cached result or claim the execution slot.

        Returns an ``IdempotencyResult`` where:
        * ``already_executed=True`` → caller should return ``cached_result``.
        * ``already_executed=False`` → caller should execute and then call
          ``result.record_result(output)``.
        """
        redis = await get_relier_redis()
        full_key = self._full_key(key)
        lock_id = f"IN_FLIGHT:{uuid.uuid4().hex}"

        raw = await redis.eval(_ACQUIRE_LUA, 1, full_key, lock_id, str(ttl))  # type: ignore[misc]

        is_existing = bool(raw[0])
        raw_val = raw[1]

        if is_existing:
            # Distinguish between a live IN_FLIGHT sentinel and a real result.
            if isinstance(raw_val, str) and raw_val.startswith("IN_FLIGHT:"):
                logger.warning(
                    "Concurrent execution detected for idempotency key.",
                    extra={"key": key},
                )
                from relier.core.exceptions import IdempotencyInFlightError

                raise IdempotencyInFlightError(key=full_key)

            logger.debug("Idempotency cache hit.", extra={"key": key})
            try:
                cached = json.loads(raw_val)
            except (json.JSONDecodeError, TypeError):
                cached = raw_val
            return IdempotencyResult(already_executed=True, cached_result=cached)

        logger.debug("Idempotency lock claimed.", extra={"key": key})
        return IdempotencyResult(
            already_executed=False,
            _key=full_key,
            _lock_id=lock_id,
            _ttl=ttl,
        )

    async def clear_lock(self, key: str, lock_id: str) -> None:
        """Release an IN_FLIGHT lock if — and only if — we own it."""
        redis = await get_relier_redis()

        # check if key already starts with the prefix
        full_key = key if key.startswith(self._prefix) else self._full_key(key)
        await redis.eval(_RELEASE_LUA, 1, full_key, lock_id)  # type: ignore[misc]


# Module-level singleton.
idempotency_manager = IdempotencyManager()


# ===========================================================================
# Developer-facing context manager
# ===========================================================================


@asynccontextmanager
async def idempotency_lock(
    key: str,
    ttl: int | None = None,
) -> AsyncGenerator[IdempotencyResult, None]:
    """Async context manager for manual idempotency control.

    On exception inside the ``async with`` block, the IN_FLIGHT sentinel is
    released automatically so the task can be safely retried.

    Example::

        async with idempotency_lock(key=event_id, ttl=86400) as result:
            if result.already_executed:
                return result.cached_result
            output = await handle_event(payload)
            await result.record_result(output)
            return output
    """
    settings = get_settings()
    actual_ttl = ttl if ttl is not None else settings.idempotency_default_ttl

    result = await idempotency_manager.check_or_claim(key, actual_ttl)

    try:
        yield result
    except Exception:
        # Release the lock on failure so the task can be retried cleanly.
        if not result.already_executed:
            await idempotency_manager.clear_lock(result._key, result._lock_id)
        raise
    finally:
        # Safety net: if the caller succeeded but forgot to call record_result(),
        # clear the IN_FLIGHT sentinel so duplicates aren't blocked for the full TTL.
        if not result.already_executed and not result._recorded:
            logger.warning(
                "Idempotency lock released without recording a result. "
                "Call `result.record_result(value)` before exiting the context.",
                extra={"key": result._key},
            )
            await idempotency_manager.clear_lock(result._key, result._lock_id)
