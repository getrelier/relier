"""
Relier Core — Idempotency Management.

Provides distributed execution deduplication and result replay semantics
for Celery tasks.

The idempotency subsystem guarantees that logically identical work is
executed at most once within the configured TTL window, even across
retries, worker crashes, and Phoenix-driven resurrection flows.

Implementation
--------------
``ACQUIRE_LUA``
    Atomically checks for an existing cached result or claims execution
    ownership by writing an ``IN_FLIGHT:<uuid>`` sentinel.

    This prevents race conditions where multiple workers concurrently
    observe a missing key and all begin execution.

``RELEASE_LUA``
    Releases an in-flight lock only if the stored value matches the
    caller's lock ID (compare-and-delete semantics).

    This prevents stale workers from deleting ownership belonging to
    another execution attempt.

Usage
-----
Decorator-level (automatic)::

    @rl_task(idempotent=True, idempotency_ttl=3600)
    async def send_invoice(invoice_id: str): ...

Manual control::

    async with idempotency_lock(key=event_id, ttl=86400) as result:
        if result.already_executed:
            return result.cached_result

        output = await do_work()
        await result.record_result(output)
        return output
"""

import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from relier.config import Settings, get_settings
from relier.core.keys import RedisKeys
from relier.storage.lua.scripts import ACQUIRE_LUA, RELEASE_LUA
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


# ===========================================================================
# Result object
# ===========================================================================


@dataclass
class IdempotencyResult:
    """
    Represents the outcome of an idempotency ownership check.

    Instances either:
    - expose a previously cached execution result, or
    - grant the caller execution ownership for the key.
    """

    already_executed: bool
    cached_result: Any = None

    # Internal execution metadata used for lock ownership and cleanup.
    _key: str = field(default="", repr=False)
    _lock_id: str = field(default="", repr=False)
    _ttl: int = field(default=3600, repr=False)
    _recorded: bool = field(default=False, repr=False)

    async def record_result(self, result: Any) -> None:
        """
        Persist the completed execution result for future duplicate requests.

        Replaces the temporary ``IN_FLIGHT`` sentinel with the final serialized
        result so subsequent executions can short-circuit immediately without
        re-running task logic.
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
    """
    Coordinates distributed idempotency ownership and result replay.

    This manager provides the low-level Redis primitives used by both the
    ``@rl_task`` decorator and manual ``idempotency_lock`` workflows.
    """

    @property
    def settings(self) -> Settings:
        """
        Resolve settings lazily at runtime.

        Avoids capturing stale configuration during module import, which is
        important for dynamically provisioned test environments.
        """
        return get_settings()

    async def check_or_claim(self, key: str, ttl: int) -> IdempotencyResult:
        """
        Atomically resolve cached execution state or claim execution ownership.

        If a completed result already exists, it is returned immediately without
        re-executing task logic.

        If another worker is actively executing the task, an
        ``IdempotencyInFlightError`` is raised so the caller can retry later.

        Otherwise, the caller receives execution ownership and is responsible
        for recording the final result.
        """
        redis = await get_relier_redis()
        full_key = RedisKeys.idempotency(key)
        lock_id = RedisKeys.in_flight()

        raw = await redis.eval(ACQUIRE_LUA, 1, full_key, lock_id, str(ttl))  # type: ignore[misc]

        is_existing = bool(raw[0])
        raw_val = raw[1]

        if is_existing:
            # Distinguish active execution ownership from a finalized cached result.
            if isinstance(raw_val, str) and "inflight" in raw_val.lower():
                logger.warning(
                    "Idempotent task already executing on another worker.",
                    extra={"key": key},
                )
                from relier.core.exceptions import IdempotencyInFlightError

                raise IdempotencyInFlightError(key=full_key)

            logger.debug("Returning cached idempotent task result.", extra={"key": key})
            try:
                cached = json.loads(raw_val)
            except (json.JSONDecodeError, TypeError):
                cached = raw_val
            return IdempotencyResult(already_executed=True, cached_result=cached)

        logger.debug("Idempotency execution ownership acquired.", extra={"key": key})
        return IdempotencyResult(
            already_executed=False,
            _key=full_key,
            _lock_id=lock_id,
            _ttl=ttl,
        )

    async def clear_lock(self, key: str, lock_id: str) -> None:
        """
        Release execution ownership only if the caller still owns the lock.

        Prevents stale or delayed workers from clearing ownership belonging to
        a newer execution attempt.
        """
        redis = await get_relier_redis()
        full_key = RedisKeys.idempotency(key)
        await redis.eval(RELEASE_LUA, 1, full_key, lock_id)  # type: ignore[misc]


# Shared process-wide idempotency manager instance.
idempotency_manager = IdempotencyManager()


# ===========================================================================
# Developer-facing context manager
# ===========================================================================


@asynccontextmanager
async def idempotency_lock(
    key: str,
    ttl: int | None = None,
) -> AsyncGenerator[IdempotencyResult, None]:
    """
    Developer-facing async context manager for manual idempotency control.

    Provides structured execution ownership handling outside the automatic
    ``@rl_task`` decorator flow.

    On failure, the in-flight ownership marker is automatically released so
    future retries are not blocked indefinitely.
    """
    settings = get_settings()
    actual_ttl = ttl if ttl is not None else settings.idempotency_default_ttl

    result = await idempotency_manager.check_or_claim(key, actual_ttl)

    try:
        yield result
    except Exception:
        # Release execution ownership on failure so retries are not blocked by
        # abandoned in-flight state.
        if not result.already_executed:
            await idempotency_manager.clear_lock(key, result._lock_id)
        raise
    finally:
        # Safety net: if execution completed but no result was recorded,
        # clear the in-flight marker so duplicate requests are not blocked
        # until TTL expiration.
        if not result.already_executed and not result._recorded:
            logger.warning(
                "Execution ownership released without recording a final result. "
                "Call `result.record_result(value)` before leaving the idempotency context.",
                extra={"key": key},
            )
            await idempotency_manager.clear_lock(key, result._lock_id)
