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

Manual control (async tasks only)::

    async with idempotency_lock(key=event_id, ttl=86400) as lock:
        if lock.already_executed:
            return lock.cached_result

        output = await do_work()
        lock.set_result(output)   # stage result; committed on context exit
        return output

    ``idempotency_lock`` requires ``async with``. Sync tasks should use
    ``@rl_task(idempotent=True)`` instead, which handles both sync and
    async task functions via the internal async orchestration layer.

    ``lock.set_result(value)`` is optional but strongly recommended. If
    omitted, the context manager commits a ``None`` sentinel on clean exit
    so future duplicates are still blocked — but ``lock.cached_result``
    will be ``None`` for those callers. On exception exit the lock is
    always released so the task can retry.
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from relier.config import Settings, get_settings
from relier.core.keys import RedisKeys
from relier.storage.lua.scripts import ACQUIRE_LUA, RELEASE_LUA
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


# In-flight sentinels are stored as the raw lock id, which always begins with
# this prefix. A finalized result is JSON-encoded and can never begin with it,
# so a prefix check unambiguously distinguishes an active execution from a
# cached result, unlike a substring match, which a result value could spoof.
_IN_FLIGHT_SENTINEL_PREFIX = "rl:inflight:"


def _make_lock_id(task_id: str | None) -> str:
    """Build an in-flight sentinel.

    When a ``task_id`` is supplied (the decorator path) it is embedded so a
    later run of the *same* task_id — a Celery retry or a Phoenix resurrection —
    can recognise the lock as its own and take it over instead of blocking
    against itself. A random suffix preserves the per-attempt uniqueness that
    ``RELEASE_LUA``'s compare-and-delete relies on. The manual
    ``idempotency_lock`` path has no task_id and keeps the plain format.
    """
    if task_id:
        return RedisKeys.in_flight(f"{task_id}:{uuid.uuid4().hex}")
    return RedisKeys.in_flight()


def _sentinel_task_id(value: str) -> str | None:
    """Extract the embedded task_id from an in-flight sentinel, if any.

    Sentinels written by the decorator path look like
    ``rl:inflight:<task_id>:<random>``; task_ids (UUIDs) and the random suffix
    contain no colons, so the task_id is the segment before the final colon.
    Plain (task_id-less) sentinels return ``None``.
    """
    rest = value[len(_IN_FLIGHT_SENTINEL_PREFIX) :]
    task_id, sep, _ = rest.rpartition(":")
    return task_id if sep else None


# Sentinel used to distinguish "set_result was never called" from
# "set_result was called with None".
_UNSET: object = object()


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
    _pending_result: Any = field(default=_UNSET, repr=False)

    def set_result(self, value: Any) -> None:
        """Stage the result to be committed when the idempotency context exits.

        This is the preferred way to record a result when using
        ``idempotency_lock`` as a context manager. The actual Redis write
        happens in ``__aexit__`` so it is guaranteed to run even if the task
        body returns early.

        If this method is never called before a clean exit, the context
        manager commits a ``None`` sentinel and logs a warning. Future
        duplicates will be blocked (``already_executed=True``) but
        ``cached_result`` will be ``None``.
        """
        self._pending_result = value

    async def _record_result(self, result: Any) -> None:
        """Persist the execution result to Redis. Internal — used by the decorator only.

        Replaces the ``IN_FLIGHT`` sentinel with the serialized result so future
        duplicate executions short-circuit. When using ``idempotency_lock`` as a
        context manager, call ``lock.set_result(value)`` instead — ``__aexit__``
        commits it automatically.
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

    async def check_or_claim(
        self, key: str, ttl: int, task_id: str | None = None
    ) -> IdempotencyResult:
        """
        Atomically resolve cached execution state or claim execution ownership.

        If a completed result already exists, it is returned immediately without
        re-executing task logic.

        If another worker is actively executing the task, an
        ``IdempotencyInFlightError`` is raised so the caller can retry later.

        Otherwise, the caller receives execution ownership and is responsible
        for recording the final result.

        When ``task_id`` is supplied and an in-flight lock is found that belongs
        to the *same* task_id, ownership is taken over rather than blocked: the
        prior holder is a dead or superseded incarnation of this exact task (a
        Phoenix resurrection or a Celery retry), not a competing duplicate.
        Cross-task deduplication — different task_ids, same arguments — is
        unaffected. Commit safety against an overlapping zombie incarnation is
        still enforced separately by the Phoenix lease/fence layer.
        """
        redis = await get_relier_redis()
        full_key = RedisKeys.idempotency(key)
        lock_id = _make_lock_id(task_id)

        # The in-flight sentinel is claimed with a short, bounded TTL so a
        # worker that dies mid-execution without releasing the lock cannot
        # block duplicates for the full result TTL. The completed result is
        # written separately (see ``_record_result``) with the result TTL.
        inflight_ttl = self.settings.idempotency_inflight_ttl
        raw = await redis.eval(ACQUIRE_LUA, 1, full_key, lock_id, str(inflight_ttl))  # type: ignore[misc]

        is_existing = bool(raw[0])
        raw_val = raw[1]

        if is_existing:
            # Distinguish active execution ownership from a finalized cached result.
            if isinstance(raw_val, str) and raw_val.startswith(
                _IN_FLIGHT_SENTINEL_PREFIX
            ):
                # Same logical task re-entering (resurrection / retry): the lock
                # is held by a prior incarnation of this very task_id, so take
                # it over instead of blocking the task against itself.
                if task_id is not None and _sentinel_task_id(raw_val) == task_id:
                    await redis.set(full_key, lock_id, ex=inflight_ttl)
                    logger.info(
                        "Idempotency in-flight lock reclaimed by the same task.",
                        extra={"key": key, "task_id": task_id},
                    )
                    return IdempotencyResult(
                        already_executed=False,
                        _key=full_key,
                        _lock_id=lock_id,
                        _ttl=ttl,
                    )

                logger.warning(
                    "Idempotent task already executing on another worker.",
                    extra={"key": key},
                )
                from relier.core.exceptions import IdempotencyInFlightError

                raise IdempotencyInFlightError(key=full_key)

            logger.info(
                "Idempotency cache hit — returning cached result.", extra={"key": key}
            )
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


class IdempotencyLock:
    """
    Async context manager backing ``idempotency_lock()``.

    On **clean exit** the staged result (set via ``lock.set_result()``) is
    committed to Redis.  If ``set_result()`` was never called, a ``None``
    sentinel is committed and a warning is logged — future duplicates are
    still blocked but ``cached_result`` will be ``None``.

    On **exception exit** the in-flight lock is released so the task can
    be retried without being blocked by an abandoned sentinel.
    """

    def __init__(self, key: str, ttl: int | None = None) -> None:
        settings = get_settings()
        self._key = key
        self._ttl = ttl if ttl is not None else settings.idempotency_default_ttl
        self._result: IdempotencyResult | None = None

    async def __aenter__(self) -> IdempotencyResult:
        self._result = await idempotency_manager.check_or_claim(self._key, self._ttl)
        return self._result

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        result = self._result
        if result is None or result.already_executed:
            return False

        if exc_type is not None:
            # Exception path: release the in-flight lock so retries are not
            # blocked by an abandoned sentinel.
            # Use self._key (raw, un-namespaced) — clear_lock applies the
            # namespace prefix itself, same as check_or_claim does.
            if not result._recorded:
                await idempotency_manager.clear_lock(self._key, result._lock_id)
            return False

        # Clean exit path.
        if result._recorded:
            # _record_result() was called directly (decorator path or old API).
            return False

        # Commit the staged value, or a None sentinel if set_result was never called.
        if result._pending_result is _UNSET:
            logger.warning(
                "Execution ownership released without a staged result. "
                "Call lock.set_result(value) before leaving the idempotency "
                "context to cache the result for future duplicates. "
                "Committing None as sentinel — future duplicates will be "
                "blocked but cached_result will be None.",
                extra={"key": result._key},
            )
            commit_value = None
        else:
            commit_value = result._pending_result

        redis = await get_relier_redis()
        await redis.set(result._key, json.dumps(commit_value), ex=self._ttl)
        result._recorded = True
        return False


def idempotency_lock(key: str, ttl: int | None = None) -> IdempotencyLock:
    """
    Developer-facing async context manager for manual idempotency control.

    Provides structured execution ownership handling outside the automatic
    ``@rl_task`` decorator flow.

    The result is committed automatically on clean exit — call
    ``lock.set_result(value)`` to stage the value before returning.
    On exception exit the in-flight lock is released so retries are not
    blocked indefinitely.

    Example::

        async with idempotency_lock(key=f"webhook:{event_id}", ttl=86400) as lock:
            if lock.already_executed:
                return lock.cached_result

            result = await process_event(payload)
            lock.set_result(result)
            return result
    """
    return IdempotencyLock(key=key, ttl=ttl)
