"""
Relier exception hierarchy.

All framework-level exceptions inherit from :class:`RelierError`, giving
callers a single broad catch and a stable failure contract regardless of
which internal subsystem raised the error.

    >>> try:
    ...     raise AdmissionRejectedError("Cluster at capacity.", retry_after=5)
    ... except RelierError as exc:
    ...     print(type(exc).__name__, exc.retry_after)
    AdmissionRejectedError 5

Centralising exceptions here avoids circular imports and ensures the
failure surface visible to library consumers never leaks raw dependency
errors (e.g. ``redis.exceptions.ConnectionError`` or ``KeyError``).
"""


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class RelierError(Exception):
    """Base class for all Relier framework exceptions.

    Catch this class to handle any Relier failure uniformly, then inspect
    the concrete subtype for finer-grained recovery logic.

    Example:
        >>> try:
        ...     raise RelierError("unexpected failure")
        ... except RelierError as exc:
        ...     print(f"Caught: {exc}")
        Caught: unexpected failure
    """


# ---------------------------------------------------------------------------
# Configuration & initialisation
# ---------------------------------------------------------------------------


class ConfigurationError(RelierError):
    """Raised when Relier settings are invalid or internally inconsistent.

    This exception surfaces during initialisation so misconfiguration is
    caught immediately at process start-up rather than silently at runtime.

    Common triggers:

    - Redis URL scheme is not ``redis://``, ``rediss://``, or
      ``redis+unix://``.
    - ``soft_timeout`` ≥ ``hard_timeout``.
    - ``idempotency_inflight_ttl`` ≤ ``hard_timeout`` (risks
      double-execution).
    - Sentinel mode enabled but ``RELIER_REDIS_SENTINEL_NODES`` is empty.

    Example:
        >>> raise ConfigurationError(
        ...     "RELIER_SOFT_TIMEOUT (30s) must be less than "
        ...     "RELIER_HARD_TIMEOUT (30s). "
        ...     "Fix: set RELIER_SOFT_TIMEOUT to a value below 30."
        ... )
        Traceback (most recent call last):
            ...
        relier.core.exceptions.ConfigurationError: RELIER_SOFT_TIMEOUT (30s) ...
    """


class RedisConnectionError(RelierError):
    """Raised when Relier cannot establish or maintain a Redis connection.

    Wraps low-level ``redis.exceptions.ConnectionError`` and related I/O
    failures so callers never need to import from ``redis.exceptions``
    directly.  The message always names the configured endpoint and provides
    a one-sentence fix suggestion.

    Example:
        >>> raise RedisConnectionError(
        ...     "Cannot reach Redis at redis://localhost:6379/0. "
        ...     "Verify Redis is running and RELIER_REDIS_URL is correct."
        ... )
        Traceback (most recent call last):
            ...
        relier.core.exceptions.RedisConnectionError: Cannot reach Redis ...
    """


class WorkerInitializationError(RelierError):
    """Raised when a Celery worker process fails its start-up validation.

    Triggered during ``worker_process_init`` when Redis is unreachable,
    has an incompatible eviction policy, or connection-pool sizing would
    exceed safe limits.  The worker refuses to accept tasks until the
    underlying issue is resolved.

    Example:
        >>> raise WorkerInitializationError(
        ...     "Worker celery@host-1 refused to start: Redis "
        ...     "maxmemory-policy must be 'noeviction'. "
        ...     "Fix: redis-cli CONFIG SET maxmemory-policy noeviction"
        ... )
        Traceback (most recent call last):
            ...
        relier.core.exceptions.WorkerInitializationError: Worker celery@host-1 ...
    """


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class IdempotencyInFlightError(RelierError):
    """Raised when another worker is executing the same idempotent task.

    This is a transient concurrency conflict, not a hard failure.  The task
    decorator catches it and retries with back-off once the in-flight lock
    clears.

    Args:
        key: The idempotency key that is currently locked by another worker.

    Example:
        >>> exc = IdempotencyInFlightError("send_invoice:a3f9c12d")
        >>> print(exc.key)
        send_invoice:a3f9c12d
    """

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(
            f"Idempotency key '{key}' is already in-flight on another worker. "
            "The decorator will automatically retry once the lock expires."
        )


# ---------------------------------------------------------------------------
# Phoenix & DLQ
# ---------------------------------------------------------------------------


class ShadowRegistryError(RelierError):
    """Raised when Phoenix task-tracking state becomes inconsistent.

    Typically indicates a race condition in resurrection logic or a Redis
    key collision.  Affected tasks are quarantined to the DLQ.
    """


class MaxResurrectionsExceededError(RelierError):
    """Raised when a task exceeds the configured resurrection safety limit.

    Repeated failures usually indicate a poison-pill payload, a
    non-deterministic crash loop, or unrecoverable execution state.
    Tasks that trigger this exception are quarantined to the DLQ.

    Args:
        task_id: The Celery task identifier of the offending task.
        count:   The resurrection attempt count that triggered the limit.

    Example:
        >>> exc = MaxResurrectionsExceededError("abc-123", count=5)
        >>> print(exc.task_id, exc.count)
        abc-123 5
    """

    def __init__(self, task_id: str, count: int) -> None:
        self.task_id = task_id
        self.count = count
        super().__init__(
            f"Task '{task_id}' has been resurrected {count} time(s) and still "
            "failed.  It has been quarantined to the DLQ to prevent an infinite "
            "crash loop.  Inspect the task payload and worker logs for root cause."
        )


# ---------------------------------------------------------------------------
# Schema & payload
# ---------------------------------------------------------------------------


class PayloadIntegrityError(RelierError):
    """Raised when a task payload fails its integrity checksum verification.

    Typically indicates payload corruption, a serialisation inconsistency,
    or broker-side tampering during transport.  The task is quarantined to
    the DLQ rather than re-executed to prevent processing corrupt data.
    """


class SchemaMigrationError(RelierError):
    """Raised when a persisted task payload cannot be migrated.

    Occurs when a worker on a newer schema version receives an envelope from
    a much older producer with no registered migration path.  The task is
    quarantined to the DLQ.
    """


# ---------------------------------------------------------------------------
# Admission control
# ---------------------------------------------------------------------------


class AdmissionRejectedError(RelierError):
    """Raised when admission control rejects new work due to capacity pressure.

    Translated to an HTTP 429 response at the API boundary.  The
    ``retry_after`` attribute carries the suggested back-off duration in
    seconds for client-side retry logic.

    Args:
        message:     Human-readable rejection reason.
        retry_after: Seconds the caller should wait before retrying.

    Example:
        >>> exc = AdmissionRejectedError("Cluster at capacity.", retry_after=5)
        >>> print(exc.retry_after)
        5
    """

    def __init__(self, message: str, retry_after: int) -> None:
        super().__init__(message)
        self.retry_after = retry_after
