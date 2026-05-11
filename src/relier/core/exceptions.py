"""
Relier exception hierarchy.

Defines all framework-level exceptions used across reliability,
resurrection, idempotency, admission-control, and schema-management
subsystems.

Centralizing exceptions avoids circular imports and provides stable
failure contracts for both internal components and user-facing APIs.
"""


class RelierError(Exception):
    """
    Base class for all Relier framework exceptions.

    Applications may catch this class to handle all Relier-originated
    failures uniformly.
    """

    pass


# ===========================================================================
# Idempotency
# ===========================================================================


class IdempotencyInFlightError(RelierError):
    """
    Raised when another worker is actively executing the same idempotent task.

    This exception signals a transient concurrency conflict rather than a
    hard failure. The task decorator catches it and retries execution with
    backoff once the in-flight lock clears.
    """

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Idempotency key '{key}' is already in-flight")


# ==========================================================================
# Phoenix & DLQ Exceptions
# ==========================================================================


class ShadowRegistryError(RelierError):
    """
    Raised when Phoenix task-tracking state becomes inconsistent or
    resurrection operations fail unexpectedly.
    """

    pass


class MaxResurrectionsExceededError(RelierError):
    """
    Raised when a task exceeds the configured resurrection safety limit.

    Repeated resurrection failures usually indicate a poison-pill workload,
    non-deterministic crash loop, or unrecoverable execution state. Tasks
    triggering this exception are quarantined to the DLQ.
    """

    def __init__(self, task_id: str, count: int) -> None:
        self.task_id = task_id
        self.count = count
        super().__init__(f"Task '{task_id}' exceeded resurrection limit ({count})")


# ===========================================================================
# Schema & Payload Exceptions
# ===========================================================================


class PayloadIntegrityError(RelierError):
    """
    Raised when a task payload fails integrity verification.

    Typically indicates payload corruption, serialization inconsistency,
    or broker-side tampering during transport.
    """

    pass


class SchemaMigrationError(RelierError):
    """
    Raised when a persisted task payload cannot be migrated to the
    currently supported schema version.
    """

    pass


# ============================================================================
# Admission Control Exceptions
# ============================================================================


class AdmissionRejectedError(RelierError):
    """
    Raised when admission control rejects new work due to capacity pressure.

    This exception is translated into an HTTP 429 response at the API layer
    and includes a retry-after duration for client-side backoff handling.
    """

    def __init__(self, message: str, retry_after: int):
        super().__init__(message)
        self.retry_after = retry_after
