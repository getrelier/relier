"""
Relier Core Exceptions.
Defines custom exception classes for specific reliability failures,
allowing users and internal decorators to handle errors gracefully.
"""


class RelierError(Exception):
    """Base exception for all Relier-related errors."""

    pass


# --- Idempotency Exceptions ---


class IdempotencyInFlightError(RelierError):
    """
    Raised when a task with the same idempotency key is currently
    being executed by another worker. Instructs Celery to back off and retry.
    """

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Task with idempotency key '{key}' is currently in-flight")


# --- Phoenix & DLQ Exceptions ---


class ShadowRegistryError(RelierError):
    """Raised when the Phoenix system fails to resurrect or track a task."""

    pass


class MaxResurrectionsExceededError(RelierError):
    """
    Raised when a task continually crashes workers and exceeds the configured
    maximum resurrection count. Triggers the Dead Letter Queue (DLQ).
    """

    def __init__(self, task_id: str, count: int) -> None:
        self.task_id = task_id
        self.count = count
        super().__init__(f"Task {task_id} exceeded max resurrections ({count})")


# --- Schema & Payload Exceptions ---


class PayloadIntegrityError(RelierError):
    """
    Raised when a task payload fails its cryptographic checksum validation,
    indicating data corruption or tampering during broker transit.
    """

    pass


class SchemaMigrationError(RelierError):
    """Raised when an in-flight task payload cannot be migrated to the current version."""

    pass


# --- Admission Control Exceptions ---


class AdmissionRejectedError(RelierError):
    """
    Raised by the Lua Admission Control script when the system is operating
    above its maximum concurrent capacity. Caught by FastAPI to return a 429.
    """

    def __init__(self, message: str, retry_after: int):
        super().__init__(message)
        self.retry_after = retry_after
