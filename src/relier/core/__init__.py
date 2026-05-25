"""
Relier Core — Reliability primitives.

Contains the logic for task resurrection (Phoenix), dead letter queues,
idempotency, schema versioning, SLO tracking, timeouts, shutdown, admission control.
"""

from relier.core.exceptions import (
    AdmissionRejectedError,
    ConfigurationError,
    IdempotencyInFlightError,
    MaxResurrectionsExceededError,
    PayloadIntegrityError,
    RedisConnectionError,
    RelierError,
    SchemaMigrationError,
    ShadowRegistryError,
    WorkerInitializationError,
)

__all__ = [
    "AdmissionRejectedError",
    "ConfigurationError",
    "IdempotencyInFlightError",
    "MaxResurrectionsExceededError",
    "PayloadIntegrityError",
    "RedisConnectionError",
    "RelierError",
    "SchemaMigrationError",
    "ShadowRegistryError",
    "WorkerInitializationError",
]
