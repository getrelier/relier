"""
Relier — Reliability layer for Celery.  Zero job loss.

Core Guarantees
---------------
- **Zero job loss** — every dispatched task is tracked in Redis and
  automatically resurrected if its worker crashes mid-execution.
- **Atomic execution** — SHA-256 idempotency keys ensure tasks run exactly
  once, even under concurrent retries.
- **Schema safety** — versioned envelopes with checksums protect against
  payload corruption during rolling deployments.
- **Observability** — full OpenTelemetry span hierarchy and Prometheus
  metrics with no extra wiring required.

Quick Start
-----------
    >>> from relier import rl_task
    >>> from relier.tasks.context import TaskContext
    >>>
    >>> @rl_task(
    ...     queue="high_priority",
    ...     idempotent=True,
    ...     soft_timeout=25,
    ...     hard_timeout=30,
    ... )
    ... async def process_order(order_id: str, ctx: TaskContext) -> dict:
    ...     if ctx.partial_result:
    ...         resume_from = ctx.partial_result["step"]
    ...     result = await charge_and_fulfil(order_id)
    ...     return result
    >>>
    >>> # FastAPI / async Django — dispatch without blocking:
    >>> receipt = await process_order.apush(order_id="ord-001")
    >>>
    >>> # Django views / Flask routes / scripts — sync dispatch:
    >>> receipt = process_order.push(order_id="ord-001")

Configuration
-------------
All settings are read from environment variables prefixed ``RELIER_``.
See :class:`~relier.config.Settings` for the full reference, or call
:func:`~relier.config.get_settings` to inspect the active configuration.

Error Handling
--------------
All exceptions raised by Relier inherit from :class:`~relier.core.exceptions.RelierError`.
Catch that base class to handle any framework error uniformly:

    >>> from relier import RelierError, AdmissionRejectedError
    >>> try:
    ...     receipt = await process_order.apush(order_id="ord-001")
    ... except AdmissionRejectedError as exc:
    ...     # Back off and retry after exc.retry_after seconds.
    ...     await asyncio.sleep(exc.retry_after)
    ... except RelierError:
    ...     # Unexpected framework error — log and alert.
    ...     raise
"""

__version__ = "0.1.1"

# ---------------------------------------------------------------------------
# Public API re-exports
#
# Everything a user needs for day-to-day Relier usage is importable from
# the top-level ``relier`` package.  Internal subsystem modules remain
# accessible via their full dotted paths for advanced use cases.
# ---------------------------------------------------------------------------

from relier.config import Settings, get_settings
from relier.core.exceptions import (
    AdmissionRejectedError,
    ConfigurationError,
    IdempotencyInFlightError,
    MaxResurrectionsExceededError,
    PayloadIntegrityError,
    RedisConnectionError,
    RelierError,
    SchemaMigrationError,
    WorkerInitializationError,
)
from relier.tasks.context import TaskContext, task_context
from relier.tasks.decorator import PUBLIC_QUEUES, RelierTask, rl_task

__all__ = [
    # Version
    "__version__",
    # Core decorator & typed handle
    "rl_task",
    "RelierTask",
    # Task context
    "TaskContext",
    "task_context",
    # Configuration
    "Settings",
    "get_settings",
    # Queue topology
    "PUBLIC_QUEUES",
    # Exceptions — base
    "RelierError",
    # Exceptions — configuration & infrastructure
    "ConfigurationError",
    "RedisConnectionError",
    "WorkerInitializationError",
    # Exceptions — task lifecycle
    "AdmissionRejectedError",
    "IdempotencyInFlightError",
    "MaxResurrectionsExceededError",
    "PayloadIntegrityError",
    "SchemaMigrationError",
]
