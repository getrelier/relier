"""
Relier Telemetry — OpenTelemetry Span Helpers.

Defines canonical span/attribute names (following the ``rl.*`` semantic
convention) and context-manager helpers for consistent span creation
across all Relier subsystems.
"""

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Span, StatusCode

# ===========================================================================
# Canonical attribute names
# ===========================================================================

ATTR_TASK_ID = "rl.task.id"
ATTR_TASK_NAME = "rl.task.name"
ATTR_TASK_QUEUE = "rl.task.queue"
ATTR_TASK_RETRY_COUNT = "rl.task.retry_count"
ATTR_TASK_IS_RESURRECTION = "rl.task.is_resurrection"
ATTR_TASK_IS_IDEMPOTENT = "rl.task.is_idempotent"
ATTR_TASK_IDEMPOTENCY_HIT = "rl.task.idempotency_hit"
ATTR_TASK_SCHEMA_VERSION = "rl.task.schema_version"
ATTR_WORKER_ID = "rl.worker.id"
ATTR_WORKER_PID = "rl.worker.pid"
ATTR_CIRCUIT_STATE = "rl.circuit.state"
ATTR_CIRCUIT_NAME = "rl.circuit.name"
ATTR_ADMISSION_RESULT = "rl.admission.result"
ATTR_INFERENCE_BACKEND = "rl.inference.backend"
ATTR_SLO_BURN_RATE = "rl.slo.burn_rate"
ATTR_TIMEOUT_SOFT_LIMIT = "rl.timeout.soft_limit"
ATTR_TIMEOUT_HARD_LIMIT = "rl.timeout.hard_limit"
ATTR_RESURRECTION_COUNT = "rl.resurrection.count"

# ===========================================================================
# Tracer instance
# ===========================================================================

tracer = trace.get_tracer("relier")


# ===========================================================================
# Span helpers
# ===========================================================================


@asynccontextmanager
async def trace_task(
    name: str,
    task_id: str,
    attributes: dict[str, Any] | None = None,
) -> AsyncGenerator[Span, None]:
    """Async context manager that wraps a block in an OTEL span.

    Example::

        async with trace_task("rl.task.execute", task_id) as span:
            span.set_attribute(ATTR_TASK_QUEUE, "high_priority")
            result = await do_work()
    """
    with tracer.start_as_current_span(name) as span:
        span.set_attribute(ATTR_TASK_ID, task_id)
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        try:
            yield span
        except Exception as exc:
            record_exception(span, exc)
            raise


@contextmanager
def trace_sync(
    name: str,
    attributes: dict[str, Any] | None = None,
) -> Generator[Span, None, None]:
    """Synchronous context manager for non-async code paths.

    Example::

        with trace_sync("rl.schema.migrate", {"schema_version": 1}) as span:
            args, kwargs = migrate(args, kwargs)
    """
    with tracer.start_as_current_span(name) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        try:
            yield span
        except Exception as exc:
            record_exception(span, exc)
            raise


def record_exception(span: Span, exc: Exception) -> None:
    """Record an exception on a span and set error status.

    Safe to call with a no-op span — checks ``is_recording()`` first.
    """
    if span and span.is_recording():
        span.record_exception(exc)
        span.set_status(trace.Status(StatusCode.ERROR, str(exc)))
