"""
Relier Telemetry — Prometheus-compatible OTEL Metric Definitions.

All counters, histograms, and gauge callbacks defined here match the metric
surface specified in the RELIER_MASTERPLAN.md §6.  Import individual
instruments wherever they need to be incremented; the meter is shared.

Instruments are initialized at module import time, safe because the
``opentelemetry-api`` ships no-op implementations that are replaced by the
real SDK providers after ``setup_telemetry()`` is called.
"""

import socket
import threading
import typing

from opentelemetry import metrics

meter = metrics.get_meter("relier", version="0.1.7")

# ===========================================================================
# Counters
# ===========================================================================

tasks_total = meter.create_counter(
    name="rl_tasks_total",
    description=(
        "Total tasks processed, labelled by status "
        "(completed | failed | resurrected | dlq | idempotency_hit)."
    ),
    unit="1",
)

admission_total = meter.create_counter(
    name="rl_admission_total",
    description="Total admission control decisions (admitted | rejected).",
    unit="1",
)

timeouts_total = meter.create_counter(
    name="rl_timeouts_total",
    description="Total timeout events triggered (soft | hard).",
    unit="1",
)

shutdowns_total = meter.create_counter(
    name="rl_shutdowns_total",
    description="Worker shutdown events (clean | handoff | forced).",
    unit="1",
)

resurrections_total = meter.create_counter(
    name="rl_resurrections_total",
    description="Total Phoenix resurrection events.",
    unit="1",
)

dlq_quarantined_total = meter.create_counter(
    name="rl_dlq_quarantined_total",
    description="Total tasks quarantined to the Dead Letter Queue.",
    unit="1",
)

idempotency_hits_total = meter.create_counter(
    name="rl_idempotency_hits_total",
    description="Total idempotency cache hits (duplicate executions prevented).",
    unit="1",
)

circuit_trips_total = meter.create_counter(
    name="rl_circuit_trips_total",
    description="Total circuit breaker trip events.",
    unit="1",
)

# ===========================================================================
# Histograms
# ===========================================================================

task_duration_ms = meter.create_histogram(
    name="rl_task_duration_ms",
    description="End-to-end task execution duration in milliseconds.",
    unit="ms",
)

task_overhead_ms = meter.create_histogram(
    name="rl_overhead_ms",
    description=(
        "Relier framework overhead per task phase "
        "(enqueue | pickup | schema | idempotency)."
    ),
    unit="ms",
)

resurrection_time_s = meter.create_histogram(
    name="rl_resurrection_time_s",
    description="Time elapsed between heartbeat expiry and task re-queue.",
    unit="s",
)

shutdown_duration_s = meter.create_histogram(
    name="rl_shutdown_duration_s",
    description="Time taken for a graceful worker shutdown (SIGTERM → clean exit).",
    unit="s",
)

# ===========================================================================
# Observable Gauges (populated via callbacks)
# ===========================================================================


# Worker-local in-flight task counter. Incremented when the decorator
# registers a task and decremented when it completes. Lives per worker
# process — the OTEL callback emits one observation per process, labelled
# with this worker's hostname; aggregating across the cluster is the
# responsibility of the Prometheus query layer.
_inflight_lock = threading.Lock()
_inflight_count = 0
_worker_hostname = socket.gethostname()


def _inflight_inc() -> None:
    """Increment the worker-local in-flight task counter."""
    global _inflight_count
    with _inflight_lock:
        _inflight_count += 1


def _inflight_dec() -> None:
    """Decrement the worker-local in-flight task counter (clamped at 0)."""
    global _inflight_count
    with _inflight_lock:
        if _inflight_count > 0:
            _inflight_count -= 1


def _inflight_tasks_callback(
    options: metrics.CallbackOptions,
) -> typing.Iterator[metrics.Observation]:
    """Emit the in-flight task count for the current worker process."""
    with _inflight_lock:
        count = _inflight_count
    yield metrics.Observation(count, {"rl.worker.id": _worker_hostname})


inflight_tasks = meter.create_observable_gauge(
    name="rl_inflight_tasks",
    callbacks=[_inflight_tasks_callback],
    description="Number of tasks currently executing on this worker.",
    unit="1",
)
