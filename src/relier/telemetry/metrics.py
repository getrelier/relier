"""
Relier Telemetry — Prometheus-compatible OTEL Metric Definitions.

All counters, histograms, and gauge callbacks defined here match the metric
surface specified in the RELIER_MASTERPLAN.md §6.  Import individual
instruments wherever they need to be incremented; the meter is shared.

Instruments are initialized at module import time — safe because the
``opentelemetry-api`` ships no-op implementations that are replaced by the
real SDK providers after ``setup_telemetry()`` is called.
"""

import typing

from opentelemetry import metrics

meter = metrics.get_meter("relier", version="0.1.0")

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


def _inflight_tasks_callback(
    options: metrics.CallbackOptions,
) -> typing.Iterator[metrics.Observation]:
    """Yield inflight task count per worker from Redis.

    This callback is intentionally a sync stub.  In production, wire it to
    the Redis sorted-set cardinalities recorded by the decorator.
    """
    # The actual values are read from Redis by the CLI / API layer.
    # This callback placeholder satisfies the OTEL SDK contract.
    yield metrics.Observation(0, {"rl.worker.id": "unknown"})


inflight_tasks = meter.create_observable_gauge(
    name="rl_inflight_tasks",
    callbacks=[_inflight_tasks_callback],
    description="Number of tasks currently executing on each worker.",
    unit="1",
)
