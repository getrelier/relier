# Metrics Reference

Relier emits OpenTelemetry metrics that your dashboards and alerts can scrape
through Prometheus, Honeycomb, Grafana Cloud, or any OTLP-compatible backend.
The bundled `docker-compose.yml` and `docker-compose.prod.yml` already wire the
full pipeline (workers -> OTel collector -> Prometheus -> Grafana).

Enable export with:

```bash
RELIER_OTEL_ENABLED=true
RELIER_OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
```

All instruments are registered under the OTel meter `relier` (version
`0.1.0`). Names below match what Prometheus actually scrapes after the OTel
collector translates them.

---

## Counters

These are monotonically increasing event counts. Use `rate()` (Prometheus) or
the equivalent in your backend to derive per-second rates.

### `rl_tasks_total`

Every task lifecycle outcome. Use this as the denominator for success-rate
calculations and as the primary input to SLO burn rates.

| Label | Values | Meaning |
|-------|--------|---------|
| `status` | `completed` | Task finished successfully |
| `status` | `failed` | Task raised an exception or hard-timed-out |
| `status` | `idempotency_hit` | Duplicate dispatch returned a cached result without re-executing |
| `rl.task.name` | fully-qualified task name | One series per task type |
| `reason` (failed only) | `timeout`, exception class name | Failure category |

**Dashboard idea, success rate per task:**

```promql
sum by (rl_task_name) (rate(rl_tasks_total{status="completed"}[5m]))
/
sum by (rl_task_name) (rate(rl_tasks_total[5m]))
```

### `rl_admission_total`

Every admission control decision.

| Label | Values |
|-------|--------|
| `result` | `admitted`, `rejected` |

**Alert idea, sustained shedding:**

```promql
rate(rl_admission_total{result="rejected"}[5m]) > 0.1
```

### `rl_timeouts_total`

Soft and hard timeout firings.

| Label | Values |
|-------|--------|
| `type` | `soft`, `hard` |
| `rl.task.name` | fully-qualified task name |

A growing `type="soft"` count without a corresponding `type="hard"` count is
healthy, it means cleanup hooks fired in time. The opposite is a sign that
soft/hard gaps are too tight to actually run cleanup.

### `rl_shutdowns_total`

Worker shutdown outcomes.

| Label | Values | Meaning |
|-------|--------|---------|
| `type` | `clean` | Worker drained all tasks before exiting |
| `type` | `handoff` | Some tasks were handed off to Phoenix via heartbeat deletion |
| `type` | `forced` | Drain timeout expired before tasks finished |

`forced` shutdowns indicate `RELIER_GRACEFUL_SHUTDOWN_TIMEOUT` may be too
short for your workload's tail latency.

### `rl_resurrections_total`

Phoenix resurrection events. Incremented only after the broker ACKs the
re-dispatch (see [Durability → leases](durability.md#layer-3-leasing-fencing)).

| Label | Values |
|-------|--------|
| `rl.task.name` | fully-qualified task name |

**Alert idea, abnormal resurrection rate:**

```promql
rate(rl_resurrections_total[5m]) > 0.5
```

That threshold is workload-specific, pick a value based on your baseline.

### `rl_dlq_quarantined_total`

Tasks pushed into the DLQ.

| Label | Values |
|-------|--------|
| `reason` | `PayloadIntegrityError`, `SchemaMigrationError`, `TimeoutError`, `max_resurrections_exceeded`, or any user exception class name |
| `rl.task.name` | fully-qualified task name |

Any non-zero rate here is worth a notification. `PayloadIntegrityError` in
particular is a serious signal, investigate broker storage immediately.

### `rl_idempotency_hits_total`

Duplicate dispatches that returned a cached result.

| Label | Values |
|-------|--------|
| `rl.task.name` | fully-qualified task name |

This is a useful sanity check, it tells you how often idempotency is doing
real work. A flat-zero counter on a task you expect duplicates for usually
means the keys aren't stable.

### `rl_circuit_trips_total`

Reserved for future circuit-breaker functionality. Currently emitted only when
Relier opens an internal protection circuit (rare).

---

## Histograms

These record value distributions. Most backends expose `_bucket`, `_sum`,
`_count` series; use `histogram_quantile()` (Prometheus) for percentiles.

### `rl_task_duration_ms`

End-to-end task execution duration, including Relier's framework overhead.

| Label | Values |
|-------|--------|
| `rl.task.name` | fully-qualified task name |

**Dashboard idea, p95 latency per task:**

```promql
histogram_quantile(0.95,
  sum by (le, rl_task_name) (rate(rl_task_duration_ms_bucket[5m]))
)
```

### `rl_overhead_ms`

Framework overhead per task phase. Useful for proving Relier isn't your
latency bottleneck.

| Label | Values |
|-------|--------|
| `phase` | `enqueue`, `pickup`, `schema`, `idempotency` |
| `rl.task.name` | fully-qualified task name |

`enqueue` covers admission check + envelope wrap on the producer side.
`pickup` + `schema` + `idempotency` are worker-side phases before user code
runs. Aggregate them to see Relier's total per-task tax.

### `rl_resurrection_time_s`

Time elapsed between heartbeat expiry and successful re-dispatch.

| Label | Values |
|-------|--------|
| `rl.task.name` | fully-qualified task name |

Compare to the theoretical minimum (`heartbeat_ttl + resurrection_check_interval`,
default 12 s), sustained higher values mean the resurrector is under load,
likely tripping the thundering-herd backpressure brakes.

### `rl_shutdown_duration_s`

Time the graceful drain phase actually took.

A distribution skewed near `graceful_shutdown_timeout` means many shutdowns
hit the wall and forced a handoff. Compare with `rl_shutdowns_total{type="forced"}`.

---

## Observable gauge

### `rl_inflight_tasks`

Current count of tasks executing per worker. Read via callback at scrape time.

| Label | Values |
|-------|--------|
| `rl.worker.id` | worker hostname (e.g. `celery@rl-worker-1`) |

This is the same data `rl tasks inflight` shows, just exposed for dashboards.

---

## Standard attributes carried on spans

Beyond metrics, every OpenTelemetry **span** Relier emits carries a consistent
set of attributes for correlation:

| Attribute | Meaning |
|-----------|---------|
| `rl.task.id` | Celery task UUID |
| `rl.task.name` | Fully-qualified task name |
| `rl.task.queue` | Queue the task was dispatched onto |
| `rl.worker.id` | Worker hostname |
| `rl.task.schema_version` | Envelope schema version |
| `rl.task.is_resurrection` | `true` if this is a resurrected incarnation |
| `rl.task.is_idempotent` | `true` if the task is `idempotent=True` |
| `rl.task.idempotency_hit` | `true` if the cache served a duplicate dispatch |
| `rl.admission.result` | `admitted` / `rejected` |

The span hierarchy is documented in
[Core Concepts → OpenTelemetry](concepts.md#opentelemetry-distributed-tracing).

---

## What to put on the default dashboard

Five panels cover most of what an operator needs:

1. **Cluster success rate**: `rate(rl_tasks_total{status="completed"})` /
   `rate(rl_tasks_total)`. Goal line at your SLO target.
2. **DLQ growth**: `rate(rl_dlq_quarantined_total) by (reason)`. Any
   non-zero rate flagged.
3. **Resurrection rate**: `rate(rl_resurrections_total)`. Spikes correlate
   with worker fleet issues.
4. **Admission shed rate**: `rate(rl_admission_total{result="rejected"})`.
   Capacity signal.
5. **p95 task latency**: `histogram_quantile(0.95, rl_task_duration_ms_bucket)`
   per `rl.task.name`. Performance regression signal.

That's the minimum. Once those are live, add `rl_shutdowns_total{type="forced"}`
(deploy health), `rl_resurrection_time_s` p95 (Phoenix lag), and
`rl_overhead_ms` (framework cost) as you need them.

---

## What Relier does NOT emit

- **Per-task input payloads.** Task arguments may contain PII; nothing about
  args/kwargs appears in metrics or spans.
- **Worker CPU / memory metrics.** Use the OTel collector's host receiver,
  cAdvisor, or your cloud provider's metrics, these are not Relier's job.
- **Redis-side metrics.** Use the Redis Exporter (or your managed Redis
  provider's metrics) for keyspace, latency, and replication lag.
