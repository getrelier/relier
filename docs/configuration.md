# Configuration

All Relier settings are read from environment variables with the `RELIER_`
prefix. The recommended approach is a `.env` file in your project root, Relier
loads it automatically via `pydantic-settings`.

```bash
# Validate your current configuration
rl config show       # display all active values
rl config validate   # check Redis policy + all env vars
```

!!! note "How configuration is loaded under the hood"
    Settings are parsed exactly once per process at first call to
    `get_settings()` (an `@lru_cache`d function in `relier/config.py`). The
    resulting `Settings` object is **frozen**, Pydantic raises if anything
    tries to mutate it. That immutability is intentional: it makes every
    worker, the resurrector, the CLI, and your producer see the *same* config
    snapshot, which prevents surprises like an admission limit raised at
    runtime taking effect only in one process. To change a value: edit `.env`
    (or set the env var) and restart the affected processes.

---

## Minimal production `.env`

```bash
RELIER_REDIS_URL=redis://redis:6379/0
RELIER_ADMISSION_LIMIT=10000
RELIER_CELERY_WORKER_COUNT=8
RELIER_CELERY_WORKER_CONCURRENCY=8
RELIER_OTEL_ENABLED=true
RELIER_OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
```

---

## Redis Connection

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_REDIS_URL` | `str` | `redis://localhost:6379/0` | Direct Redis connection URL. Ignored if `RELIER_REDIS_USE_SENTINEL=true`. |
| `RELIER_REDIS_PASSWORD` | `str` | none | Redis `requirepass` value. Sensitive, never commit to version control. |
| `RELIER_REDIS_MAX_CONNECTIONS` | `int` | `20` | Connection pool size per worker process. Total connections = workers × this value. |
| `RELIER_REDIS_SOCKET_TIMEOUT` | `float` | `5.0` | Socket read timeout in seconds. |
| `RELIER_REDIS_CONNECT_TIMEOUT` | `float` | `2.0` | Connection establishment timeout in seconds. |
| `RELIER_REDIS_HEALTH_CHECK_INTERVAL` | `int` | `30` | Pool health check interval in seconds. |

!!! warning "Redis persistence is required"
    Relier stores all task state in Redis. Without persistence, a Redis restart drops every heartbeat and payload, tasks in flight are lost.

    Minimum required Redis config:
    ```
    appendonly yes
    appendfsync everysec
    ```
    See [Deployment](deployment.md) for full Redis setup.

---

## Redis Sentinel (High Availability)

For production deployments that need Redis HA without manual failover:

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_REDIS_USE_SENTINEL` | `bool` | `false` | Route connections through Redis Sentinel instead of a direct URL. |
| `RELIER_REDIS_SENTINEL_NODES` | `str` | none | Comma-separated `host:port` list. Example: `sentinel-1:26379,sentinel-2:26379,sentinel-3:26379` |
| `RELIER_REDIS_SENTINEL_MASTER_NAME` | `str` | `relier-master` | Sentinel master group name. |
| `RELIER_REDIS_SENTINEL_PASSWORD` | `str` | none | Sentinel `requirepass` value (if set). |

```bash
RELIER_REDIS_USE_SENTINEL=true
RELIER_REDIS_SENTINEL_NODES=sentinel-1:26379,sentinel-2:26379,sentinel-3:26379
RELIER_REDIS_SENTINEL_MASTER_NAME=relier-master
```

---

## Worker Pool

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_CELERY_WORKER_COUNT` | `int` | `8` | Number of worker processes. Used to validate pool size. |
| `RELIER_CELERY_WORKER_CONCURRENCY` | `int` | `8` | Concurrent tasks per worker. Set this in your `celery worker` command with `--concurrency`. |

!!! tip "Pool sizing formula"
    Set `RELIER_REDIS_MAX_CONNECTIONS` ≥ (`RELIER_CELERY_WORKER_CONCURRENCY` × 3).
    Each worker task needs up to 3 simultaneous Redis connections (heartbeat, inflight, idempotency).

    `rl config validate` will warn you if the pool is undersized.

---

## Phoenix Resurrector

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_HEARTBEAT_TTL` | `int` | `10` | Heartbeat key TTL in seconds. Worker death is detected within this window. Shorter = faster detection, more Redis writes. |
| `RELIER_MAX_RESURRECTIONS` | `int` | `5` | Maximum resurrection attempts before a task is quarantined to the DLQ. |
| `RELIER_RESURRECTION_CHECK_INTERVAL` | `int` | `2` | How often the resurrector scans for dead tasks (seconds). |
| `RELIER_RESURRECTION_REQUEUE_DELAY` | `float` | `0.05` | Delay between individual task re-queues in a batch (seconds). Prevents broker flooding. |
| `RELIER_RESURRECTION_BATCH_SIZE` | `int` | `1000` | Maximum tasks resurrected per scan pass. |
| `RELIER_RESURRECTION_MAX_QUEUE_DEPTH` | `int` | `10000` | If the recovery queue has more than this many tasks, skip the resurrection scan. Prevents runaway resurrection under sustained failures. |

**Detection latency formula:**

```
worst-case detection = RELIER_HEARTBEAT_TTL + RELIER_RESURRECTION_CHECK_INTERVAL
                     = 10s + 2s = 12s theoretical maximum

measured p99 = 8.9s  (resurrector catches most expiries within one TTL window)
```

With defaults the theoretical ceiling is 12 seconds; the measured p99 is 8.9 seconds. The resurrector scans every 2 seconds, so it typically detects a stale heartbeat well before the full TTL elapses.

**Tuning tradeoff:** lowering `RELIER_HEARTBEAT_TTL` below 5 seconds risks false-positive resurrections — a GC pause or heavy swap event can delay a heartbeat refresh by 2–4 seconds on a loaded worker, which would look like a dead worker at shorter TTLs. The 10-second default is a deliberate tradeoff between detection speed and false-positive rate. Only lower it if your workers run on dedicated, lightly-loaded hosts.

!!! info "Why these knobs exist, thundering-herd protection"
    `RELIER_RESURRECTION_BATCH_SIZE`, `RELIER_RESURRECTION_MAX_QUEUE_DEPTH`, and
    `RELIER_RESURRECTION_REQUEUE_DELAY` together form Relier's defence against
    a mass-failure thundering herd. When 100 workers die at once, naive
    resurrection would flood the broker. Instead:

    - The resurrector replays at most `RESURRECTION_BATCH_SIZE` tasks per scan
      pass.
    - Before each pass, it checks the recovery-queue depth. If it's already at
      `RESURRECTION_MAX_QUEUE_DEPTH`, the scan is *deferred*, expired tasks
      stay in the index and are picked up on a future pass once the workers
      have drained things.
    - `RESURRECTION_REQUEUE_DELAY` paces the resurrector itself so it can't
      CPU-pin during sustained failure storms.

    See [Durability → Thundering-herd defences](durability.md#layer-5-thundering-herd)
    for the full picture, including the internal semaphore that bounds
    concurrent broker submissions.

---

## Idempotency

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_IDEMPOTENCY_DEFAULT_TTL` | `int` | `3600` | Default result cache TTL in seconds (1 hour). |
| `RELIER_IDEMPOTENCY_INFLIGHT_TTL` | `int` | `120` | How long the `IN_FLIGHT` sentinel lives. Must be longer than `RELIER_HARD_TIMEOUT`. |

!!! warning "IN_FLIGHT TTL must exceed hard_timeout"
    If `RELIER_IDEMPOTENCY_INFLIGHT_TTL` is shorter than `hard_timeout`, the in-flight sentinel can expire before the task hard-times-out, allowing another worker to claim the same key and begin a duplicate execution while the first worker is still running.

    Relier validates this at decoration time and raises `ValueError` if violated.

---

## Timeouts

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_SOFT_TIMEOUT` | `int` | `25` | Global default soft timeout in seconds. Overridden per-task with `@rl_task(soft_timeout=N)`. |
| `RELIER_HARD_TIMEOUT` | `int` | `30` | Global default hard timeout in seconds. Overridden per-task with `@rl_task(hard_timeout=N)`. |
| `RELIER_GRACEFUL_SHUTDOWN_TIMEOUT` | `int` | `30` | How long to wait for in-flight tasks during `SIGTERM` drain before handing them off. |

---

## Checkpointing

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_CHECKPOINT_MAX_INLINE_BYTES` | `int` | `262144` | Maximum size of a checkpoint stored directly in Redis (256KB). Larger checkpoints spill to the filesystem backend. |
| `RELIER_CHECKPOINT_BACKEND` | `str` | `"inline"` | Storage backend for large checkpoints: `"inline"` (Redis only) or `"filesystem"`. |
| `RELIER_CHECKPOINT_DIR` | `str` | `/var/lib/relier/checkpoints` | Directory for filesystem checkpoints. Must be shared across all workers (e.g., a mounted volume). |

---

## Admission Control

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_ADMISSION_LIMIT` | `int` | `5000` | Maximum tasks dispatched per admission window. |
| `RELIER_ADMISSION_WINDOW` | `int` | `10` | Window duration in seconds. Requests beyond the limit return HTTP 429. |

**Effective rate** = `RELIER_ADMISSION_LIMIT / RELIER_ADMISSION_WINDOW` requests/second.

Default: 5000 / 10s = 500 tasks/second sustained. Burst up to 5000 in any 10-second window.

---

## OpenTelemetry

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_OTEL_ENABLED` | `bool` | `false` | Enable OpenTelemetry export. |
| `RELIER_OTEL_EXPORTER_OTLP_ENDPOINT` | `str` | `http://localhost:4317` | OTLP gRPC endpoint (OpenTelemetry Collector, Grafana Agent, etc.). |

When `RELIER_OTEL_ENABLED=true`, Relier exports:
- Distributed traces (spans) via OTLP gRPC
- Metrics via OTLP gRPC (Prometheus scrape also available via `prometheus-client`)

---

## General

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RELIER_LOG_LEVEL` | `str` | `"INFO"` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `RELIER_SECRET_KEY` | `str` | `"change-in-production"` | Reserved for future signing features. Set this to a random value in production. |

---

## Updating configuration

```bash
# Write a value to .env and print what changed
rl config set RELIER_HEARTBEAT_TTL 15

# Then restart your workers for the change to take effect
rl worker restart rl-worker-1
```

Settings are read once at worker startup. The only way to apply a change to a running worker is to restart it.
