# CLI Reference

Complete reference for the `rl` command-line interface. Run `rl --help` for a
summary, or `rl <group> --help` for options in a specific group.

## How the CLI works underneath

The `rl` CLI is a Typer app that talks directly to the same Redis cluster your
workers use. There is no daemon, no API server, no extra process: every command
is a short-lived Python program that connects to Redis (via Sentinel if
configured), reads or writes the relevant `rl:*` keys, and exits. That means:

- **Every command requires Redis to be reachable.** If Redis is down, the CLI
  fails with a clear connection error, the same way workers do.
- **CLI calls are read-only by default.** Mutating operations (`rl dlq purge`,
  `rl admin reset-admission`, `rl tasks cancel`) require an explicit flag or
  argument.
- **The CLI is async-first.** Most commands are `async def` functions wrapped in
  a Typer adapter that runs them via `asyncio.run`. That lets a single command
  parallelise multiple Redis round-trips with `asyncio.gather` for example,
  `rl tasks inflight` fetches per-worker counts concurrently.
- **No special daemon mode for `rl run-resurrector`.** That command is just an
  ordinary process: it runs the Phoenix resurrection loop and exits cleanly on
  SIGTERM.
- **Cluster commands (`rl cluster …`) shell out to `docker compose`.** They
  assume there is a `docker-compose.yml` in the current directory and that
  Docker is installed. Without Docker, the bare-metal `make` targets do
  equivalent work.

Each command section below has a **Under the hood** note describing the exact
Redis keys touched. If you're debugging or building automation against Relier,
those notes are the authoritative source, the CLI is a thin pretty-printer on
top of those reads.

---

## Global options

```bash
rl [OPTIONS] COMMAND [ARGS]...
```

| Option | Description |
|--------|-------------|
| `--version`, `-v` | Print the installed Relier version and exit. |
| `--help` | Show the top-level help message. |

---

## `rl run-resurrector`

Start the Phoenix resurrector engine. This is the long-running process that
watches for dead workers and re-queues their orphaned tasks.

```bash
rl run-resurrector [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--loglevel` | `info` | Logging level: `debug`, `info`, `warning`, `error`. |
| `--interval` | from config | Override the resurrection check interval in seconds. |

Run this in a dedicated container (the "guardian"):

```bash
rl run-resurrector
rl run-resurrector --loglevel debug
rl run-resurrector --interval 5   # check every 5 seconds instead of the default
```

**Under the hood.** This runs `PhoenixRegistry.resurrection_loop()`, an async
loop that every `resurrection_check_interval` seconds:
1. `ZRANGEBYSCORE rl:phoenix:expiry_index 0 now`, list every task whose
   heartbeat deadline has passed.
2. For each candidate, check `EXISTS rl:hb:{task_id}`. If the heartbeat is
   still alive, the worker is fine; update the expiry score and skip.
3. If expired, acquire `SET rl:lock:resurrect:{task_id} NX EX 30` to prevent
   two resurrectors racing.
4. Increment `rl:resurrections:{task_id}` (after broker ACK only). If it
   exceeds `RELIER_MAX_RESURRECTIONS`, quarantine to `rl:dlq` instead.
5. Atomic Lua: mint a fence token, set `rl:lease:{task_id}` and
   `rl:fence:{task_id}`, then re-dispatch the task to the internal `re-queue`
   queue with the fence tokens injected into kwargs.

The resurrector is **single-instance by design**. Running two does not cause
duplicate execution (the distributed lock prevents it), but it wastes scan
cycles.

---

## `rl doctor`

Check the health of Relier's infrastructure dependencies (Redis, etc.) and exit with code 1 if anything is unreachable.

```bash
rl doctor
```

No options. Useful as a liveness probe in Docker and Kubernetes:

```bash
rl doctor && echo "All systems go"
```

**Under the hood.** Calls `redis_manager.ping()` against the configured Redis
endpoint (Sentinel-aware if `RELIER_REDIS_USE_SENTINEL=true`). The exit code is
the only side effect, there are no writes to Redis. Pair with `rl config
validate` if you also want to check `maxmemory-policy` and connection-pool
sizing.

---

## `rl man`

Print the Relier manual. Shows `docs/rl.md` if it exists in the project directory, otherwise tries the installed man page.

```bash
rl man
```

---

## `rl bench`

Measure Relier's dispatch overhead versus raw Celery dispatch. Runs both
paths against the built-in `chaos_noop` task and reports a percentile
distribution.

```bash
rl bench [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--iterations` | `1000` | Number of measured dispatches per path. |
| `--warmup` | `100` | Warmup iterations discarded before measuring (covers Redis `SCRIPT LOAD`, connection-pool warmup, and asyncio loop settling). |

```bash
rl bench                              # default: 1000 + 100 warmup
rl bench --iterations 5000 --warmup 500
```

**Under the hood.** Force-imports `relier.tasks.app.celery_app` so the
shared-task registry binds to the Redis broker, then:

1. **Warmup:** N dispatches per path, discarded. This pays the one-time
   costs (Redis `SCRIPT LOAD` for the admission Lua, connection-pool
   establishment, asyncio loop startup) so they don't pollute the
   measurement.
2. **Baseline:** N calls to `chaos_noop.delay(...)`, timed individually
   with `time.perf_counter`.
3. **Relier path:** N calls to `await chaos_noop.apush(...)`, timed
   individually.
4. **Percentiles:** sorts the samples and emits p50, p95, p99, plus a mean
   trimmed of the top 1% (so a single AOF-fsync hiccup doesn't drag the
   mean up).

The output also reports the platform and Python version it ran on, because
the slow path on Windows + localhost Redis is roughly 30–50% slower than
the same code on Linux + uvloop. **Trust the p50 across multiple runs**,
not a single percentage from one run microbenchmarks at this scale are
intrinsically noisy.

`.delay()` is used here as the baseline of comparison only, never call it
in application code (see [API → Dispatch methods](api-reference.md#dispatch-methods-apush-push-delay)).

---

## `rl cluster`  Docker stack management

Commands for managing the full Relier Docker Compose stack.

```bash
rl cluster COMMAND [ARGS]...
```

### `rl cluster up`

Start the full Relier stack. Builds images and starts all services in detached mode by default.

```bash
rl cluster up [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--detach` / `--no-detach`, `-d` | detached | Run in detached mode (background). |

```bash
rl cluster up           # start detached (default)
rl cluster up --no-detach   # attach and stream logs
```

---

### `rl cluster down`

Gracefully shut down all services in the Relier stack.

```bash
rl cluster down
```

---

### `rl cluster status`

Show the state of Docker Compose services and Redis connectivity.

```bash
rl cluster status
```

---

### `rl cluster scale`

Scale the worker pool to a specific number of replicas.

```bash
rl cluster scale WORKERS
```

| Argument | Description |
|----------|-------------|
| `WORKERS` | Number of worker replicas to run. |

```bash
rl cluster scale 4
rl cluster scale 1   # scale down to a single worker
```

---

### `rl cluster logs`

Tail logs from one or all services in the Relier stack.

```bash
rl cluster logs [OPTIONS] [SERVICE]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--follow`, `-f` | `false` | Stream logs continuously (like `tail -f`). |

| Argument | Description |
|----------|-------------|
| `SERVICE` | Service name to filter (e.g. `worker`, `guardian`, `redis`). Omit for all services. |

```bash
rl cluster logs                      # tail all services
rl cluster logs --follow             # stream all services
rl cluster logs --follow guardian    # stream just the resurrector
rl cluster logs worker               # last logs from workers
```

---

## `rl tasks`   Task monitoring and control

Commands for monitoring in-flight tasks and managing their lifecycle.

```bash
rl tasks COMMAND [ARGS]...
```

### `rl tasks list`

List all tasks currently executing across the cluster, with per-worker metrics.

```bash
rl tasks list
```

---

### `rl tasks inflight`

Show in-flight tasks with optional live-refresh mode.

```bash
rl tasks inflight [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--follow`, `-f` | `false` | Enable live-refreshing view, updated every 2 seconds. |
| `--worker` | all | Filter to a specific worker ID. |

```bash
rl tasks inflight                          # one-shot snapshot
rl tasks inflight --follow                 # live refresh
rl tasks inflight --worker rl-worker-1     # filter to one worker
rl tasks inflight -f --worker rl-worker-2  # live + filtered
```

Output columns: `Worker`, `Status`, `In-Flight`, `Completed`, `Failed`, `Success Rate`.

Footer shows: cluster totals, queue depth, p95 latency.

**Under the hood.** Reads in parallel:
- `ZRANGEBYSCORE rl:workers -inf +inf`  every worker last-seen timestamp.
- For each worker: `ZCARD rl:inflight:{worker_id}` (active tasks),
  `GET rl:m:w:{worker_id}:success` / `:failed` (per-session counters).
- `LRANGE rl:task_durations 0 -1` for p95 latency.

`--follow` re-runs the whole snapshot every 2 s with a live Rich-Table refresh
in place. No long-lived Redis subscription.

---

### `rl tasks top`

Show a top-like summary of cluster throughput, active workers, and the five workers with highest task counts.

```bash
rl tasks top
```

---

### `rl tasks inspect`

Show the full payload, state, and metadata for a specific task.

```bash
rl tasks inspect TASK_ID
```

| Argument | Description |
|----------|-------------|
| `TASK_ID` | The Celery task UUID. |

Output is formatted JSON with syntax highlighting. Fields include:

- `task_id`:  the UUID
- `status`:  `RUNNING`, `QUARANTINED`, `COMPLETED_OR_ORPHANED`, or `UNKNOWN`
- `resurrection_count`:  how many times Phoenix has re-queued this task
- `payload`:  the signed envelope (args, kwargs, task_name, queue)
- `dlq`:  quarantine entry, if the task is in the DLQ

```bash
rl tasks inspect task_abc123
```

---

### `rl tasks retry`

Re-queue a failed or quarantined task by ID. Checks the DLQ first; falls back to the Phoenix payload for orphaned tasks.

```bash
rl tasks retry TASK_ID
```

| Argument | Description |
|----------|-------------|
| `TASK_ID` | The task UUID to retry. |

```bash
rl tasks retry task_abc123
```

If the task is in the DLQ, this calls `DeadLetterQueue.release()` and preserves the resurrection count. If it's an orphaned (not quarantined) task, it re-submits the Phoenix payload directly.

---

### `rl tasks cancel`

Revoke and cancel a running or queued task.

```bash
rl tasks cancel [OPTIONS] TASK_ID
```

| Option | Default | Description |
|--------|---------|-------------|
| `--terminate` / `--no-terminate` | `true` | Send SIGTERM to the running task process. `--no-terminate` marks it revoked without killing it (only prevents future execution). |

| Argument | Description |
|----------|-------------|
| `TASK_ID` | The task UUID to cancel. |

```bash
rl tasks cancel task_abc123           # revoke + SIGTERM
rl tasks cancel --no-terminate task_abc123   # mark revoked, don't terminate
```

---

### `rl tasks logs`

Stream state transitions for a specific task from Redis.

```bash
rl tasks logs [OPTIONS] TASK_ID
```

| Option | Default | Description |
|--------|---------|-------------|
| `--follow`, `-f` | `false` | Poll for state changes every 2 seconds until the task completes or is quarantined. |

| Argument | Description |
|----------|-------------|
| `TASK_ID` | The task UUID to follow. |

```bash
rl tasks logs task_abc123           # one-shot state snapshot
rl tasks logs --follow task_abc123  # stream until completion
```

!!! note
    This command shows task state transitions from Redis (running → completed, running → quarantined, etc.). For full stdout/stderr log aggregation, you need a log backend like Loki or Elasticsearch wired to your workers.

---

## `rl worker`  Worker management

Commands for monitoring individual workers and managing their lifecycle.

```bash
rl worker COMMAND [ARGS]...
```

### `rl worker status`

List all active workers with their per-session execution metrics.

```bash
rl worker status
```

Output columns: `Worker ID`, `Status`, `Active` (in-flight), `Success`, `Failed`, `Success Rate`.

Footer shows cluster totals.

---

### `rl worker drain`

Send a graceful shutdown signal to a specific worker. The worker stops accepting new tasks and either finishes current work or hands tasks off to Phoenix.

```bash
rl worker drain WORKER_ID
```

| Argument | Description |
|----------|-------------|
| `WORKER_ID` | The worker hostname (e.g. `celery@rl-worker-1`). |

```bash
rl worker drain celery@rl-worker-1
```

Use this before taking a worker offline for maintenance. The worker exits cleanly; your process manager (Docker, Kubernetes, systemd) does not restart it unless configured to do so.

---

### `rl worker restart`

Send a graceful shutdown signal to a specific worker for a rolling restart. The worker exits cleanly and the process manager restarts it automatically.

```bash
rl worker restart WORKER_ID
```

| Argument | Description |
|----------|-------------|
| `WORKER_ID` | The worker hostname. |

```bash
rl worker restart celery@rl-worker-1
```

Functionally identical to `drain` from Relier's perspective the difference is intent and what the process manager does afterward.

---

### `rl worker reset`

Reset the per-worker session metrics (success/failed counts) for a specific worker.

```bash
rl worker reset WORKER_ID
```

| Argument | Description |
|----------|-------------|
| `WORKER_ID` | The worker hostname to reset. |

```bash
rl worker reset celery@rl-worker-1
```

---

## `rl dlq`  Dead Letter Queue

Commands for inspecting, releasing, and purging quarantined tasks.

```bash
rl dlq COMMAND [ARGS]...
```

### `rl dlq list`

Show all quarantined tasks currently in the DLQ.

```bash
rl dlq list
```

Output columns: `ID`, `TASK`, `RESURRECTIONS`, `QUARANTINED_AT`, `LAST_ERROR`.

**Under the hood.** `HGETALL rl:dlq` followed by JSON-decoding each value.
Sorted in-process by `quarantined_at`. The DLQ is a single Redis hash keyed by
task ID, entry count = `HLEN rl:dlq`. Purging is `DEL rl:dlq` plus cleanup of
any checkpoint blobs referenced by the quarantined envelopes.

---

### `rl dlq inspect`

View the full JSON payload and error context of a quarantined task.

```bash
rl dlq inspect TASK_ID
```

| Argument | Description |
|----------|-------------|
| `TASK_ID` | The task UUID to inspect. |

```bash
rl dlq inspect task_f8a2b1
```

Displays formatted JSON with syntax highlighting. Fields include the full original payload, error reason, resurrection count, any partial checkpoint, and quarantine timestamp.

---

### `rl dlq release`

Un-quarantine a task and re-submit it to its original queue. Preserves the resurrection count so the task can't bypass `max_resurrections` by being repeatedly released.

```bash
rl dlq release TASK_ID
```

| Argument | Description |
|----------|-------------|
| `TASK_ID` | The task UUID to release. |

```bash
rl dlq release task_f8a2b1
```

Exit code 1 if the task is not found in the DLQ.

---

### `rl dlq retry-all`

Re-submit all quarantined tasks to their original queues. Shows per-task success or failure.

```bash
rl dlq retry-all
```

Use this after fixing the root cause that sent tasks to the DLQ. Resurrection counts are preserved.

---

### `rl dlq purge`

Permanently delete all tasks in the DLQ. Requires `--confirm` to prevent accidental data loss.

```bash
rl dlq purge [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--confirm` | `false` | Explicitly confirm the deletion. Required to proceed. |

```bash
rl dlq purge --confirm
```

!!! warning "This is irreversible"
    Purged tasks are gone. Use `rl dlq release` or `rl dlq retry-all` if you want to retry them first.

---

## `rl slo`  SLO monitoring

Commands for viewing error budget burn rates and generating reports.

```bash
rl slo COMMAND [ARGS]...
```

### `rl slo status`

Show the current error budget burn rates across all time windows.

```bash
rl slo status [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--target`, `-t` | `0.999` | SLO target as a decimal fraction (e.g. `0.999` = 99.9%). |

```bash
rl slo status
rl slo status --target 0.9999   # stricter SLO target
```

Output shows burn rates for 1h, 6h, and 3d windows:

```
  Window   Burn Rate   Status
  ──────────────────────────────
  1h       0.42x       HEALTHY
  6h       0.38x       HEALTHY
  3d       0.21x       HEALTHY

Budget used: 0.3 min (0.6% of monthly)
All reliability targets are being met.
```

**Burn rate interpretation:**

| Burn Rate | Meaning |
|-----------|---------|
| `0x` | No failures,  pristine |
| `< 1x` | Under budget,  healthy |
| `1x` | Exactly on budget |
| `> 1x` | Burning budget too fast,  attention needed |
| `≥ 14.4x` | Budget exhausted in ~2 hours,  critical |

---

### `rl slo report`

Generate a detailed burn-rate report with projected monthly budget consumption.

```bash
rl slo report [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--period` | `3d` | Reporting period: `1h`, `6h`, or `3d`. |
| `--format`, `-f` | `table` | Output format: `table` or `json`. |

```bash
rl slo report
rl slo report --format json
rl slo report --period 1h --format json
```

JSON output:

```json
{
  "1h": 0.42,
  "6h": 0.38,
  "3d": 0.21
}
```

---

## `rl chaos`  Chaos engineering

Commands for triggering deliberate failures to validate cluster reliability. See the [Chaos Guide](chaos-guide.md) for detailed explanation of each scenario.

```bash
rl chaos COMMAND [ARGS]...
```

!!! warning
    Run chaos commands against a non-production cluster.

### `rl chaos worker-kill`

Kill a random or specific worker process with SIGKILL (unclean death).

```bash
rl chaos worker-kill [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--worker` | random | Specific worker container name to kill. |
| `--seed` | `false` | Dispatch a long-running task before the kill. |
| `--seed-duration` | `30` | Duration of the seeded task in seconds. |
| `--watch` | `false` | Stream Phoenix resurrection events after the kill. |
| `--watch-duration` | `30` | How long to stream events (seconds). |

```bash
rl chaos worker-kill
rl chaos worker-kill --seed --watch --watch-duration 60
rl chaos worker-kill --worker relier-worker-1 --seed
```

---

### `rl chaos network-partition`

Simulate a network partition between workers and Redis for a fixed duration.

```bash
rl chaos network-partition [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--secs` / `--duration` | `15` | Duration of the simulated outage in seconds. |

```bash
rl chaos network-partition
rl chaos network-partition --secs 30
```

---

### `rl chaos load-spike`

Flood the dispatch path to exercise admission control.

```bash
rl chaos load-spike [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--rps` | `100` | Requests per second target. |
| `--duration` | `10` | Duration of the spike in seconds. |

```bash
rl chaos load-spike
rl chaos load-spike --rps 2000 --duration 30
```

Output: `accepted`, `rejected`, `errored` counts.

---

### `rl chaos task-corrupt`

Inject a malformed "poison pill" envelope into the queue to test payload integrity enforcement.

```bash
rl chaos task-corrupt
```

The corrupted task should land in `rl dlq list` immediately with `reason: PayloadIntegrityError`.

---

### `rl chaos slow-task`

Dispatch a task that sleeps past the configured `hard_timeout` to test timeout enforcement.

```bash
rl chaos slow-task [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--duration` | `35` | Seconds the task will sleep. Should exceed `RELIER_HARD_TIMEOUT`. |

```bash
rl chaos slow-task
rl chaos slow-task --duration 60
```

---

## `rl config`  Configuration management

Commands for viewing, validating, and updating Relier configuration.

```bash
rl config COMMAND [ARGS]...
```

### `rl config show`

Print all active configuration settings as a table.

```bash
rl config show
```

Sensitive values (passwords, secrets, URLs) are masked with `********`.

---

### `rl config validate`

Validate the current configuration and Redis setup. Checks:

- Redis `maxmemory-policy noeviction` (critical: required for zero-job-loss)
- Connection pool pressure (`RELIER_REDIS_MAX_CONNECTIONS` vs. worker concurrency)
- All `RELIER_*` environment variables

```bash
rl config validate
```

Exit code 1 if any critical check fails. Safe to run in CI or as a startup check.

---

### `rl config set`

Update a configuration value in the local `.env` file.

```bash
rl config set KEY VALUE
```

| Argument | Description |
|----------|-------------|
| `KEY` | The config key to set (e.g. `RELIER_HEARTBEAT_TTL`). |
| `VALUE` | The new value. |

```bash
rl config set RELIER_HEARTBEAT_TTL 15
rl config set RELIER_MAX_RESURRECTIONS 10
rl config set RELIER_LOG_LEVEL DEBUG
```

Searches for `.env` starting from the current directory and walking up to the root. Appends the key if it doesn't exist; replaces the value if it does.

!!! note
    Settings are read once at worker startup. After running `rl config set`, restart your workers for the change to take effect:
    ```bash
    rl worker restart celery@rl-worker-1
    ```

---

## `rl admission`  Admission control

Commands for monitoring admission control status.

```bash
rl admission COMMAND [ARGS]...
```

### `rl admission status`

Show the current admission control status, including how many requests have been admitted in the current window.

```bash
rl admission status
```

```
Admission Control Status

Status: ALLOWING (1240/5000, 24.8%)
Window: 10s
```

When the limit is hit:

```
Status: SHEDDING (5001/5000)
Window: 10s
```

**Under the hood.** `GET rl:admission:celery-dispatch` for the current count
and `TTL` for the time until the window resets. Compares against
`RELIER_ADMISSION_LIMIT` (read from `Settings`). The admission counter is
maintained by an atomic Lua script in `core/admission.py`; this command just
reads its state. No writes.

---

## `rl admin`  Cluster administration

Low-level administrative tools. These commands modify Redis state directly, use with care.

```bash
rl admin COMMAND [ARGS]...
```

### `rl admin config`

Display the active cluster configuration (abbreviated view of key settings).

```bash
rl admin config
```

---

### `rl admin purge-locks`

Force-delete all idempotency locks and in-flight sentinels in Redis.

```bash
rl admin purge-locks
```

Use this to unstick tasks that are permanently stuck in the `IN_FLIGHT` state (for example, after a corrupted Redis shutdown that left sentinels without corresponding workers). This is a recovery operation,  use only when you understand the implications.

!!! warning
    Clearing in-flight sentinels can allow duplicate execution of idempotent tasks if the original task is somehow still running. Verify workers are not executing the affected tasks before running this command.

---

### `rl admin reset-admission`

Reset the cluster admission control counters.

```bash
rl admin reset-admission
```

Clears all `rl:admission:*` keys in Redis, immediately allowing new requests regardless of the previous window state. Use this to manually unblock a cluster that tripped admission control.
