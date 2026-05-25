# Troubleshooting & FAQ

Common issues, what they mean, and how to fix them. If you don't find your
issue here, run these three commands first, they cover 80% of problems:

```bash
rl doctor              # Is Redis reachable?
rl config validate     # Is the cluster configured correctly?
rl tasks inflight      # Is anything actually running?
```

---

## Startup issues

### `RuntimeError: Relier cannot reach Redis (...). Refusing to start.`

Relier preflight-checks Redis at worker / resurrector startup. This error
means the configured Redis endpoint is unreachable.

**Check, in order:**

1. **Is Redis actually running?**

   ```bash
   redis-cli -h <host> -p <port> ping
   # Expect: PONG
   ```

2. **Is `RELIER_REDIS_URL` correct?**

   ```bash
   echo $RELIER_REDIS_URL
   # Or:
   rl config show | grep RELIER_REDIS_URL
   ```

3. **Sentinel?** If `RELIER_REDIS_USE_SENTINEL=true`, check
   `RELIER_REDIS_SENTINEL_NODES` resolves and that the master named
   `RELIER_REDIS_SENTINEL_MASTER_NAME` is monitored by the quorum:

   ```bash
   redis-cli -h <sentinel-host> -p 26379 sentinel master relier-master
   ```

4. **Firewall / network policy**: common in Kubernetes when a NetworkPolicy
   blocks egress from worker pods to the Redis service.

### `RuntimeError: Relier requires Redis maxmemory-policy='noeviction', but got '<other>'.`

Relier refuses to start if Redis would silently evict heartbeats and payloads
under memory pressure. Fix the Redis config:

```
maxmemory-policy noeviction
```

The shipped `scripts/redis/redis.conf` already does this. Managed services
usually have a config knob; for Redis CLI you can set it dynamically:

```bash
redis-cli CONFIG SET maxmemory-policy noeviction
redis-cli CONFIG REWRITE
```

See [Deployment → Production Redis configuration](deployment.md#production-redis-configuration)
for why this matters.

### "Can I point `RELIER_REDIS_URL` at the same Redis I use for caching?"

**In development: yes.** Memory limits don't apply locally so the policies
don't conflict.

**In production: no.** The problem is `maxmemory-policy`. Caches need an
eviction policy (`allkeys-lru`, `volatile-lru`, etc.) so Redis automatically
drops old entries when memory fills up. Relier needs `noeviction` so Redis
*never* silently drops a heartbeat or payload. These two requirements are
mutually exclusive and `maxmemory-policy` is an **instance-wide** setting
it applies to every key on that Redis, not per-database.

Using different databases (`/0` vs `/1`) on the same instance does **not**
help, both databases share the same policy.

The fix is a dedicated Redis instance for Relier:

```bash
# Relier — noeviction + AOF
RELIER_REDIS_URL=redis://relier-redis:6379/0

# Your app cache — allkeys-lru, no persistence needed
CACHE_URL=redis://cache-redis:6379/0
```

The two instances can run on the same host if needed; they just need separate
ports or separate Redis processes.

### `ValueError: Unknown public queue '...'`

You decorated a task with a queue Relier doesn't know about. The three valid
public queues are `high_priority`, `default`, `low_priority`. The fourth
queue, `re-queue`, is **internal** and rejected, only Phoenix may publish
into it.

```python
@rl_task(queue="urgent")           # ✗ raises ValueError
@rl_task(queue="high_priority")    # ✓
```

If you need more queue lanes than three, you can add them via Celery's
`task_queues` configuration, but think first, Relier's three queues are
usually enough.

### `ValueError: hard_timeout (X s) must be < IDEMPOTENCY_INFLIGHT_TTL (Y s)`

If `hard_timeout` exceeds `RELIER_IDEMPOTENCY_INFLIGHT_TTL` (default 120 s),
the in-flight idempotency sentinel can expire while a task is still running,
letting another worker claim the same key, a duplicate execution. Either:

- raise `RELIER_IDEMPOTENCY_INFLIGHT_TTL` (and `idempotency_ttl` accordingly), or
- shorten `hard_timeout`.

Safe formula: `hard_timeout < IN_FLIGHT_TTL - 10 s`.

### `ValueError: Timeout parameters are only supported for async functions.`

You decorated a `def` function with `soft_timeout=` or `hard_timeout=`. The
two-tier timeout machinery uses asyncio cancellation and only works on
coroutines. Refactor:

```python
# Before:
@rl_task(hard_timeout=10)
def my_task(x): ...                  # ✗

# After:
@rl_task(hard_timeout=10)
async def my_task(x):
    return await asyncio.to_thread(blocking_call, x)
```

---

## "My task never runs"

### Symptoms: dispatched task never appears in `rl tasks inflight`

Walk down the chain from producer to worker:

1. **Did dispatch actually happen?**

   ```bash
   rl admission status
   # If SHEDDING, your apush is raising AdmissionRejectedError before dispatch.
   ```

2. **Are there any workers consuming the task's queue?**

   ```bash
   rl worker status
   ```

   If you decorated with `queue="high_priority"` but the only worker is
   running `-Q default`, the task sits in Redis forever.

3. **Is the queue depth growing?**

   ```bash
   rl tasks inflight     # footer shows queue depth
   ```

   Growing queue + idle workers = workers aren't subscribed to the right queue.

4. **Was the task quietly DLQ'd?** A `PayloadIntegrityError` lands in the DLQ
   without ever executing:

   ```bash
   rl dlq list
   ```

### Symptoms: workers run normal tasks but never pick up an `@rl_task`

Almost always one of:

- **Module not imported.** Celery only registers tasks defined in modules it
  imports. Make sure `relier.tasks.app` (or your own Celery app whose imports
  include your task module) is what the worker process loads.

- **Different broker.** A common slip when migrating from raw Celery: your
  producer ends up using Celery's default `amqp://` (RabbitMQ) instead of
  Relier's Redis broker because `celery_app` hasn't been imported yet. Symptoms
  include hangs on dispatch. Fix: ensure `from relier.tasks.app import
  celery_app` happens before the first dispatch. `rl bench` does this
  explicitly for the same reason.

---

## "My task ran twice"

Relier's idempotency only kicks in when you ask for it. Check:

1. **Did you set `idempotent=True`?**

   ```python
   @rl_task(idempotent=True)
   async def charge(...): ...
   ```

2. **Are the arguments stable?** Auto-keyed idempotency hashes
   `(task_name, args, kwargs)`. If your kwargs include a `request_id` that
   changes between retries, each retry gets a different key. Use
   [`idempotency_lock`](api-reference.md#idempotency_lock) with an explicit key.

3. **`max_resurrections` exhausted into DLQ-release?** Re-releasing a DLQ task
   preserves the resurrection count, so it can't bypass `max_resurrections`
   but a *manual* `celery_app.send_task` from your own code can. If you've got
   ad-hoc replay scripts, audit them.

---

## "My task is in the DLQ"

`rl dlq inspect <task_id>` shows the reason. The common ones:

| `reason` | Meaning | Likely fix |
|---|---|---|
| `PayloadIntegrityError` | Envelope checksum mismatch, payload was tampered with or storage corrupted. | Re-enqueue from source, investigate broker corruption. Never auto-retry these. |
| `SchemaMigrationError` | A migration function raised. | Inspect the migration in your code; fix it, redeploy, then `rl dlq release`. |
| `TimeoutError` / `HardTimeoutError` | Task exceeded `hard_timeout`. | Profile and reduce work; or raise the timeout. |
| `max_resurrections_exceeded` | Task crashed 5+ workers running it. | Likely a poison pill or a code bug. Inspect args; fix the bug; release. |
| Any other exception name | Your task raised that exception. | Read the stack trace in the DLQ entry; fix the underlying code. |

To release a single task after fixing the root cause:

```bash
rl dlq release <task_id>
```

To release everything once the cluster is healthy again:

```bash
rl dlq retry-all
```

Releasing **preserves the resurrection count**, so a task that previously hit
`max_resurrections` won't get infinite chances after release.

---

## "Phoenix isn't resurrecting"

If you kill a worker and the task never reappears:

1. **Is the resurrector actually running?**

   ```bash
   # Docker dev/prod
   docker compose ps | grep resurrector

   # Bare metal, the make target runs in the foreground; check the terminal.
   ```

2. **Is anything in the expiry index?**

   ```bash
   redis-cli ZRANGE rl:phoenix:expiry_index 0 -1 WITHSCORES
   ```

   If empty, the task wasn't registered (it was a fast-completing task that
   ran before the heartbeat was written, or the producer dispatched via
   `.delay()` and skipped envelope wrapping).

3. **Check the expected detection latency.** Resurrection takes up to
   `RELIER_HEARTBEAT_TTL + RELIER_RESURRECTION_CHECK_INTERVAL` seconds, plus a
   broker round-trip. With defaults that's `10 + 2 = 12 s`, total ≤ 35 s. Wait
   the full window before declaring it broken.

4. **Backpressure.** If `RELIER_RESURRECTION_MAX_QUEUE_DEPTH` is exceeded
   (default 10 000 messages in `re-queue`), the resurrector intentionally
   skips scan passes so it doesn't outrun the recovery workers. Drain `re-queue`
   first, or raise the threshold.

5. **Worker pool for `re-queue`.** Resurrected tasks land on the internal
   `re-queue` queue. If no worker consumes it, they sit there. The bundled
   `docker-compose.yml` has `worker-recovery` for exactly this. In your own
   deployment, make sure at least one worker has `-Q re-queue` (or that your
   default workers consume it too).

---

## "I dispatched and got `AdmissionRejectedError` instantly"

Admission control is doing exactly what it's designed to do.

```bash
rl admission status
```

If `SHEDDING`, you've exceeded `RELIER_ADMISSION_LIMIT` requests within the
current `RELIER_ADMISSION_WINDOW` (default 5000 per 10 s = 500 RPS sustained).

Options:

- **Catch it at the API edge** and return HTTP 429 with the `Retry-After`
  header (see [Patterns → Pattern 8](patterns.md#pattern-8)).
- **Raise the limit** if it's set too low for your real capacity:
  ```bash
  rl config set RELIER_ADMISSION_LIMIT 20000
  ```
  Then restart the producer processes (admission control reads the limit at
  process startup).
- **Manually reset** if the cluster is stuck in a bad state:
  ```bash
  rl admin reset-admission
  ```

---

## "Checkpoint too large"

```
CheckpointTooLargeError: Checkpoint for task 'X' is N bytes, which exceeds
the 262144-byte inline limit.
```

By default, Relier rejects checkpoints over 256 KB rather than bloating Redis.
Two fixes:

1. **Make the checkpoint smaller.** Often the checkpoint is bigger than it
   needs to be e.g., storing the entire output instead of a cursor. Save
   only the minimum needed to resume.

2. **Enable filesystem spillover** for legitimately-large state:

   ```
   RELIER_CHECKPOINT_BACKEND=filesystem
   RELIER_CHECKPOINT_DIR=/var/lib/relier/checkpoints
   ```

   The directory **must be shared** across every worker and the resurrector.
   The bundled `docker-compose.prod.yml` does this with the `redis_checkpoints`
   named volume. See [API → `ctx.set_partial`](api-reference.md#ctx-set-partial).

---

## Observability questions

### "How do I tell if a task ran twice?"

For an `idempotent=True` task, the second run will be an idempotency hit
rather than a re-run. Watch the `relier.idempotency.hits` counter or query:

```bash
redis-cli INCR rl:m:global:success   # only +1 even if dispatched 100 times
```

For non-idempotent tasks, there's no automatic dedup. Check your downstream
side effects (DB rows, Stripe charges, emails). This is one of the reasons
`idempotent=True` exists.

### "Where do I see resurrection events?"

While they're happening, the live watcher in `rl chaos worker-kill --watch`
streams them. Historical resurrections are visible per-task:

```bash
redis-cli GET rl:resurrections:<task_id>
# Or:
rl tasks inspect <task_id>       # shows resurrection_count
```

### "Why is my SLO burn rate jumping?"

The burn rate is `failures / (allowed_error_rate × total_events)`. A small
absolute number of failures on a slow window can produce a big burn rate.
`rl slo status` shows the multiple at 1 h / 6 h / 3 d  sustained burn over a
long window is what matters.

---

## Docker / Compose questions

### "I changed `.env` but my workers still use the old value"

Two things to check:

1. Compose loads `.env` at startup. If you changed `.env` after `make dev` was
   already running, restart: `make dev-down && make dev`.

2. Each worker reads settings exactly once at process boot. `rl config set`
   updates `.env`, but doesn't restart anything. Either restart the worker
   container, or `rl worker restart <hostname>` to gracefully cycle one
   worker.

### "Network partition test didn't resolve DNS after reconnect"

The `rl chaos network-partition` command detaches the Redis container from
every Docker network it's on, sleeps, then reconnects, preserving the original
DNS aliases (in particular the Compose service name `redis`). If you've forked
the compose file and renamed the container, set `REDIS_CONTAINER` to match.

---

## "I want to opt out of feature X"

| Feature | How to disable |
|---------|-----------------|
| OpenTelemetry export | `RELIER_OTEL_ENABLED=false` |
| Admission control | Effectively disabled by raising `RELIER_ADMISSION_LIMIT` very high. The Lua script will still run. There's no "off" switch because that's how the producer knows about cluster pressure. |
| Idempotency for a task | Don't pass `idempotent=True`. |
| Phoenix resurrection | Don't run the resurrector. Tasks that die will stay dead, you've turned off the core guarantee. |
| Checkpointing | Don't call `ctx.set_partial`. Checkpoint storage is opt-in. |

---

## When in doubt

Show, don't guess. Most issues become obvious with one of these:

```bash
rl tasks inspect <task_id>     # what state is this task in?
rl dlq inspect <task_id>       # why was it quarantined?
rl worker status               # are workers alive?
rl admission status            # is the cluster shedding?
rl slo status                  # is the failure rate trending up?
docker compose logs -f worker  # what does the worker think?
```

If none of those reveal the cause, file an issue with the output of `rl
doctor`, `rl config show`, and the relevant log lines.
