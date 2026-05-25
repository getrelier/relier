# API Reference

Relier exposes one decorator (`@rl_task`) and a handful of supporting types.
Everything else is internal.

The library is **async-first**: the orchestration layer is built on asyncio,
and every Relier worker process runs a persistent event loop that lives for the
worker's lifetime. Your tasks can be `async def` or plain `def`, Relier
bridges them either way. The producer-side API (`apush` / `push`) is the same
shape regardless of which web framework you dispatch from.

> **Built for FastAPI, works with anything.** Relier is designed for
> asyncio-native web frameworks (FastAPI, Starlette, async Django), but `push`
> exists specifically so sync code paths (Flask, classic Django views, CLIs,
> management commands, scripts running inside a Celery task) can dispatch
> reliably without owning an event loop. The reliability guarantees are
> identical regardless of which one you call.

---

## `@rl_task`

The main decorator. Wraps any async (or sync) function with Relier's full
reliability stack.

```python
from relier.tasks.decorator import rl_task

@rl_task(
    queue="default",
    idempotent=False,
    idempotency_ttl=3600,
    soft_timeout=None,
    hard_timeout=None,
    on_soft_timeout=None,
)
async def my_task(arg1: str, arg2: int = 0) -> dict:
    ...
```

### Parameters

#### `queue`
**Type:** `str` **Default:** `"default"`

The Celery queue this task is routed to. Three **public** queues are
pre-configured:

| Queue | Intended use |
|-------|-------------|
| `"high_priority"` | User-facing, latency-sensitive tasks |
| `"default"` | Standard background work |
| `"low_priority"` | Batch jobs, non-urgent processing |

There is also an **internal** queue, `re-queue`, that the Phoenix resurrector
uses to inject resurrected tasks. **Decorating a task with `queue="re-queue"`
raises `ValueError`**, only Relier itself may publish there. The internal
queue exists so you can dedicate a worker pool to recovery traffic (see
`docker-compose.yml`'s `worker-recovery` service) without sharing capacity
with normal work.

Workers consume queues based on their `-Q` flag:

```bash
# Consume normal traffic only
celery -A relier.tasks.app worker -Q high_priority,default,low_priority

# Dedicate this pool to Phoenix recovery
celery -A relier.tasks.app worker -Q re-queue
```

---

#### `idempotent`
**Type:** `bool` **Default:** `False`

When `True`, Relier atomically prevents duplicate execution. If the same task
with the same arguments is submitted while one is already running, or has
already completed, the second submission either retries (in-flight) or
returns the cached result (completed) without re-executing.

The idempotency key is derived by hashing the task name + arguments. For
non-deterministic arguments (random IDs, timestamps, large blobs), use
[`idempotency_lock`](#idempotency_lock) for explicit control over the key.

```python
@rl_task(idempotent=True, idempotency_ttl=3600)
async def send_invoice(invoice_id: str) -> dict:
    ...
```

---

#### `idempotency_ttl`
**Type:** `int` (seconds) **Default:** `3600`

How long to cache the result of a completed idempotent task. After this TTL
expires, the same arguments can trigger a fresh execution.

**Constraint:** `idempotency_ttl` must be greater than `RELIER_IDEMPOTENCY_INFLIGHT_TTL`
(default 120 s), which itself must be greater than `hard_timeout`. Relier
validates this at decoration time and raises `ValueError` if violated. The
reason: if the in-flight sentinel expires before the task hard-times-out,
another worker could claim the same key and run a duplicate.

---

#### `soft_timeout`
**Type:** `int | None` (seconds) **Default:** `None`

After this many seconds, the `on_soft_timeout` hook is called. The task is
**not** cancelled, it keeps running until either it completes or
`hard_timeout` fires. Use the soft window to checkpoint progress and release
locks.

If `soft_timeout` is set, `hard_timeout` must also be set and must be strictly
greater. Relier validates this at decoration time.

```python
@rl_task(soft_timeout=25, hard_timeout=30, on_soft_timeout=save_state)
async def long_running_job(job_id: str) -> dict:
    ...
```

---

#### `hard_timeout`
**Type:** `int | None` (seconds) **Default:** `None`

After this many seconds, the task coroutine is cancelled unconditionally via
asyncio cancellation. The task is quarantined to the DLQ with
`reason=TimeoutError`.

If only `hard_timeout` is set (no `soft_timeout`), there is no cleanup window,
the task is cancelled immediately when it expires.

!!! warning "Timeouts only apply to async tasks"
    The two-tier timeout machinery uses asyncio cancellation. Decorating a
    plain `def` function with `soft_timeout=` or `hard_timeout=` raises
    `ValueError` at decoration time. If you need timeouts on sync code, refactor
    it to `async def` and `asyncio.to_thread()` the blocking calls inside.

---

#### `on_soft_timeout`
**Type:** `Callable[[TaskContext], Awaitable[None]] | None` **Default:** `None`

An async function called when the soft timeout fires. Receives a
[`TaskContext`](#taskcontext) with the current task state. Has
`hard_timeout - soft_timeout` seconds to complete before the hard timeout
fires.

```python
async def checkpoint_progress(ctx: TaskContext) -> None:
    # Save partial work so the next incarnation can resume.
    await ctx.set_partial({"items_processed": 750, "last_cursor": "item_750"})
    # Release any external locks.
    await release_lock(ctx.args[0])

@rl_task(soft_timeout=25, hard_timeout=30, on_soft_timeout=checkpoint_progress)
async def process_large_batch(batch_id: str) -> dict:
    ...
```

The hook runs on the same event loop as the task, so it must be `async def`
and non-blocking.

---

### Dispatch methods (`apush`, `push`, `delay`)

Every function decorated with `@rl_task` is bound to a per-task Celery `Task`
subclass that adds two Relier-aware dispatch methods on top of Celery's native
`.delay()` / `.apply_async()`.

```python
await my_task.apush(*args, **kwargs)  # async, preferred
my_task.push(*args, **kwargs)         # sync wrapper over apush
my_task.delay(*args, **kwargs)        # raw Celery dispatch, do not use
```

The distinction matters. `apush` and `push` are the only dispatch paths that
run admission control, wrap the payload in Relier's signed envelope, and inject
OpenTelemetry context. Choose based on whether the caller is async or sync.

#### `await task.apush(*args, **kwargs)` → `AsyncResult`

**Use this in FastAPI, Starlette, async Django, or any other asyncio code.**

```python
# FastAPI route
@app.post("/invoices/{invoice_id}/send")
async def dispatch_invoice(invoice_id: str):
    await send_invoice.apush(invoice_id)
    return {"status": "queued"}
```

What `apush` does, in order:

1. **Admission check**: atomic Lua `INCR` against
   `rl:admission:celery-dispatch`. Raises `AdmissionRejectedError` (with
   `retry_after`) if the cluster window is full.
2. **Envelope wrap**: generates a UUID4 `task_id`, builds the schema envelope
   (`schema_version`, `task_id`, `payload`, sha256 `checksum`, `enqueued_at`).
3. **OTel context injection**: current trace context goes into
   `envelope._otel_context` so the worker can continue the distributed trace.
4. **Dispatch**: `celery_app.send_task(name, args=(envelope,), queue=queue,
   task_id=task_id)`. Returns the Celery `AsyncResult`.

It does **not** wait for the task to finish, fire-and-forget, like Celery's
own `.delay()`.

#### `task.push(*args, **kwargs)` → `AsyncResult`

**Use this in Flask, classic (sync) Django, management commands, scripts, or
inside a Celery task that wants to enqueue another task.**

```python
# Flask route
@app.route("/invoices/<invoice_id>/send", methods=["POST"])
def dispatch_invoice(invoice_id):
    send_invoice.push(invoice_id)
    return {"status": "queued"}, 202

# Django view (sync)
def dispatch_invoice(request, invoice_id):
    send_invoice.push(invoice_id)
    return JsonResponse({"status": "queued"}, status=202)
```

`push` is a thin synchronous wrapper around `apush`:

- If called **inside a Celery worker** (whose persistent event loop is already
  running), `push` schedules the coroutine via
  `asyncio.run_coroutine_threadsafe(self.apush(...), worker_loop)` and waits up
  to 5 s for the dispatch to be acknowledged by the broker.
- If called **outside Celery** (e.g., from a Flask or Django request handler),
  `push` falls back to `asyncio.run(self.apush(...))`.

Either way, the reliability semantics are identical to `apush`. The only
difference is that you didn't have to `await`.

#### `task.delay(*args, **kwargs)` / `task.apply_async(...)` - **do not use**

These are Celery's native dispatch interfaces. They are available because
`@rl_task` is, underneath, a Celery `shared_task`. They **bypass Relier's
admission control and schema envelope**, so:

- Admission control will not reject overloaded clusters when dispatched via
  `.delay()`.
- The worker won't see a signed envelope, so checksum verification is skipped
  and schema migration cannot run. Tasks enqueued by `.delay()` are processed
  as legacy payloads.
- The OTel producer span is not injected.

There is one place where `.delay()` is correct: the `rl bench` command uses it
as the *baseline* in a like-for-like comparison against `.apush()`. That is the
only intended user of `.delay()`. **In application code, always use `apush` or
`push`.**

| If you are here… | Use this |
|---|---|
| Async route or coroutine | `await task.apush(...)` |
| Sync route, script, management command | `task.push(...)` |
| Existing Celery code you're migrating | Replace `.delay()` with `.push()` |
| `.delay()` in new code | Don't. Use `push`. |

#### Handling `AdmissionRejectedError`

```python
from relier.core.exceptions import AdmissionRejectedError

try:
    await my_task.apush(payload)
except AdmissionRejectedError as exc:
    raise HTTPException(
        status_code=429,
        headers={"Retry-After": str(exc.retry_after)},
        detail="Service at capacity.",
    )
```

`exc.retry_after` is the number of seconds until the admission window resets,
hand it to the client as the standard HTTP `Retry-After` header.

---

## `TaskContext`

A dataclass passed to cleanup hooks and accessible from inside any `@rl_task`
function. Use it to read task identity, save checkpoints, or read a checkpoint
that a previous incarnation wrote.

### Accessing the context inside a task

Two ways. Pick whichever fits the call site:

```python
# 1. Declare `ctx` as a kwarg, Relier injects it automatically.
@rl_task()
async def my_task(item_id: str, ctx: TaskContext) -> dict:
    print(ctx.task_id, ctx.worker_id)

# 2. Read it from a contextvar proxy.
from relier.tasks.context import task_context

@rl_task()
async def my_task(item_id: str) -> dict:
    print(task_context.task_id, task_context.worker_id)
```

The contextvar proxy raises `RuntimeError` if accessed outside a running task,
which is what you want, it makes accidental reads fail loudly in tests.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `task_id` | `str` | Celery task UUID. Stable across resurrections. |
| `task_name` | `str` | Fully qualified task name (`module.func`). |
| `args` | `tuple` | Positional arguments as originally passed. |
| `kwargs` | `dict` | Keyword arguments as originally passed. |
| `worker_id` | `str` | Hostname of the executing worker (`celery@…`). |
| `partial_result` | `Any` | Checkpoint from a previous incarnation. `None` on first run. |
| `metadata` | `dict` | Arbitrary metadata, extensible. |
| `started_at` | `float` | Unix timestamp when execution began. |

### `await ctx.set_partial(data: Any) -> None` {#ctx-set-partial}

Save a checkpoint. Whatever you pass here becomes `ctx.partial_result` on the
next resurrection, so the resurrected incarnation can resume from where the
previous one left off.

#### How does the checkpoint know where the task got to?

It doesn't, **you tell it**. Relier has no way to introspect "the task was on
line 47 of your loop." That's not the level it operates at. The checkpoint is
just an arbitrary JSON-serialisable blob you choose to persist; what goes
inside is your application's responsibility.

There are two patterns for getting progress out of the task body. Pick the
one that fits your workload.

##### Pattern A: Checkpoint inline (recommended for incremental work)

The task body itself calls `ctx.set_partial` after each unit of work. The
soft-timeout hook is unnecessary because the latest progress is already in
Redis when the worker dies.

```python
from relier.tasks.context import TaskContext

@rl_task(hard_timeout=60)
async def process_items(cursor: str = "start", ctx: TaskContext = None) -> dict:
    start = (ctx.partial_result or {}).get("last_cursor", cursor)

    async for item in fetch_items(start):
        await process(item)
        # Persist progress after each item. The resurrected task picks up here.
        await ctx.set_partial({"last_cursor": item.id})

    return {"status": "done"}
```

Trade-offs

- ✅ Simplest mental model, no hook, no shared state.
- ✅ Fine-grained recovery: you lose at most one item's work.
- ✅ Works without `soft_timeout`/`on_soft_timeout`.
- ⚠️ One Redis write per item. Cheap, but not free; if your loop iterates
  millions of items per second, throttle the writes (only save every N items).

##### Pattern B: Track progress in `ctx.metadata`, save in the soft hook

The task body writes progress into `ctx.metadata` (an in-memory dict on the
context object, no Redis writes). The soft-timeout hook reads
`ctx.metadata` and persists a final checkpoint just before the hard timeout
fires.

```python
from relier.tasks.context import TaskContext

async def save_checkpoint(ctx: TaskContext) -> None:
    # Reads whatever the task body recorded into ctx.metadata.
    last = ctx.metadata.get("last_cursor")
    if last is not None:
        await ctx.set_partial({"last_cursor": last})

@rl_task(soft_timeout=55, hard_timeout=60, on_soft_timeout=save_checkpoint)
async def process_items(cursor: str = "start", ctx: TaskContext = None) -> dict:
    start = (ctx.partial_result or {}).get("last_cursor", cursor)

    async for item in fetch_items(start):
        await process(item)
        # In-memory only, no Redis write per item.
        ctx.metadata["last_cursor"] = item.id

    return {"status": "done"}
```

Trade-offs

- ✅ Zero Redis writes during normal execution only one write at the
  soft-timeout boundary.
- ✅ Best for very hot loops where per-item checkpointing would dominate
  the work.
- ⚠️ Only protects you against soft-timeout-triggered handoff. **A SIGKILL or
  OOM-kill bypasses the soft hook entirely**, the task is resurrected with
  whatever `set_partial` last persisted (often nothing). For real crash
  resilience, combine: write to `ctx.metadata` per item and *also* call
  `set_partial` periodically (every N items, or every M seconds).

##### Pattern C: Hybrid (periodic durable checkpoints + final rescue)

The conservative production default: cheap inline progress with periodic durable saves.

```python
@rl_task(soft_timeout=55, hard_timeout=60, on_soft_timeout=save_checkpoint)
async def process_items(cursor: str = "start", ctx: TaskContext = None) -> dict:
    start = (ctx.partial_result or {}).get("last_cursor", cursor)
    count = 0

    async for item in fetch_items(start):
        await process(item)
        ctx.metadata["last_cursor"] = item.id
        count += 1

        # Durable checkpoint every 100 items.
        if count % 100 == 0:
            await ctx.set_partial({"last_cursor": item.id})

    return {"status": "done", "processed": count}
```

This combines crash resilience (you re-do at most 100 items after a SIGKILL)
with low write overhead (one Redis hit per 100 items, not per item).

#### Size limits: read this before checkpointing anything bigger than a JSON document

Redis is Relier's coordination plane. It runs with `maxmemory-policy
noeviction`, replicates writes to every replica, and appends to the AOF. A
careless checkpoint i.e an embedding vector, an image, a model snapshot
serialised straight into Redis would bloat memory, flood replication, and can
take the entire cluster down.

`CheckpointStore` routes every checkpoint through a size-aware store:

| Checkpoint size | What happens | Configured by |
|---|---|---|
| ≤ `RELIER_CHECKPOINT_MAX_INLINE_BYTES` (default **256 KB**) | Stored inline in `rl:phoenix:{task_id}` hash field `partial_result`. Fast, no external dependency. | always on |
| > 256 KB **with** `RELIER_CHECKPOINT_BACKEND=filesystem` | Gzipped, written to `RELIER_CHECKPOINT_DIR/{task_id}.ckpt`. Only a tiny reference envelope (`{ref, bytes, codec}`) stays in Redis. | `filesystem` backend |
| > 256 KB **with** `RELIER_CHECKPOINT_BACKEND=inline` (default) | **Rejected**, raises `CheckpointTooLargeError`. | default behaviour |

The default is to **reject oversized checkpoints**, deliberately. Silently
bloating Redis is worse than a loud error: the loud error tells you that you
need to either (a) shrink the checkpoint, or (b) configure a blob backend.

```python
# Production: enable filesystem spillover with a shared volume.
RELIER_CHECKPOINT_BACKEND=filesystem
RELIER_CHECKPOINT_DIR=/var/lib/relier/checkpoints
```

**The filesystem backend MUST be shared storage**, every worker and the
resurrector must see the same path, because a resurrected task is replayed on
a different process than the one that wrote its checkpoint. Use a mounted
volume (Docker volume, NFS, EFS), not node-local disk. The bundled
`docker-compose.prod.yml` does this with the `redis_checkpoints` named volume.

`CheckpointTooLargeError` is a subclass of `RuntimeError`. Catch it if you
have a fallback (e.g., write the bulky state to S3 yourself and checkpoint just
the S3 key):

```python
from relier.core.checkpoint import CheckpointTooLargeError

async def save_progress(ctx: TaskContext) -> None:
    try:
        await ctx.set_partial(huge_state)
    except CheckpointTooLargeError:
        s3_key = await upload_to_s3(huge_state)
        await ctx.set_partial({"s3_key": s3_key})
```

#### Lifecycle of a checkpoint

- Reclaimed immediately when `PhoenixRegistry.complete(task_id)` runs (task
  finished, including DLQ quarantine).
- Reclaimed when the DLQ is purged via `rl dlq purge --confirm`.
- Orphaned blobs (whose owning Phoenix hash TTL-expired without cleanup) are
  swept by `CheckpointStore.sweep()` after `24h + 1h` driven by the
  resurrection loop.

---

## `idempotency_lock`

A context manager for **manual** idempotency control. Use this when you need a
custom key or conditional logic that the auto-keyed `idempotent=True` flag
can't express.

```python
from relier.core.idempotency import idempotency_lock

@rl_task()
async def handle_webhook(event_id: str, event_type: str, payload: dict) -> dict:
    # Use event_id as the idempotency key, not a hash of all args.
    async with idempotency_lock(key=f"webhook:{event_id}", ttl=86400) as lock:
        if lock.already_executed:
            return lock.cached_result
        result = await process_event(event_type, payload)
        await lock.record_result(result)
        return result
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `key` | `str` | required | Idempotency key. Must be unique per logical operation. |
| `ttl` | `int` | required | Cache TTL in seconds. |

### Lock object

| Attribute / Method | Type | Description |
|---|---|---|
| `lock.already_executed` | `bool` | `True` if this key has a cached result |
| `lock.cached_result` | `Any` | Deserialised cached result (only valid if `already_executed`) |
| `await lock.record_result(result)` | — | Store the result in the cache |

---

## `DeadLetterQueue`

Direct access to DLQ operations. For most operational tasks, prefer the
`rl dlq` CLI commands, they wrap the same calls.

```python
from relier.core.dlq import DeadLetterQueue
```

### `await DeadLetterQueue.list_tasks(limit=None)`

Returns all quarantined tasks sorted by quarantine time (newest first). Each
entry is a `dict`:

| Field | Description |
|-------|-------------|
| `task_id` | Task UUID |
| `task_name` | Fully qualified task name |
| `queue` | Original target queue |
| `args` | Original positional arguments |
| `kwargs` | Original keyword arguments |
| `partial_result` | Last checkpoint before quarantine (if any) |
| `reason` | Error type that triggered quarantine |
| `resurrections` | Number of resurrection attempts |
| `quarantined_at` | ISO-8601 timestamp |

### `await DeadLetterQueue.inspect(task_id)`

Returns the full quarantine envelope for one task, or `None` if not found.

### `await DeadLetterQueue.release(task_id)` → `bool`

Un-quarantines a task and re-submits it to its original queue. Returns `True`
on success, `False` if not found. Resurrection count is **preserved** so a
task can't bypass `max_resurrections` by being repeatedly released.

### `await DeadLetterQueue.purge()` → `int`

Permanently deletes all DLQ entries. Returns the deleted count. Also releases
any checkpoint blobs the quarantined tasks owned.

---

## `SLOMetrics`

Read SLO data programmatically.

```python
from relier.core.slo import SLOMetrics

# Current burn rate for a window
rate = await SLOMetrics.get_burn_rate(window="1h", target_slo=0.999)

# All windows at once
report = await SLOMetrics.get_report()
# {"1h": 0.42, "6h": 0.38, "3d": 0.21}
```

Valid windows: `"1h"`, `"6h"`, `"3d"`.

### `await SLOMetrics.record_event(status)` → `None`

Manually record a task outcome. Relier calls this automatically only use it
for custom integrations.

- `status="success"`, task completed successfully
- `status="failure"`, task failed or was quarantined

---

## Exceptions

```python
from relier.core.exceptions import (
    RelierError,                # base class
    IdempotencyInFlightError,   # another worker holds the idempotency key
    AdmissionRejectedError,     # cluster at capacity (check exc.retry_after)
    PayloadIntegrityError,      # checksum mismatch on received payload
    SchemaMigrationError,       # migration function raised
    HardTimeoutError,           # task exceeded hard_timeout
)
```

`from relier.core.checkpoint import CheckpointTooLargeError` raised by
`ctx.set_partial(...)` when the checkpoint exceeds the inline limit and no
filesystem backend is configured. Subclass of `RuntimeError`, not `RelierError`.

### `AdmissionRejectedError`

```python
try:
    await my_task.apush(arg)
except AdmissionRejectedError as exc:
    print(exc.retry_after)  # seconds until the admission window resets
```

### `IdempotencyInFlightError`

This is caught and handled internally by `@rl_task` it triggers a Celery
retry with 5-second backoff. You should not need to handle it yourself unless
you're using `idempotency_lock` manually.

---

## Settings

```python
from relier.config import get_settings

settings = get_settings()
print(settings.heartbeat_ttl)     # 10
print(settings.max_resurrections) # 5
```

All settings are read-only after process startup (the `Settings` model is
frozen). To change a value, update `.env` and restart workers:

```bash
rl config set RELIER_HEARTBEAT_TTL 15
# then restart workers
```

See [Configuration](configuration.md) for the full settings reference.
