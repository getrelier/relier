# Patterns Cookbook

Common shapes of task code, with explanations of why each one works the way it
does. Use these as templates; adapt to your problem.

---

## Pattern 1: Safe-to-retry external API call

The most common pattern. The task hits an external service that *should* be
idempotent but you don't want to rely on luck. Relier guarantees exactly-once
execution on your side.

```python
@rl_task(
    queue="default",
    idempotent=True,
    idempotency_ttl=86400,     # cache result for 24 h
    soft_timeout=8,            # warn at 8 s
    hard_timeout=10,           # cancel at 10 s
)
async def charge_customer(customer_id: str, amount_cents: int) -> dict:
    return await stripe.charge(customer_id, amount_cents)
```

**What you get:**

- Same `(customer_id, amount_cents)` dispatched twice within 24 h → runs once.
  Second dispatch returns the cached result without touching Stripe.
- Worker dies between Stripe ACK and Celery ACK → Phoenix re-queues.
  Idempotency check sees the cached result and returns it.
- Stripe hangs → hard timeout at 10 s, DLQ'd with `reason=TimeoutError`.

**When NOT to use:**

- When the arguments aren't a stable identifier (e.g., contain timestamps or
  free-form payloads). Use [Pattern 2: manual idempotency key](#pattern-2)
  instead.

---

## Pattern 2: Manual idempotency key {#pattern-2}

Auto-keyed `idempotent=True` hashes all arguments. When that's not what you
want e.g., a webhook payload contains a timestamp that changes between
retries use `idempotency_lock` with a key you control.

```python
from relier.core.idempotency import idempotency_lock

@rl_task()
async def process_webhook(event_id: str, payload: dict) -> dict:
    # event_id is the stable identifier; payload may differ across deliveries.
    async with idempotency_lock(key=f"webhook:{event_id}", ttl=86400) as lock:
        if lock.already_executed:
            return lock.cached_result          # short-circuit on duplicate
        result = await handle_event(payload)
        lock.set_result(result)                # sync — committed automatically on exit
        return result
```

**Why the prefix (`webhook:`)?** Keeps idempotency keys for different feature
areas from colliding. There's no enforcement just convention.

---

## Pattern 3: Long task with resumable checkpoints {#pattern-3}

A task that processes a long list and might be killed mid-flight. The
question every reader asks: **how does the checkpoint know where the loop
got to?** Relier doesn't introspect your code. The task body must record
progress somewhere accessible, and you have three honest choices.

### 3a: Checkpoint inline (simplest, most crash-resilient)

The task body calls `ctx.set_partial` after each item. No soft hook needed:
the latest progress is in Redis the moment the worker dies, so even a SIGKILL
loses at most one item.

```python
from relier.tasks.context import TaskContext

@rl_task(hard_timeout=60)
async def reprocess_items(start_cursor: str = "0", ctx: TaskContext = None) -> dict:
    cursor = (ctx.partial_result or {}).get("last_cursor", start_cursor)
    count = 0
    async for item in fetch(cursor):
        await handle(item)
        # Persist progress after each item.
        await ctx.set_partial({"last_cursor": item.id})
        count += 1
    return {"processed": count}
```

One Redis write per item is fine for thousands-per-minute workloads. Move to
pattern 3b or 3c when you're processing millions per minute.

### 3b: Track in `ctx.metadata`, save only at soft timeout

The task body writes to `ctx.metadata` (an in-memory dict on the context,
no Redis traffic). A soft-timeout hook reads `ctx.metadata` and persists a
single checkpoint just before the hard timeout fires.

```python
from relier.tasks.context import TaskContext

async def save_progress(ctx: TaskContext) -> None:
    # ctx.metadata holds whatever the task body wrote into it.
    last = ctx.metadata.get("cursor")
    if last is not None:
        await ctx.set_partial({"last_cursor": last})

@rl_task(soft_timeout=55, hard_timeout=60, on_soft_timeout=save_progress)
async def reprocess_items(start_cursor: str = "0", ctx: TaskContext = None) -> dict:
    cursor = (ctx.partial_result or {}).get("last_cursor", start_cursor)
    count = 0
    async for item in fetch(cursor):
        await handle(item)
        ctx.metadata["cursor"] = item.id   # in-memory only
        count += 1
    return {"processed": count}
```

⚠️ **Caveat:** the soft-timeout hook **only fires on timeout**. A SIGKILL or
OOM-kill or `kill -9` bypasses it entirely. If the worker dies in any way
other than running out the clock, the resurrected task starts from
`partial_result` as it was when the previous incarnation began, usually
nothing. Pure pattern 3b is fine for "this task will probably finish but if
it doesn't I want one safety save," but it's a *weak* crash-resilience
guarantee.

### 3c: Hybrid, periodic durable + soft hook (production default)

Combines pattern 3a's crash resilience with pattern 3b's low write overhead.
Recommended for any long-running task that goes to production.

```python
@rl_task(soft_timeout=55, hard_timeout=60, on_soft_timeout=save_progress)
async def reprocess_items(start_cursor: str = "0", ctx: TaskContext = None) -> dict:
    cursor = (ctx.partial_result or {}).get("last_cursor", start_cursor)
    count = 0
    async for item in fetch(cursor):
        await handle(item)
        ctx.metadata["cursor"] = item.id
        count += 1

        # Durable checkpoint every 100 items.
        if count % 100 == 0:
            await ctx.set_partial({"last_cursor": item.id})

    return {"processed": count}
```

The most you re-do after a SIGKILL is the items since the last 100-item
durable save. The soft hook (using the same `save_progress` from 3b) saves a
final checkpoint at the timeout boundary so an orderly handoff loses nothing.

### Size budget applies to all three patterns

Checkpoints over 256 KB are **rejected by default** with
`CheckpointTooLargeError`. For larger resumable state (embeddings, partial
files), enable the filesystem backend:

```bash
RELIER_CHECKPOINT_BACKEND=filesystem
RELIER_CHECKPOINT_DIR=/var/lib/relier/checkpoints   # mounted into every worker
```

See [API → `ctx.set_partial`](api-reference.md#ctx-set-partial) for the
full rules.

---

## Pattern 4: Receiving the context as a kwarg

If you'd rather receive the `TaskContext` explicitly instead of reading it
from the contextvar, declare a `ctx` kwarg. Relier detects it via
`inspect.signature` and injects automatically:

```python
@rl_task()
async def process_with_ctx(item_id: str, ctx: TaskContext) -> None:
    logger.info("running", extra={"task_id": ctx.task_id, "worker": ctx.worker_id})
```

Caller code stays the same:

```python
await process_with_ctx.apush("item-42")     # no need to pass ctx
```

This is purely a style choice, the contextvar proxy and the kwarg approach
read the same object. The kwarg form makes the dependency explicit, which
helps in tests:

```python
# Test the function as plain Python, bypassing the decorator:
await process_with_ctx.__wrapped__(
    None, "item-42",
    ctx=TaskContext(task_id="t1", task_name="test", args=(), kwargs={}),
)
```

---

## Pattern 5: Two-tier timeout with cleanup

The cleanup hook releases external resources before the hard timeout cancels
the task.

```python
async def release_resources(ctx: TaskContext) -> None:
    # ctx.args / ctx.kwargs are the originals
    item_id = ctx.args[0]
    await release_external_lock(item_id)
    await ctx.set_partial({"items_done": ctx.metadata.get("done", 0)})

@rl_task(
    soft_timeout=25,        # cleanup window: 5 seconds
    hard_timeout=30,
    on_soft_timeout=release_resources,
)
async def process_with_external_lock(item_id: str) -> None:
    async with acquire_external_lock(item_id):
        await long_running_work(item_id)
```

**The cleanup hook runs on the same event loop as the task.** It must be
`async def` and must complete within `hard_timeout - soft_timeout` seconds, or
the hard timeout will fire before it finishes.

**Timeouts only apply to `async def` tasks.** Decorating a plain `def` with
timeout parameters raises `ValueError` at decoration time. Use `asyncio.to_thread`
to wrap blocking calls inside an async task.

---

## Pattern 6: Dedicate a worker pool to a queue

When latency-critical work and bulk work compete for the same workers, fast
work suffers. Split them.

```python
@rl_task(queue="high_priority")
async def confirm_payment(...): ...

@rl_task(queue="low_priority")
async def regenerate_thumbnails(...): ...
```

Then run two worker pools:

```bash
# Pool A — only fast work
celery -A relier.tasks.app worker --concurrency=4 -Q high_priority --hostname=worker-fast@%h

# Pool B — bulk + default work
celery -A relier.tasks.app worker --concurrency=16 -Q default,low_priority --hostname=worker-bulk@%h
```

A third pool for the internal `re-queue` queue is recommended in production,
this is what isolates Phoenix resurrections from user traffic. The bundled
`docker-compose.yml` ships exactly this layout (`worker-high`,
`worker-default`, `worker-recovery`).

---

## Pattern 7: Fan-out from one task

A task that triggers many smaller tasks. Use `push` inside the parent no
need to `await` each one if you only need fire-and-forget.

```python
@rl_task()
async def import_all_users() -> None:
    user_ids = await fetch_all_user_ids()
    for user_id in user_ids:
        import_user.push(user_id)        # sync convenience inside worker

@rl_task(idempotent=True)
async def import_user(user_id: str) -> None:
    ...
```

**Beware admission control.** `apush` / `push` runs admission against the
cluster's `celery-dispatch` window. A loop that dispatches 100 k tasks in one
shot will hit the limit. If you need a bigger window, raise
`RELIER_ADMISSION_LIMIT`, or insert a small `await asyncio.sleep(0)` to yield.

---

## Pattern 8: Catching admission rejection at the edge {#pattern-8}

Always handle `AdmissionRejectedError` at your API boundary. The client
deserves to know it should retry, and when.

```python
from relier.core.exceptions import AdmissionRejectedError

@app.post("/tasks/submit")
async def submit(req: TaskRequest):
    try:
        await process.apush(req.data)
    except AdmissionRejectedError as exc:
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": str(exc.retry_after)},
            detail="Service at capacity.",
        )
    return {"status": "queued"}
```

`exc.retry_after` is in seconds and equals the TTL remaining on the admission
window. Clients that respect `Retry-After` will recover gracefully without
hammering you.

---

## Pattern 9: Reading `TaskContext` from inside the task body

Two equivalent ways:

```python
# A) Receive it as a kwarg (preferred for testability)
@rl_task()
async def with_ctx(item_id: str, ctx: TaskContext) -> None:
    print(ctx.task_id)

# B) Read it from the contextvar proxy
from relier.tasks.context import task_context
@rl_task()
async def with_proxy(item_id: str) -> None:
    print(task_context.task_id)
```

The proxy raises `RuntimeError` if accessed outside a running task, which
makes test failures explicit.

---

## Pattern 10: Tests for `@rl_task` functions

The decorator returns a Celery `Task` subclass, but the underlying function is
accessible for unit tests:

```python
# Direct call, bypasses the entire reliability stack. Fast, deterministic.
result = await send_invoice.__wrapped__(None, "INV-1")

# Or, if you want to exercise admission + envelope + idempotency,
# run an integration test against a real (or test-container) Redis.
```

For integration tests, use `testcontainers[redis]` to spin up a real Redis
inside the test suite:

```python
import pytest
from testcontainers.redis import RedisContainer

@pytest.fixture(scope="session")
def redis_url():
    with RedisContainer("redis:7") as r:
        yield f"redis://{r.get_host()}:{r.get_exposed_port(6379)}/0"
```

Relier's own test suite uses this exact setup. See `tests/integration/`.

---

## Anti-patterns

These look like reasonable patterns but are subtly broken.

### Don't call `.delay()` or `.apply_async()` on a `@rl_task`

```python
await send_invoice.apush(...)    # ✓ Relier
send_invoice.push(...)           # ✓ Relier
send_invoice.delay(...)          # ✗ bypasses admission + envelope
send_invoice.apply_async(...)    # ✗ bypasses admission + envelope
```

These exist because `@rl_task` is built on Celery's `shared_task`, but in
application code they're the wrong choice. The worker will accept them as
legacy unsigned messages, which means no checksum verification, no schema
migration, and no admission backpressure.

### Don't decorate a sync function with `soft_timeout` / `hard_timeout`

```python
@rl_task(hard_timeout=10)
def my_task(x: int):    # ✗ raises ValueError at decoration time
    ...
```

The timeout machinery uses asyncio cancellation. Refactor the task to
`async def` and use `asyncio.to_thread` for blocking calls:

```python
@rl_task(hard_timeout=10)
async def my_task(x: int):
    result = await asyncio.to_thread(blocking_call, x)
    return result
```

### Don't publish into `queue="re-queue"`

That queue is reserved for Phoenix-injected resurrections. Decorating with
`queue="re-queue"` raises `ValueError`. Pick one of `high_priority`,
`default`, or `low_priority`.

### Don't set `idempotency_ttl` shorter than `hard_timeout`

```python
@rl_task(idempotent=True, idempotency_ttl=5, hard_timeout=30)    # ✗
async def my_task(...): ...
```

If the in-flight idempotency sentinel expires before the task hard-times-out,
a second worker can claim the key and run a duplicate. Relier checks this at
decoration time and raises `ValueError`.

### Don't oversize checkpoints

Calling `await ctx.set_partial(huge_blob)` with the default `inline` backend
raises `CheckpointTooLargeError`. Either shrink the checkpoint, or enable the
filesystem backend with a shared volume. Never silence the error, the
checkpoint becoming a black hole in production is worse than the loud
exception.
