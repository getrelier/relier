# Rolling Deploys & Schema Migrations

A rolling deploy means new code goes out gradually, half your workers run the
new version while the other half still run the old one, and Redis holds tasks
enqueued by *either*. If the new code changes a task's argument shape, the
deploy can produce silently failing tasks unless you migrate the schema.

Relier ships a schema-versioning mechanism specifically for this case.

---

## The problem, concretely

Suppose v1 of your code has:

```python
@rl_task()
async def send_invoice(invoice_id: str): ...
```

…and v2 changes it to:

```python
@rl_task()
async def send_invoice(invoice_id: str, region: str = "global"): ...
```

A rolling deploy means there's a window where:

- v1 producers enqueue `send_invoice("INV-1")`, no `region`.
- v2 workers pick the payload up and call `send_invoice(invoice_id="INV-1",
  region=???)`.

If the signature change is backward-compatible (here it is `region` has a
default), nothing breaks. If you renamed an argument, changed a required
field, or changed types workers raise `TypeError` and the task gets DLQ'd.

---

## How Relier's envelope solves this

Every payload `apush` / `push` dispatches is wrapped in a versioned envelope:

```json
{
  "schema_version": 1,
  "task_id": "task_abc123",
  "payload": { "args": ["INV-1"], "kwargs": {} },
  "checksum": "sha256:a3f9...",
  "enqueued_at": "2026-05-15T02:00:00Z"
}
```

The worker unwraps the envelope, verifies the checksum, then applies any
**registered migrations** to bring the payload up to the current schema
version before calling your function.

---

## Your route never changes only the task does

This is the part that trips people up, so let's be concrete. The migration is a
**worker-side** concern. The code that *dispatches* the task your FastAPI
route, Flask view, or script, stays exactly the same before, during, and after
the deploy:

```python
# main.py — UNCHANGED across the whole migration
@app.post("/invoices/{invoice_id}/send")
async def dispatch_invoice(invoice_id: str) -> dict:
    receipt = await send_invoice.apush(invoice_id)   # same call, v1 and v2
    return {"task_id": receipt.id, "status": "queued"}
```

The route always calls `send_invoice.apush(invoice_id)`. It doesn't know or care
which schema version is current, Relier stamps the envelope with
`CURRENT_VERSION` for it. All the version-awareness lives in two places:

1. `SchemaRegistry.CURRENT_VERSION`, the number stamped onto new envelopes.
2. The migration function, which upgrades *old* envelopes when a worker picks
   them up.

So the mental model is: **producers stay dumb, workers get smart.** A v1 producer
keeps enqueueing v1 envelopes; a v2 worker quietly upgrades them on arrival.

Here's the whole timeline in plain English, for adding a required `region` arg:

| When | A v1 producer enqueues | A v2 worker receives | What happens |
|---|---|---|---|
| Before deploy | `send_invoice("INV-1")` → v1 envelope | (still v1 worker) | Runs as `send_invoice("INV-1")` |
| Mid-deploy | `send_invoice("INV-1")` → v1 envelope | v2 worker | Migration adds `region="global"`, then runs `send_invoice("INV-1", region="global")` |
| After deploy | `send_invoice("INV-1")` → v2 envelope | v2 worker | No migration needed; runs directly |

No failed tasks, no special-casing in your route, no deploy-window downtime —
provided you registered the migration *before* the new workers start pulling
old payloads.

---

## Step-by-step: a real schema change

You want to make `region` required (and ban the implicit default). Here's the
safe sequence:

### Step 1: bump the schema version

In your code that *defines* the task, increment `SchemaRegistry.CURRENT_VERSION`.
This is a global per-process value. Pick the next integer:

```python
from relier.core.schema import SchemaRegistry

# In your application bootstrap:
SchemaRegistry.CURRENT_VERSION = 2
```

(Or set it once, in a module imported by everything that dispatches.)

### Step 2: register the migration

```python
from relier.core.schema import SchemaRegistry

@SchemaRegistry.register_migration("my_app.tasks.send_invoice", from_version=1)
def migrate_v1_to_v2(args, kwargs):
    # v1 callers omitted `region`; default it to "global".
    if "region" not in kwargs:
        kwargs["region"] = "global"
    return args, kwargs
```

The first argument to `register_migration` is the **full task name**
typically `module.function`. You can see the registered name with
`rl tasks inspect`.

Migrations are applied sequentially: v1 → v2 → v3 → … until the payload
reaches `CURRENT_VERSION`. So if you skip ahead from v1 to v3 in one deploy,
you need to register *both* migrations (v1 → v2 and v2 → v3).

### Step 3: change the function signature

```python
@rl_task()
async def send_invoice(invoice_id: str, region: str) -> dict:   # no default
    ...
```

By the time a v2 worker calls this, the migration has already added the
`region` kwarg, so the signature is satisfied even for v1-enqueued payloads.

### Step 4: deploy

Roll out v2 workers and producers. While the deploy is in progress:

- v1 producers enqueue v1 envelopes. v2 workers migrate them to v2 on pickup.
- v2 producers enqueue v2 envelopes. v1 workers see them as `schema_version=2`
  payloads, there's no v1 migration that knows how to handle these. **The
  task is quarantined to the DLQ with `reason=SchemaMigrationError`.**

That's the unavoidable cost of changing schema mid-deploy. The mitigations:

- **Drain the queue between deploys.** Bring producers down, wait for the
  queue to clear, then deploy producers + workers together. The Phoenix
  recovery queue must be empty too.
- **Make v2 *strictly compatible* with v1 first**, deploy, then make a v3 that
  requires the new shape. Same idea as additive DB migrations.

### Step 5: remove the migration once v1 payloads are gone

Once all v1 producers are gone and the queue has cycled fully, the v1 → v2
migration is dead code. Delete it. Keep `SchemaRegistry.CURRENT_VERSION = 2`
in place so future migrations have a baseline.

---

## What workers do when they see an unknown version

If a worker picks up an envelope with `schema_version=2` but doesn't have a
migration registered (because the worker is still running v1), the task is
**not silently retried with garbage**:

- Envelope validation runs first. If the structure is malformed, the task is
  DLQ'd with `PayloadIntegrityError`.
- Migration runs next. If `version < CURRENT_VERSION` but no migration exists
  for `version → version+1`, the worker logs a warning and passes the payload
  through unchanged (best-effort). If the migration function itself raises,
  the task is DLQ'd with `SchemaMigrationError`.

The decorator catches both of these and quarantines the task before user code
runs. You can always release them later with `rl dlq release` after the cluster
is back in a consistent state.

---

## Checksum integrity

Every envelope carries a SHA-256 checksum of the payload (with stable key
ordering). At pickup, the worker recomputes the checksum and compares. A
mismatch means the payload was tampered with or corrupted, there is no safe
retry, so the task goes straight to the DLQ with
`reason=PayloadIntegrityError`.

This catches:

- Broker-side corruption (rare but possible with hardware faults)
- Bugs in custom serialisation
- Deliberate tampering at rest

If you ever see a sudden spike of `PayloadIntegrityError` in the DLQ, look at
your broker / Redis storage layer first.

---

## Safe-deploy checklist

Before a deploy that changes any task signature:

- [ ] Migration registered for every old `schema_version` you might still see
- [ ] `SchemaRegistry.CURRENT_VERSION` bumped in producers AND workers
- [ ] The new task code is backwards-compatible with the migrated args (i.e.,
      defaults are sane for the values the migration injects)
- [ ] `rl dlq list` is empty before the rollout (so existing DLQ entries don't
      get confused with new ones)
- [ ] Recovery queue (`re-queue`) is drained, old resurrection payloads use the
      *old* envelope version
- [ ] You can describe the rollout order: producers-then-workers, or
      workers-then-producers (workers-first is safer they can handle either
      version)

---

## Why not just version the function name?

A simpler approach is to write a new task with a different name:

```python
# v1: keep around
@rl_task()
async def send_invoice(invoice_id: str): ...

# v2: new name
@rl_task()
async def send_invoice_v2(invoice_id: str, region: str): ...
```

This works for additive cases and avoids the migration plumbing entirely. The
trade-off is that **callers** must know about both names during the transition.
For internal-only tasks this is often fine; for tasks with many callers, the
migration approach is cleaner.

Pick based on blast radius. Schema migrations are the right tool when the
caller list is large; renaming is the right tool when the call site is one or
two places.
