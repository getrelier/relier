# Quickstart

Get Relier running in under 5 minutes.

---

## 1. Install Relier

```bash
pip install relier
```

## 2. Start Redis

Relier needs Redis with persistence enabled. The quickest way locally is Docker:

```bash
docker run -d --name relier-redis \
  -p 6379:6379 \
  redis:7-alpine \
  redis-server --appendonly yes --appendfsync everysec
```

!!! note "Why persistence?"
    The `--appendonly yes` flag enables Redis AOF persistence. Without it, a Redis restart drops every heartbeat and payload Relier has stored, breaking the zero-job-loss guarantee. See [Deployment](deployment.md) for production Redis setup.

## 3. Configure Relier

Create a `.env` file in your project root:

```bash
RELIER_REDIS_URL=redis://localhost:6379/0
```

That's the only required setting. Everything else has sensible defaults.

## 4. Define your first reliable task

```python
# tasks.py
from relier.tasks.decorator import rl_task

@rl_task(
    queue="default",
    idempotent=True,           # same input → same output, never runs twice
    soft_timeout=25,           # cleanup hook fires at 25s
    hard_timeout=30,           # task killed unconditionally at 30s
)
async def send_invoice(invoice_id: str) -> dict:
    """Send an invoice email. Safe to retry, will never double-charge."""
    result = await charge_stripe(invoice_id)
    await send_email(invoice_id)
    return {"charged": True, "invoice_id": invoice_id}
```

!!! tip "New to async?"
    Relier tasks are `async def` functions. If your existing Celery tasks are regular `def` functions, Relier supports those too just drop the `async` keyword. The async bridge is handled for you either way.

## 5. Dispatch tasks

Relier has two dispatch methods on every `@rl_task`. Pick the one that
matches your call site.

```python
# FastAPI / Starlette / async Django use apush
from fastapi import FastAPI
from tasks import send_invoice

app = FastAPI()

@app.post("/invoices/{invoice_id}/send")
async def dispatch_invoice(invoice_id: str):
    await send_invoice.apush(invoice_id)   # async dispatch
    return {"status": "queued"}
```

```python
# Flask / classic Django / scripts / management commands use push
from flask import Flask
from tasks import send_invoice

app = Flask(__name__)

@app.post("/invoices/<invoice_id>/send")
def dispatch_invoice(invoice_id):
    send_invoice.push(invoice_id)           # sync dispatch
    return {"status": "queued"}, 202
```

Both run the same reliability stack, admission control, schema envelope, OTel
context and both are fire-and-forget (they return as soon as the broker has
the task).

!!! warning "Don't call `.delay()` or `.apply_async()` on a `@rl_task`"
    Those are Celery's native dispatch methods. They bypass Relier's admission
    control and skip the signed envelope, so the worker accepts the payload as
    a legacy unsigned message. **Always use `apush` (async) or `push` (sync).**
    See [API reference → Dispatch methods](api-reference.md#dispatch-methods-apush-push-delay).

## 6. Start the Relier stack

Pick whichever way fits your environment. All three are documented in
[Deployment](deployment.md).

```bash
# A) Bare metal: two terminals, no Docker needed
make worker         # terminal 1: Celery worker
make resurrector    # terminal 2: Phoenix resurrector

# B) Docker: single-node Redis + workers + resurrector + OTel/Grafana
make dev

# C) Production HA stack: Sentinel + replicas + backup sidecar
export REDIS_PASSWORD=...
export SENTINEL_PASSWORD=...
make prod
```

The bare-metal targets are just shortcuts. Underneath they run:

```bash
celery -A relier.tasks.app worker -l info -Q high_priority,default,low_priority,re-queue
rl run-resurrector
```

## 7. Verify everything is working

```bash
# Check that Redis and Docker are healthy
rl doctor

# See what's running right now
rl tasks inflight

# Check your SLO burn rate
rl slo status
```

You should see output like:

<div class="rl-terminal">
<pre>
<span class="rl-p">$</span> rl tasks inflight

  Worker           Status       In-Flight  ✓ Completed  ✗ Failed  Success Rate
  <span class="rl-ok">rl-worker-1</span>      <span class="rl-ok">● BUSY</span>       <span class="rl-mag">1</span>          <span class="rl-ok">42</span>           <span class="rl-dim">0</span>         <span class="rl-ok">100.0%</span>
    <span class="rl-dim">└─</span> <em>send_invoice</em>   <span class="rl-dim">4f8a1b…</span>   <span class="rl-dim">12.4s</span>
  <span class="rl-ok">rl-worker-2</span>      <span class="rl-dim">○ IDLE</span>       <span class="rl-dim">0</span>          <span class="rl-ok">38</span>           <span class="rl-dim">0</span>         <span class="rl-ok">100.0%</span>

 ┌ Cluster Health ────────────────────────────────────────────────────────────────────┐
 │ <span class="rl-ok">● 1 Active</span>  <span class="rl-ok">✔ 80 Session (24h)</span>  <span class="rl-info">✔ 80 Lifetime</span>  <span class="rl-warn">✗ 0 Failed</span>  <span class="rl-ok">♻ 0 Resurrected</span>  <span class="rl-dim">☢ 0 Quarantined  Depth: 0  p95: N/A</span> │
 └────────────────────────────────────────────────────────────────────────────────────┘
</pre>
</div>

---

## What just happened?

When you called `await send_invoice.apush(invoice_id)`:

1. **Admission check**: Relier verified the cluster isn't overloaded.
2. **Schema wrapping**: the payload was signed with a checksum and versioned.
3. **Dispatch**: sent to the Redis broker with the original `invoice_id`.

When a Celery worker picked it up:

1. **Checksum verified**: payload integrity confirmed before execution.
2. **Idempotency claimed**: only one worker can run this invoice_id at a time.
3. **Heartbeat registered**: Phoenix starts watching this task.
4. **Your function ran**: `send_invoice("INV-123")` executed.
5. **Result cached**: if this exact invoice_id is retried, Relier returns the cached result without re-running.
6. **Heartbeat cleared**: Phoenix knows the task completed cleanly.

---

## What happens when a worker dies?

Let's prove it works. In a separate terminal, try the chaos test:

```bash
# Seed a long-running task, kill the worker, watch Phoenix resurrect it
rl chaos worker-kill --seed --watch --watch-duration 60
```

You'll see output like:

```
SEED  Dispatched 30s long-running task. marker=chaos-kill-seed-a3f9c1
CHAOS Worker terminated.
WATCH Streaming resurrection events for 60s...
  -> task_abc123: RESURRECTED (awaiting pickup)
  -> task_abc123: ALIVE (revived by replacement worker)
WATCH Done. 1 task(s) observed in monitor.
```

That's Phoenix in action. The task survived a SIGKILL with zero intervention from you.

---

## Next steps

- **Never used Celery before?** [Celery Primer](celery-primer.md): the basics in
  five minutes.
- **[Core Concepts](concepts.md)**: understand *why* Relier works the way it does.
- **[Integration Recipes](integrations.md)**: FastAPI, Flask, Django, scripts.
- **[Patterns Cookbook](patterns.md)**: copy-paste shapes for common cases
  (idempotency keys, dedicated workers, resumable checkpoints).
- **[Troubleshooting & FAQ](troubleshooting.md)**: first place to look when
  something breaks.
- **[API Reference](api-reference.md)**: all `@rl_task` parameters explained.
- **[Configuration](configuration.md)**: tune timeouts, pool sizes, admission
  control, and more.
