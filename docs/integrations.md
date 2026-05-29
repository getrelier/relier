# Integration Recipes

Plug Relier into the Python web framework you're already using. The reliability
guarantees are identical across all of them, the only thing that changes is
the dispatch method (`apush` for async, `push` for sync) and where you put the
exception handler.

> **TL;DR:** import your task, call `.apush(...)` in async handlers or
> `.push(...)` in sync ones, catch `AdmissionRejectedError` and convert it to
> HTTP 429. That's the entire integration.

---

## FastAPI (async, primary integration)

Relier is designed for FastAPI. The producer-side code is naturally async, the
trace context is propagated end-to-end, and admission rejections map cleanly
onto FastAPI's exception handlers.

### Project layout

```
my_app/
├── main.py              # FastAPI app + routes
├── tasks.py             # @rl_task definitions
└── .env                 # RELIER_REDIS_URL=...
```

### `tasks.py`

```python
from relier import rl_task

@rl_task(
    queue="default",
    idempotent=True,
    idempotency_ttl=3600,
    soft_timeout=25,
    hard_timeout=30,
)
async def send_invoice(invoice_id: str) -> dict:
    # Anything you'd do in a normal async function works here.
    result = await charge_card(invoice_id)
    await email_invoice(invoice_id)
    return {"charged": True, "invoice_id": invoice_id}
```

### `main.py`

```python
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from relier.core.exceptions import AdmissionRejectedError

from tasks import send_invoice

app = FastAPI()

@app.exception_handler(AdmissionRejectedError)
async def admission_handler(_: Request, exc: AdmissionRejectedError) -> JSONResponse:
    # Map cluster-at-capacity into a standard 429 with Retry-After.
    return JSONResponse(
        status_code=429,
        headers={"Retry-After": str(exc.retry_after)},
        content={"detail": "Service at capacity, retry later."},
    )

@app.post("/invoices/{invoice_id}/send")
async def dispatch_invoice(invoice_id: str) -> dict:
    await send_invoice.apush(invoice_id)
    return {"status": "queued"}
```

### Running it

In three terminals (or via `make dev` for a Docker stack):

```bash
# 1. The web app
uvicorn main:app --reload

# 2. The Celery worker
celery -A relier.tasks.app worker -l info -Q high_priority,default,low_priority,re-queue --include=tasks

# 3. The Phoenix resurrector
rl run-resurrector
```

### Trace continuity

When you `await task.apush(...)` from inside a request, Relier injects the
current OpenTelemetry trace context into the task envelope. The worker extracts
it and the task span becomes a child of your HTTP span. This works
automatically when `RELIER_OTEL_ENABLED=true`.

---

## Flask (sync)

Flask doesn't own an event loop, so use `push`, it bridges into asyncio
internally without making you `await` anything.

### `tasks.py`

```python
from relier import rl_task

@rl_task(idempotent=True)
async def send_invoice(invoice_id: str) -> dict:
    # The task itself is still async, Relier runs it on the worker's loop.
    result = await charge_card(invoice_id)
    return result
```

### `app.py`

```python
from flask import Flask, jsonify
from relier.core.exceptions import AdmissionRejectedError

from tasks import send_invoice

app = Flask(__name__)

@app.errorhandler(AdmissionRejectedError)
def admission_handler(exc: AdmissionRejectedError) -> tuple:
    response = jsonify(detail="Service at capacity, retry later.")
    response.status_code = 429
    response.headers["Retry-After"] = str(exc.retry_after)
    return response

@app.post("/invoices/<invoice_id>/send")
def dispatch_invoice(invoice_id: str) -> tuple:
    send_invoice.push(invoice_id)        # sync dispatch, no await
    return jsonify(status="queued"), 202
```

### Why `push` and not `apush`

`push` is a thin sync wrapper around `apush`. Inside a Flask handler (a sync
function), there is no running event loop, so `push` calls
`asyncio.run(apush(...))` for you. The dispatch finishes in a couple of
milliseconds.

If you're *inside* a Celery worker (whose persistent event loop is running),
`push` instead schedules the dispatch onto that loop via
`run_coroutine_threadsafe`. You don't have to think about which case applies,
`push` figures it out.

---

## Django (sync views)

Same pattern as Flask. Use `push` in sync views; use `apush` in `async def`
views (`asgi` Django).

### Sync view

```python
# views.py
from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_POST
from relier.core.exceptions import AdmissionRejectedError

from .tasks import send_invoice

@require_POST
def dispatch_invoice(request: HttpRequest, invoice_id: str) -> JsonResponse:
    try:
        send_invoice.push(invoice_id)
    except AdmissionRejectedError as exc:
        response = JsonResponse(
            {"detail": "Service at capacity, retry later."},
            status=429,
        )
        response["Retry-After"] = str(exc.retry_after)
        return response
    return JsonResponse({"status": "queued"}, status=202)
```

### Async view (Django 4.1+)

```python
async def dispatch_invoice(request: HttpRequest, invoice_id: str) -> JsonResponse:
    try:
        await send_invoice.apush(invoice_id)
    except AdmissionRejectedError as exc:
        response = JsonResponse(
            {"detail": "Service at capacity, retry later."},
            status=429,
        )
        response["Retry-After"] = str(exc.retry_after)
        return response
    return JsonResponse({"status": "queued"}, status=202)
```

### Django management commands

Management commands are sync entry points. Use `push`:

```python
# management/commands/replay_invoices.py
from django.core.management.base import BaseCommand
from myapp.tasks import send_invoice

class Command(BaseCommand):
    def handle(self, *args: Any, **opts: Any) -> None:
        for invoice in queryset:
            send_invoice.push(invoice.id)
        self.stdout.write(f"Queued {queryset.count()} invoices.")
```

---

## Scripts, cron jobs, CLI tools

Anywhere you can run a Python interpreter, you can dispatch a Relier task. Use
`push` for sync, `asyncio.run(apush(...))` for async-style scripts.

```python
# replay.py, sync script
from tasks import send_invoice

for invoice_id in load_invoice_ids():
    send_invoice.push(invoice_id)
```

```python
# replay.py, async script
import asyncio
from tasks import send_invoice

async def main() -> None:
    for invoice_id in load_invoice_ids():
        await send_invoice.apush(invoice_id)

asyncio.run(main())
```

---

## Dispatching from inside a task

Sometimes a task needs to enqueue follow-up work. **Which method you use depends
on whether the task body is `async` or `sync`** this is the one place where
`push` and `apush` are *not* interchangeable.

### From an `async` task body use `await apush`

An `async` task body runs **on the worker's persistent event loop**. Calling the
synchronous `push` from there would block that loop waiting on a coroutine that
needs the same loop to make progress, a deadlock. Relier detects this and
raises `RuntimeError` rather than hanging. Always `await apush`:

```python
@rl_task()
async def import_user(user_id: str) -> None:
    profile = await fetch_profile(user_id)
    await store(profile)
    # Fan out follow-up work, fire-and-forget.
    await send_welcome_email.apush(user_id)
    await regenerate_avatar.apush(user_id)
```

### From a `sync` task body use `push`

A `sync` task body runs in a worker thread (via `asyncio.to_thread`), *not* on
the event loop, so there is no loop to block. Use `push`:

```python
@rl_task()
def import_user(user_id: str) -> None:
    profile = fetch_profile(user_id)        # sync I/O
    store(profile)
    send_welcome_email.push(user_id)        # sync dispatch, safe here
```

!!! warning "`push` from async code raises `RuntimeError`"
    Calling `task.push(...)` from inside any running event loop — an async task
    body, a FastAPI route, or an async Django view raises:

    ```
    RuntimeError: <task>.push() was called from inside a running event loop …
                  Use `await <task>.apush(...)` instead.
    ```

    The rule is simple: **async context → `await apush`; sync context → `push`.**

---

## What about `.delay()` and `.apply_async()`?

`@rl_task` is built on Celery's `shared_task`, so these methods technically
work. **They bypass Relier's admission control and skip the signed envelope.**
Tasks dispatched via `.delay()` enter the queue as plain Celery payloads and
the worker treats them as legacy unsigned messages.

The only intentional caller of `.delay()` in this codebase is `rl bench`,
which uses it as the baseline for comparing dispatch overhead.

**In application code, always use `apush` or `push`.**

---

## Quickly checking it works

After wiring an integration, three commands tell you the reliability stack is
live:

```bash
# 1. Connectivity
rl doctor

# 2. Config sanity
rl config validate

# 3. End-to-end: dispatch from your app, then watch the task run
rl tasks inflight --follow
```

If the task appears in `rl tasks inflight` and disappears when it completes,
the integration works. If it goes into `rl dlq list` instead, see
[Troubleshooting](troubleshooting.md).
