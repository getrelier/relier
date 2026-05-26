# A Quick Celery Primer (for people who haven't used it)

If you've used Celery before, skip this page it covers basics. If you
haven't, this is the shortest possible explanation that will let you read the
rest of the Relier docs without confusion.

---

## What Celery does

**Celery is a Python task queue.** It lets you take work that would be too slow
to do inside an HTTP request, sending email, processing files, talking to
external APIs, running ML inference, and hand it to background processes that
chew through it independently.

```mermaid
flowchart LR
  A["Your web app\n(HTTP)"] -- "1. push task" --> B["Broker\n(Redis)"]
  B -- "2. worker pops task" --> C["Celery worker\n(your code runs here)"]
```

The three pieces:

| Piece | What it is | In Relier |
|-------|-----------|-----------|
| **Producer** | The code that *enqueues* tasks. Usually your web app. | A FastAPI/Flask/Django handler calling `await task.apush(...)` or `task.push(...)`. |
| **Broker** | The queue between producers and workers. | Redis. (Celery also supports RabbitMQ; Relier specifically uses Redis.) |
| **Worker** | A process that picks tasks off the queue and runs them. | The `celery -A relier.tasks.app worker` process. |

---

## What a "task" is

A task is a Python function with a decorator that marks it as queueable. With
vanilla Celery:

```python
from celery import Celery
celery_app = Celery("my_app", broker="redis://localhost:6379/0")

@celery_app.task
def send_email(to: str, subject: str):
    smtp.send(to, subject, ...)

# Anywhere in your code:
send_email.delay("alice@example.com", "Welcome")
# → put on queue, returns immediately, worker picks it up later
```

With Relier, you decorate with `@rl_task` instead, and dispatch with `.apush()`
(async) or `.push()` (sync):

```python
from relier.tasks.decorator import rl_task

@rl_task()
async def send_email(to: str, subject: str) -> None:
    await smtp.send(to, subject, ...)

# Async context (FastAPI):
await send_email.apush("alice@example.com", "Welcome")

# Sync context (Flask, Django, scripts):
send_email.push("alice@example.com", "Welcome")
```

The function is just Python anything you can do in a normal function, you
can do here.

---

## "Queues" (multiple lanes of traffic)

A Celery worker can be told to consume from specific named queues. This lets
you isolate fast/slow work and dedicate capacity:

```bash
# Worker A: only takes high-priority work
celery -A relier.tasks.app worker -Q high_priority

# Worker B: takes default + batch work
celery -A relier.tasks.app worker -Q default,low_priority
```

Tasks pick a queue at decoration time:

```python
@rl_task(queue="high_priority")
async def confirm_payment(...): ...

@rl_task(queue="low_priority")
async def regenerate_thumbnails(...): ...
```

Relier ships with **three public queues** (`high_priority`, `default`,
`low_priority`) plus an **internal** queue (`re-queue`) that the Phoenix
resurrector uses to inject resurrected tasks. You can run a separate worker
pool for the internal queue so user traffic and recovery traffic don't compete
for the same workers the bundled `docker-compose.yml` already does this with
the `worker-recovery` service.

---

## What goes wrong with vanilla Celery (the gap Relier fills)

Celery works fine when nothing dies. But when it does, and it always does
eventually you hit one or more of these:

1. **Worker dies mid-task.** The task is just gone. No retry, no trace.
2. **Network blip retries the task.** Your customer's card is charged twice.
3. **A task hangs.** It holds a worker slot forever. Other tasks back up.
4. **Deploy time.** SIGTERM kills workers; in-flight tasks vanish.
5. **You can't see what's running.** `celery inspect` gives you a tarpit of
   text.
6. **One bad payload poisons your workers.** A malformed task keeps crashing
   workers as it loops through retry.
7. **Rolling deploy.** New code can't read payloads enqueued by old code.
8. **Traffic spike.** Queue fills with work that workers can't keep up with,
   memory bloats, everything cascades.

Relier wraps each of these. The mechanism is the same in every case: **state
about every active task lives in Redis** (the broker you're already using), so
when something fails, there's a recoverable record. See
[Core Concepts](concepts.md) for what each mechanism does.

---

## Relier vs vanilla Celery (side by side)

```python
# ===================================================================
#  Vanilla Celery
# ===================================================================
@celery_app.task(bind=True, max_retries=3)
def charge_customer(self, customer_id: str, amount_cents: int):
    try:
        return stripe.charge(customer_id, amount_cents)
    except Exception as exc:
        # Manual retry, manual idempotency, manual everything.
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)

charge_customer.delay("cus_abc", 5000)

# • No protection from worker death (task gone if process dies)
# • No protection from duplicate charges on retry
# • No timeout, could hang forever
# • No backpressure if you flood with these
# =====================================================================


# =====================================================================
#  Relier
# =====================================================================
@rl_task(
    queue="high_priority",
    idempotent=True,           # exactly-once via atomic Redis Lua
    soft_timeout=8,            # warning at 8s, cleanup hook can fire
    hard_timeout=10,           # killed at 10s
)
async def charge_customer(customer_id: str, amount_cents: int) -> dict:
    return await stripe.charge(customer_id, amount_cents)

await charge_customer.apush("cus_abc", 5000)

# • Worker dies → Phoenix resurrects within ~12s
# • Same args dispatched twice → runs once, cached result returned
# • Hangs past 10s → cancelled, DLQ'd with traceable reason
# • Cluster floods → AdmissionRejectedError raised, HTTP 429 to client
# =====================================================================
```

---

## Where to go from here

| You want to… | Go to |
|---|---|
| See a 5-minute end-to-end setup | [Quickstart](quickstart.md) |
| Understand how Phoenix / idempotency / DLQ work | [Core Concepts](concepts.md) |
| Plug Relier into FastAPI / Flask / Django | [Integration Recipes](integrations.md) |
| Pick the right pattern for retries, batches, locks | [Patterns Cookbook](patterns.md) |
| Get unstuck when something breaks | [Troubleshooting & FAQ](troubleshooting.md) |
| Read the deep-dive on internals | [Architecture](architecture.md) |

Want to go deeper on Celery itself? The official
[Celery documentation](https://docs.celeryq.dev/en/stable/) covers advanced
routing, beat scheduling, canvas workflows (chains, chords, groups), and
production deployment in detail.
