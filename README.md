# Relier

**Your Celery workers will crash tonight. Your tasks should still complete.**

[![CI](https://github.com/getrelier/relier/actions/workflows/ci.yml/badge.svg)](https://github.com/getrelier/relier/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/relier.svg)](https://pypi.org/project/relier/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Docs](https://img.shields.io/badge/docs-getrelier.github.io-purple.svg)](https://getrelier.github.io/relier)
[![Status](https://img.shields.io/badge/status-pre--1.0-orange.svg)](#production-status)

Relier makes Celery reliable. One decorator wraps your existing tasks with
crash recovery, exactly-once execution, two-tier timeouts, graceful shutdown,
admission control, and a DLQ without changing your function bodies or your
Redis broker.

Every task either completes, hands off to another worker, or lands in the Dead
Letter Queue with a traceable reason. **Nothing silently disappears.**

→ [Landing page](https://getrelier.github.io/relier/landing/) &nbsp;·&nbsp;
[Docs](https://getrelier.github.io/relier/) &nbsp;·&nbsp;
[Quickstart](https://getrelier.github.io/relier/quickstart/)

---

## What changes

Vanilla Celery:

```python
@celery_app.task
def charge_customer(customer_id: str, amount_cents: int):
    return stripe.charge(customer_id, amount_cents)

charge_customer.delay("cus_abc", 5000)
# - Worker dies mid-charge      -> task lost
# - Network blip causes retry   -> customer charged twice
# - Stripe hangs                -> task hangs the worker forever
# - Traffic spike               -> queue floods, cascade failure
```

With Relier (same function, four added kwargs):

```python
from relier.tasks.decorator import rl_task

@rl_task(
    queue="high_priority",
    idempotent=True,        # exactly-once via atomic Redis Lua
    soft_timeout=8,         # cleanup hook fires at 8s
    hard_timeout=10,        # cancelled at 10s
)
async def charge_customer(customer_id: str, amount_cents: int):
    return await stripe.charge(customer_id, amount_cents)

await charge_customer.apush("cus_abc", 5000)
# - Worker dies     -> Phoenix re-queues within ~8s (p99), same args; idempotency
#                      stops a double-charge
# - Network blip    -> cached result returned, no second charge
# - Stripe hangs    -> cancelled at 10s, quarantined to DLQ with full payload
# - Traffic spike   -> AdmissionRejectedError with Retry-After, HTTP 429 ready
```

That's the entire migration. Your function body doesn't change. Your call site
swaps `.delay(...)` for `await task.apush(...)` (async) or `task.push(...)`
(sync, for Flask / Django views / scripts).

---

## What Relier solves

| Problem | Vanilla Celery | With Relier |
|---|---|---|
| Worker OOM-killed mid-task | Lost forever, no trace | Phoenix re-queues within ~9 s (p99) |
| Non-idempotent retries | Your problem to solve | `idempotent=True`  atomic Lua, exactly-once |
| No task timeouts | Zombie tasks block workers | Two-tier soft/hard timeout with cleanup hooks |
| Ungraceful deploys | ~40% of in-flight tasks silently lost | SIGTERM drain + handoff to other workers |
| No visibility | `celery inspect`, then squint | `rl tasks inflight --follow`, structured output |
| Traffic spikes | Queue floods, cascade failures | Atomic admission control, `Retry-After` |
| Poison-pill tasks | Crash workers forever | Quarantined to DLQ after `max_resurrections` |
| Schema drift on rolling deploy | Old payloads on new code fail silently | Versioned envelope + sequential migrations |

All eight covered. Same Celery programming model. Same Redis broker. No new
infrastructure to operate beyond what you already have.

---

## What Relier is — and is not

**Relier is a thin wrapper around Celery, not a replacement for it.**

You keep your workers (`celery -A relier.tasks.app worker`), your Redis broker,
your queue names, your `@task` intuition. Relier adds a lifecycle layer on top:
heartbeat tracking, resurrection, idempotency, timeouts, graceful shutdown. Your
function bodies don't change. Your infrastructure doesn't change. You add one
decorator, switch `.delay()` to `.push()`, and you're done.

---

**Relier is not Temporal or Hatchet.**

[Temporal](https://temporal.io) and [Hatchet](https://hatchet.run) are workflow
engines. They model *multi-step workflows* with deterministic replay, activity
retries across process restarts, and saga compensation. That's a fundamentally
different problem — and a fundamentally different programming model. If you need
long-running workflows spanning hours, human approval steps, or saga rollbacks,
use one of those.

Relier is for teams that already have Celery tasks and want them to stop
disappearing. No workflow model. No deterministic replay. No new service to
operate. Same Redis you already have.

---

**Relier is not a DAG runner.**

[Prefect](https://prefect.io), [Airflow](https://airflow.apache.org),
[Dagster](https://dagster.io), [Luigi](https://github.com/spotify/luigi) — these
schedule and orchestrate pipelines of dependent tasks. They have UIs, schedulers,
and retry policies baked into a pipeline definition. Relier has none of that.

Relier makes individual Celery tasks reliable. What those tasks do, when they run,
and how they depend on each other is still your problem — and Celery's.

---

**vs. building it yourself.** Most teams write some subset of this — an
idempotency table, sometimes a heartbeat-based resurrector, occasionally a DLQ.
The pieces are individually well-understood. Composing them correctly (fence tokens
for the GC-pause-victim case, AOF + `noeviction` preflight checks, thundering-herd
defences on resurrection batches) is what Relier exists to spare you from. The
chaos suite ships first-party so you can verify the guarantees hold on your own
cluster, not just trust ours.

---

## Install

```bash
pip install relier
```

Requirements: Python 3.11+, Redis 7+ with AOF persistence and
`maxmemory-policy noeviction`. Relier preflight-checks both and refuses to
start if either is wrong.

---

## Quickstart

```python
# tasks.py
from relier.tasks.decorator import rl_task

@rl_task(idempotent=True, hard_timeout=30)
async def send_invoice(invoice_id: str) -> dict:
    await charge_card(invoice_id)
    await email_invoice(invoice_id)
    return {"invoice_id": invoice_id}
```

```python
# FastAPI
@app.post("/invoices/{invoice_id}/send")
async def dispatch(invoice_id: str):
    await send_invoice.apush(invoice_id)
    return {"status": "queued"}
```

```bash
# Three processes - bare metal, no Docker required
celery -A relier.tasks.app worker -l info -Q high_priority,default,low_priority,re-queue
rl run-resurrector
uvicorn main:app
```

Or get the full stack (Redis + workers + resurrector + OTel + Grafana):

```bash
make dev          # docker-compose.yml, single-node Redis with AOF
make prod         # docker-compose.prod.yml, Redis HA with Sentinel + backup
```

Full quickstart: [docs/quickstart.md](https://getrelier.github.io/relier/quickstart/).

---

## Verify it works (chaos suite, first-party)

```bash
# Seed a long-running task, SIGKILL the worker that's running it,
# watch Phoenix re-queue it onto a healthy worker, live.
rl chaos worker-kill --seed --watch --watch-duration 60
```

Five chaos scenarios ship with Relier: `worker-kill`, `network-partition`,
`load-spike`, `task-corrupt`, `slow-task`. They let you prove the reliability
claims against your own cluster, your own task code, your own Redis. Most
projects ship a test suite; Relier also ships a chaos suite.

Full guide: [docs/chaos-guide.md](https://getrelier.github.io/relier/chaos-guide/).

---

## Performance

Measured by the built-in bench suite (`docker compose -f docker-compose.bench.yml up --build`) on Linux with prefork workers and synthetic 0.5 s tasks. All claims verified end-to-end not microbenchmarks against a mock.

```
Linux (Docker, python:3.11-slim, prefork=4) | Redis 7.2 AOF | 500 tasks × 5 kills

Metric                              Relier 0.1         Vanilla Celery
----------------------------------------------------------------------
Task delivery rate (5 SIGKILL)      100%               92.0%
OOM recovery avg / p99              7.3 s / 9.4 s      ∞ lost
Dual-OOM (2 concurrent tasks)       2/2 · 7.5 s        both lost
Idempotency (50 submissions)        1 execution        50 executions
Admission control p99 / max         0.763 ms / 1.72 ms n/a
Graceful shutdown (3 cycles)        100%               0%
Dispatch overhead (net avg)         +2.28 ms           —
File descriptor leak                Δ 0 (stable)       n/a
----------------------------------------------------------------------
```

**+2.28 ms per dispatch** pays for: atomic admission check, SHA-256-signed envelope wrap, heartbeat registration. On any task that does real work (a DB query, an HTTP call, an AI inference), this is invisible.

At 3.1 ms average per dispatch, **a single async producer sustains ~320 `apush()` calls/second** per thread. FastAPI producers fan out well past 1,000/second.

The admission control Lua script stays under 1 ms at p99 (0.763 ms), meaning the tail-latency cost of the admission check is bounded for the vast majority of requests.

![Bench dashboard end of run](docs/assets/images/screenshot-2.png)

Full methodology, per-test breakdowns, and Docker Compose instructions: [docs/benchmarks.md](docs/benchmarks.md).

---

## What's in the box

- **Zero job loss (Phoenix Pattern)**: heartbeat-based crash detection, atomic re-queue with lease + fence tokens.
- **Exactly-once via idempotency**: atomic Redis Lua, claim/in-flight/completed states.
- **Two-tier timeouts**: soft (cleanup hook) + hard (asyncio cancellation), enforced on async tasks.
- **Checkpointing**: `ctx.set_partial(state)` in the soft-timeout hook saves progress to Redis; the next resurrection resumes from that state instead of starting over.
- **Graceful shutdown**: SIGTERM drain phase, handoff to Phoenix for tasks that won't finish in time.
- **Dead Letter Queue**: full payload + reason + resurrection history. CLI to inspect, release, retry, purge.
- **Admission control**: atomic Lua-based fixed-window limiter, returns `Retry-After`.
- **SLO burn-rate tracking**: 1h / 6h / 3d windows, Google SRE-style burn rates, JSON or table output.
- **Schema versioning**: signed envelopes with sequential migrations for rolling deploys — old workers and new workers can run simultaneously without payload mismatches.
- **Full OpenTelemetry**: every lifecycle event emits spans and metrics. Bundled OTel -> Prometheus -> Grafana stack.
- **Redis HA out of the box**: Sentinel-based failover, replicas, hourly RDB backups, optional S3 offsite.
- **Async-first, sync-compatible**: `apush` for asyncio (FastAPI), `push` for sync code (Flask, Django, scripts).
- **Chaos suite**: five scenarios to verify the guarantees on your cluster.

Full feature reference: [docs/](https://getrelier.github.io/relier/).

---

## Documentation

| | |
|---|---|
| [Quickstart](https://getrelier.github.io/relier/quickstart/) | 5-minute working setup |
| [Celery Primer](https://getrelier.github.io/relier/celery-primer/) | If you've never used Celery |
| [Core Concepts](https://getrelier.github.io/relier/concepts/) | What each mechanism does and why |
| [Integration Recipes](https://getrelier.github.io/relier/integrations/) | FastAPI, Flask, Django, scripts |
| [Patterns Cookbook](https://getrelier.github.io/relier/patterns/) | Idempotency keys, checkpoints, dedicated workers |
| [Troubleshooting & FAQ](https://getrelier.github.io/relier/troubleshooting/) | First place to look when things break |
| [API Reference](https://getrelier.github.io/relier/api-reference/) | Every `@rl_task` option, every dispatch method |
| [Configuration](https://getrelier.github.io/relier/configuration/) | Every `RELIER_*` env var |
| [CLI Reference](https://getrelier.github.io/relier/cli-reference/) | Every `rl` subcommand, what it touches in Redis |
| [Deployment](https://getrelier.github.io/relier/deployment/) | Bare metal, Docker dev, Docker prod, Kubernetes |
| [Durability & HA](https://getrelier.github.io/relier/durability/) | What's protected against which failure mode |
| [Architecture](https://getrelier.github.io/relier/architecture/) | Internals: async bridge, Redis keys, Lua scripts |
| [Metrics Reference](https://getrelier.github.io/relier/metrics/) | OTel metric names and labels for dashboards |
| [Chaos Guide](https://getrelier.github.io/relier/chaos-guide/) | How to verify the guarantees yourself |

---

## Production status

Relier is pre-1.0. The API is stabilising but may change before 1.0. The
internals (Redis key layout, Lua scripts, fence-token protocol) are
production-grade and have been validated against the bundled chaos suite,
including under network partitions and mass worker failure.

If you're considering it for production: read
[Durability & HA](https://getrelier.github.io/relier/durability/) first, then
run the chaos suite against a staging cluster that mirrors your prod setup.
File issues for anything that surprises you. Those are the inputs that get
the project to 1.0.

---

## Contributing

Issues and pull requests welcome. Particularly valuable:

- Real-world workloads that don't fit the current [Patterns Cookbook](https://getrelier.github.io/relier/patterns/)
- Failure modes the [durability matrix](https://getrelier.github.io/relier/durability/) doesn't cover
- Documentation gaps you hit while integrating
- Performance numbers from your environment (`make bench` output plus a one-line spec)

```bash
git clone https://github.com/getrelier/relier
cd relier
cp .env.example .env             # fill in your Redis URL
make setup                       # venv + dev deps + pre-commit
make test                        # unit tests
make test-integration            # integration tests against test-container Redis
make bench                       # synthetic bench smoke (no Docker, ~2 min)
make bench-docker                # full bench in Docker with Prometheus + Grafana
```

Open a PR against `main`. Quality gates: `make lint check test` must pass; `make test-integration` is recommended if you touched anything in `core/` or `tasks/`.

---

## Community

- **Issues** — bugs, feature requests, questions via the issue templates above
- **Discussions** — [github.com/getrelier/relier/discussions](https://github.com/getrelier/relier/discussions) — ideas, integrations, show and tell
- **X / Twitter** — [@relierdev](https://x.com/relierdev) — release announcements and short-form updates
- **Releases** — watch this repo for new releases; the changelog is in each GitHub Release

---

## Licence

MIT. See [LICENSE](LICENSE).

---

## Acknowledgements

Built on Celery, Redis, asyncio, and OpenTelemetry. The Phoenix Pattern owes
its name to the obvious metaphor; the fence-token approach is borrowed from
Martin Kleppmann's writeups on distributed locking. The explicit-checkpoint
philosophy is shared with Faust, Temporal (despite their different model),
and AWS Step Functions — when production systems converge on a design choice,
it's worth noticing.
