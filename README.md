# Relier

**Production reliability layer for Celery. Zero job loss.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Docs](https://img.shields.io/badge/docs-getrelier.github.io-purple.svg)](https://getrelier.github.io/relier)
[![Status](https://img.shields.io/badge/status-pre--1.0-orange.svg)](#production-status)

Every task either completes, hands off to another worker, or lands in the Dead
Letter Queue with a traceable reason. Nothing silently disappears.

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
# - Worker dies     -> Phoenix re-queues within ~12s, same args; idempotency
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
| Worker OOM-killed mid-task | Lost forever, no trace | Phoenix re-queues within ~12s |
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

## How it compares

**vs. vanilla Celery.** Drop-in. Your tasks keep their shape, your workers
are still `celery -A relier.tasks.app worker`. Relier wraps the lifecycle to
guarantee delivery. Most users adopt incrementally, one task at a time.

**vs. Temporal.** Temporal is a workflow engine with a different programming
model (deterministic replay, durable execution, sagas). It's the right answer
for multi-step workflows that span hours or days. Relier is the right answer
for "I have Celery tasks and I just want them not to disappear." Different
tools, different problem shapes.

**vs. building it yourself.** Most teams write some subset of this, usually
an idempotency table, sometimes a heartbeat-based resurrector, occasionally a
DLQ. The pieces are well-understood individually. Composing them correctly
(fence tokens for the GC-pause-victim-commits-stale case, AOF + `noeviction`
preflight checks, thundering-herd defences on resurrection batches) is what
Relier exists to spare you from. The chaos suite ships first-party so you can
verify the guarantees hold on your cluster, not just trust ours.

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

**Relier adds about 1 millisecond per dispatch on Linux.**

Producer-side overhead vs raw Celery `.delay()`, measured by the built-in
`rl bench` command (1000 measured iterations + 100 warmup, top 1% outliers
trimmed from the mean, median of 4 back-to-back runs):

```
Linux 6.6 (WSL2 x86_64) | Python 3.11 | docker compose exec worker-default rl bench

                         p50       p95       p99     mean*
-----------------------------------------------------------
Plain Celery (.delay)  1.26ms    1.88ms    2.28ms    1.28ms
Relier        (.apush) 2.17ms    2.80ms    3.30ms    2.18ms
-----------------------------------------------------------
Overhead at p50:  +0.91 ms   (the steady-state cost)
Overhead at p99:  +1.02 ms   (essentially no tail penalty)
```

Two things to notice. **p99 is within 200 µs of p50**, meaning there are no
fat-tail events in the hot path, no AOF fsync hiccups, no GC pauses
landing in the measurement window. And **variance across four runs is
~30 µs at p50**, so the number is the system, not the noise floor.

The 1 ms overhead pays for: atomic admission check, SHA-256-signed envelope
wrap, OpenTelemetry context injection. On the worker side, total framework
overhead is ~5–8 ms per task (schema verification + idempotency Lua +
Phoenix registration). On any task that does real work, a Stripe call
(100–300 ms), a DB query (1–5 ms), an HTTP fetch, a 1 ms framework tax is
invisible.

Practical: at 2.17 ms per dispatch, **a single producer thread sustains
~460 dispatches/second**. Async producers (FastAPI, etc.) push well past
1000/second per worker.

### Other environments

| Environment | Plain p50 | Relier p50 | Overhead |
|---|---|---|---|
| Linux + Docker (WSL2) | 1.26 ms | 2.17 ms | +0.91 ms |
| Windows 10 + localhost Redis | 2.15 ms | 3.79 ms | +1.64 ms |

Windows is consistently ~1.7× slower on both paths because of slower TCP
loopback, coarser scheduler quanta, and the absence of `uvloop`. Absolute
overhead stays roughly platform-stable, most of the slowdown is in the
baseline, not the framework cost.

Run `rl bench` against your own Redis to see your environment's numbers.
**Trust the p50 across multiple runs**, not a single percentage from one
run, microbenchmarks at this scale are noisy by nature.

---

## What's in the box

- **Zero job loss (Phoenix Pattern)**: heartbeat-based crash detection, atomic re-queue with lease + fence tokens.
- **Exactly-once via idempotency**: atomic Redis Lua, claim/in-flight/completed states.
- **Two-tier timeouts**: soft (cleanup hook) + hard (asyncio cancellation), enforced on async tasks.
- **Graceful shutdown**: SIGTERM drain phase, handoff to Phoenix for tasks that won't finish in time.
- **Dead Letter Queue**: full payload + reason + resurrection history. CLI to inspect, release, retry, purge.
- **Admission control**: atomic Lua-based fixed-window limiter, returns `Retry-After`.
- **SLO burn-rate tracking**: 1h / 6h / 3d windows, Google SRE-style burn rates, JSON or table output.
- **Schema versioning**: signed envelopes with sequential migrations for rolling deploys.
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

- Real-world workloads that don't fit the current Patterns Cookbook
- Failure modes the durability matrix doesn't cover
- Documentation gaps you hit while integrating
- Performance numbers from your environment (`rl bench` output plus a one-line spec)

```bash
git clone https://github.com/getrelier/relier
cd relier
make setup              # creates the venv, installs dev deps, sets up pre-commit
make test               # unit tests
make test-integration   # integration tests against a test-container Redis
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
and AWS Step Functions, when production systems converge on a design choice,
it's worth noticing.
