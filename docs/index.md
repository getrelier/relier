# Relier

**Reliability layer for Celery. Zero job loss.**

!!! warning "Pre-1.0: public API may change"
    Relier is currently `v0.x.y`. The **core engine** (Phoenix resurrector,
    idempotency, schema envelope, admission control, fence-token protocol)
    is production-grade and has been validated against the first-party
    chaos suite.

    The **public API layout** is still stabilising. Minor version bumps
    (`0.y.0`) may introduce breaking changes to decorator options,
    dispatch helpers, and CLI command shapes. Pin to a minor version
    (e.g. `relier==0.1.*`) until 1.0 ships.

    See the [versioning policy](#versioning-policy) at the bottom of this
    page and the [CHANGELOG](https://github.com/getrelier/relier/blob/main/CHANGELOG.md)
    for the migration log.

---

Relier wraps your existing Celery setup with a self-healing reliability layer.
Tasks that enter your system always exit it, either successfully, or in the
Dead Letter Queue with a full traceable reason.

```python
# Before Relier, a plain Celery task.
# If the worker dies mid-execution, this is gone. Silently.
@celery_app.task
def process_order(order_id: str):
    charge_card(order_id)
    send_receipt(order_id)

# After Relier, same function, zero changes to your business logic.
# Worker dies? Relier re-queues it automatically, within 35 seconds.
# Called twice with the same order_id? Runs once. Card charged once.
@rl_task(idempotent=True)
async def process_order(order_id: str):
    await charge_card(order_id)
    await send_receipt(order_id)
```

### Built for FastAPI, works with anything

Relier is **async-first**: the orchestration layer runs on a persistent asyncio
event loop owned by each worker process, so async tasks dispatch and execute
without spinning up a new loop per call. FastAPI integration is the design
target, and `await task.apush(...)` is the canonical dispatch path.

Relier also runs cleanly on top of sync frameworks, Flask, classic
(synchronous) Django, management commands, plain scripts via `task.push(...)`.
`push` is a thin sync wrapper over `apush` that bridges back to the worker's
loop when one is running and falls back to `asyncio.run()` otherwise. The
reliability guarantees are identical either way.

---

## What problem does Relier solve?

Celery is a great task queue. But out of the box, it has some sharp edges:

| Problem | What breaks | Without Relier |
|---------|-------------|----------------|
| Worker OOM-killed | All in-flight tasks | Lost forever, no trace |
| Non-idempotent retries | Double charges, duplicate emails | Your problem to solve |
| No task timeouts | Zombie tasks block workers | Manual implementation |
| Ungraceful deploys | Tasks mid-flight during restart | ~40% silently lost |
| Zero visibility | What's running right now? | Check logs and hope |
| Traffic spikes | Queue floods, cascade failures | Manual rate limiting |
| Poison-pill tasks | One bad payload loops forever | Workers keep dying |
| Rolling deploy schema drift | Old payloads on new code | Silent failures |

Relier solves all 8.

---

## Key Features

### Zero job loss (Phoenix Pattern)
Every task registers a heartbeat in Redis. If a worker is killed mid-task, the heartbeat expires. A background resurrector detects this within 35 seconds and re-queues the task on a healthy worker automatically, with the original payload intact.

### Safe retries (Idempotency)
An atomic Redis Lua script ensures a task only executes once, even when retried multiple times after failure. Pass `idempotent=True` and Relier handles the rest.

### Timeout enforcement
Two-tier timeout: a soft timeout fires your cleanup hook so you can save partial progress. A hard timeout terminates unconditionally. Both emit OpenTelemetry events.

### Graceful shutdown
Relier intercepts SIGTERM (deploys, scale-downs, Kubernetes evictions) and waits for in-flight tasks to finish. Tasks that won't finish in time are handed off to another worker not dropped.

### Dead Letter Queue
Tasks that can't be recovered after repeated attempts are quarantined in the DLQ with a full payload, stack trace, and resurrection history. Inspect and re-release them at any time via the CLI.

### Admission control
A Redis Lua script enforces cluster-wide rate limits atomically. Returns `Retry-After` on rejection. Workers never see traffic above configured capacity.

### Full OpenTelemetry observability
Every task, retry, resurrection, timeout, and DLQ quarantine emits OTEL spans and metrics. Plug into Grafana, Jaeger, Honeycomb, or any OTLP endpoint.

---

## Requirements

- Python 3.11+
- Redis 7+ with AOF persistence and `maxmemory-policy noeviction`. See
  [deployment guide](deployment.md). Relier preflight-checks this and refuses
  to start if either is wrong.
- Celery 5.4+
- Any Python web framework (FastAPI, Flask, Django, Starlette, …) optional;
  Relier works inside any Python process, including plain scripts and CLIs.

---

## Installation

```bash
pip install relier
```

That's it. No GPU, no native extensions, no database required. Just Python + Redis.

---

## Where to go next

### If you're new

| I want to… | Go here |
|---|---|
| Get something running in 5 minutes | [Quickstart](quickstart.md) |
| Learn what Celery is (and how Relier sits on top) | [Celery Primer](celery-primer.md) |
| Understand how Relier's mechanisms work | [Core Concepts](concepts.md) |
| Plug Relier into FastAPI / Flask / Django | [Integration Recipes](integrations.md) |

### Once you're building

| I want to… | Go here |
|---|---|
| Pick the right pattern for retries, batches, locks | [Patterns Cookbook](patterns.md) |
| Get unstuck when something breaks | [Troubleshooting & FAQ](troubleshooting.md) |
| Deploy safely across rolling code changes | [Rolling Deploys & Schema Migrations](rolling-deploys.md) |
| Deploy bare-metal, Docker dev, Docker prod, or Kubernetes | [Deployment](deployment.md) |
| Verify reliability with chaos tests | [Chaos Guide](chaos-guide.md) |

### Reference

| I want to… | Go here |
|---|---|
| See all `@rl_task` options + dispatch methods | [API Reference](api-reference.md) |
| See all `rl` CLI commands and what they touch in Redis | [CLI Reference](cli-reference.md) |
| Configure Relier for production | [Configuration](configuration.md) |
| Set up dashboards and alerts | [Metrics Reference](metrics.md) |
| Read the deep-dive on internals | [Architecture](architecture.md) |
| Understand exactly what's protected against what failure | [Durability, HA, & Failure Boundaries](durability.md) |

---

## Versioning policy

Relier follows [Semantic Versioning](https://semver.org), with one
explicit caveat for the pre-1.0 series:

- **`0.PATCH.x`**: bug fixes, doc updates, internal refactors.
  Always safe to upgrade.
- **`0.MINOR.0`**: feature additions **and** potentially breaking
  changes to the public API surface (`@rl_task` options, dispatch
  helpers, CLI command shapes, `relier.config.Settings` field names).
  Read the [CHANGELOG](https://github.com/getrelier/relier/blob/main/CHANGELOG.md)
  before bumping.
- **`1.0.0`**: locks the public API. From that point on, breaking
  changes require a major-version bump per standard SemVer.

The **core engine** (Redis key layout, Lua scripts, fence-token
protocol, schema envelope format) is treated as an internal contract
even pre-1.0: changes there are versioned and migrated, never silent.
