# Changelog

All notable changes to Relier are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with the pre-1.0 caveat documented in
[`docs/index.md`](https://github.com/getrelier/relier/blob/main/docs/index.md#versioning-policy):
minor (`0.y.0`) bumps may introduce breaking public-API changes until
`1.0.0` ships.

Releases are also published as GitHub Releases with auto-generated notes
from commit messages this file is the curated, human-edited summary
intended for adopters who need a single place to read migration impact.



## [0.1.2] — 2026-05-26

Patch release: eight bug fixes, two developer-experience improvements to the
schema migration workflow, and a documentation pass covering every
developer-facing API surface.

### Bug fixes

- **`push()`/`apush()` now raises `RedisConnectionError` when Redis is
  unreachable** instead of propagating a 60-line Celery/redis-py traceback.
  The error message names the configured Redis URL and shows the one-line
  Docker start command. (`src/relier/tasks/decorator.py`)

- **`TaskContext` injected into `actual_kwargs` no longer mutates the
  envelope in-place.** `SchemaRegistry.unwrap_and_migrate()` returned
  `kwargs` by reference; injecting `ctx` into the live dict propagated back
  into `phoenix_payload`, making the envelope non-serializable and causing
  `DeadLetterQueue.quarantine()` to raise `TypeError` instead of storing a
  DLQ record. Fixed by returning a shallow copy of `kwargs`.
  (`src/relier/core/schema.py`)

- **Soft-timeout hook now receives the live `TaskContext`.** `TimeoutEnforcer`
  previously created a fresh `TaskContext`, so `ctx.metadata` was always `{}`
  inside `on_soft_timeout` hooks regardless of what the task body had written.
  The decorator now passes its own `ctx` instance to `TimeoutEnforcer.run()`.
  (`src/relier/core/timeouts.py`, `src/relier/tasks/decorator.py`)

- **Async bridge no longer leaves a ghost coroutine after timeout.**
  When `future.result(timeout=...)` expired in the `loop.is_running()` branch,
  `future.cancel()` was never called, leaving `_orchestrate()` running in the
  background — emitting heartbeats and writing Redis — after Celery had already
  marked the task failed. Now cancels immediately on timeout.
  (`src/relier/tasks/decorator.py`)

- **`idempotency_lock` auto-commits the result on context exit.**
  The previous manual API required an explicit `await lock.record_result(value)`
  call; omitting it silently broke idempotency without raising an error. Added
  a synchronous `lock.set_result(value)` method that stages the value;
  `__aexit__` commits it on clean exit. `record_result()` is unchanged and
  still used internally by the `@rl_task` decorator.
  (`src/relier/core/idempotency.py`)

- **`rl chaos worker-kill` no longer prints "Worker terminated." when nothing
  was killed.** The scenario now returns `True`/`False`; the CLI prints a clear
  "No worker containers found — is the stack running via `make dev`?" message
  on `False`. (`src/relier/chaos/worker_kill.py`, `src/relier/cli/chaos.py`)

- **Admission control fail-open log includes the exception class and message.**
  Previously the message was `"Admission control error; failing open."` with
  the cause buried in `extra`. Now formats as
  `"Admission control check failed (ConnectionError: …) — failing open."` so
  the root cause is visible in plain CLI output too.
  (`src/relier/core/admission.py`)

- **CLI log output no longer silently drops `extra` fields.** `RichHandler`
  renders only `%(message)s`; any context placed only in `extra` was invisible.
  The admission fail-open site (and the pattern doc) now includes actionable
  context as positional format args so it appears in both plain CLI output and
  structured aggregators. (`src/relier/core/admission.py`)

### Improvements

- **`@rl_task(name=...)` — explicit stable task identity.** Task names were
  always derived as `f"{module}.{func_name}"`, tying identity to the import
  path. Renaming or moving a function silently broke schema migration key
  lookups, orphaned Phoenix heartbeat keys, and caused Celery to treat the old
  and new names as different tasks. Passing an explicit `name=` overrides the
  auto-derived name for registration, dispatch, migration lookup, and Phoenix —
  the identity survives any refactor. (`src/relier/tasks/decorator.py`)

- **`SchemaRegistry.validate()` runs at worker startup.** The migration loop
  is `while version < CURRENT_VERSION` — registering a migration without
  bumping `CURRENT_VERSION` produces `while 1 < 1: ...` which never fires.
  This previously surfaced as a `TypeError` hours or days later when an
  old-schema payload arrived. Two new checks now catch it early:
  (1) a `logger.warning` at `register_migration()` call time if
  `from_version >= CURRENT_VERSION`; (2) `SchemaRegistry.validate()`, called
  once from `init_worker_process()` after all modules are loaded, logs
  `CRITICAL` for every misconfigured migration on the final consistent state.
  (`src/relier/core/schema.py`, `src/relier/tasks/app.py`)

### Documentation

- **api-reference.md** — new `SchemaRegistry` section (two-step migration
  workflow, `CURRENT_VERSION` bump ordering warning, `register_migration()`
  parameter table, `validate()` subsection with CRITICAL log example, full
  structured-log event table); `@rl_task` `name=` parameter documented with
  stable naming convention; `TaskContext` proxy pattern documented with
  helper-function example and when-to-use guidance.

- **concepts.md** — eight targeted improvements: `release_your_lock` example
  annotated as user code (not a Relier API); `idempotency_lock` async-only
  `!!! warning`; hard-timeout 300 s fallback `!!! warning`; `ctx.metadata` vs
  `ctx.partial_result` callout; `celery_app` requirement warning before the
  mental-model diagram; admission fail-open path added to the Mermaid flowchart;
  checkpoint size-budget table moved into a collapsible block; schema versioning
  example shows the two-step workflow with `CURRENT_VERSION = 2`.

- **quickstart.md** — chaos Docker requirement upgraded from a code comment
  to a visible `!!! info` admonition listing which scenarios work without
  Docker.

- **running.md** — Tier 2 (Docker) section now notes that `rl chaos` kill-based
  scenarios require the Docker dev stack, with a pointer to the chaos guide.

- **mkdocs.yml** — `running.md` and `rl.md` were missing from the site nav;
  both added (no more build warnings).

### Tests

- Unit: `TestValidate` (5 tests) and `TestRegistrationGuard` (4 tests) for
  `SchemaRegistry.validate()` and the registration-time guard.
- Integration: named-task name stability, schema migration E2E (v1→v2 on
  worker pickup, migration key tied to explicit `name=`), app bootstrap
  (`init_worker_process` startup checks, `validate()` call path).
- Unit: `idempotency_lock` `set_result()`, auto-commit on `__aexit__`, result
  staged but not recorded path.

## [0.1.1] — 2026-05-25

Patch release to fix CI/CD pipeline. No changes to library code or public API.

- **fix(ci):** `uv run --with` replaces broken `uv pip install --system` in docs workflow (externally-managed Python on Debian runners)
- **fix(ci):** Wire `GITHUB_TOKEN` into git remote for `mkdocs gh-deploy` (ghp-import does not inherit checkout credentials)
- **fix(ci):** Switch GitHub Release step to `RELEASE_TOKEN` PAT — org policy disables `GITHUB_TOKEN` write permissions for workflows

## [0.1.0] — 2026-05-25

Initial public release. Establishes the core reliability engine — pre-1.0:
the engine internals (Phoenix protocol, Lua scripts, schema envelope) are
stable, the public API surface may still shift in `0.y.0` bumps.

- **Phoenix resurrector** — heartbeat-based crash detection, atomic
  re-queue with lease + fence-token protocol, distributed coordination
  lock to prevent double-resurrection.
- **Idempotency** — exactly-once execution via atomic Redis Lua,
  claim / in-flight / completed states with TTL-bounded locks.
- **Schema envelope** — versioned, signed payloads with sequential
  migrations for safe rolling deploys.
- **Two-tier timeouts** — soft (cleanup hook) + hard (asyncio
  cancellation) for async tasks; sync tasks reject timeout config at
  decoration time.
- **Checkpointing** — `ctx.set_partial(state)` persists progress to
  Redis (inline) or filesystem (large blobs); resurrected tasks resume.
- **Graceful shutdown** — SIGTERM drain phase, handoff of unfinished
  work to Phoenix.
- **Dead Letter Queue** — full payload + reason + resurrection history,
  CLI to inspect / release / retry / purge.
- **Admission control** — atomic Lua-backed fixed-window limiter,
  returns `Retry-After`.
- **SLO burn-rate tracking** — 1h / 6h / 3d windows, Google SRE-style
  burn rates, JSON or table output.
- **OpenTelemetry pipeline** — every lifecycle event emits spans and
  metrics; OTLP exporters, bundled Prometheus + Grafana stack.
- **Redis HA** — Sentinel-based failover, replicas, hourly RDB backups,
  optional S3 offsite.
- **Async-first, sync-compatible** — `apush` (asyncio) and `push`
  (sync) share one reliability stack.
- **Chaos suite** — `worker-kill`, `network-partition`, `load-spike`,
  `task-corrupt`, `slow-task`.
- **CLI (`rl`)** — `tasks`, `cluster`, `worker`, `dlq`, `slo`,
  `admission`, `chaos`, `config`, `admin` command groups, plus
  `doctor`, `run-resurrector`, `bench`, `man`.

[Unreleased]: https://github.com/getrelier/relier/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/getrelier/relier/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/getrelier/relier/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/getrelier/relier/releases/tag/v0.1.0
