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



## [0.1.4] — 2026-05-29

Patch release focused on dispatch-boundary correctness, type-checker /
IntelliSense quality, quieter resurrection logs, and CI notification fixes.

### Bug fixes

- **Stacking `@rl_task` twice now fails fast with a clear `ValueError`.** A
  stray `@rl_task(...)` decorator with no function directly beneath it would
  fall through onto the next definition, double-wrapping an already-decorated
  task. At execution time this blew up with an opaque
  `RecursionError: maximum recursion depth exceeded` deep inside
  `inspect`/`celery/local.py`. The decorator now detects an already-decorated
  target at decoration time and explains exactly what to fix.
  (`src/relier/tasks/decorator.py`)

- **`push()` from async code raises a clear `RuntimeError` instead of
  deadlocking.** Calling the synchronous `push()` from a running event loop (a
  FastAPI route or an `async` task body) scheduled the dispatch onto the loop
  and then blocked the same loop waiting for it — a 5-second hang ending in a
  confusing timeout. `push()` now detects a running loop on the calling thread
  and tells you to use `await task.apush(...)`. Sync task bodies (which run in a
  worker thread) are unaffected. (`src/relier/tasks/decorator.py`)

### Developer experience

- **Dispatch methods are now typed as `TaskReceipt`, not `Any`.** `apush` /
  `push` return a Celery `AsyncResult`, but Celery ships no type information,
  so type checkers treated the receipt as opaque — `receipt.id` had no
  autocomplete and no checking. A new `TaskReceipt` `Protocol` describes the
  live slice Relier promises (`id`, `status`/`state`, `result`, `ready()`,
  `successful()`, `failed()`, `get()`). `receipt.id` now resolves to `str` and
  typos on the receipt are flagged, with no runtime wrapper and no behaviour
  change. Exported from the top level: `from relier import TaskReceipt`.
  (`src/relier/tasks/decorator.py`, `src/relier/__init__.py`)

### Observability

- **Resurrection logs no longer spam every scan interval.** The "Phoenix
  resurrection pass complete" line fired on every pass while a task was merely
  being *monitored* (`monitored > 0`), emitting an identical line every ~2s
  until that task finished — noise that leaked into worker logs. The pass
  summary now logs only when a task was actually recovered, and reads
  "Phoenix recovered N orphaned task(s) onto healthy workers". State changes
  (reclaimed / died-again / completed) are still logged individually, and the
  "recovered task finished successfully" confirmation is now visible instead of
  being buried. (`src/relier/core/phoenix.py`)

### CI

- **Nightly Slack notifications guard against an unset `SLACK_WEBHOOK`.** The
  failure-only Slack step previously ran `curl` against an empty URL when the
  secret was missing; it now skips cleanly with a workflow warning.
- **High-scale chaos can be opt-in on manual runs.** The 10k×8 job is gated on
  `github.event.schedule`, which is unset for `workflow_dispatch`, so a manual
  run always skipped it. A new `run_high_scale` dispatch input opts in, while
  the Monday auto-schedule is unchanged. (`.github/workflows/nightly.yml`)

### Documentation

- Examples now use the friendlier top-level import `from relier import rl_task`.
- Dispatch, admission-control (429), and result-handling docs now show FastAPI,
  Flask, and Django side by side with copy-paste-runnable code.
- New `TaskReceipt` API reference; documented that `receipt.get()` blocks and
  must not be called inline in async handlers.
- Corrected the "dispatch from inside a task" guidance (async body → `apush`,
  sync body → `push`) and added troubleshooting entries for the two new errors.
- Resurrection timing clarified: detection is typically ~12s (heartbeat TTL +
  scan interval), with 35s as the conservative worst-case ceiling.
- Removed the standalone Starlette recipe (FastAPI, built on Starlette, covers
  it).

## [0.1.3] — 2026-05-27

Patch release focused on observability fixes, Redis Cluster preparation, and a
substantial bench-suite expansion that surfaces the real per-task scaling story.

### Bug fixes

- **Idempotency cache hits now log at INFO instead of DEBUG.** Workers run at
  INFO by default, so deduplication was previously invisible in standard
  operations — operators could not confirm idempotency was working without
  flipping log level. Promoted with a clearer message so every duplicate
  dispatch is observable. (`src/relier/core/idempotency.py`)

- **Resurrector logs now render structured `extra=` fields.** `rl
  run-resurrector` configures `RichHandler` with `format="%(message)s"`, which
  silently dropped every `extra={}` attribute attached to phoenix log records
  (task_id, ghost_worker, attempt count, has_checkpoint, etc). A new
  `_StructuredRichFormatter` appends those fields to the rendered message so
  operators can see which task was resurrected, which worker died, and how
  many attempts remain without flipping to DEBUG. (`src/relier/cli/main.py`)

- **Cold-start grace period for "never claimed" resurrection warnings.**
  `_monitor_resurrected_tasks` previously fired "never claimed" on the very
  next pass after a re-queue (~2s), but a cold Celery worker takes 10–20s
  before its first queue poll. The monitor value now encodes the re-queue
  timestamp as `"state:ts"` and a new `resurrection_claim_grace_period`
  setting (default 30s) gates the warning. Parse falls open on missing
  timestamp to preserve old behaviour for stray monitor entries.
  (`src/relier/config.py`, `src/relier/core/phoenix.py`,
  `src/relier/cli/chaos.py`)

- **Embedded resurrection scanner log demoted to DEBUG.** Every Celery worker
  embeds a Phoenix scanner alongside the dedicated `rl run-resurrector`
  process (distributed locks make multiple scanners safe). The INFO-level
  "Phoenix resurrector started" log appeared on every worker boot, making
  users think they had accidentally launched a standalone resurrector. The
  scanner now logs at DEBUG with a comment clarifying the embedded-design.
  The dedicated `rl run-resurrector` process still prints its own startup
  banner. (`src/relier/core/phoenix.py`)

### Reliability and Redis Cluster preparation

- **Per-task and worker-scoped Redis keys now use hash tags.** Keys for one
  task (`hb`, `phoenix`, `lease`, `fence`, `resurrections`, `lock:resurrect`)
  wrap the task_id in `{...}` so Redis Cluster hashes only the task segment
  and colocates every key for one task on a single shard. Worker-scoped keys
  (`inflight`, `presence`, `m:w`) are tagged on `worker_id` for the same
  reason. Without this, `RESURRECT_LUA` and `VALIDATE_LUA` would fail with
  `CROSSSLOT` under Cluster because their two `KEYS` arguments would hash to
  different slots. Singletons (`workers`, `monitoring`, `phoenix:expiry_index`,
  `dlq`, etc.) stay untagged. (`src/relier/core/keys.py`)

### Benchmark suite

The bench gained four new measurements and a third comparison column, producing
the data backing the new "Scaling ceiling" section of `docs/benchmarks.md`.

- **Test 5 third column: vanilla Celery with `task_acks_late=True`.** The
  most obvious objection to Relier — "why not just flip the flag?" — is now
  answered in the bench. Result: flipping `task_acks_late=True` recovers
  about half the lost tasks (96.2% vs 92.0% default) but cannot match
  Relier's 100% because the Redis broker's `visibility_timeout` default
  (~1 hour) gates redelivery long after most completions would have
  happened. The third column also tracks per-task execution counts so
  duplicates are reported when redelivery does fire.
  (`bench/vanilla_acks_late_app.py`, `bench/bench.py`)

- **Test 7 sub-test: steady-state Redis ops/sec.** Runs a fleet of solo-pool
  workers, takes an idle-worker baseline, then measures with N tasks
  inflight. The delta is the per-task steady-state coordination cost. The
  finding: per-task steady-state Redis traffic is below measurement noise.
  Long-running tasks are effectively free at the coordination level.
  Capacity scales with task **turnover rate**, not inflight count.
  (`bench/bench.py`, `bench/config.py`)

- **Test 8: cold-start to first-task latency.** Measures wall-clock from
  worker process start to first task completion. Three trials; reports
  avg / p50 / p99. Matters for serverless and scale-to-zero deployments.

- **Test 9: resurrection under load.** Spawns N solo-pool workers each
  holding one inflight task, then SIGKILLs them all at once (fleet-wide OOM
  scenario). Measures wall-clock from kill to each orphan being re-picked-up
  by a replacement. Reports p50 / p99 / first / last.

- **Grafana dashboard refresh.** Four new panels in
  `bench/grafana/dashboards/bench-overview.json`: steady-state ops/sec
  breakdown, resurrection-under-load task pickups, cold-start p50, and
  cold-start p99.

### Documentation

- **Benchmarks page refreshed end-to-end.** All numbers regenerated from the
  v0.1.3 bench run (2026-05-27). New "Scaling ceiling and per-task
  coordination cost" section explains the per-task cost breakdown, the
  workload-shape capacity table, and the three paths past single-master
  Redis (vertical, Cluster, RabbitMQ broker).

- **README scaling section added.** Honest capacity numbers — Relier
  comfortably handles workloads up to ~1,000 tasks/sec end-to-end on
  single-master Redis (covering 100M tasks/day at 1 s average task duration).

---

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
