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

[Unreleased]: https://github.com/getrelier/relier/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/getrelier/relier/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/getrelier/relier/releases/tag/v0.1.0
