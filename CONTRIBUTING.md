# Contributing to Relier

Thanks for considering a contribution. Relier is a reliability engine, so
the bar for changes that affect the core (Phoenix resurrector, idempotency,
schema envelope, admission control) is deliberately higher than for
documentation or CLI ergonomics. This file describes the workflow that
keeps reviews fast and merges safe.

## Where to start

The highest-leverage contributions right now:

- **Real-world workloads** that don't fit the current
  [Patterns Cookbook](https://getrelier.github.io/relier/patterns/).
- **Failure modes** the
  [durability matrix](https://getrelier.github.io/relier/durability/) doesn't cover.
- **Documentation gaps** you hit while integrating.
- **Benchmark numbers** from your environment `make bench` output plus
  a one-line spec (OS, Python, Redis topology, hardware).

Browse the [issue tracker](https://github.com/getrelier/relier/issues)
for tagged starter items, or open a new issue describing what you want
to change before writing code if it affects the public API or core
engine.

## Development setup

```bash
git clone https://github.com/getrelier/relier
cd relier
cp .env.example .env             # fill in your Redis URL
make setup                       # uv venv + dev deps + pre-commit hooks
```

This installs the project editable, every dev dependency from the `dev`
group in `pyproject.toml`, and a pre-commit hook that runs `ruff` and
trailing-whitespace checks on staged files.

## The quality gate

Every PR must pass the local gate before review:

```bash
make lint              # ruff check + ruff format --check
make check             # lint + mypy strict
make test              # unit suite (188 tests, ~10s, must run with zero warnings)
make test-integration  # integration suite requires reachable Redis (testcontainers)
```

`make test-integration` is mandatory if you touched anything under
`src/relier/core/` or `src/relier/tasks/`. It uses `testcontainers` to
spin up a real Redis 7 container so we never mock the broker.

## Commit style

Relier uses [Conventional Commits](https://www.conventionalcommits.org/).
The `publish.yml` workflow generates GitHub Release notes from commit
messages, and the curated [CHANGELOG.md](CHANGELOG.md) is grouped by
the same conventions.

Common prefixes:

- `feat:` — user-visible new behaviour
- `fix:` — user-visible bug fix
- `perf:` — performance change with no behavioural difference
- `refactor:` — internal restructuring, no behavioural difference
- `docs:` — documentation only
- `test:` — test-only changes
- `chore:` — build / CI / tooling / dependencies

Add a scope when it makes triage easier: `feat(phoenix): ...`,
`fix(cli): ...`, `docs(deployment): ...`.

Breaking changes go in the body with a `BREAKING CHANGE:` footer so the
changelog automation picks them up:

```
feat(api): rename rl_task.idempotency_ttl to idempotency_default_ttl

BREAKING CHANGE: callers using the old keyword argument must rename it.
```

## Pull request flow

1. Branch from `main`. Prefix the branch name with the change type
   (`feat/...`, `fix/...`, `docs/...`, `chore/...`).
2. Make the change. Update `CHANGELOG.md` under `## [Unreleased]`.
3. Run `make lint check test` locally. CI will re-run them but local
   feedback is faster.
4. Open the PR using the template. Include:
   - **Summary** — what changed, why.
   - **Test plan** — how you verified it (commands, screenshots,
     `make test-integration` output for core changes).
   - **Risk** — what could break, what's the rollback story.
5. Wait for CI green + one approving review. CI must pass on its own
   merits; avoid `--no-verify` and never bypass the coverage gates
   (95% unit, 80% integration).

## Tests

- **Unit tests** (`tests/unit/`) — no network, no Redis, no Celery
  worker. Mock at the boundary. Must run with zero warnings under
  `pytest -W error`.
- **Integration tests** (`tests/integration/`) — real Redis via
  `testcontainers`, real Celery worker (`testcontainers`-managed),
  end-to-end task dispatch + execution. These are slower but
  authoritative.
- **Chaos suite** (`src/relier/chaos/`) — verifies the reliability
  claims under fault injection. Run nightly in CI; run locally with
  `rl chaos <scenario> --watch`.

When adding tests for a new feature, prefer an integration test that
exercises the full producer-broker-worker path over a unit test with
heavy mocking. The mocks lie eventually; Redis does not.

## Areas with extra care

These areas have invariants that are easy to break silently:

- **`src/relier/core/phoenix.py`** — fence-token / lease protocol. The
  invariant is: at most one worker holds a valid lease for a given
  task at any time. Changes need a corresponding integration test.
- **`src/relier/storage/lua/`** — Redis Lua scripts run atomically on
  the server. Modifying them changes the durability story; document
  why in the commit body.
- **`src/relier/core/schema.py`** — envelope format and migrations.
  Rolling deploys depend on old workers handling new envelopes and
  vice versa; never remove a migration without a major version bump.
- **`src/relier/config.py`** — every field is a public API surface
  (people set `RELIER_*` env vars in production). Renames require a
  `BREAKING CHANGE` footer.

## Reporting bugs and security issues

- **Bugs** — use the [bug report template](https://github.com/getrelier/relier/issues/new/choose).
- **Security vulnerabilities** — do **not** open a public issue. See
  [SECURITY.md](SECURITY.md) for the disclosure process.

## Code of conduct

Be kind, be specific, assume good faith. We don't have a separate
CoC document; the standard expectation applies: harassment, personal
attacks, and discriminatory language are not acceptable in any project
space (issues, PRs, discussions).
