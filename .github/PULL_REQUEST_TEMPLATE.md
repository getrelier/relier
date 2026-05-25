## What this PR does

<!-- One sentence: the change and its motivation. -->

## Type of change

- [ ] Bug fix
- [ ] New feature / primitive
- [ ] Performance improvement
- [ ] Refactor (no behaviour change)
- [ ] Documentation
- [ ] CI / tooling
- [ ] Dependency update

## Checklist

### Code quality
- [ ] `make check` passes (ruff + mypy)
- [ ] `pre-commit run --all-files` passes (or `make setup` to install hooks, then commit runs them automatically)
- [ ] No debug `print()` / `breakpoint()` left in `src/`
- [ ] New public API has type annotations

### Tests
- [ ] New or updated unit tests cover the change
- [ ] Integration tests cover the change (if it touches Redis, Celery, or the decorator)
- [ ] Coverage stays at or above 90% (`make test-integration`)

### Reliability (for changes to core primitives)
- [ ] Considered fence-token correctness (stale worker cannot commit after resurrection)
- [ ] Considered idempotency invariants (no double-execution window)
- [ ] Considered Redis AOF + noeviction assumptions
- [ ] Chaos suite still validates the guarantees (`make bench` or `python -m bench.bench --synthetic`)

### Documentation
- [ ] `docs/` updated if this changes user-visible behaviour
- [ ] `.env.example` updated if new config fields were added
- [ ] CLI help text updated if new `rl` commands were added

## Testing notes

<!-- How did you verify this? Paste relevant test output, bench results, or `rl tasks inflight` screenshots. -->

## Breaking changes

<!-- If this changes the public API, Redis key layout, Lua scripts, or wire format — describe the impact and migration path. -->
