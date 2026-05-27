# Running Relier

Relier is a plain Python library, not a service you have to containerize. The
engine (the `@rl_task` decorator, the Phoenix resurrector, idempotency, the
DLQ) is pure Python. Like Celery, it runs as ordinary processes; Docker is a
convenience for dev and prod, not a requirement.

There is exactly one hard dependency: **a reachable Redis**, the same way
Celery needs a broker. Relier preflight-checks Redis at startup and refuses to
start with a clear error if it is unreachable, nothing comes up half-working.

## pip install vs. cloned repo

The `make` targets (`make worker`, `make dev`, `make prod`) are **only available if you've cloned the Relier repo**. They require the project's `Makefile`, virtualenv, and config files.

If you installed Relier via `pip install relier` into your own project, skip the `make` targets and run the Celery worker and resurrector directly; see [Tier 1 below](#tier-1-bare-metal-no-docker).

## The three ways to run it

| Tier | Redis | How | Use for |
|------|-------|-----|---------|
| Bare metal | You provide one | `celery` + `rl` commands (or `make worker` / `make resurrector` in the cloned repo) | Local development, tests, CI |
| Dev (Docker) | Single node, AOF+RDB | `make dev` (cloned repo only) | A full local cluster mirroring prod shape |
| Prod (Docker) | HA: master + replicas + Sentinel | `make prod` (cloned repo only) | Production |

Relier always runs the same two process types: **Celery workers** and the
**Phoenix resurrector**. Only the surrounding infrastructure changes per tier.

---

## Tier 1: Bare metal (no Docker)

**Prerequisites:** Python 3.11+, and a Redis reachable from your machine
(`brew install redis && redis-server`, a system package, or any remote
instance).

### If you installed via pip

Set the Redis URL in your shell (or in a `.env` file) and start the two processes directly. The `--include` flag tells the worker which module contains your `@rl_task` functions; without it the worker boots but ignores incoming tasks.

=== "macOS / Linux"

    ```sh
    export RELIER_REDIS_URL=redis://localhost:6379/0

    # terminal 1: Celery worker (replace 'tasks' with your task module name)
    celery -A relier.tasks.app worker -l info -Q high_priority,default,low_priority,re-queue --include=tasks

    # terminal 2: Phoenix resurrector
    rl run-resurrector
    ```

=== "Windows (PowerShell)"

    ```powershell
    $env:RELIER_REDIS_URL = "redis://localhost:6379/0"

    # terminal 1: Celery worker (replace 'tasks' with your task module name)
    # --pool=solo is required on Windows; prefork's named-pipe IPC crashes under spawn
    celery -A relier.tasks.app worker -l info -Q high_priority,default,low_priority,re-queue --include=tasks --pool=solo

    # terminal 2: Phoenix resurrector
    rl run-resurrector
    ```

### If you cloned the repo (contributing / dev)

```sh
make setup                       # create the venv and install Relier
export RELIER_REDIS_URL=redis://localhost:6379/0   # the default; override as needed

make worker                      # terminal 1: a Celery worker
make resurrector                 # terminal 2: the Phoenix resurrector
```

The `make worker` target runs the same `celery -A relier.tasks.app worker` command without `--include`, which is intentional. In the cloned repo there are no user task modules to discover; the worker only needs to run Relier's infrastructure (heartbeats, Phoenix, shutdown). The bench suite has its own entry point (`bench.worker_app`) that imports its tasks explicitly in the module body.

If Redis is not running, both processes exit immediately with:

```
RuntimeError: Relier cannot reach Redis (localhost:6379). ... Refusing to start.
```

Start Redis (or fix `RELIER_REDIS_URL`) and re-run, nothing starts in a broken
state.

---

## Tier 2: Dev (Docker)

A full local cluster: one Redis node (AOF + RDB persistence) plus the worker
pool and the resurrector. Mirrors the production *shape* without the HA weight.

```sh
make dev          # build + start, detached
make dev-logs     # follow logs
make dev-down     # stop
```

Defined in `docker-compose.yml`.

The Docker dev stack also unlocks the full **chaos suite** (`rl chaos worker-kill`, `rl chaos network-partition`, etc.). Those commands use `docker kill` internally and only work when the stack is running here; bare-metal workers can run the non-Docker scenarios (`load-spike`, `slow-task`, `task-corrupt`) but not the kill-based ones. See the [Chaos Guide](chaos-guide.md).

---

## Tier 3: Production (Docker)

The HA topology (Redis master + 2 replicas + 3 Sentinels + a backup sidecar)
defined in `docker-compose.prod.yml`. Sentinel-aware client wiring turns on via
environment variables (see below).

```sh
export REDIS_PASSWORD=...         # required
export SENTINEL_PASSWORD=...      # required
make prod
make prod-down
```

The same manifest runs locally if you want to exercise failover; see
`scripts/redis/README.md`.

---

## Configuration

All settings are environment variables with the `RELIER_` prefix (full list in
`src/relier/config.py`). The ones that matter for running:

| Variable | Default | Purpose |
|----------|---------|---------|
| `RELIER_REDIS_URL` | `redis://localhost:6379/0` | Redis endpoint (bare-metal / dev) |
| `RELIER_REDIS_USE_SENTINEL` | `false` | Route through Sentinel for HA |
| `RELIER_REDIS_SENTINEL_NODES` | none | `host:port,host:port,...` when Sentinel is on |
| `RELIER_REDIS_SENTINEL_MASTER_NAME` | `relier-master` | Monitored master group name |
| `RELIER_CHECKPOINT_BACKEND` | `inline` | `inline` \| `filesystem` for large checkpoints |

Bare-metal and dev run direct-to-Redis (`RELIER_REDIS_USE_SENTINEL=false`);
the production manifest sets the Sentinel variables for you.
