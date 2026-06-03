# Relier Benchmark

Validates every claim in `docs/benchmarks.md` using **real AI workloads** (Ollama) or fast
synthetic sleep tasks.

## What it tests

| Metric | Relier claim | Vanilla Celery |
|--------|-------------|----------------|
| Task delivery rate | 100% | ~92% |
| Worker OOM recovery | < 10 s p99 | ∞ (lost) |
| Duplicate prevention | 100% | 0% |
| Admission control p99 | < 1 ms | n/a |
| Graceful shutdown | 100% | 0% |
| Overhead per task | < 10 ms net | baseline |
| Cold-start to first task | informational | n/a |
| Resurrection under load (N=5) | < 120 s p99 | ∞ (lost) |
| Redis ops/sec (steady-state) | informational | n/a |

CPU% is recorded for every test that runs workers.

## On vanilla Celery settings

The vanilla worker uses **Celery's documented defaults**: `task_acks_late=False` and
`task_reject_on_worker_lost=False`. These are not handicaps they are what every new Celery
user ships. `task_acks_late=False` means the broker ACKs a task the moment a worker picks it
up; if the worker is killed before the task finishes the task is simply gone. That is the
designed default behaviour and why Celery loses tasks under crashes.

`worker_prefetch_multiplier` is set to `1` (vs the Celery default of 4), which is actually
more favourable to vanilla: fewer tasks are held in-worker memory at the moment of a kill.
The settings are documented in `bench/vanilla_app.py`.

## Prerequisites

### 1 · Redis with AOF persistence

```bash
docker run -d --name relier-redis \
  -p 6379:6379 \
  redis:7-alpine \
  redis-server --appendonly yes --appendfsync everysec
```

### 2 · Ollama with both models (Ollama mode only)

```bash
# Ollama must be running at http://localhost:11434
ollama pull nomic-embed-text   # 274 MB — embedding
ollama pull gemma3:4b          # 3.3 GB — generation
```

### 3 · Relier installed

From the project root:

```bash
# With uv (recommended)
uv sync

# Or pip
pip install -e .
```

### 4 · Bench extras

```bash
pip install psutil rich
```

## Run

```bash
# From the project root — real Ollama AI workloads
python -m bench.bench

# Synthetic mode: asyncio.sleep tasks, no GPU required, runs in minutes
python -m bench.bench --synthetic

# Scale mode: high-volume on EVERY test, not just delivery (implies --synthetic)
python -m bench.bench --scale
```

The runner manages all worker subprocesses itself; no need to start anything manually.

### Scale profile

`--scale` (or `BENCH_SCALE=scale`) raises the sample size on every test so the
p99s, dedup rates, and recovery counts all rest on a meaningful N rather than a
token sample. It implies `--synthetic`. Values per profile:

| Test | standard (synthetic) | scale |
|------|----------------------|-------|
| Dispatch overhead | 200 dispatches | 2 000 |
| Admission control | 5 000 samples | 50 000 |
| Duplicate prevention | 50 submissions | 2 000 |
| OOM recovery | 5 cycles | 20 |
| Delivery rate | 500 tasks × 5 kills | 10 000 × 10 |
| Graceful shutdown | 20 tasks × 3 cycles | 200 × 5 |
| Cold-start | 3 trials | 10 |
| Resurrection under load | 5 inflight | 25 |
| Synthetic task sleep | 0.5 s | 0.05 s |

Any `BENCH_*` env var still overrides the profile value. The
`BENCH_PHOENIX_LOAD_WORKERS` knob is process-bound (one Celery worker process
each), so it tops out at 25 in scale mode — raise it only if the host can
sustain that many concurrent workers.

## How each test works

### Test 1 · Dispatch overhead
Dispatches 200 tasks via `apush()` (Relier) and 200 via `delay()` (vanilla).
Measures wall-clock time per dispatch. The difference is Relier's net overhead.
No workers needed pure producer-side measurement.

### Test 2 · Admission control p99
Calls `admission_control.check_capacity()` 1 000 times (5 000 in synthetic mode) and
computes p99. This is the atomic Redis Lua INCR that enforces cluster-wide rate limits.

### Test 3 · Duplicate prevention
Dispatches the same `embed_document` task 10 × with `idempotent=True`.
Counts actual executions via a Redis counter incremented in the task body.
Relier executes once; vanilla executes 10 times.

### Test 4 · Worker OOM recovery
Dispatches a 50-second task, waits 8 s, then `SIGKILL`s the worker.
A replacement worker + Phoenix resurrector are already running.
Measures seconds from kill to task re-start on the new worker.
Repeated 1 cycle (Ollama) or 5 cycles (synthetic); reports avg and p99.
Also runs a **Dual-OOM** sub-test: 2 tasks in-flight, 1 SIGKILL, Phoenix must resurrect both.

### Test 5 · Delivery rate under crash
Submits 30 tasks (Ollama) or 500 tasks (synthetic), kills the worker and replaces it
1 time (Ollama) or 5 times (synthetic).
Counts completions from a Redis list. Relier resurrects in-flight tasks;
vanilla loses one per kill. CPU% is tracked throughout.

### Test 6 · Graceful shutdown
Submits 15 tasks (Ollama) or 20 tasks (synthetic) with 10 s work each,
waits 40% of task time, then sends `SIGTERM`.
Relier drains in-flight tasks then hands the rest off to a replacement worker.
Vanilla terminates immediately. Repeated 1 cycle (Ollama) or 3 cycles (synthetic).

### Test 7 · Resource overhead
Measures idle worker RSS and the number of Redis keys + bytes written per in-flight Relier
task. Also checks for file-descriptor leaks (open fds before vs after a task completes).

Sub-test: **steady-state Redis ops/sec.** Runs `PHOENIX_LOAD_WORKERS` solo-pool workers,
measures a 30 s idle baseline (workers running, no tasks), then a 60 s window with N tasks
inflight. Both are reported **as measured** — the bench deliberately does *not* subtract
baseline from total, because busy workers poll the broker less than idle ones, so the
inflight figure routinely lands *below* baseline and a subtraction would floor to a
meaningless ~0. The per-task coordination cost Relier actually adds is the heartbeat
refresh: 2 ops (EXPIRE + ZADD) every `heartbeat_ttl/2` seconds, i.e. 0.4 ops/sec/task at
the default `heartbeat_ttl=10`. That figure is derived from the protocol and extrapolated
linearly (~400/s at 1k inflight, ~4 000/s at 10k) rather than inferred from the noisy
broker-polling delta.

### Test 8 · Cold-start to first-task latency
Dispatches a task while the worker is *not* running, starts a fresh worker, and measures
wall-clock from process start to task completion. Repeated 3 times. Reports avg / p50 / p99.
Matters for serverless and scale-to-zero deployments.

### Test 9 · Resurrection under load
Spawns `PHOENIX_LOAD_WORKERS` solo-pool workers, each holding one inflight task, then
SIGKILLs them all at once (fleet-wide OOM scenario). Replacement workers are running in
parallel. Measures wall-clock from kill to each orphaned task being re-picked-up by a
replacement worker, reports p50 / p99 / first / last. Claim: p99 < 120 s.

## Customise

| Env var | Default | Description |
|---------|---------|-------------|
| `RELIER_REDIS_URL` | `redis://localhost:6379/0` | Redis URL |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama base URL |
| `BENCH_SCALE` | `standard` | Set to `scale` for the high-volume profile (see above) |
| `BENCH_BATCH_SIZE` | `30` (Ollama) / `500` (synthetic) / `10000` (scale) | Tasks for delivery-rate test |
| `BENCH_DELIVERY_KILL_CYCLES` | `1` (Ollama) / `5` (synthetic) / `10` (scale) | Worker kills during delivery test |
| `BENCH_OOM_CYCLES` | `1` (Ollama) / `5` (synthetic) / `20` (scale) | OOM kill/resurrect cycles |
| `BENCH_IDEMPOTENCY_SUBMISSIONS` | `10` (Ollama) / `50` (synthetic) / `2000` (scale) | Duplicate-dispatch count |
| `BENCH_ADMISSION_SAMPLES` | `1000` (Ollama) / `5000` (synthetic) / `50000` (scale) | Admission-control samples |
| `BENCH_OVERHEAD_SAMPLES` | `200` / `2000` (scale) | Dispatch-overhead samples |
| `BENCH_SHUTDOWN_TASKS` | `15` (Ollama) / `20` (synthetic) / `200` (scale) | Tasks per graceful-shutdown cycle |
| `BENCH_SHUTDOWN_CYCLES` | `1` (Ollama) / `3` (synthetic) / `5` (scale) | Graceful-shutdown cycles |
| `BENCH_COLD_START_TRIALS` | `3` / `10` (scale) | Cold-start latency trials |
| `BENCH_WORKER_CONCURRENCY` | `4` | Worker concurrency (Linux/Mac prefork) |
| `BENCH_SYNTHETIC_SLEEP` | `0.5` / `0.05` (scale) | Task sleep duration in synthetic mode (seconds) |
| `BENCH_OPS_MEASURE_S` | `60` | Test 7 steady-state ops measurement window |
| `BENCH_PHOENIX_LOAD_WORKERS` | `20` (Ollama) / `5` (synthetic) / `25` (scale) | Concurrent inflight tasks for Tests 7 and 9 |

Edit `bench/config.py` to change any constant directly.
