# Relier Benchmark

Validates every claim in `docs/benchmarks.md` using **real AI workloads** (Ollama) or fast
synthetic sleep tasks.  

## What it tests

| Metric | Relier claim | Vanilla Celery |
|--------|-------------|----------------|
| Task delivery rate | 100% | ~92% |
| Worker OOM recovery | 9.4 s p99 | ∞ (lost) |
| Duplicate prevention | 100% | 0% |
| Admission control p99 | < 1 ms | n/a |
| Graceful shutdown | 100% | ~60% |
| Overhead per task | +2.28 ms net | 0.85 ms baseline |

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
```

The runner manages all worker subprocesses itself; no need to start anything manually.

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

## Customise

| Env var | Default | Description |
|---------|---------|-------------|
| `RELIER_REDIS_URL` | `redis://localhost:6379/0` | Redis URL |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama base URL |
| `BENCH_BATCH_SIZE` | `30` (Ollama) / `500` (synthetic) | Tasks for delivery-rate test |
| `BENCH_WORKER_CONCURRENCY` | `4` | Worker concurrency (Linux/Mac prefork) |
| `BENCH_SYNTHETIC_SLEEP` | `0.5` | Task sleep duration in synthetic mode (seconds) |

Edit `bench/config.py` to change any constant directly.
