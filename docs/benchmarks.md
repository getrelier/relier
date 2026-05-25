# Benchmarks

Every number on this page was produced by the `bench/` test suite, a standalone application that runs against live Redis and measures each Relier claim against an equivalent vanilla Celery setup.

Results below are from Linux (Docker, prefork pool) with synthetic 0.5 s tasks.
Run it yourself: `docker compose -f docker-compose.bench.yml up --build`

## Results

| Metric | Relier 0.1 | Vanilla Celery | Verified |
|--------|-------------|----------------|----------|
| Task delivery rate (500 tasks, 5 kills) | **100%** | 92.0% | ✓ |
| Worker OOM recovery (5 cycles) | **7.3 s avg · 9.4 s p99** | ∞ lost | ✓ |
| Dual-OOM (2 in-flight tasks, 1 kill) | **2/2 recovered · 7.5 s** | both lost | ✓ |
| Duplicate prevention (50 submissions) | **1/50 ran** | 50/50 ran | ✓ |
| Admission control p99 | **0.763 ms** (p99.9 1.44 ms · max 1.72 ms) | n/a | ✓ |
| Graceful shutdown (3 cycles) | **100%** | 0% | ✓ |
| Overhead per task (200 dispatches) | **2.28 ms** net (p99 5.17 ms) | 0.85 ms baseline | ✓ |
| Worker RAM (idle) | **340.4 MB** (+103.9 MB vs vanilla) | 236.5 MB | — |
| Redis per in-flight task | **2,008 bytes** (12 keys) | 0 bytes | — |
| File descriptor leak | **Δ 0** (stable) | n/a | — |

Tested on: Linux (Docker, python:3.11-slim-bookworm), Redis 7.2 with AOF + noeviction, Celery prefork pool, BENCH_WORKER_CONCURRENCY=4.

---

## What each test measures

### Task delivery rate

Dispatches 500 tasks (each sleeping 0.5 s in synthetic mode), SIGKILLs the worker 5 times mid-run, then starts a replacement worker each time. Counts total completions.

- **Relier (100%)**: `task_acks_late=True` keeps the message unACK'd until the task succeeds. Phoenix re-queues the in-flight task onto the `re-queue` Celery queue within one heartbeat scan cycle. The replacement worker drains it.
- **Vanilla (92.0%)**: `task_acks_late=False` ACKs on pickup. Each kill loses the one task mid-execution. 8 tasks dropped across 5 kills; the rest survive in the queue.

The ~8% loss is structural the consequence of default Celery ACK semantics. At 10M tasks/day this is 800,000 lost tasks.

### Worker OOM recovery

Dispatches a long-running task, waits 4 s for it to start, SIGKILLs the worker, starts a replacement alongside the Phoenix resurrector. Repeated 5 times.

- **Relier (7.3 s avg · 9.4 s p99)**: Phoenix detects the stale heartbeat within one scan cycle and re-queues the orphaned task onto `re-queue`. The replacement worker picks it up. All 5 cycles recovered.
- **Vanilla (lost)**: No heartbeat, no resurrector. Task is gone.

#### Dual-OOM variant

Dispatches 2 tasks to the same worker simultaneously, kills the worker with both in-flight. Both are independently detected and resurrected by Phoenix.

- **2/2 recovered · 7.5 s detection**: Phoenix handles overlapping orphans correctly. Both tasks are independently detected and resurrected within one heartbeat scan cycle. ✓ < 45 s claim.

### Duplicate prevention

Dispatches the same `doc_id` 50 times in rapid succession with `idempotent=True`.

- **Relier (1/50 ran)**: The first dispatch acquires the idempotency slot and executes. The remaining 49 are deduplicated at admission via an atomic Lua check; they return immediately without spawning work.
- **Vanilla (50/50 ran)**: No dedup. All 50 dispatches execute. In a real pipeline: 50× GPU cost + 50 duplicate vectors in your store.

### Admission control latency

Runs 5,000 consecutive admission checks (the atomic Lua script Relier executes on every `push()`) and measures latency.

| | avg | p95 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| Linux (Docker) | 0.316 ms | 0.546 ms | 0.763 ms | 1.44 ms | 1.72 ms |

The claim is p99 < 1 ms, comfortably met. The p99.9 (1.44 ms) and max (1.72 ms) include cold-start outliers from the first samples before the Lua script is cached by Redis.

### Graceful shutdown

Dispatches 20 tasks (0.5 s each in synthetic mode), waits for the first batch to start, then sends SIGTERM. Repeated 3 cycles.

- **Relier (100% all cycles)**: The worker finishes its in-flight tasks, hands unstarted tasks back to Phoenix on the `re-queue` queue, then exits cleanly. Zero work lost.
- **Vanilla (0%)**: SIGTERM with prefork pool drops tasks mid-execution immediately. Tasks still in the broker queue survive, but in-flight tasks are gone.

### Overhead per task

Dispatches 200 no-op tasks with `apush()` and 200 with vanilla `.delay()`.

| | avg | p50 | p95 | p99 |
|---|---|---|---|---|
| Relier | 3.13 ms | 1.62 ms | 2.13 ms | 5.17 ms |
| Vanilla | 0.85 ms | 0.80 ms | 0.98 ms | 1.24 ms |
| **Net overhead** | **2.28 ms** | — | — | — |

The 2.28 ms average overhead covers: atomic admission check + SHA-256 envelope wrap + heartbeat registration. On any task that does real work (a DB query, an HTTP call, an AI inference), this is invisible.

### Worker RAM and Redis overhead

**Worker RAM (idle)**

A Relier worker uses ~340 MB RSS at idle vs ~236 MB for vanilla: a delta of +104 MB. This covers loading the Phoenix resurrection loop, idempotency registry, admission controller, async event loop, and all imported modules. The cost is paid once per worker process, not per task.

**Redis per in-flight task**

While a task is executing, Relier writes 12 Redis keys totalling ~2,008 bytes (heartbeat, idempotency slot, task state, fence tokens, queue registrations). Vanilla writes nothing. At 10,000 concurrent tasks this is ~20 MB of additional Redis working set: negligible on any modern Redis deployment.

**File descriptor stability**

Open file descriptors: 195 at worker idle → 195 after task completion (Δ = 0). No leak detected. The reliability stack does not accumulate file handles across task executions.

---

## How to reproduce

**Docker (recommended — Linux prefork, isolated Redis, Grafana included):**

```bash
# Default: 500 tasks, synthetic 0.5 s tasks, 5 OOM cycles
docker compose -f docker-compose.bench.yml up --build

# Scale to 10k tasks
BENCH_BATCH_SIZE=10000 docker compose -f docker-compose.bench.yml up --build

# Scale to 100k tasks
BENCH_BATCH_SIZE=100000 BENCH_WORKER_CONCURRENCY=8 \
  docker compose -f docker-compose.bench.yml up --build
```

While the bench is running, open Grafana at http://localhost:3001 (admin / bench) to watch queue depth, task completion rate, and Phoenix resurrections in real time.

### What you'll see

**Mid-run**: queue depth spikes as 500 tasks are dispatched and SIGKILL cycles fire, the Task Completion Rate panel shows Relier and Vanilla diverging in real time, and the Resurrections counter steps up once per kill as Phoenix detects each stale heartbeat.

![Bench dashboard mid-run](assets/images/screenshot-1.png)

**End of run**: Redis Clients drops to 1 (all workers exited cleanly), the Task Completion Rate lines have settled showing the final Relier vs Vanilla gap, Resurrections holds its final count, and Redis memory is flat at baseline, no accumulation across the full test suite.

![Bench dashboard end of run](assets/images/screenshot-2.png)

Note: the `re-queue` spike during each SIGKILL is sub-second faster than the 5s dashboard refresh so it doesn't appear as a visible spike in the queue depth graph. What you see instead is the Relier completion line never flattening, because orphaned tasks are already back on a worker before the next scrape.

**Local (Ollama, real AI workloads):**

```bash
uv sync
uv pip install psutil rich
python -m bench.bench          # ~15 min, requires Ollama + nomic-embed-text + gemma3:4b
python -m bench.bench --synthetic  # ~20 min, no GPU required
```

---

## Platform notes

| | Linux / Docker (prefork) | Windows (solo pool) |
|--|--------------------------|---------------------|
| Admission control p99 | **0.763 ms** | ~1.6 ms (loopback overhead) |
| Dispatch overhead net | **2.28 ms** | ~1.4 ms extra |
| Vanilla graceful shutdown | 0% (in-flight tasks lost) | 0% (SIGTERM immediate) |
| Concurrency | True parallel workers (prefork) | Sequential (1 task at a time) |
| OOM detection avg | **7.3 s** | ~8–12 s |

Windows TCP loopback adds ~0.6–1.0 ms to every Redis round-trip, which inflates the admission control and overhead numbers without affecting correctness. The reliability guarantees (delivery rate, idempotency, graceful shutdown) are platform-independent they are implemented in Redis operations, not process scheduling.

The vanilla graceful shutdown figure (0% Linux) reflects the prefork pool's behaviour: tasks still in the broker queue survive SIGTERM, but the task actively executing in a worker subprocess at signal time is dropped. Relier's drain phase prevents this.
