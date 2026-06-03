# Known Limitations

Known coordination / scaling constraints. Not blocking for most production
workloads — the bench
shows per-task steady-state Redis cost is below measurement noise and
capacity scales with task turnover rate, comfortable to ~1,000 tasks/sec
end-to-end on single-master Redis (see `docs/benchmarks.md` § "Scaling
ceiling and per-task coordination cost"). These ship when a customer
hits a workload shape that needs them.

## Issue — Worker-level heartbeats instead of per-task

### What's there today

`PhoenixRegistry._refresh_loop` refreshes one heartbeat per active task on
`heartbeat_ttl / 2`. At the default `heartbeat_ttl=10`, every inflight task
issues `EXPIRE` + `ZADD` every 5 seconds — exactly 0.4 ops/sec/task in
isolation, below the bench noise floor at v0.1 scale.

### Why it matters

Per-task heartbeats create one liveness key per inflight task. At 100k
inflight that's 100k ephemeral keys — Redis memory, not ops. Two orders of
magnitude fewer keys are possible if liveness is tracked per-worker.

### What changes (sketch)

- Add `rl:worker:{worker_id}:hb` — single key per worker, refreshed by the
  worker process
- Add `rl:worker:{worker_id}:tasks` — hash of tasks the worker owns
- Resurrector scans dead **workers** then enumerates the worker's task set,
  instead of scanning per-task expiry
- Design doc lives on the `feature/worker-level-heartbeats` branch

### Honest ROI

**Updated after high-scale bench (10k tasks × 0.05s, 2026-06-03).**

The original estimate ("throughput win is effectively zero") was wrong at
high concurrency with short tasks. The 10k run recorded CPU avg 43.2%
(Relier) vs 1.2% (vanilla). Root causes, in order of impact:

1. **Asyncio background task storm.** Every in-flight task spawns a
   background coroutine refreshing every `heartbeat_ttl / 2 = 5s`. At
   10,000 concurrent 50ms tasks the event loop schedules and cancels 10,000
   coroutines per cycle — almost none fire before the task completes. The
   scheduling overhead dominates. Worker-level heartbeats collapse this to
   one coroutine per worker.

2. **Resurrection scanner is O(n_tasks).** The scanner reads a ZSET with
   one entry per in-flight task. At 10k inflight it's scanning 10,000
   entries every 2s. Worker-level heartbeats make it O(n_workers).

3. **System-wide CPU measurement.** `monitor.py` uses
   `psutil.cpu_percent(interval=None)` — includes the Redis server. 150k
   Redis ops in a short window adds visible server-side CPU.

At realistic production task durations (>100ms) the background coroutine
completes at least one refresh cycle and the cost is amortised — this is
why the 500-task × 0.5s bench showed only 2.4% vs 0.6%. The problem
surface is high-throughput + short tasks.

Expected improvement after worker-level heartbeats: 60–75% reduction in
the CPU gap at 10k × 0.05s. Dispatch costs (admission Lua, SHA-256
envelope, idempotency ops) remain O(n_tasks) and can't be batched.

The keyspace win remains: 500× fewer liveness keys at 100k inflight.

### Cost

2–3 weeks of careful work (revised up from "3–5 days" — the 10k bench
revealed the asyncio coroutine storm, which means the refresh loop design
needs rethinking, not just re-keying). Touches `phoenix.py`,
`decorator.py`, the resurrector scanner, and 6+ test files. Highest
correctness risk of any fix on this list — fence tokens and the zombie
worker protocol must be preserved exactly.

Target: v0.2. Trigger: any customer reporting high CPU at >1k tasks/sec
with sub-100ms tasks, or keyspace ceiling hit at 100k+ inflight.

---

## Issue — Sharded expiry index

### What's there today

`RedisKeys.phoenix_expiry_index()` is a single global ZSET. Every
`register`, `_refresh_loop`, `complete`, and `_scan_and_resurrect` hits
it. Under Redis Cluster, all of that traffic lands on one shard regardless
of how many masters you have.

### Why it matters

Pairs with Issue #4 (hash tags, shipped in v0.1.3) to make Relier
horizontally scalable. Without this, a customer running Redis Cluster
still has a single hot key gating resurrection.

### What changes

Split into `N` buckets keyed by `hash(task_id) % N`:
- `rl:phoenix:expiry_index:{0}` ... `rl:phoenix:expiry_index:{N-1}`
- Resurrector fans out across shards in a pipeline

### Cost

1–2 days. Mechanical change in `RedisKeys` + `phoenix.py` scanner.
Acceptance test: 3-node Redis Cluster runs the bench suite cleanly.

---

## Issue — Adaptive heartbeat TTL

### What's there today

`Settings.heartbeat_ttl` is process-global. A 30-minute ETL task refreshes
its heartbeat 360 times. The first 359 of those carry no information.

### What changes

- After N successful refreshes, scale the refresh interval up to a
  configurable ceiling (e.g. 60s for stable long-running jobs)
- Allow `@rl_task(heartbeat_ttl=...)` per-task override

### Cost

Half a day. Narrow benefit — mostly helps long-running ETL workloads.
Skip unless a customer asks.

---

## Issue — Split observability Redis

### What's there today

`rl:m:global:*`, `rl:m:w:*`, `rl:slo:*:*`, `rl:task_durations` are served
by the same Redis as leases, fences, and heartbeats. During a traffic
spike, a flood of SLO bucket increments contends with the same main
thread that's trying to acquire resurrection leases.

### What changes

Add `RELIER_REDIS_METRICS_URL` env var so metrics + SLO writes can go to
a separate Redis instance. Correctness-critical writes stay on the
primary. Or move SLO/metrics out of Redis entirely → Prometheus (already
wired via OTel).

### Cost

Half a day for the config option; cleaner long-term is the Prometheus
migration.

---

## Issue — SLO bucket key cleanup

### What's there today

`RedisKeys.slo_bucket(status, bucket)` creates one key per `(status,
time_bucket)` pair. Each bucket has a TTL but during the window the
keyspace grows linearly with traffic and status diversity.

### What changes

Either:
1. Move SLO/metrics out of Redis entirely → Prometheus
2. Collapse `slo_bucket` into a single hash per window: `rl:slo:{bucket}`
   with one field per status

### Cost

Single-PR fix for option 2. Option 1 is the long-term move and pairs with
the split-observability-Redis item above.
