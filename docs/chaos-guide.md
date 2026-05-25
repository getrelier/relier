# Chaos Guide

Chaos engineering is the practice of deliberately breaking things to confirm your system can survive them. Relier ships a built-in chaos suite, a set of scenarios that trigger real failures against your running cluster so you can verify that the guarantees hold before you find out in production.

!!! warning "Run against a non-production cluster"
    The chaos commands trigger real worker kills, network partitions, and queue floods. Always run them against a dedicated test cluster, not your production environment.

---

## How it works

The `rl chaos` commands talk to the chaos engine (`relier.chaos.engine`), which is a thin dispatcher that maps scenario names to registered scenario implementations. Each scenario lives in `relier/chaos/` and is automatically registered when the chaos package is imported.

The commands are designed to be composed:

```bash
# Seed a task, kill a worker, and watch Phoenix resurrect it
rl chaos worker-kill --seed --watch --watch-duration 60
```

---

## Scenarios

### `rl chaos worker-kill`, Kill a worker process

The most fundamental chaos test. Terminates a Celery worker with SIGKILL (not SIGTERM, this is an unclean death with no graceful shutdown). The goal is to confirm Phoenix resurrects any task the worker was running.

```bash
rl chaos worker-kill [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--worker` | random | Specific worker container to kill. Omit to kill a random worker. |
| `--seed` | `false` | Dispatch a long-running task before the kill so Phoenix has something to resurrect. |
| `--seed-duration` | `30` | How long the seeded task sleeps (seconds). Must be long enough that it's still running when the kill fires. |
| `--watch` | `false` | Stream Phoenix resurrection events after the kill. |
| `--watch-duration` | `30` | How long to stream resurrection events (seconds). |

**The full test: seed, kill, watch:**

```bash
rl chaos worker-kill --seed --watch --watch-duration 60
```

You should see output like:

```
SEED  Dispatched 30s long-running task. marker=chaos-kill-seed-a3f9c1
CHAOS Worker terminated.
WATCH Streaming resurrection events for 60s...
  -> task_abc123: RESURRECTED (awaiting pickup)
  -> task_abc123: ALIVE (revived by replacement worker)
WATCH Done. 1 task(s) observed in monitor.
```

**What to verify:**

- The task appears as RESURRECTED within `RELIER_HEARTBEAT_TTL + RELIER_RESURRECTION_CHECK_INTERVAL` seconds (default: 12s).
- The task appears as ALIVE once a healthy worker picks it up.
- No duplicate execution. If the task is idempotent, the result should be the same as if it ran normally.

**What could go wrong:**

| Symptom | Likely cause |
|---------|-------------|
| Task never appears as RESURRECTED | Guardian (resurrector) is not running |
| Task is RESURRECTED but never ALIVE | No healthy workers are available to pick it up |
| Task appears twice in your output | Heartbeat TTL is set shorter than your resurrection check interval, reduce `RELIER_RESURRECTION_CHECK_INTERVAL` |

---

### `rl chaos network-partition`, Isolate Redis from workers

Simulates a network partition between the workers and Redis for a fixed duration. This tests two things: that workers handle Redis unavailability gracefully (they should), and that they recover correctly when connectivity is restored.

```bash
rl chaos network-partition [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--secs` / `--duration` | `15` | Duration of the outage in seconds. |

```bash
rl chaos network-partition --secs 20
```

**What to verify:**

- Workers log Redis connection errors but do not crash.
- Heartbeats resume once connectivity is restored (heartbeat TTL is 10s by default, heartbeats should refresh within one TTL window of connectivity returning).
- Tasks that were running during the partition are either completed normally or resurrected by Phoenix after the heartbeat TTL expires.
- Admission control fails open. If Redis is unreachable, `apush()` should still admit requests rather than throwing an error.

**What could go wrong:**

| Symptom | Likely cause |
|---------|-------------|
| Worker crashes during partition | Worker code has unhandled Redis exceptions, check error handling in task code |
| Tasks are double-executed after recovery | Heartbeat TTL is shorter than the partition duration, tasks looked dead to the resurrector, which re-queued them before the partition ended |

**Tip:** If your `RELIER_HEARTBEAT_TTL` is 10s and your partition is 20s, expect Phoenix to resurrect the running tasks during the partition. This is correct behavior, after 10s with no heartbeat, the task looks dead. Set partition duration below `RELIER_HEARTBEAT_TTL` to test recovery without resurrection.

---

### `rl chaos load-spike`, Flood the dispatch path

Generates a burst of dispatch requests to exercise admission control. Tasks are sent via `apush()` so the full admission control path runs.

```bash
rl chaos load-spike [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--rps` | `100` | Requests per second target. |
| `--duration` | `10` | Duration of the spike in seconds. |

```bash
rl chaos load-spike --rps 2000 --duration 10
```

Output:

```
CHAOS Load spike of 2000 RPS finished — accepted=5000 rejected=14983 errored=0
```

**What to verify:**

- When `--rps × --duration > RELIER_ADMISSION_LIMIT`, you should see rejections in the output.
- `AdmissionRejectedError` is raised for the rejected requests, your API layer should return HTTP 429.
- Workers are not overloaded by tasks that slipped through, queue depth (`rl tasks inflight`) should stabilise once the spike ends.
- No `errored` count, errors indicate exceptions outside of admission control (Redis unavailable, etc.).

**Checking admission status during the spike:**

```bash
rl admission status
```

```
Admission Control Status

Status: SHEDDING (5001/5000, 100.0%)
Window: 10s
```

**What could go wrong:**

| Symptom | Likely cause |
|---------|-------------|
| No rejections even above the limit | Admission control is not enabled or Redis is unreachable (fails open) |
| High `errored` count | Unhandled exception in dispatch path, check `rl cluster logs` |
| Workers overwhelmed after spike | `RELIER_ADMISSION_LIMIT` is too high for your current worker capacity |

---

### `rl chaos task-corrupt`, Inject a poison pill

Injects a malformed task envelope directly into the queue, one with a bad checksum. When a worker picks this up, the payload integrity check fails and the task is sent to the DLQ immediately, without executing.

```bash
rl chaos task-corrupt
```

Output:

```
CHAOS Poison pill injected — expect a DLQ quarantine.
```

**What to verify:**

- The corrupted task appears in `rl dlq list` within a few seconds.
- The DLQ entry has `reason: PayloadIntegrityError`.
- No task code was executed, a corrupted payload must never run.
- The rest of the cluster continues processing normally, one bad task should not affect other workers.

```bash
# After running task-corrupt:
rl dlq list
```

```
ID              TASK               RESURRECTIONS  QUARANTINED_AT       LAST_ERROR
──────────────────────────────────────────────────────────────────────────────────
task_f8a2b1     (corrupted)        0/5            2026-05-20 14:22     PayloadIntegrityError
```

**What could go wrong:**

| Symptom | Likely cause |
|---------|-------------|
| Task is not in DLQ | Integrity check is not running, verify `PayloadIntegrityError` is raised in the worker execution path |
| Worker crashes instead of quarantining | Unhandled exception, integrity errors should be caught and routed to DLQ |

---

### `rl chaos slow-task`, Trigger timeout enforcement

Dispatches a task that sleeps for longer than the configured `hard_timeout`. This exercises the full timeout path: soft timeout fires, cleanup hook runs (if configured), hard timeout fires, task is cancelled, and the task ends up in the DLQ.

```bash
rl chaos slow-task [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--duration` | `35` | How long the task will sleep. Must exceed `RELIER_HARD_TIMEOUT` (default: 30s). |

```bash
rl chaos slow-task --duration 60
```

Output:

```
CHAOS Slow task (60s) dispatched. marker=chaos-slow-abc123
```

**What to verify:**

- If `RELIER_SOFT_TIMEOUT` is set (default: 25s), the soft timeout fires and any cleanup hook runs.
- At `RELIER_HARD_TIMEOUT` (default: 30s), the task is unconditionally cancelled.
- The task appears in `rl dlq list` with `reason: HardTimeoutError`.
- The worker that ran the task is still alive and accepting new work, a timed-out task must not bring down the worker.

```bash
# Watch for the quarantine:
rl tasks inflight --follow
# Then:
rl dlq list
```

**What could go wrong:**

| Symptom | Likely cause |
|---------|-------------|
| Task is not cancelled after `hard_timeout` seconds | Hard timeout is not configured, check `RELIER_HARD_TIMEOUT` |
| Worker stops accepting tasks after the slow task | Hard timeout is killing the worker process instead of cancelling the coroutine, review async cancellation handling |
| Task is resurrected instead of going to DLQ | `hard_timeout` is shorter than `RELIER_HEARTBEAT_TTL`, the heartbeat expires before the timeout fires, so Phoenix sees a "dead worker" |

---

## Composing chaos tests

These scenarios are more useful in combination. Some things worth testing:

### Can Phoenix handle multiple simultaneous worker deaths?

```bash
# Scale to 4 workers first
rl cluster scale 4

# Kill them all and watch
rl chaos worker-kill --seed --seed-duration 60 --watch --watch-duration 120
rl chaos worker-kill --seed --seed-duration 60
rl chaos worker-kill --seed --seed-duration 60
```

Check that all seeded tasks are resurrected, and that the resurrection batch size (`RELIER_RESURRECTION_BATCH_SIZE`) doesn't become a bottleneck.

### Does admission control hold during a load spike after a worker kill?

```bash
# Kill a worker to reduce capacity
rl chaos worker-kill

# Immediately spike load
rl chaos load-spike --rps 1000 --duration 30

# Check that rejection is happening at the right rate
rl admission status
```

### Does idempotency survive a resurrection?

If a task is `idempotent=True` and gets resurrected:

1. The resurrected task should see `IN_FLIGHT` from the original execution (if the original worker is still alive) and retry with backoff.
2. Or the original worker died, the in-flight sentinel expired, and the resurrected task claims the key and runs normally.

Check `rl dlq list`, you should see no duplicate completions.

---

## Watching the cluster during chaos

These commands are useful to run alongside chaos scenarios:

```bash
# Live view of running tasks (refreshes every 2s)
rl tasks inflight --follow

# SLO burn rate — goes up during chaos
rl slo status

# DLQ — tasks that didn't survive
rl dlq list

# Worker status — which workers are alive
rl worker status

# Admission control pressure
rl admission status
```

---

## Interpreting results

After any chaos run, the three questions to answer:

1. **Did any tasks get lost?** Compare the tasks you seeded with `rl tasks inflight` + `rl dlq list`. Every task should either complete, be quarantined with a traceable reason, or be in progress.

2. **Did the cluster recover to a healthy state?** After the chaos ends, `rl slo status` burn rate should return to under 1x, and `rl worker status` should show all workers back online.

3. **Were any tasks duplicated?** If your tasks have side effects (charges, emails, writes), check your external system for duplicate records. Relier's idempotency prevents this for `idempotent=True` tasks, but non-idempotent tasks can be executed twice if resurrected.
