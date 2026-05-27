# Durability, HA, and Failure Boundaries

Relier makes one promise: **zero job loss**. Every task either completes, hands
off to another worker, or lands in the DLQ with a traceable reason. This page
explains exactly how, what each layer guarantees, what it doesn't, and where
the boundaries are.

If you operate a Relier cluster in production, read this. If you're just
writing tasks, the [Core Concepts](concepts.md) page is enough.

---

## What "zero job loss" actually means

Concretely: from the moment your producer's `await task.apush(arg)` returns
without raising, the task is guaranteed to be processed at least once, even
if the worker that picks it up dies, even if Redis briefly fails over to a
replica, even if the entire worker fleet is recycled mid-flight. The only
escape paths are:

1. **Success.** The task ran and (for idempotent tasks) its result is cached.
2. **DLQ.** The task can't be safely retried, bad payload, exceeded resurrection
   limit, hard-timed-out, or any unhandled exception. The full envelope and
   error context are preserved.

There is no third option where a task is just *gone*. Every mechanism on this
page exists to keep that promise true under specific failure modes.

---

## Layer 1, Redis persistence: `everysec` and the 1-second loss window

Relier coordinates everything through Redis: heartbeats, payloads, idempotency
locks, admission counters, SLO events. Every one of those writes goes through
Redis's persistence machinery before the cluster can be considered durable.

### What `appendfsync everysec` guarantees

The bundled `scripts/redis/redis.conf` ships:

```
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec
```

`everysec` means: Redis buffers writes in memory and calls `fsync(2)` on the
AOF file **once per second**. The OS-level guarantee is:

> If the Redis process (or the host) crashes, you lose **at most 1 second**
> of acknowledged writes that hadn't yet been fsynced.

In practice that means: if a Phoenix payload was written 500 ms before a
host crash, it might be gone after restart. Two things keep this from being a
catastrophe:

1. **Phoenix is idempotent against this.** A task whose payload was lost in
   the crash window also has no heartbeat (heartbeats live in the same Redis,
   so they died together). The producer hasn't seen its dispatch acknowledged
   yet, `apush` blocks on the broker write. If `apush` returns successfully,
   the envelope hit Redis disk within at most 1 second, with replication
   normally tightening that further (see Layer 2).

2. **The replica is fsynced independently.** Sentinel-managed replicas
   stream writes from the master and fsync on their own clock. After a
   master crash, the promoted replica usually has the last write the master
   acknowledged, even though the master's AOF didn't have time to fsync it.

### Why not `appendfsync always`?

`always` calls `fsync` on every write. It eliminates the 1-second window but
adds 1–5 ms of disk latency to every Redis operation, which on the dispatch
hot path means every `apush` blocks on a real disk write. The throughput
trade is severe; `everysec` is the standard production setting and the one
Relier validates against.

### What `everysec` does NOT protect against

- **An OS-level filesystem corruption.** AOF can be repaired with
  `redis-check-aof --fix`, but a torn write to the disk block itself needs
  recovery from the RDB or backup.
- **Disk full.** Once the AOF can't be appended, Redis stops accepting writes.
  This is also why `maxmemory-policy noeviction` is mandatory: Relier wants
  to *fail* writes, not silently drop keys.

### AOF rewrite and the `no-appendfsync-on-rewrite no` choice

Redis periodically rewrites the AOF to compact it (`auto-aof-rewrite-percentage 100`,
`auto-aof-rewrite-min-size 64mb` in the bundled config). During the rewrite,
there's a child process generating a fresh AOF while the parent keeps serving
new writes.

The relevant setting:

```
no-appendfsync-on-rewrite no
```

Two interpretations:

| Value | Behaviour during rewrite | Trade-off |
|-------|--------------------------|-----------|
| `no` (Relier's default) | Keep fsyncing every second. | Durability stays at 1s. Latency may spike if the rewrite causes I/O contention on the same disk. |
| `yes` | Skip fsyncs while a rewrite is running. | No latency spike, but a crash mid-rewrite can lose every write since the rewrite began (potentially many seconds). |

Relier picks `no` because **the whole point of the system is the durability
promise**. A latency spike during AOF rewrite is observable in your dashboards
and you can plan capacity around it (provision IOPS, put AOF on a separate
volume). Silent extension of the data-loss window is much worse.

If your dispatch latency is unacceptably noisy during rewrites, the answer is
faster disks (NVMe SSD, provisioned IOPS), not turning off durability. See
[Deployment → Tuning AOF rewrite cadence](deployment.md#tuning-aof-rewrite-cadence-for-high-throughput-deployments)
for the specific knobs.

### The fast-restart pair: AOF + RDB

The bundled config also keeps RDB snapshots:

```
save 900 1
save 300 10
save 60 10000
aof-use-rdb-preamble yes
```

The AOF gets an RDB preamble at the head, so a restart loads the RDB-format
base in microseconds and only replays the appended deltas. RDB snapshots are
also what the backup sidecar archives, see Layer 4.

---

## Layer 2, High availability with Sentinel (not Cluster) {#layer-2-sentinel}

Relier uses **Redis Sentinel** for high availability, not Redis Cluster. The
distinction matters because the two solve different problems.

| Topology | What it does | When to use |
|----------|--------------|-------------|
| **Sentinel** (used by Relier) | One logical Redis, with a master that fails over to a replica when it dies. All data on every node. | Operational HA for ≤ tens-of-GB working sets. Standard for coordination workloads. |
| **Cluster** (not used) | Sharded across many masters, each with replicas. Data partitioned by hash slot. | Sharding for hundreds-of-GB-plus working sets. |

Relier's working set is small (`rl:*` keys for in-flight tasks; large
checkpoints spill to the filesystem backend, not Redis). It fits comfortably
on one master. Sentinel gives the failover without the operational overhead
of cluster routing, MULTI/EXEC restrictions, and Lua script slot constraints, all of which Relier uses extensively.

### Sentinel quorum and failover timing

The bundled `scripts/redis/sentinel.conf`:

```
sentinel monitor relier-master relier-redis-master 6379 2
sentinel down-after-milliseconds relier-master 5000
sentinel failover-timeout relier-master 15000
sentinel parallel-syncs relier-master 1
```

- **Three Sentinels**: quorum is `2` of 3. A single isolated Sentinel cannot
  trigger a split-brain failover.
- **`down-after-milliseconds 5000`**: a master is "subjectively down" after
  5 s of no response. **This is deliberately below the 10 s default
  `RELIER_HEARTBEAT_TTL`.** It means a Redis failover completes within Relier's
  worker-death detection window, so a failover doesn't itself look like a mass
  worker death.
- **`failover-timeout 15000`**: the upper bound for the full failover cycle.
- **`parallel-syncs 1`**: replicas re-sync from the new master one at a time
  after promotion, so reads stay available.
- **`resolve-hostnames yes`**: Sentinel pins to container hostnames, not
  IPs. Container IPs change across restarts; without this, Sentinel would
  hold a stale IP forever.

### How Relier's clients use Sentinel

`relier.storage.redis.RedisManager` connects via the `Sentinel.master_for()`
client, not a direct URL. The client transparently re-resolves the current
master through the quorum on connection. When `RELIER_REDIS_USE_SENTINEL=true`,
`RELIER_REDIS_URL` is ignored, every worker, the resurrector, and the CLI
discover the master independently.

After a failover:

1. Sentinels detect master down → quorum vote → promote a replica.
2. The Redis client pool reconnects through Sentinel and gets the new master.
3. Worker tasks that were mid-flight either finish (if their writes already
   landed on the now-promoted replica) or have their heartbeats expire and
   get resurrected (if the master died holding writes that hadn't replicated
   yet).

No tasks are lost. Some may be replayed.

### Connection pool guard, why pools are per-event-loop

`RedisManager` keeps a **separate connection pool per asyncio event loop**, not
a single global pool. This is enforced by `loop_id = id(asyncio.get_running_loop())`
as the cache key.

Two reasons:

1. **Celery prefork.** When Celery forks worker processes, the child inherits
   the parent's open file descriptors, including any Redis sockets. Sharing a
   socket across processes is undefined behaviour and produces hard-to-debug
   protocol corruption. `RedisManager._get_safe_log_url`'s sibling logic
   evicts foreign-loop pools at first access in the child, so the forked
   worker can't accidentally write through the parent's pool.

2. **Async-bridge thread.** Each worker process owns a dedicated event loop
   thread (see [Architecture](architecture.md#the-asynccelery-bridge)).
   `redis.asyncio` pools are loop-affined, using a pool from a different
   loop raises `RuntimeError: Task attached to a different loop`. Per-loop
   pools make this impossible.

You will normally only see one pool per worker process: the one created
inside the persistent event loop at boot.

---

## Layer 3, Resurrection, leasing, and fencing {#layer-3-leasing-fencing}

When a worker dies, Phoenix re-queues its tasks. That sounds simple. The
complication is that "dead" is hard to define, a worker could be slow, GC-
pausing, network-partitioned, or actually dead, and from Redis's point of view
they all look the same.

### Leasing, one resurrection per task at a time

Resurrection is coordinated through a Redis lease (a key with a short TTL):

```
rl:lease:{task_id}    (180 s TTL) fence token of the current incarnation
rl:fence:{task_id}    (600 s TTL) same token, longer-lived for commit checks
```

When the resurrector decides a task needs to be replayed, it runs an atomic
Lua script (`RESURRECT_LUA`) that mints a fresh fence token and writes both
keys conditionally on the lease being free. If another resurrector already
claimed the lease, the script returns failure and this resurrector skips the
task. **Two resurrectors cannot simultaneously dispatch the same task**, even
during a race window, exactly one wins.

The lease is also a barrier against re-running the SAME resurrection too
quickly: until the 180 s TTL elapses, no new resurrection of that task can
start.

### Fencing, old workers can't commit stale results

The harder case: an old worker doesn't *know* it's been declared dead. Walk
through a scenario:

1. Worker A is running task T. It's slow (long GC pause, network blip).
2. A's heartbeat expires. The resurrector mints fence token `abc` and dispatches
   T to Worker B.
3. Worker B runs T to completion under fence token `abc`. B's commit Lua script
   verifies `rl:fence:{T} == "abc"`, yes: the result is recorded, the
   lease released.
4. Worker A wakes up from its pause and tries to commit *its* result for T.

Without fencing, A's stale result would overwrite B's correct commit (Redis
has no built-in optimistic concurrency control on hashes). With fencing:

- A's commit check looks for `rl:fence:{T}`, either it's gone (TTL expired,
  or B's commit cleared it) or it contains B's token, not A's.
- The commit Lua returns "stale" and A's result is silently discarded.

The decorator code lives in `relier/tasks/decorator.py` around the
`validate_commit` call. Search for `# FENCING CHECK (END)` to see the exact
gate.

### Resurrection counters increment only on broker ACK

A subtle point: when the resurrector re-dispatches a task, it does NOT
increment `rl:resurrections:{task_id}` immediately. The counter is incremented
**inside `_bg_send`**, only after the broker has acknowledged receipt. The
broker ACK is the durability boundary until it lands, the task is still
sitting in the expiry index and will be retried on the next scan.

This prevents a network partition between the resurrector and the broker from
inflating the count and falsely quarantining a healthy task. Sequence:

1. Scan finds expired heartbeat → resurrection counter is `n`.
2. `RESURRECT_LUA` mints fence token, lease acquired.
3. Background `send_task` attempts to publish to `re-queue`.
4a. **If broker ACK succeeds:** counter `INCR` to `n+1`, expiry index entry
    removed, monitor entry set. Done.
4b. **If broker ACK fails:** counter unchanged, expiry index entry left in
    place, lease eventually expires (180 s TTL), task is retried on a future
    scan.

The lease guarantees at-most-one resurrection per task per 180 s; the
broker-ACK gate guarantees the counter is honest about successful replays.

---

## Layer 4, Backup strategy (BGSAVE on a replica)

The bundled `docker-compose.prod.yml` includes a `redis-backup` sidecar that
runs `scripts/redis/backup.sh` continuously.

### What it does

```
Every BACKUP_INTERVAL_SECONDS (default: 3600 = 1 hour):
  1. Connect to a REPLICA (default: relier-redis-replica-1)
  2. Snapshot LASTSAVE timestamp
  3. Send BGSAVE
  4. Poll LASTSAVE until it advances (or 120 s timeout)
  5. Gzip /data/dump.rdb → /backups/relier-redis-{ts}.rdb.gz
  6. If BACKUP_S3_BUCKET set: aws s3 cp to S3
  7. Rotate: keep newest BACKUP_RETENTION archives (default: 168 = 7 days)
```

### Why backup the replica, not the master

`BGSAVE` forks the Redis process and the child writes the full dataset to
disk. On the master, that fork pauses request handling briefly (copy-on-write
page setup) and creates a temporary 2× memory spike both of which hit
production latency. The replica has no live write load, so the fork is cheap.

The replica is fed via async replication from the master, so the snapshot is
within ~1 RTT of master state. For backup purposes that's identical to a
master snapshot, without the latency cost.

### Restoring from backup

The backup is an RDB file, Redis's native dump format. To restore:

1. Stop all writers.
2. Stop the Redis cluster.
3. Unzip the chosen archive: `gzip -d relier-redis-XXX.rdb.gz`.
4. Replace the master's `/data/dump.rdb` with the restored file.
5. Remove the AOF (`/data/appendonly.aof*`), Redis prefers AOF over RDB on
   startup, so leaving it stale will undo the restore.
6. Start the master. It loads the RDB, then replicas resync from it.
7. Re-enable workers.

Relier tasks that were in flight at the time of the restored snapshot will
re-appear in the expiry index. The resurrector picks them up and replays them
through the recovery queue. Some duplicates are possible (idempotent tasks
return cached results; non-idempotent tasks run again).

### Offsite copies

Set `BACKUP_S3_BUCKET` and the sidecar will additionally `aws s3 cp` each
archive to S3-compatible storage (the image must include the `aws` CLI; the
default Redis image does not bake your own or use a sidecar image with `awscli`).

---

## Layer 5, Thundering-herd defences {#layer-5-thundering-herd}

When 100 workers die simultaneously (Kubernetes node failure, AWS AZ outage),
naively resurrecting all their tasks would flood the broker and overwhelm
whatever fresh worker pool comes online. Relier has three brakes:

### Brake 1, Bounded scan batch

```
RELIER_RESURRECTION_BATCH_SIZE = 1000   # default
```

A single scan pass replays at most 1000 tasks. The next pass runs after
`RELIER_RESURRECTION_CHECK_INTERVAL` (default 2 s), so a 10 000-task mass
failure replays over 10 batches across 20 s, not in one burst.

### Brake 2, Recovery-queue backpressure

```
RELIER_RESURRECTION_MAX_QUEUE_DEPTH = 10000   # default
```

Before each scan, the resurrector checks `LLEN re-queue`. If the recovery
queue already holds at least this many messages, the scan is **deferred**,
expired tasks stay in the expiry index and get picked up on a later pass.
This stops the resurrector from outrunning the workers draining the recovery
queue.

When you see "Resurrection scan deferred: recovery queue backlog too deep" in
the logs, that's brake 2 holding the line.

### Brake 3, Per-task requeue delay

```
RELIER_RESURRECTION_REQUEUE_DELAY = 0.05   # seconds, default
```

After each scan-and-batch-dispatch cycle, the resurrector sleeps 50 ms before
the next scan. Small, but enough to prevent CPU-pinning the resurrector
during sustained failure storms.

### Brake 4, Bounded concurrent broker submissions

In `PhoenixRegistry`:

```python
_send_semaphore = asyncio.Semaphore(50)
```

Caps how many background `send_task` coroutines can be talking to the broker
simultaneously. Without this, a batch of 1000 resurrections would fan out 1000
concurrent broker writes, overwhelming the broker's connection pool and any
upstream proxies.

### What you should monitor

When a real incident happens, watch:

```
# Recovery queue depth: brake 2 keeps this bounded
redis-cli LLEN re-queue

# How far behind the expiry index is: should drain across passes
redis-cli ZCARD rl:phoenix:expiry_index

# Per-task resurrection counts: if any approach max_resurrections,
# poison-pill quarantines are coming
rl tasks inspect <task_id>
```

A healthy recovery shows the expiry index draining smoothly while `re-queue`
stays bounded around `max_queue_depth`. An unhealthy one shows the expiry
index growing, which means workers aren't pulling fast enough.

---

## Failure-mode summary table

| Failure | What guarantees survival |
|--------|---------------------------|
| Worker SIGKILL'd mid-task | Heartbeat expires → Phoenix re-queues. Idempotency stops duplicate side effects. |
| Worker OOM-killed | Same as SIGKILL. |
| Worker SIGTERM'd (deploy) | Graceful drain, tasks that won't finish in time have heartbeat deleted → Phoenix re-queues elsewhere. |
| Redis master crashes | Sentinel quorum promotes a replica within ~5 s. Clients reconnect; in-flight tasks either finish or resurrect via the new master. |
| Whole Redis cluster crashes | AOF restart loads up to 1 s prior to crash. Tasks in that 1 s window with no broker ACK simply weren't dispatched. Tasks dispatched but unacknowledged on the producer side are lost but their producer never got a success from `apush`, so the producer knows to retry. |
| Single host loses disk | Sentinel fails over to a replica on another host. Hourly RDB backups protect against multi-host disk loss. |
| Resurrector process dies | The resurrection-lock TTL (30 s) lets a replacement resurrector pick up where the dead one left off. Don't run two resurrectors as a "redundancy" measure, one is enough; running two doubles the scan load without improving safety. |
| Multiple resurrectors scanning the same task | Distributed lock (`rl:lock:resurrect:{task_id}` with `NX EX 30`) ensures exactly one wins. |
| Worker comes back from a GC pause and tries to commit a result Phoenix already replayed | Fence token mismatch → commit rejected. |
| Bad payload from a tampered broker message | Checksum mismatch → DLQ with `PayloadIntegrityError`. Never executes. |
| Schema-version skew during rolling deploy | Registered migration runs first; if no migration exists, task DLQ's with `SchemaMigrationError`. |
| Mass failure floods resurrector | Batch size + recovery-queue depth + send semaphore + requeue delay all cap throughput. |
| Producer flood | Admission control rejects with `Retry-After`. |
| Checkpoint too large to store inline | `CheckpointTooLargeError`: loud, not silent. Configure filesystem backend if you need bigger checkpoints. |

---

## What Relier does NOT protect against

Honest list:

- **A bug in your task code.** If your task does the wrong thing, idempotency
  caches the wrong result. Test your tasks.
- **An external service mis-honouring idempotency.** Stripe, your DB,
  whatever you call, Relier guarantees exactly-once on its side, but if the
  downstream system charges twice from a retry, that's a downstream bug.
- **Total data-centre loss without offsite backups.** Enable
  `BACKUP_S3_BUCKET` if you care about this.
- **Clock skew between Sentinels.** Use NTP. Sentinel's quorum depends on
  reasonably synchronised clocks.
- **Split-brain Redis from a partitioned network where one side has a quorum
  Sentinel and a master.** This is the same constraint every quorum-based
  system has. Three Sentinels in three failure domains is the standard
  defence.

Everything else listed in the failure-mode table is covered. If you find a
failure mode that isn't, write it up as an issue. The whole point of the
project is that the list of "things that silently lose work" is empty.
