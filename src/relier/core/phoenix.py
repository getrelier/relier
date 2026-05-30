"""
Relier Core — Phoenix Task Resurrection.

Implements the Shadow Registry pattern for crash recovery and automatic
task resurrection.

Each active task maintains a Redis heartbeat with a short TTL. If a
worker crashes, is OOM-killed, or disappears unexpectedly, the heartbeat
expires and the resurrector loop safely re-queues the task on another
worker.

Design notes
------------
Heartbeat refresh
    Each task owns a background asyncio refresh coroutine that extends
    its heartbeat TTL until completion.

Redis scanning
    Resurrection discovery uses cursor-based ``SCAN`` iteration rather
    than ``KEYS`` to avoid blocking Redis during large keyspace scans.

Distributed resurrection lock
    A short-lived ``SET NX`` lock prevents multiple resurrector
    processes from concurrently replaying the same task.

Closure safety
    Background send operations bind task state explicitly to avoid
    Python late-binding closure bugs during asynchronous scheduling.
"""

import asyncio
import contextlib
import json
import logging
import secrets
import time
from typing import Any

from celery import Celery
from redis.asyncio import Redis

from relier.config import Settings, get_settings
from relier.core.checkpoint import CheckpointStore
from relier.core.dlq import DeadLetterQueue
from relier.core.keys import RedisKeys
from relier.storage.lua.scripts import (
    CLEANUP_LUA,
    COMMIT_CHECK_LUA,
    RESURRECT_LUA,
    VALIDATE_LUA,
)
from relier.storage.redis import get_relier_redis
from relier.telemetry.metrics import resurrection_time_s, resurrections_total
from relier.telemetry.spans import ATTR_TASK_ID, ATTR_TASK_NAME, tracer

logger = logging.getLogger(__name__)

# Celery queue that resurrected tasks are replayed onto, drained by the
# dedicated recovery worker pool.
_RECOVERY_QUEUE = "re-queue"


class PhoenixRegistry:
    """
    Coordinates Phoenix task lifecycle tracking and crash recovery.

    Responsible for:
    - heartbeat ownership
    - task state persistence
    - worker death detection
    - automatic resurrection
    - partial-state recovery
    """

    # Tracks the background heartbeat refresh task for each active execution.
    #
    # Intentionally process-local and class-level so any coroutine within the
    # worker can cancel refresh ownership during task completion.
    #
    # Safe because each Celery worker process owns a single asyncio event loop.

    _active_loops: dict[str, "asyncio.Task[None]"] = {}

    # Bound concurrent resurrection broker submissions to avoid unbounded
    # background task fan-out during mass worker failures.
    _send_semaphore = asyncio.Semaphore(50)

    @classmethod
    def _get_settings(cls) -> Settings:
        """
        Resolve settings lazily at runtime.

        Avoids capturing stale configuration during module import, which is
        important for dynamically provisioned test environments.
        """
        return get_settings()

    # ==========================================================================
    # Task registration & heartbeat ownership
    # ==========================================================================

    @classmethod
    async def register(
        cls,
        task_id: str,
        worker_id: str,
        payload: dict[str, Any],
    ) -> None:
        """
        Register a task in the Phoenix shadow registry.

        Registration persists enough execution metadata for:
        - worker death detection
        - resurrection replay
        - checkpoint recovery
        - operational inspection

        A background heartbeat refresh coroutine is started immediately after
        registration succeeds.
        """
        settings = cls._get_settings()
        redis = await get_relier_redis()

        # Store execution metadata in a single Redis hash so resurrection state,
        # checkpoints, and worker ownership remain co-located.
        task_key = RedisKeys.phoenix(task_id)

        # Calculate when this task's heartbeat will expire
        now = time.time()
        expiry_timestamp = now + settings.heartbeat_ttl

        pipe = redis.pipeline()
        pipe.set(
            RedisKeys.heartbeat(task_id),
            worker_id,
            ex=settings.heartbeat_ttl,
        )
        pipe.hset(
            task_key,
            mapping={
                "payload": json.dumps(payload),
                "worker_id": worker_id,
                # Wall-clock time: the event-loop clock has an arbitrary,
                # per-process epoch and is not a usable timestamp.
                "registered_at": str(int(now)),
            },
        )
        pipe.expire(task_key, 86400)  # 24h TTL

        pipe.zadd(RedisKeys.phoenix_expiry_index(), {task_id: expiry_timestamp})

        await pipe.execute()

        loop_task = asyncio.create_task(cls._refresh_loop(task_id, worker_id))
        cls._active_loops[task_id] = loop_task

    @classmethod
    async def _refresh_loop(cls, task_id: str, worker_id: str) -> None:
        """
        Continuously refresh the task heartbeat until execution completes.

        Heartbeat expiration is treated as implicit worker death by the
        resurrector subsystem.
        """
        redis = await get_relier_redis()
        hb_key = RedisKeys.heartbeat(task_id)
        settings = cls._get_settings()
        interval = settings.heartbeat_ttl / 2.0
        expiry_index_key = RedisKeys.phoenix_expiry_index()

        try:
            while True:
                await asyncio.sleep(interval)

                # Extend heartbeat and update expiry index atomically
                now = time.time()
                new_expiry = now + settings.heartbeat_ttl

                pipe = redis.pipeline()
                pipe.expire(hb_key, settings.heartbeat_ttl)
                pipe.zadd(expiry_index_key, {task_id: new_expiry})
                results = await pipe.execute()

                # `results[0]` corresponds to the return value from `expire`.
                # If the expire call returned a falsy value the heartbeat key
                # no longer exists and the refresh loop should exit.
                if not results or not results[0]:
                    logger.warning(
                        "Heartbeat refresh stopped because the heartbeat key disappeared.",
                        extra={"task_id": task_id, "worker_id": worker_id},
                    )
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Unexpected error while refreshing Phoenix heartbeat.",
                extra={
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "error_type": type(exc).__name__,
                },
            )
        finally:
            cls._active_loops.pop(task_id, None)

    # ===========================================================================
    # Task completion & cleanup
    # ===========================================================================
    @classmethod
    async def complete(cls, task_id: str) -> None:
        """
        Remove all Phoenix tracking state for a successfully completed task.

        Payload state is deleted before the heartbeat key so the resurrector
        cannot observe a partially-cleaned state where payload data exists
        without a live heartbeat.
        """
        loop_task = cls._active_loops.pop(task_id, None)

        if loop_task and not loop_task.done():
            loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await loop_task

        redis = await get_relier_redis()

        # Release any blob-backed checkpoint before the Phoenix hash (which
        # holds the reference) is deleted, otherwise the blob would leak.
        checkpoint_field = await redis.hget(  # type: ignore[misc]
            RedisKeys.phoenix(task_id), "partial_result"
        )
        if checkpoint_field:
            await CheckpointStore.gc(checkpoint_field)

        pipe = redis.pipeline()
        # Delete persisted execution state before removing the heartbeat to
        # prevent false-positive resurrection windows.
        pipe.delete(RedisKeys.phoenix(task_id))
        pipe.delete(RedisKeys.heartbeat(task_id))
        pipe.delete(RedisKeys.resurrection(task_id))
        pipe.zrem(RedisKeys.phoenix_expiry_index(), task_id)
        await pipe.execute()

    @classmethod
    async def _is_active(cls, task_id: str) -> bool:
        """Return whether a live Phoenix heartbeat exists for the task."""
        redis = await get_relier_redis()
        return bool(await redis.exists(RedisKeys.heartbeat(task_id)))

    @classmethod
    async def _has_live_worker(cls, redis: Redis) -> bool:
        """Return whether any worker has heartbeated within the liveness window.

        A resurrected task is replayed onto the ``re-queue`` broker queue, but
        nothing can consume it until a worker is online. Worker presence is
        tracked in ``RedisKeys.workers()`` as a sorted set scored by last
        heartbeat; a worker is considered available if its score is newer than
        ``heartbeat_ttl`` ago, the same window used for crash detection. Dead
        worker rows linger in the set (they are pruned only after a long
        retention window), so membership alone is not enough — the score must
        be fresh.
        """
        settings = cls._get_settings()
        cutoff = time.time() - settings.heartbeat_ttl
        count = await redis.zcount(RedisKeys.workers(), cutoff, float("inf"))
        return int(count) > 0

    # ===========================================================================
    # Resurrection coordinator
    # ===========================================================================

    @classmethod
    async def resurrection_loop(cls) -> None:
        """
        Continuously detect dead workers and replay recoverable tasks.

        Intended to run inside a dedicated resurrection coordinator process.

        This loop:
        - monitors previously resurrected tasks
        - scans for expired heartbeats
        - applies resurrection safety limits
        - quarantines poison-pill workloads
        - replays recoverable tasks onto healthy workers
        """
        from relier.core.dlq import DeadLetterQueue  # avoid circular import
        from relier.core.validation import validate_redis_reachable
        from relier.tasks.app import celery_app  # avoid circular import

        dead_letter_queue = DeadLetterQueue()

        settings = cls._get_settings()
        redis = await get_relier_redis()

        # Fail fast: the resurrector cannot coordinate anything without Redis.
        await validate_redis_reachable(redis, settings)

        # Every Celery worker embeds this scanner alongside the dedicated
        # `rl run-resurrector` process. Logged at DEBUG so a regular
        # `celery -A` boot doesn't show two "resurrector started" lines
        # (the dedicated process prints its own banner via the CLI).
        # Distributed locks make multiple scanners safe; a worker seeing
        # this in DEBUG output is expected behaviour, not a stray launch.
        logger.debug(
            "Phoenix resurrection scanner started",
            extra={
                "check_interval": settings.resurrection_check_interval,
                "max_resurrections": settings.max_resurrections,
                "heartbeat_ttl": settings.heartbeat_ttl,
            },
        )

        loop_count = 0
        while True:
            try:
                loop_count += 1
                start_time = asyncio.get_running_loop().time()

                if loop_count % 10 == 0:
                    logger.debug(
                        f"Phoenix resurrector heartbeat (loop={loop_count})",
                        extra={"uptime_loops": loop_count},
                    )

                # Periodically reclaim checkpoint blobs orphaned by tasks
                # whose Phoenix hash TTL-expired. A no-op unless a blob
                # backend is configured.
                if loop_count % 300 == 0:
                    await CheckpointStore.sweep()

                monitored_count = await cls._monitor_resurrected_tasks(redis)
                resurrected_count = await cls._scan_and_resurrect(
                    redis, dead_letter_queue, celery_app
                )

                duration = asyncio.get_running_loop().time() - start_time

                # Only announce a pass when we actually recovered something.
                # Logging every pass merely because a task is still being
                # *monitored* (monitored_count > 0) spams the worker/CLI logs
                # with an identical line every scan interval until that task
                # finishes. State changes during monitoring (reclaimed, died
                # again, completed) are already logged individually by
                # _monitor_resurrected_tasks, so steady-state monitoring stays
                # quiet here.
                if (resurrected_count or 0) > 0:
                    logger.info(
                        f"Phoenix recovered {resurrected_count} orphaned "
                        "task(s) onto healthy workers",
                        extra={
                            "resurrected": resurrected_count,
                            "monitored": monitored_count,
                            "duration_ms": int(duration * 1000),
                        },
                    )

            except Exception as exc:
                logger.error(
                    "Unhandled exception in resurrection coordinator loop.",
                    extra={"error_type": type(exc).__name__, "loop": loop_count},
                    exc_info=True,
                )
            await asyncio.sleep(settings.resurrection_check_interval)

    # ===========================================================================
    # Internal resurrection helpers
    # ===========================================================================

    @classmethod
    async def _monitor_resurrected_tasks(cls, redis: Redis) -> int:
        """
        Track previously resurrected tasks through their post-replay lifecycle.

        Tasks transition through:
        - resurrected but not yet claimed
        - successfully revived
        - failed again after replay
        - fully completed

        The monitor registry is paged with ``HSCAN`` rather than loaded whole,
        and all state mutations are flushed in a single pipelined round-trip,
        so a pass costs O(1) round-trips regardless of how many tasks are
        being tracked.
        """
        monitor_key = RedisKeys.monitor()
        expiry_index_key = RedisKeys.phoenix_expiry_index()
        settings = cls._get_settings()

        # Page through the registry instead of loading it entirely; it can
        # grow large during mass-failure resurrection bursts.
        monitoring_data: dict[str, str] = {}
        cursor = 0
        while True:
            cursor, batch = await redis.hscan(monitor_key, cursor=cursor, count=500)
            monitoring_data.update(batch)
            if cursor == 0:
                break

        if not monitoring_data:
            return 0

        task_ids = list(monitoring_data.keys())

        logger.debug(
            f"Tracking {len(task_ids)} resurrected task(s)",
            extra={"task_ids": task_ids},
        )

        pipe = redis.pipeline()
        for t_id in task_ids:
            pipe.exists(RedisKeys.heartbeat(t_id))
            pipe.exists(RedisKeys.phoenix(t_id))

        results = await pipe.execute()

        transitions = {"alive": 0, "dead_again": 0, "lost": 0, "completed": 0}

        # All monitor mutations are collected and flushed once at the end.
        writes = redis.pipeline()
        now = time.time()

        # A replayed task that no worker can pick up yet is waiting in the
        # re-queue, not lost. Resolve worker availability once per pass so the
        # "never claimed" branch can tell a genuine claim-miss apart from "no
        # consumer online".
        has_live_worker = await cls._has_live_worker(redis)

        for i, t_id in enumerate(task_ids):
            hb_exists = bool(results[i * 2])
            payload_exists = bool(results[i * 2 + 1])

            # Monitor values are written as "<state>:<resurrected_at>". The
            # timestamp gates the "never claimed" warning so it cannot fire
            # within the cold-start window. A missing timestamp (corrupted
            # write or pre-existing test fixture) defaults to 0.0, which
            # makes the grace check pass immediately.
            state_str, _, ts_str = monitoring_data[t_id].partition(":")
            state = int(state_str)
            resurrected_at = float(ts_str) if ts_str else 0.0

            if state == 0 and hb_exists:
                # A healthy worker reclaimed execution ownership.
                writes.hset(monitor_key, t_id, "1")
                transitions["alive"] += 1
                logger.info(
                    "Recovered task reclaimed by a healthy worker",
                    extra={"task_id": t_id},
                )

            elif state == 1 and not hb_exists and payload_exists:
                # The replacement worker also disappeared before completion.
                # It re-registered on pickup, so it is already back in the
                # expiry index, just release it from the monitor.
                writes.hdel(monitor_key, t_id)
                transitions["dead_again"] += 1
                logger.warning(
                    "Resurrected task died AGAIN - releasing back to scan",
                    extra={"task_id": t_id},
                )

            elif state == 0 and not hb_exists and payload_exists:
                # Dispatched for resurrection but no worker ever registered a
                # heartbeat. Hold off until the cold-start grace period
                # elapses, otherwise a worker mid-startup looks identical to
                # a broker drop and the warning fires as a false positive.
                if now - resurrected_at < settings.resurrection_claim_grace_period:
                    continue
                # Grace expired, but if no worker is online the replayed
                # message is simply sitting in the re-queue with no consumer —
                # not lost. Hold the monitor entry (and the resurrection lease,
                # which that queued message validates against) until a worker
                # returns. Re-dispatching here would be pointless: the still-held
                # lease would only block it, and the scanner would collide with
                # its own lease ("claimed by another resurrector"). Logged at
                # debug so a quiet, worker-less cluster doesn't spam warnings.
                if not has_live_worker:
                    logger.debug(
                        "Resurrected task awaiting a live worker - holding.",
                        extra={"task_id": t_id},
                    )
                    continue
                # A worker is alive yet never claimed the replay: treat as a
                # genuine miss. Release the resurrection lease first so the next
                # scan can dispatch a fresh incarnation — otherwise the
                # coordinator's own still-valid lease blocks the re-dispatch for
                # its full TTL. The task is NOT in the expiry index, so re-add
                # it explicitly.
                writes.delete(RedisKeys.lease(t_id))
                writes.hdel(monitor_key, t_id)
                writes.zadd(expiry_index_key, {t_id: now})
                transitions["lost"] += 1
                logger.warning(
                    "Resurrected task never claimed - releasing back to scan",
                    extra={"task_id": t_id},
                )

            elif not payload_exists:
                # Execution completed and Phoenix state was cleaned up normally.
                writes.hdel(monitor_key, t_id)
                transitions["completed"] += 1
                logger.info(
                    "Recovered task finished successfully (resurrection confirmed)",
                    extra={"task_id": t_id},
                )

        await writes.execute()

        # Emit transition metrics only when task state changed during the pass.
        if any(transitions.values()):
            logger.info("Resurrection monitoring summary", extra=transitions)

        return len(task_ids)

    @staticmethod
    def _decode_hash(raw: dict[Any, Any]) -> dict[str, str]:
        """
        Normalize a Redis hash to ``str`` keys/values.

        The async client runs with ``decode_responses=True`` in production,
        but this keeps the resurrection path robust if a differently
        configured client (or a test double) hands back ``bytes``.
        """

        def _d(v: Any) -> Any:
            return v.decode() if isinstance(v, bytes) else v

        return {_d(k): _d(v) for k, v in raw.items()}

    @classmethod
    async def _scan_and_resurrect(
        cls,
        redis: Redis,
        dead_letter_queue: DeadLetterQueue,
        celery_app: Celery,
    ) -> int:
        """
        Scan for orphaned Phoenix entries and replay recoverable tasks.

        A task is considered orphaned when:
        - payload state still exists
        - the heartbeat has expired
        - no other resurrector owns the replay lock

        The cheap liveness/monitor checks and the payload loads are pipelined
        across all candidates, so a scan costs a small constant number of
        round-trips instead of O(N) sequential calls, which matters during
        mass worker failure, when the expiry index is largest.
        """
        settings = cls._get_settings()
        resurrected_count = 0
        now = time.time()

        expiry_index_key = RedisKeys.phoenix_expiry_index()
        monitor_key = RedisKeys.monitor()

        # Backpressure: if the recovery queue is already deep, defer this scan
        # so the resurrector cannot replay tasks faster than the recovery
        # workers drain them, otherwise a mass failure would balloon the
        # broker. Expired tasks remain in the expiry index for a later pass.
        queue_depth: int = await redis.llen(_RECOVERY_QUEUE)  # type: ignore[misc]
        if queue_depth >= settings.resurrection_max_queue_depth:
            logger.warning(
                "Resurrection scan deferred: recovery queue backlog too deep.",
                extra={
                    "queue_depth": queue_depth,
                    "limit": settings.resurrection_max_queue_depth,
                },
            )
            return 0

        # Get the next batch of tasks whose heartbeat should have expired. The
        # fetch is capped to the number of available queue slots so a single
        # scan never dispatches more tasks than the re-queue broker can absorb
        # without exceeding resurrection_max_queue_depth.
        slots_available = settings.resurrection_max_queue_depth - queue_depth
        fetch_limit = min(settings.resurrection_batch_size, slots_available)
        expired_task_ids = await redis.zrangebyscore(
            expiry_index_key,
            min=0,
            max=now,
            start=0,
            num=fetch_limit,
        )

        if not expired_task_ids:
            return 0

        # Batch the heartbeat + monitor checks for every candidate into a
        # single pipelined round-trip.
        pipe = redis.pipeline()
        for task_id in expired_task_ids:
            pipe.exists(RedisKeys.heartbeat(task_id))
            pipe.hexists(monitor_key, task_id)
        checks = await pipe.execute()

        stale_index_ids: list[str] = []
        candidates: list[str] = []
        for i, task_id in enumerate(expired_task_ids):
            hb_exists = bool(checks[i * 2])
            is_monitored = bool(checks[i * 2 + 1])
            # A live heartbeat or an active monitor entry means the task no
            # longer needs scan-driven resurrection; drop the stale index row.
            if hb_exists or is_monitored:
                stale_index_ids.append(task_id)
            else:
                candidates.append(task_id)

        if stale_index_ids:
            await redis.zrem(expiry_index_key, *stale_index_ids)

        if not candidates:
            return 0

        # Batch the payload loads for the remaining candidates.
        pipe = redis.pipeline()
        for task_id in candidates:
            pipe.hgetall(RedisKeys.phoenix(task_id))
        raw_states = await pipe.execute()

        for task_id, raw_state in zip(candidates, raw_states, strict=True):
            if not raw_state:
                # Orphaned index entry, Phoenix state already gone.
                await redis.zrem(expiry_index_key, task_id)
                continue

            # Acquire the distributed replay lock so only one coordinator
            # processes this task.
            lock_key = RedisKeys.resurrect_lock(task_id)
            acquired = await redis.set(lock_key, "1", nx=True, ex=30)
            if not acquired:
                logger.debug(
                    "Skipping resurrection because another coordinator owns the replay lock.",
                    extra={"task_id": task_id},
                )
                continue

            state_data = cls._decode_hash(raw_state)

            # Resurrection attempt number. The counter is incremented only
            # once a broker replay is *confirmed* (see `_bg_send`), so a
            # broker outage cannot inflate the count and falsely quarantine
            # an otherwise-healthy task.
            res_key = RedisKeys.resurrection(task_id)
            raw_count = await redis.get(res_key)
            attempt = int(raw_count or 0) + 1

            # Recover the persisted execution payload required for replay.
            raw_payload = state_data.get("payload")
            try:
                payload = json.loads(raw_payload) if raw_payload else {}
            except (TypeError, json.JSONDecodeError) as exc:
                logger.error(
                    "Failed to decode task payload",
                    extra={"task_id": task_id, "error_type": type(exc).__name__},
                )
                payload = {}

            # Recover any persisted checkpoint state for resumable execution.
            raw_partial = state_data.get("partial_result")
            try:
                partial_data = json.loads(raw_partial) if raw_partial else None
            except (TypeError, json.JSONDecodeError):
                logger.error(
                    "Failed to decode partial result", extra={"task_id": task_id}
                )
                partial_data = None

            # Quarantine workloads that repeatedly destabilize workers.
            if attempt > settings.max_resurrections:
                logger.error(
                    "Task exceeded resurrection safety limit; quarantining to DLQ.",
                    extra={
                        "task_id": task_id,
                        "attempt": attempt,
                        "max": settings.max_resurrections,
                    },
                )
                await dead_letter_queue.quarantine(
                    task_id,
                    reason="max_resurrections_exceeded",
                    payload=payload,
                )
                await redis.delete(RedisKeys.phoenix(task_id), res_key)
                await redis.hdel(monitor_key, task_id)  # type: ignore[misc]
                await redis.zrem(expiry_index_key, task_id)
                continue

            if not payload:
                logger.warning(
                    "Skipping resurrection because no replay payload exists.",
                    extra={"task_id": task_id},
                )
                continue

            # Inject checkpoint state so execution can resume from the last
            # persisted recovery point.
            if partial_data:
                if "kwargs" not in payload:
                    payload["kwargs"] = {}
                payload["kwargs"]["checkpoint"] = partial_data

            # Remove stale inflight ownership associated with the dead worker.
            ghost_worker_id = state_data.get("worker_id") or ""
            if ghost_worker_id:
                removed = await redis.zrem(RedisKeys.inflight(ghost_worker_id), task_id)
                if removed:
                    logger.debug(
                        "Cleaned up ghost worker inflight entry",
                        extra={"ghost_worker": ghost_worker_id, "task_id": task_id},
                    )

            logger.warning(
                "Worker death detected; replaying orphaned task.",
                extra={
                    "task_id": task_id,
                    "task_name": payload.get("task_name", "unknown"),
                    "attempt": attempt,
                    "max_attempts": settings.max_resurrections,
                    "ghost_worker": ghost_worker_id,
                    "queue": payload.get("queue", "default"),
                    "has_checkpoint": partial_data is not None,
                },
            )

            # Replay the task. Post-replay monitoring, the resurrection +
            # global counters, and expiry-index removal are all applied by
            # the dispatch path only *after* the broker send is confirmed
            # (see `_bg_send`). A failed send therefore leaves the task in the
            # expiry index for a later retry instead of dropping it.
            if await cls.resurrect_task(task_id, payload, celery_app):
                resurrected_count += 1
                raw_registered = state_data.get("registered_at")
                if raw_registered:
                    with contextlib.suppress(ValueError, TypeError):
                        elapsed = now - float(raw_registered)
                        resurrection_time_s.record(
                            max(0.0, elapsed),
                            {"rl.task.name": payload.get("task_name", "unknown")},
                        )

        await asyncio.sleep(settings.resurrection_requeue_delay)

        logger.debug(
            f"Checked {len(expired_task_ids)} expired tasks, "
            f"resurrected {resurrected_count}"
        )

        return resurrected_count

    @classmethod
    async def _safe_bg_send(
        cls,
        task_id: str,
        payload: dict,
        celery_app: Celery,
    ) -> None:
        """
        Bound concurrent broker replay submissions during resurrection bursts.
        """
        try:
            async with cls._send_semaphore:
                await cls._bg_send(
                    task_id,
                    payload,
                    celery_app,
                )
        except Exception as exc:
            logger.error(
                "Unexpected failure while scheduling resurrected task replay.",
                extra={
                    "task_id": task_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )

    @classmethod
    async def _bg_send(
        cls,
        task_id: str,
        payload: dict[str, Any],
        celery_app: Celery,
    ) -> None:
        """
        Replay a resurrected task onto the Celery broker.

        Broker submission is executed in a thread pool because Celery's
        publishing APIs are synchronous.
        """
        try:
            loop = asyncio.get_running_loop()

            logger.info(
                "Submitting resurrected task to broker.",
                extra={
                    "task_id": task_id,
                    "task_name": payload.get("task_name"),
                    "queue": payload.get("queue", "default"),
                },
            )

            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: celery_app.send_task(
                        payload["task_name"],
                        args=payload.get("args", []),
                        kwargs=payload.get("kwargs", {}),
                        queue=_RECOVERY_QUEUE,
                        task_id=task_id,
                    ),
                ),
                timeout=10.0,
            )

            logger.info(
                "Resurrected task successfully re-queued.",
                extra={
                    "task_id": task_id,
                    "task_name": payload.get("task_name"),
                },
            )

            # The broker replay is now confirmed. Only at this point do we
            # record the resurrection: begin post-replay monitoring, bump the
            # resurrection + global counters, and drop the task from the
            # expiry index. Gating this on a successful send means a failed
            # send leaves the task in the expiry index for a later retry
            # instead of silently losing it.
            redis = await get_relier_redis()
            pipe = redis.pipeline()
            # Encode the re-queue timestamp alongside the state so the monitor
            # can apply a grace period before declaring the task "never
            # claimed" — a cold worker can take 10-20s before its first poll,
            # and without this gate the warning fires on the very next pass.
            pipe.hset(RedisKeys.monitor(), task_id, f"0:{int(time.time())}")
            pipe.incr(RedisKeys.resurrection(task_id))
            pipe.incr(RedisKeys.metric_global("resurrected"))
            pipe.zrem(RedisKeys.phoenix_expiry_index(), task_id)
            await pipe.execute()

            resurrections_total.add(
                1, {"rl.task.name": payload.get("task_name", "unknown")}
            )

            with tracer.start_as_current_span(
                "rl.resurrection.queue",
                attributes={
                    ATTR_TASK_ID: task_id,
                    ATTR_TASK_NAME: payload.get("task_name", "unknown"),
                    "rl.resurrection.queue": _RECOVERY_QUEUE,
                },
            ):
                pass
        except TimeoutError:
            logger.error(
                "Timed out while re-queueing resurrected task.",
                extra={"task_id": task_id, "timeout": 10.0},
            )
        except Exception as exc:
            logger.error(
                "Failed to submit resurrected task to broker.",
                extra={
                    "task_id": task_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )

    # ==============================================================================
    # RESURRECTION
    # ==============================================================================

    _active_resurrections: dict[str, "asyncio.Task[None]"] = {}

    @classmethod
    async def resurrect_task(
        cls,
        task_id: str,
        payload: dict[str, Any],
        celery_app: Celery,
    ) -> bool:
        """
        Atomically resurrect with leasing + fencing using Redis LUA.

        - Generates a new fence token (incarnation ID).
        - Claims a short lease (prevents duplicate pickup).
        - Stores the fence (prevents zombie writes later).
        - Submits to Celery with tokens attached.

        Returns:
            ``True`` if this coordinator acquired the lease and scheduled the
            replay dispatch; ``False`` if another coordinator already owns the
            lease and the task was skipped.
        """
        redis = await get_relier_redis()
        fence_token = secrets.token_hex(16)
        lease_key = RedisKeys.lease(task_id)
        fence_key = RedisKeys.fence(task_id)

        # Lease acquisition prevents concurrent resurrectors from
        # dispatching the same task simultaneously.

        lease_acquired = await redis.eval(
            RESURRECT_LUA, 2, lease_key, fence_key, fence_token, "180", "600"
        )  # type: ignore[misc]

        if lease_acquired != 1:
            logger.warning(
                "Lease already claimed by another resurrector - skipping",
                extra={"task_id": task_id, "lease-key": lease_key},
            )
            return False

        logger.info(
            "Acquired resurrection lease",
            extra={
                "task_id": task_id,
                "lease_key": lease_key,
                "lease_ttl": 180,
            },
        )

        # Enrich payload with fencing metadata
        enriched = {
            **payload,
            "kwargs": {
                **payload.get("kwargs", {}),
                "_fence_token": fence_token,
                "_lease_key": lease_key,
                "_fence_key": fence_key,
            },
        }

        # Dispatch asynchronously so resurrection does not block the scanner loop.
        bg_task = asyncio.create_task(
            cls._safe_bg_send(
                task_id,
                enriched,
                celery_app,
            )
        )
        cls._active_resurrections[task_id] = bg_task

        def cleanup(_) -> None:  # type: ignore[no-untyped-def]
            cls._active_resurrections.pop(task_id, None)

        bg_task.add_done_callback(cleanup)
        return True

    @classmethod
    async def _wait_for_resurrection(cls, timeout: float = 5.0) -> None:
        """Wait for all pending resurrection dispatches to complete. Test helper only."""
        if not cls._active_resurrections:
            return

        await asyncio.wait_for(
            asyncio.gather(*cls._active_resurrections.values(), return_exceptions=True),
            timeout=timeout,
        )

    # ==========================================================================
    # EXECUTION VALIDATION
    # ==========================================================================
    @classmethod
    async def validate_execution(
        cls,
        task_id: str,
        redis: Any,
        fence_token: str | None,
        lease_key: str | None,
        fence_key: str | None,
    ) -> bool:
        """
        Validate lease ownership BEFORE execution begins.

        Rejects:
        - Duplicate pickups
        - Zombie workers
        - Superseded resurrections
        """
        if not all([fence_token, lease_key, fence_key]):
            return True

        logger.debug(
            "Validating lease + fence.",
            extra={"task_id": task_id},
        )

        result = await redis.eval(
            VALIDATE_LUA,
            2,
            lease_key,
            fence_key,
            fence_token,
        )

        if result == 0:
            logger.info(
                "Duplicate execution rejected, lease mismatch.",
                extra={"task_id": task_id, "lease_key": lease_key},
            )
            return False

        if result == 2:
            logger.info(
                "Zombie execution rejected, stale fence.",
                extra={"task_id": task_id, "lease_key": lease_key},
            )

            # Cleanup stale lease.
            if lease_key and fence_token:
                await cls.release_lease(redis, lease_key, fence_token)

            return False

        logger.info(
            "Lease + fence validation passed.",
            extra={"task_id": task_id, "lease_key": lease_key},
        )

        return True

    # =========================================================================
    # COMMIT VALIDATION
    # =========================================================================

    @classmethod
    async def validate_commit(
        cls,
        task_id: str,
        redis: Any,
        fence_token: str | None,
        lease_key: str | None,
        fence_key: str | None,
    ) -> bool:
        """
        Validate fence BEFORE committing results.

        Prevents stale/zombie writes.
        """
        if not all([fence_token, lease_key, fence_key]):
            return True

        logger.debug(
            "Validating fence before commit.",
            extra={"task_id": task_id, "lease_key": lease_key},
        )

        result = await redis.eval(
            COMMIT_CHECK_LUA,
            1,
            fence_key,
            fence_token,
        )

        if result != 1:
            logger.warning(
                "Zombie result discarded.",
                extra={"task_id": task_id, "lease_key": lease_key},
            )
            return False

        logger.info(
            "Fence validation passed, committing results.",
            extra={"task_id": task_id, "lease_key": lease_key},
        )

        # Release lease now that execution completed successfully.
        if lease_key and fence_token:
            await cls.release_lease(redis, lease_key, fence_token)

        return True

    # =========================================================================
    # LEASE CLEANUP
    # =========================================================================

    @classmethod
    async def release_lease(
        cls,
        redis: Any,
        lease_key: str,
        fence_token: str,
    ) -> None:
        """
        Release lease ONLY if caller still owns it.

        Prevents deleting another worker's lease accidentally.
        """
        try:
            await redis.eval(
                CLEANUP_LUA,
                1,
                lease_key,
                fence_token,
            )

        except Exception as exc:
            logger.error(
                "Failed to release lease.",
                extra={"error": str(exc)},
            )

    # ==========================================================================
    # Checkpoint persistence
    # ==========================================================================

    @classmethod
    async def update_partial_state(cls, task_id: str, state: Any) -> None:
        """
        Persist resumable checkpoint state for an active task.

        Checkpoint data is injected back into the task during resurrection so
        execution can continue from the last persisted recovery point.

        Storage is delegated to :class:`CheckpointStore`, which keeps small
        checkpoints inline in Redis and spills large ones to a blob backend so
        the coordination store cannot be exhausted by oversized payloads.

        Raises:
            CheckpointTooLargeError: the checkpoint exceeds the inline limit
                and no blob backend is configured.
        """
        await CheckpointStore.save(task_id, state)
