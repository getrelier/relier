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
from typing import Any

from celery import Celery
from redis.asyncio import Redis

from relier.config import Settings, get_settings
from relier.core.dlq import DeadLetterQueue
from relier.core.keys import RedisKeys
from relier.storage.lua.scripts import (
    CLEANUP_LUA,
    COMMIT_CHECK_LUA,
    RESURRECT_LUA,
    VALIDATE_LUA,
)
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


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
                "registered_at": str(int(asyncio.get_running_loop().time())),
            },
        )
        pipe.expire(task_key, 86400)  # 24h TTL
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

        try:
            while True:
                await asyncio.sleep(interval)
                extended = await redis.expire(hb_key, settings.heartbeat_ttl)
                if not extended:
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
        pipe = redis.pipeline()
        # Delete persisted execution state before removing the heartbeat to
        # prevent false-positive resurrection windows.
        pipe.delete(RedisKeys.phoenix(task_id))
        pipe.delete(RedisKeys.heartbeat(task_id))
        pipe.delete(RedisKeys.resurrection(task_id))
        await pipe.execute()

    @classmethod
    async def is_active(cls, task_id: str) -> bool:
        """Return whether a live Phoenix heartbeat exists for the task."""
        redis = await get_relier_redis()
        return bool(await redis.exists(RedisKeys.heartbeat(task_id)))

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
        from relier.tasks.app import celery_app  # avoid circular import

        dead_letter_queue = DeadLetterQueue()

        settings = cls._get_settings()
        redis = await get_relier_redis()

        logger.info(
            "Phoenix resurrector started",
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

                monitored_count = await cls._monitor_resurrected_tasks(redis)
                resurrected_count = await cls._scan_and_resurrect(
                    redis, dead_letter_queue, celery_app
                )

                duration = asyncio.get_event_loop().time() - start_time

                if (monitored_count or 0) > 0 or (resurrected_count or 0) > 0:
                    logger.info(
                        "Phoenix resurrection pass complete",
                        extra={
                            "monitored": monitored_count,
                            "resurrected": resurrected_count,
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
        """
        monitor_key = RedisKeys.monitor()
        raw = await redis.hgetall(monitor_key)  # type: ignore[misc]
        if not raw:
            return 0

        monitoring_data = {
            (k.decode() if isinstance(k, bytes) else k): (
                v.decode() if isinstance(v, bytes) else v
            )
            for k, v in raw.items()
        }

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

        transitions = {"alive": 0, "dead_again": 0, "completed": 0}

        for i, t_id in enumerate(task_ids):
            hb_exists = bool(results[i * 2])
            payload_exists = bool(results[i * 2 + 1])
            state = int(monitoring_data[t_id])

            if state == 0 and hb_exists:
                # A healthy worker reclaimed execution ownership.
                await redis.hset(monitor_key, t_id, "1")  # type: ignore[misc]
                transitions["alive"] += 1
                logger.info("Resurrected task is now alive", extra={"task_id": t_id})

            elif state == 1 and not hb_exists and payload_exists:
                # The replacement worker also disappeared before completion.
                await redis.hdel(monitor_key, t_id)  # type: ignore[misc]
                transitions["dead_again"] += 1
                logger.warning(
                    "Resurrected task died AGAIN - releasing back to scan",
                    extra={"task_id": t_id},
                )

            elif not payload_exists:
                # Execution completed and Phoenix state was cleaned up normally.
                await redis.hdel(monitor_key, t_id)  # type: ignore[misc]
                transitions["completed"] += 1
                logger.info(
                    "Resurrected task completed successfully", extra={"task_id": t_id}
                )

        # Emit transition metrics only when task state changed during the pass.
        if any(transitions.values()):
            logger.info("Resurrection monitoring summary", extra=transitions)

        return len(task_ids)

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
        """
        settings = cls._get_settings()
        resurrected_count = 0
        scanned_count = 0

        async for tasks_key in redis.scan_iter(
            match=f"{RedisKeys.PREFIX}:phoenix:*", count=100
        ):
            scanned_count += 1

            state_data = await redis.hgetall(tasks_key)  # type: ignore[misc]
            if not state_data:
                continue

            tasks_key_str = (
                tasks_key.decode("utf-8")
                if isinstance(tasks_key, bytes)
                else str(tasks_key)
            )

            t_id = tasks_key_str.split(":")[-1]
            hb_key = RedisKeys.heartbeat(t_id)
            monitor_key = RedisKeys.monitor()

            # Ignore healthy tasks and tasks already undergoing replay tracking.
            pipe = redis.pipeline()
            pipe.hexists(monitor_key, t_id)
            pipe.exists(hb_key)
            is_monitored, hb_exists = await pipe.execute()

            if is_monitored or hb_exists:
                # Task is alive or already being resurrected
                continue

            # Acquire distributed replay ownership for this task.
            lock_key = RedisKeys.resurrect_lock(t_id)
            acquired = await redis.set(lock_key, "1", nx=True, ex=30)

            if not acquired:
                logger.debug(
                    "Skipping resurrection because another coordinator owns the replay lock.",
                    extra={"task_id": t_id},
                )
                continue

            res_key = RedisKeys.resurrection(t_id)
            count = await redis.incr(res_key)

            # Recover the persisted execution payload required for replay.
            raw_payload = state_data.get(b"payload") or state_data.get("payload")
            try:
                payload = json.loads(raw_payload) if raw_payload else {}
            except (TypeError, json.JSONDecodeError) as exc:
                logger.error(
                    "Failed to decode task payload",
                    extra={"task_id": t_id, "error_type": type(exc).__name__},
                )
                payload = {}

            # Recover any persisted checkpoint state for resumable execution.
            raw_partial = state_data.get(b"partial_result") or state_data.get(
                "partial_result"
            )
            try:
                partial_data = json.loads(raw_partial) if raw_partial else None
            except (TypeError, json.JSONDecodeError):
                logger.error("Failed to decode partial result", extra={"task_id": t_id})
                partial_data = None

            # Quarantine workloads that repeatedly destabilize workers.
            if count > settings.max_resurrections:
                logger.error(
                    "Task exceeded resurrection safety limit; quarantining to DLQ.",
                    extra={
                        "task_id": t_id,
                        "attempt": count,
                        "max": settings.max_resurrections,
                    },
                )
                await dead_letter_queue.quarantine(
                    t_id,
                    reason="max_resurrections_exceeded",
                    payload=payload,
                    partial_result=partial_data,
                )
                await redis.delete(RedisKeys.phoenix(t_id), res_key)
                await redis.hdel(monitor_key, t_id)  # type: ignore[misc]
                continue

            if not payload:
                logger.warning(
                    "Skipping resurrection because no replay payload exists.",
                    extra={"task_id": t_id},
                )
                continue

            # Inject checkpoint state so execution can resume from the last
            # persisted recovery point.
            if partial_data:
                if "kwargs" not in payload:
                    payload["kwargs"] = {}
                payload["kwargs"]["checkpoint"] = partial_data

            raw_worker = state_data.get(b"worker_id") or state_data.get("worker_id")
            ghost_worker_id = (
                raw_worker.decode("utf-8")
                if isinstance(raw_worker, bytes)
                else str(raw_worker)
            )

            # Remove stale inflight ownership associated with the dead worker.
            if ghost_worker_id:
                removed = await redis.zrem(RedisKeys.inflight(ghost_worker_id), t_id)
                if removed:
                    logger.debug(
                        "Cleaned up ghost worker inflight entry",
                        extra={"ghost_worker": ghost_worker_id, "task_id": t_id},
                    )

            logger.warning(
                "Worker death detected; replaying orphaned task.",
                extra={
                    "task_id": t_id,
                    "task_name": payload.get("task_name", "unknown"),
                    "attempt": count,
                    "max_attempts": settings.max_resurrections,
                    "ghost_worker": ghost_worker_id,
                    "queue": payload.get("queue", "default"),
                    "has_checkpoint": partial_data is not None,
                },
            )

            # Begin post-replay lifecycle tracking before broker submission.
            await redis.hset(monitor_key, t_id, "0")  # type: ignore[misc]
            # Track global resurrection count for CLI metrics
            await redis.incr(RedisKeys.metric_global("resurrected"))

            # Replay resurrected tasks which goes to a dedicated re-queue worker to minimize
            # recovery latency after worker loss.
            await cls.resurrect_task(
                t_id,
                payload,
                celery_app,
            )

            resurrected_count += 1

        # Emit scan metrics only when Phoenix entries were inspected.
        if scanned_count > 0:
            logger.debug(
                f"Scanned {scanned_count} phoenix entries, resurrected {resurrected_count}",
                extra={"scanned": scanned_count, "resurrected": resurrected_count},
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
                        queue="re-queue",
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
    @classmethod
    async def resurrect_task(
        cls,
        task_id: str,
        payload: dict[str, Any],
        celery_app: Celery,
    ) -> None:
        """
        Atomically resurrect with leasing + fencing using Redis LUA.

        - Generates a new fence token (incarnation ID).
        - Claims a short lease (prevents duplicate pickup).
        - Stores the fence (prevents zombie writes later).
        - Submits to Celery with tokens attached.
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
            return

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
        asyncio.create_task(
            cls._safe_bg_send(
                task_id,
                enriched,
                celery_app,
            )
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
                "Duplicate execution rejected — lease mismatch.",
                extra={"task_id": task_id, "lease_key": lease_key},
            )
            return False

        if result == 2:
            logger.info(
                "Zombie execution rejected — stale fence.",
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
            "Fence validation passed — committing results.",
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
        """
        redis = await get_relier_redis()
        # Persist the latest checkpoint snapshot for resurrection recovery.
        await redis.hset(
            RedisKeys.phoenix(task_id),
            "partial_result",
            json.dumps(state),
        )  # type: ignore[misc]
