"""
Relier Core — Phoenix Task Resurrection.

Implements the Shadow Registry pattern: every active task emits a heartbeat
TTL key in Redis.  When a worker is OOM-killed the key expires and the
Resurrector loop re-queues the task on a surviving worker.

Key design decisions
--------------------
* ``_refresh_loop`` is a background asyncio Task cancelled in ``complete()``.
* ``resurrection_loop`` uses ``scan_iter`` (cursor-based) instead of ``KEYS``
  to avoid blocking the Redis event loop on large keyspaces.
* A short-lived ``nx`` lock prevents multiple resurrector processes from
  concurrently re-queuing the same task.
* The ``_bg_send`` closure captures ``t_id`` / ``payload`` via default
  argument binding to avoid the late-binding closure bug.
"""

import asyncio
import contextlib
import json
import logging
from typing import Any

from relier.config import Settings, get_settings
from relier.core.dlq import DeadLetterQueue
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class PhoenixRegistry:
    """Manages task heartbeats, payload persistence, and resurrection."""

    HEARTBEAT_KEY = "rl:hb:{task_id}"
    TASKS_STATE_KEY = "rl:phoenix:{task_id}"
    RESURRECTIONS_KEY = "rl:resurrections:{task_id}"
    RESURRECT_LOCK = "rl:lock:resurrect:{task_id}"
    MONITOR_KEY = "rl:monitoring"

    # Tracks the asyncio Task running each heartbeat refresh loop.
    # Intentionally class-level: shared across all coroutines in the same
    # worker process so that complete() can cancel any task's refresh loop.
    # Safe because a single Celery worker is single-process with one event loop.
    _active_loops: dict[str, "asyncio.Task[None]"] = {}

    # Prevent unbounded tasks spawning
    _send_semaphore = asyncio.Semaphore(50)

    @classmethod
    def _get_settings(cls) -> Settings:
        """Lazy-load settings locally to pick up testcontainer URLs."""
        return get_settings()

    # ==========================================================================
    # Registration
    # ==========================================================================

    @classmethod
    async def register(
        cls,
        task_id: str,
        worker_id: str,
        payload: dict[str, Any],
    ) -> None:
        """Register a task in the shadow registry and begin heartbeat refresh.

        Args:
            task_id:   Celery task ID.
            worker_id: Celery worker hostname (e.g. ``"celery@hostname"``).
            payload:   JSON-serializable dict stored for resurrection use.
        """
        settings = cls._get_settings()
        redis = await get_relier_redis()

        # Use a single Hash key for everything related to this task instance
        task_key = cls.TASKS_STATE_KEY.format(task_id=task_id)

        pipe = redis.pipeline()
        pipe.set(
            cls.HEARTBEAT_KEY.format(task_id=task_id),
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
        """Background coroutine that keeps the heartbeat TTL alive."""
        redis = await get_relier_redis()
        hb_key = cls.HEARTBEAT_KEY.format(task_id=task_id)
        settings = cls._get_settings()
        interval = settings.heartbeat_ttl / 2.0

        try:
            while True:
                await asyncio.sleep(interval)
                extended = await redis.expire(hb_key, settings.heartbeat_ttl)
                if not extended:
                    logger.warning(
                        "Failed to refresh heartbeat TTL; key no longer exists.",
                        extra={"task_id": task_id, "worker_id": worker_id},
                    )
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Heartbeat refresh error.",
                extra={"task_id": task_id, "worker_id": worker_id, "error": str(exc)},
            )
        finally:
            cls._active_loops.pop(task_id, None)

    # ===========================================================================
    # Completion
    # ===========================================================================

    @classmethod
    async def complete(cls, task_id: str) -> None:
        """Clean up the registry when a task finishes successfully.

        Deletes the payload key first so the resurrector cannot observe a
        window where the payload exists but the heartbeat is gone.
        """
        loop_task = cls._active_loops.pop(task_id, None)

        if loop_task and not loop_task.done():
            loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await loop_task

        redis = await get_relier_redis()
        pipe = redis.pipeline()
        # Delete payload BEFORE heartbeat to close the detection window.
        pipe.delete(cls.TASKS_STATE_KEY.format(task_id=task_id))
        pipe.delete(cls.HEARTBEAT_KEY.format(task_id=task_id))
        pipe.delete(cls.RESURRECTIONS_KEY.format(task_id=task_id))
        await pipe.execute()

    @classmethod
    async def is_active(cls, task_id: str) -> bool:
        """Return whether a task is currently registered in the shadow registry."""
        redis = await get_relier_redis()
        return bool(await redis.exists(cls.HEARTBEAT_KEY.format(task_id=task_id)))

    # ===========================================================================
    # Resurrection loop (runs in the dedicated resurrector container)
    # ===========================================================================

    @classmethod
    async def resurrection_loop(cls) -> None:
        """Background loop that detects and revives tasks from dead workers.

        Intended to run in a dedicated ``guardian`` / ``resurrector`` container.
        Exits only if the process is killed.
        """
        from relier.core.dlq import DeadLetterQueue  # avoid circular import
        from relier.tasks.app import celery_app  # avoid circular import

        dead_letter_queue = DeadLetterQueue()

        settings = cls._get_settings()
        redis = await get_relier_redis()
        logger.info("Phoenix resurrector started.")

        while True:
            try:
                await cls._monitor_resurrected_tasks(redis)
                await cls._scan_and_resurrect(redis, dead_letter_queue, celery_app)
                logger.debug("Resurrector pass complete.")
            except Exception as exc:
                logger.error(
                    "Resurrector loop error.",
                    extra={"error": str(exc)},
                    exc_info=True,
                )
            await asyncio.sleep(settings.resurrection_check_interval)

    # ===========================================================================
    # Private helpers
    # ===========================================================================

    @classmethod
    async def _monitor_resurrected_tasks(cls, redis: Any) -> None:
        """Phase 1: Track previously resurrected tasks back to completion."""
        raw = await redis.hgetall(cls.MONITOR_KEY)
        if not raw:
            return

        logger.debug(
            "Monitoring resurrected tasks.",
            extra={"count": len(raw)},
        )

        # Normalize Redis bytes to str
        monitoring_data = {
            (k.decode() if isinstance(k, bytes) else k): (
                v.decode() if isinstance(v, bytes) else v
            )
            for k, v in raw.items()
        }

        task_ids = list(monitoring_data.keys())

        pipe = redis.pipeline()
        for t_id in task_ids:
            pipe.exists(cls.HEARTBEAT_KEY.format(task_id=t_id))
            pipe.exists(cls.TASKS_STATE_KEY.format(task_id=t_id))

        results = await pipe.execute()

        # Step through results in pairs (hb_exists, payload_exists)
        for i, t_id in enumerate(task_ids):
            hb_exists = bool(results[i * 2])
            payload_exists = bool(results[i * 2 + 1])
            state = int(monitoring_data[t_id])

            if state == 0 and hb_exists:
                # Heartbeat re-appeared — task is running on new worker.
                await redis.hset(cls.MONITOR_KEY, t_id, 1)
                logger.info("Resurrected task is alive.", extra={"task_id": t_id})

            elif state == 1 and not hb_exists and payload_exists:
                # Worker died again — release back to the main scan loop.
                logger.warning(
                    "Resurrected task died again; re-releasing to scan.",
                    extra={"task_id": t_id},
                )
                await redis.hdel(cls.MONITOR_KEY, t_id)

            elif not payload_exists:
                # Payload cleaned up — task completed successfully.
                logger.info(
                    "Resurrected task completed successfully.",
                    extra={"task_id": t_id},
                )
                await redis.hdel(cls.MONITOR_KEY, t_id)

    @classmethod
    async def _scan_and_resurrect(
        cls,
        redis: Any,
        dead_letter_queue: DeadLetterQueue,
        celery_app: Any,
    ) -> None:
        """Phase 2: Scan for new dead tasks and re-queue them."""
        settings = cls._get_settings()

        async for tasks_key in redis.scan_iter(match="rl:phoenix:*", count=100):
            state_data = await redis.hgetall(tasks_key)
            if not state_data:
                continue

            tasks_key_str = (
                tasks_key.decode("utf-8")
                if isinstance(tasks_key, bytes)
                else str(tasks_key)
            )

            t_id = tasks_key_str.split(":")[-1]
            hb_key = cls.HEARTBEAT_KEY.format(task_id=t_id)

            # Skip tasks that are alive or already being monitored.
            pipe = redis.pipeline()
            pipe.hexists(cls.MONITOR_KEY, t_id)
            pipe.exists(hb_key)
            is_monitored, hb_exists = await pipe.execute()

            if is_monitored or hb_exists:
                continue

            # Acquire a distributed lock to prevent duplicate resurrection.
            lock_key = cls.RESURRECT_LOCK.format(task_id=t_id)
            acquired = await redis.set(lock_key, "1", nx=True, ex=30)

            if not acquired:
                continue

            res_key = cls.RESURRECTIONS_KEY.format(task_id=t_id)
            count = await redis.incr(res_key)

            # Safely parse payload
            raw_payload = state_data.get(b"payload") or state_data.get("payload")
            try:
                payload = json.loads(raw_payload) if raw_payload else {}
            except (TypeError, json.JSONDecodeError):
                logger.error("Failed to decode task payload.", extra={"task_id": t_id})
                payload = {}

            # Safely parse partial state
            raw_partial = state_data.get(b"partial_result") or state_data.get(
                "partial_result"
            )
            try:
                partial_data = json.loads(raw_partial) if raw_partial else None
            except (TypeError, json.JSONDecodeError):
                logger.error(
                    "Failed to decode partial result.", extra={"task_id": t_id}
                )
                partial_data = None

            # Check for Quarantine
            if count > settings.max_resurrections:
                logger.error(
                    "Task exceeded max resurrections; quarantining.",
                    extra={"task_id": t_id, "count": count},
                )
                await dead_letter_queue.quarantine(
                    t_id,
                    reason="max_resurrections_exceeded",
                    payload=payload,
                    partial_result=partial_data,
                )
                await redis.delete(cls.TASKS_STATE_KEY.format(task_id=t_id), res_key)
                await redis.hdel(cls.MONITOR_KEY, t_id)
                continue

            # 4. Resurrection Path - Abort if payload is completely missing/corrupted
            if not payload:
                continue

            # 5. Inject partial result for Resurrection
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

            if ghost_worker_id:
                await redis.zrem(f"rl:inflight:{ghost_worker_id}", t_id)

            logger.warning(
                "Worker death detected; resurrecting task.",
                extra={
                    "task_id": t_id,
                    "attempt": count,
                    "max": settings.max_resurrections,
                    "ghost_worker": ghost_worker_id,
                },
            )

            # Mark for monitoring BEFORE sending to prevent any detection gap.
            await redis.hset(cls.MONITOR_KEY, t_id, 0)

            asyncio.create_task(cls._safe_bg_send(t_id, payload, celery_app))

    @classmethod
    async def _safe_bg_send(cls, task_id: str, payload: dict, celery_app: Any) -> None:
        """Wrapper to limit concurrency of background sends."""
        try:
            async with cls._send_semaphore:
                await cls._bg_send(task_id, payload, celery_app)
        except Exception as exc:
            logger.error(
                "Error in background send wrapper.",
                extra={"task_id": task_id, "error": str(exc)},
                exc_info=True,
            )

    @classmethod
    async def _bg_send(
        cls,
        task_id: str,
        payload: dict[str, Any],
        celery_app: Any,
    ) -> None:
        """Re-queue a dead task on the Celery broker in a background task."""
        try:
            loop = asyncio.get_running_loop()

            logger.info(
                "Attempting to send task to broker.",
                extra={"task_id": task_id, "task_name": payload.get("task_name")},
            )

            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: celery_app.send_task(
                        payload["task_name"],
                        args=payload.get("args", []),
                        kwargs=payload.get("kwargs", {}),
                        queue=payload.get("queue", "default"),
                        task_id=task_id,
                    ),
                ),
                timeout=10.0,
            )
            logger.info("Task re-queued.", extra={"task_id": task_id})
        except Exception as exc:
            logger.error(
                "Failed to re-queue task.",
                extra={"task_id": task_id, "error": str(exc)},
                exc_info=True,
            )

    # ==========================================================================
    # Partial State
    # ==========================================================================

    @classmethod
    async def update_partial_state(cls, task_id: str, state: Any) -> None:
        """Update the persistent record with partial progress."""
        redis = await get_relier_redis()
        # Merge or overwrite the partial_result in the hash
        await redis.hset(
            cls.TASKS_STATE_KEY.format(task_id=task_id),
            "partial_result",
            json.dumps(state),
        )  # type: ignore[misc]
