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
import json
import logging
from typing import Any

from relier.config import Settings, get_settings
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class PhoenixRegistry:
    """Manages task heartbeats, payload persistence, and resurrection."""

    HEARTBEAT_KEY = "rl:hb:{task_id}"
    PAYLOAD_KEY = "rl:payload:{task_id}"
    RESURRECTIONS_KEY = "rl:resurrections:{task_id}"
    RESURRECT_LOCK = "rl:lock:resurrect:{task_id}"
    MONITOR_KEY = "rl:monitoring"

    # Tracks the asyncio Task running each heartbeat refresh loop.
    _active_loops: dict[str, asyncio.Task] = {}

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
        pipe = redis.pipeline()
        pipe.set(
            cls.HEARTBEAT_KEY.format(task_id=task_id),
            worker_id,
            ex=settings.heartbeat_ttl,
        )
        pipe.set(
            cls.PAYLOAD_KEY.format(task_id=task_id),
            json.dumps(payload),
            ex=86400,  # Payload persists for 24 h.
        )
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

        redis = await get_relier_redis()
        pipe = redis.pipeline()
        # Delete payload BEFORE heartbeat to close the detection window.
        pipe.delete(cls.PAYLOAD_KEY.format(task_id=task_id))
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
        from relier.core.dlq import dead_letter_queue  # avoid circular import
        from relier.tasks.app import celery_app  # avoid circular import

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
        monitoring_data = await redis.hgetall(cls.MONITOR_KEY)
        if not monitoring_data:
            return

        logger.debug(
            "Monitoring resurrected tasks.",
            extra={"count": len(monitoring_data)},
        )

        for task_id, state_str in monitoring_data.items():
            state = int(state_str)
            hb_key = cls.HEARTBEAT_KEY.format(task_id=task_id)
            payload_key = cls.PAYLOAD_KEY.format(task_id=task_id)

            hb_exists = await redis.exists(hb_key)
            payload_exists = await redis.exists(payload_key)

            if state == 0 and hb_exists:
                # Heartbeat re-appeared — task is running on new worker.
                await redis.hset(cls.MONITOR_KEY, task_id, 1)
                logger.info("Resurrected task is alive.", extra={"task_id": task_id})

            elif state == 1 and not hb_exists and payload_exists:
                # Worker died again — release back to the main scan loop.
                logger.warning(
                    "Resurrected task died again; re-releasing to scan.",
                    extra={"task_id": task_id},
                )
                await redis.hdel(cls.MONITOR_KEY, task_id)

            elif not payload_exists:
                # Payload cleaned up — task completed successfully.
                logger.info(
                    "Resurrected task completed successfully.",
                    extra={"task_id": task_id},
                )
                await redis.hdel(cls.MONITOR_KEY, task_id)

    @classmethod
    async def _scan_and_resurrect(
        cls,
        redis: Any,
        dead_letter_queue: Any,
        celery_app: Any,
    ) -> None:
        """Phase 2: Scan for new dead tasks and re-queue them."""
        async for p_key in redis.scan_iter(match="rl:payload:*", count=100):
            settings = cls._get_settings()
            if isinstance(p_key, bytes):
                p_key = p_key.decode("utf-8")

            t_id = p_key.split(":")[-1]
            hb_key = cls.HEARTBEAT_KEY.format(task_id=t_id)

            # Skip tasks that are alive or already being monitored.
            if await redis.hexists(cls.MONITOR_KEY, t_id):
                continue
            if await redis.exists(hb_key):
                continue

            # Acquire a distributed lock to prevent duplicate resurrection.
            lock_key = cls.RESURRECT_LOCK.format(task_id=t_id)
            acquired = await redis.set(lock_key, "1", nx=True, ex=10)
            if not acquired:
                continue

            res_key = cls.RESURRECTIONS_KEY.format(task_id=t_id)
            count = await redis.incr(res_key)

            if count > settings.max_resurrections:
                logger.error(
                    "Task exceeded max resurrections; quarantining.",
                    extra={"task_id": t_id, "count": count},
                )
                await dead_letter_queue.quarantine(
                    t_id, reason="max_resurrections_exceeded"
                )
                await redis.delete(p_key, res_key)
                await redis.hdel(cls.MONITOR_KEY, t_id)
                continue

            raw_payload = await redis.get(p_key)
            if not raw_payload:
                continue

            payload = json.loads(raw_payload)
            ghost_worker_id = payload.get("worker_id")
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

            # Capture loop variables explicitly to avoid late-binding closure bugs.
            async def _bg_send(
                _t_id: str = t_id,
                _payload: dict = payload,
            ) -> None:
                try:
                    loop = asyncio.get_running_loop()
                    await asyncio.wait_for(
                        loop.run_in_executor(
                            None,
                            lambda: celery_app.send_task(
                                _payload["task_name"],
                                args=_payload.get("args", []),
                                kwargs=_payload.get("kwargs", {}),
                                queue=_payload.get("queue", "default"),
                                task_id=_t_id,
                            ),
                        ),
                        timeout=10.0,
                    )
                    logger.info("Task re-queued.", extra={"task_id": _t_id})
                except Exception as exc:
                    logger.error(
                        "Failed to re-queue task.",
                        extra={"task_id": _t_id, "error": str(exc)},
                    )

            asyncio.create_task(_bg_send())
