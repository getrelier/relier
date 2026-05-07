"""
Relier Core — Dead Letter Queue (DLQ).

Quarantines tasks that exceed their maximum resurrection limit or suffer
catastrophic, unrecoverable failures.  Quarantined tasks are parked in a
Redis Hash and can be inspected, released, or purged via the CLI or API.

DLQ key layout::

    rl:dlq                  — Hash: {task_id → JSON envelope}
"""

import json
import logging
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, cast

from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """Manages quarantine, inspection, and release of permanently failed tasks."""

    DLQ_HASH_KEY = "rl:dlq"

    @classmethod
    async def quarantine(
        cls,
        task_id: str,
        reason: str,
        payload: dict[str, Any] | None = None,
        partial_result: Any | None = None,
    ) -> None:
        """Move a task into the DLQ and clean up its active Phoenix state.

        Args:
            task_id: The Celery task ID.
            reason:  Human-readable reason (e.g., ``"max_resurrections_exceeded"``).
            payload: Optional payload dict.  If omitted, it is fetched from Redis.
        """
        redis = await get_relier_redis()

        # Fetch payload from Redis if not supplied.  The Phoenix state is
        # typically stored as a Hash with a `payload` field, but older code or
        # tooling may store a raw JSON string at the key. Try both formats so
        # quarantine is resilient.
        if payload is None:
            phoenix_key = f"rl:phoenix:{task_id}"
            raw = await cast(Awaitable[Any], redis.hget(phoenix_key, "payload"))
            if not raw:
                # Fallback: try a plain string value at the same key.
                raw = await cast(Awaitable[Any], redis.get(phoenix_key))

            payload = (
                json.loads(raw)
                if raw
                else {"error": "Payload lost prior to quarantine."}
            )

        # Fetch resurrection count
        raw_count = raw_count = await cast(
            Awaitable[Any], redis.get(f"rl:resurrections:{task_id}")
        )
        resurrection_count = int(raw_count) if raw_count else 0

        # 3. Capture the partial result safely
        raw_partial = await cast(
            Awaitable[Any], redis.hget(f"rl:phoenix:{task_id}", "partial_result")
        )

        # Parse it if it exists and wasn't explicitly provided in the args
        if not partial_result and raw_partial:
            try:
                partial_result = json.loads(raw_partial)
            except json.JSONDecodeError:
                partial_result = None

        dlq_entry: dict[str, Any] = {
            "task_id": task_id,
            "task_name": payload.get("task_name", "unknown"),
            "queue": payload.get("queue", "default"),
            "args": payload.get("args", []),
            "kwargs": payload.get("kwargs", {}),
            "partial_result": partial_result,
            "reason": reason,
            # Backwards compatibility: some callers / consumers expect an
            # `error` field rather than `reason`.
            "error": reason,
            "resurrections": resurrection_count,
            "quarantined_at": datetime.now(UTC).isoformat(),
        }

        # Atomic: add to DLQ and delete all active state in one pipeline.
        pipe = redis.pipeline()
        pipe.hset(cls.DLQ_HASH_KEY, task_id, json.dumps(dlq_entry))
        pipe.delete(
            f"rl:hb:{task_id}",
            f"rl:phoenix:{task_id}",
            f"rl:resurrections:{task_id}",
            f"rl:lock:resurrect:{task_id}",
        )
        await cast(Awaitable[Any], pipe.execute())

        logger.critical(
            "Task quarantined to DLQ.",
            extra={
                "task_id": task_id,
                "reason": reason,
                "resurrections": resurrection_count,
            },
        )

    @classmethod
    async def list_tasks(cls, count: int = 100) -> list[dict[str, Any]]:
        """Return up to *count* quarantined tasks, sorted newest-first."""
        redis = await get_relier_redis()

        results: list[dict[str, Any]] = []
        cursor = 0
        while True:
            cursor, data = await redis.hscan(
                cls.DLQ_HASH_KEY, cursor=cursor, count=count
            )
            for raw_json in data.values():
                if raw_json:
                    try:
                        results.append(json.loads(raw_json))
                    except json.JSONDecodeError:
                        logger.warning("Skipping malformed DLQ entry.")
            if cursor == 0:
                break

        results.sort(key=lambda x: x.get("quarantined_at", ""), reverse=True)
        return results

    @classmethod
    async def inspect(cls, task_id: str) -> dict[str, Any] | None:
        """Fetch the full envelope for a specific quarantined task."""
        redis = await get_relier_redis()
        raw = await redis.hget(cls.DLQ_HASH_KEY, task_id)  # type: ignore[misc]
        if raw:
            return json.loads(raw)  # type: ignore[no-any-return]
        return None

    @classmethod
    async def release(cls, task_id: str) -> bool:
        """Remove a task from the DLQ and re-submit it to the Celery broker.

        Returns:
            ``True`` on success, ``False`` if the task was not found.
        """
        from relier.tasks.app import celery_app

        redis = await get_relier_redis()
        raw = await redis.hget(cls.DLQ_HASH_KEY, task_id)  # type: ignore[misc]

        if not raw:
            logger.warning(
                "Release failed: task not found in DLQ.", extra={"task_id": task_id}
            )
            return False

        entry = json.loads(raw)

        if entry.get("task_name") == "unknown":
            logger.error(
                "Cannot release task: task_name is unknown.",
                extra={"task_id": task_id},
            )
            return False

        # Prepare kwargs, injecting stored partial_result as `checkpoint` if present
        kwargs = entry.get("kwargs") or {}
        partial = entry.get("partial_result")
        if partial is not None:
            kwargs["checkpoint"] = partial

        logger.debug(
            "Releasing DLQ task: sending to broker.",
            extra={"task_id": task_id, "task_name": entry.get("task_name")},
        )

        celery_app.send_task(
            entry["task_name"],
            args=entry.get("args", []),
            kwargs=kwargs,
            queue=entry.get("queue", "default"),
            task_id=task_id,  # Preserve original ID so Phoenix tracking continues.
        )

        # Preserve the resurrection count so poison-pill tasks can't cycle
        # through DLQ releases indefinitely.
        prev_count = entry.get("resurrections", 0)
        if prev_count > 0:
            await redis.set(f"rl:resurrections:{task_id}", str(prev_count))

        await redis.hdel(cls.DLQ_HASH_KEY, task_id)  # type: ignore[misc]
        logger.info(
            "Task released from DLQ.",
            extra={"task_id": task_id, "queue": entry.get("queue")},
        )
        return True

    @classmethod
    async def purge(cls) -> int:
        """Permanently delete all tasks in the DLQ.

        Returns:
            The number of tasks deleted.
        """
        redis = await get_relier_redis()
        count = await redis.hlen(cls.DLQ_HASH_KEY)  # type: ignore[misc]
        await redis.delete(cls.DLQ_HASH_KEY)
        logger.warning("DLQ purged.", extra={"count": count})
        return int(count) if count else 0
