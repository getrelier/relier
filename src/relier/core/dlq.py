"""
Relier Core — Dead Letter Queue (DLQ).

Provides quarantine storage for tasks that can no longer be executed
safely, including poison-pill workloads, resurrection-limit exhaustion,
and unrecoverable task failures.

Quarantined tasks are persisted in Redis and can later be inspected,
released, retried, or permanently purged through the CLI.

DLQ storage layout::

    rl:dlq  — Redis hash mapping {task_id -> JSON envelope}
"""

import json
import logging
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, cast

from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """
    Central DLQ manager for quarantined task lifecycle operations.

    Responsible for:
    - quarantining unrecoverable tasks
    - preserving diagnostic metadata
    - re-submitting released tasks
    - preventing infinite poison-pill resurrection loops
    """

    DLQ_HASH_KEY = RedisKeys.dlq_hash()

    @classmethod
    async def quarantine(
        cls,
        task_id: str,
        reason: str,
        payload: dict[str, Any] | None = None,
        partial_result: Any | None = None,
    ) -> None:
        """
        Quarantine a task and remove all active execution state.

        Tasks enter the DLQ after exceeding resurrection limits or encountering
        failures considered unsafe to retry automatically.

        The quarantine record preserves enough metadata for:
        - forensic inspection
        - manual replay
        - partial-result recovery
        - operational debugging
        """
        redis = await get_relier_redis()

        # Recover the original task payload if the caller did not provide one.
        #
        # Phoenix state is normally stored as a Redis hash containing a ``payload``
        # field. Older tooling or recovery paths may instead persist the payload as
        # a raw JSON string at the same key.
        #
        # Support both layouts so DLQ quarantine remains backward-compatible and
        # resilient to mixed storage formats.

        if payload is None:
            raw = await cast(Awaitable[Any], redis.hget(RedisKeys.phoenix(task_id), "payload"))
            if not raw:
                # Fallback: try a plain string value at the same key.
                raw = await cast(Awaitable[Any], redis.get(RedisKeys.phoenix(task_id)))

            payload = (
                json.loads(raw)
                if raw
                else {"error": "Payload lost prior to quarantine."}
            )

        # Preserve resurrection history so operators can distinguish transient
        # failures from poison-pill workloads.
        raw_count = await cast(
            Awaitable[Any], redis.get(RedisKeys.resurrection(task_id))
        )
        resurrection_count = int(raw_count) if raw_count else 0

        # Recover any persisted checkpoint or partial execution result so released
        # tasks can resume from the last known safe state.
        raw_partial = await cast(
            Awaitable[Any], redis.hget(RedisKeys.phoenix(task_id), "partial_result")
        )

        # Caller-provided partial results take precedence over persisted state.
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
            # Backward compatibility: older consumers may still expect ``error``
            # instead of ``reason``.
            "error": reason,
            "resurrections": resurrection_count,
            "quarantined_at": datetime.now(UTC).isoformat(),
        }

        # Persist the DLQ record and clear active execution state together to
        # minimize orphaned Phoenix metadata.
        pipe = redis.pipeline()
        pipe.hset(cls.DLQ_HASH_KEY, task_id, json.dumps(dlq_entry))
        pipe.delete(
            RedisKeys.heartbeat(task_id),
            RedisKeys.phoenix(task_id),
            RedisKeys.resurrection(task_id),
            RedisKeys.resurrect_lock(task_id),
        )
        await cast(Awaitable[Any], pipe.execute())

        logger.critical(
            "Task moved to dead-letter queue.",
            extra={
                "task_id": task_id,
                "reason": reason,
                "resurrections": resurrection_count,
            },
        )

    @classmethod
    async def list_tasks(cls, count: int = 100) -> list[dict[str, Any]]:
        """
        Return quarantined tasks ordered by most recent quarantine time first.

        Malformed entries are skipped defensively so corrupted DLQ records do
        not break inspection tooling.
        """
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
                        logger.warning("Skipping malformed DLQ payload during scan.")
            if cursor == 0:
                break

        results.sort(key=lambda x: x.get("quarantined_at", ""), reverse=True)
        return results

    @classmethod
    async def inspect(cls, task_id: str) -> dict[str, Any] | None:
        """
        Fetch the full persisted DLQ envelope for a quarantined task.

        Returns ``None`` if the task is not present in the DLQ.
        """
        redis = await get_relier_redis()
        raw = await redis.hget(cls.DLQ_HASH_KEY, task_id)  # type: ignore[misc]
        if raw:
            return json.loads(raw)  # type: ignore[no-any-return]
        return None

    @classmethod
    async def release(cls, task_id: str) -> bool:
        """
        Release a quarantined task back to the broker.

        The original task ID is preserved so resurrection tracking, telemetry,
        and inflight visibility continue seamlessly across the replay attempt.

        If a partial result exists, it is injected into the task kwargs as a
        checkpoint payload.
        """
        from relier.tasks.app import celery_app

        redis = await get_relier_redis()
        raw = await redis.hget(cls.DLQ_HASH_KEY, task_id)  # type: ignore[misc]

        if not raw:
            logger.warning(
                "DLQ release requested for unknown task.", extra={"task_id": task_id}
            )
            return False

        entry = json.loads(raw)

        if entry.get("task_name") == "unknown":
            logger.error(
                "Cannot release DLQ task without a valid task name.",
                extra={"task_id": task_id},
            )
            return False

        # Prepare kwargs, injecting stored partial_result as `checkpoint` if present
        kwargs = entry.get("kwargs") or {}
        partial = entry.get("partial_result")
        if partial is not None:
            kwargs["checkpoint"] = partial

        logger.debug(
            "Replaying quarantined task from DLQ.",
            extra={"task_id": task_id, "task_name": entry.get("task_name")},
        )

        celery_app.send_task(
            entry["task_name"],
            args=entry.get("args", []),
            kwargs=kwargs,
            queue=entry.get("queue", "default"),
            task_id=task_id,  # Preserve original ID so Phoenix tracking continues.
        )

        # Preserve resurrection history so poison-pill tasks cannot bypass
        # quarantine limits through repeated manual releases..
        prev_count = entry.get("resurrections", 0)
        if prev_count > 0:
            await redis.set(RedisKeys.resurrection(task_id), str(prev_count))

        await redis.hdel(cls.DLQ_HASH_KEY, task_id)  # type: ignore[misc]
        logger.info(
            "Task released from DLQ.",
            extra={"task_id": task_id, "queue": entry.get("queue")},
        )
        return True

    @classmethod
    async def purge(cls) -> int:
        """
        Permanently remove all quarantined tasks from the DLQ.

        This operation is irreversible and clears all persisted failure history.
        """
        redis = await get_relier_redis()
        count = await redis.hlen(cls.DLQ_HASH_KEY)  # type: ignore[misc]
        await redis.delete(cls.DLQ_HASH_KEY)
        logger.warning("Dead-letter queue purged.", extra={"count": count})
        return int(count) if count else 0
