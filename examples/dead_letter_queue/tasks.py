import asyncio
import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")
os.environ.setdefault("RELIER_MAX_RESURRECTIONS", "2")  # fail fast for demo

from relier import rl_task


@rl_task(queue="default")
async def poison_pill(record_id: str) -> dict:
    """
    Always raises — exhausts resurrections and lands in the DLQ.
    Inspect with:  rl dlq list
    Release with:  rl dlq release <task_id>
    Discard with:  rl dlq discard <task_id>
    """
    raise ValueError(f"unrecoverable error processing record {record_id}")


@rl_task(queue="default", idempotent=True)
async def recoverable_task(record_id: str) -> dict:
    """Safe to re-run after being released from the DLQ."""
    await asyncio.sleep(0.1)
    return {"record_id": record_id, "status": "ok"}
