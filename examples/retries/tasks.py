import asyncio
import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")
os.environ.setdefault("RELIER_MAX_RESURRECTIONS", "5")

from relier import rl_task, task_context


@rl_task(queue="default", idempotent=True, idempotency_ttl=3600)
async def process_payment(payment_id: str, amount_cents: int) -> dict:
    """
    Idempotent task: safe to re-run after a worker crash.
    Phoenix will re-queue this automatically if the worker dies mid-execution;
    idempotency ensures the charge happens exactly once per payment_id.
    """
    await asyncio.sleep(0.1)  # simulate external payment API call
    return {"payment_id": payment_id, "status": "charged", "amount_cents": amount_cents}


@rl_task(queue="default")
async def bulk_import(batch_id: str, total_rows: int) -> dict:
    """
    Checkpointed task: resumes from last saved row after a crash.
    Set RELIER_MAX_RESURRECTIONS to control how many times Phoenix retries.
    """
    start = 0
    if task_context.partial_result:
        start = task_context.partial_result.get("processed", 0)

    for i in range(start, total_rows):
        await asyncio.sleep(0.005)  # simulate row processing
        if i % 50 == 0:
            await task_context.set_partial({"processed": i})

    return {"batch_id": batch_id, "rows": total_rows}
