import asyncio
from datetime import timedelta

from relier import rl_task
from relier.tasks.app import celery_app


@rl_task(queue="high_priority", idempotent=True)
async def charge_customer(order_id: str, amount_cents: int) -> dict:
    await asyncio.sleep(0.05)
    return {"order_id": order_id, "charged": amount_cents}


@rl_task(queue="default")
async def send_receipt(order_id: str, email: str) -> dict:
    await asyncio.sleep(0.1)
    return {"order_id": order_id, "email": email, "sent": True}


@rl_task(queue="low_priority")
async def cleanup_expired_records() -> dict:
    deleted = 0  # replace with actual DB cleanup
    return {"deleted": deleted}


celery_app.conf.beat_schedule = {
    "cleanup-every-hour": {
        "task": cleanup_expired_records.name,
        "schedule": timedelta(hours=1),
    },
}
