import asyncio
import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from relier import rl_task


# high_priority  — latency-sensitive, must finish fast
@rl_task(queue="high_priority", idempotent=True, idempotency_ttl=120)
async def charge_card(order_id: str, amount_cents: int) -> dict:
    await asyncio.sleep(0.05)
    return {"order_id": order_id, "charged": amount_cents}


# default — normal background work
@rl_task(queue="default")
async def send_order_confirmation(order_id: str, email: str) -> dict:
    await asyncio.sleep(0.1)
    return {"order_id": order_id, "email": email, "sent": True}


# low_priority — bulk / best-effort; won't starve high_priority
@rl_task(queue="low_priority")
async def generate_monthly_invoice(customer_id: str, month: str) -> dict:
    await asyncio.sleep(1.5)  # simulate heavy PDF generation
    return {"customer_id": customer_id, "month": month, "pages": 4}
