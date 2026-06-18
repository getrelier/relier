import asyncio
import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from relier import rl_task

# Each step receives the previous step's return value as its first positional argument.
# Returning order_id (a string) threads it through the chain cleanly.


@rl_task(queue="default")
async def validate_order(order_id: str) -> str:
    await asyncio.sleep(0.05)
    # raises ValueError here to abort the entire chain
    return order_id


@rl_task(queue="default")
async def reserve_inventory(order_id: str) -> str:
    await asyncio.sleep(0.1)
    return order_id


@rl_task(queue="high_priority")
async def process_payment(order_id: str) -> str:
    await asyncio.sleep(0.05)
    return order_id


@rl_task(queue="default")
async def send_confirmation(order_id: str) -> dict:
    await asyncio.sleep(0.1)
    return {"order_id": order_id, "done": True}
