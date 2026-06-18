import asyncio
import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from relier import rl_task


@rl_task(queue="default")
async def process_image(image_id: str, operation: str) -> dict:
    await asyncio.sleep(0.2)  # simulate image processing
    return {"image_id": image_id, "operation": operation, "done": True}


@rl_task(queue="low_priority")
async def aggregate_results(results: list) -> dict:
    completed = sum(1 for r in results if r.get("done"))
    return {"total": len(results), "completed": completed}
