"""
Realistic @rl_task definitions used exclusively by the integration test suite.

These tasks live outside src/ and are never part of the installed package.
They mirror the kind of tasks a real application would define, so the tests
read naturally and exercise Relier's machinery the same way a user would.
"""

import asyncio

from relier.storage.redis import get_relier_redis
from relier.tasks.context import task_context
from relier.tasks.decorator import rl_task


@rl_task(idempotent=True)
async def counter_task(key: str) -> int:
    """Idempotent counter — increments once per unique key."""
    redis = await get_relier_redis()
    return int(await redis.incr(f"test:counter:{key}"))


@rl_task(hard_timeout=2)
async def slow_task(duration: int, marker_key: str) -> str:
    """Sleeps for `duration` seconds; hard_timeout=2 triggers the timeout machinery."""
    redis = await get_relier_redis()
    await redis.set(f"test:slow:{marker_key}:started", "1", ex=300)
    await asyncio.sleep(duration)
    await redis.set(f"test:slow:{marker_key}:finished", "1", ex=300)
    return "done"


@rl_task()
async def long_running_task(duration: int, marker_key: str) -> str:
    """Long-running task that yields in small increments; used by kill/resurrect tests."""
    redis = await get_relier_redis()
    await redis.set(f"test:longrun:{marker_key}:started", "1", ex=600)
    for _ in range(duration * 10):
        await asyncio.sleep(0.1)
    await redis.set(f"test:longrun:{marker_key}:finished", "1", ex=600)
    return "done"


@rl_task()
async def checkpoint_task(steps: int, marker: str) -> str:
    """Checkpointed task; resumes from the last saved step after resurrection."""
    redis = await get_relier_redis()
    start_step = 0
    if task_context.partial_result:
        start_step = task_context.partial_result.get("last_step", 0)
    for i in range(start_step, steps):
        await asyncio.sleep(0.1)
        await task_context.set_partial({"last_step": i + 1})
    await redis.set(f"test:checkpoint:{marker}:finished", "1", ex=300)
    return "done"


@rl_task()
async def failing_task() -> None:
    """Always raises; exercises the DLQ and failure path."""
    raise ValueError("Intentional test failure")


@rl_task(name="tests.named_echo_task")
async def named_echo_task(value: str) -> str:
    """Task registered under an explicit stable name; exercises @rl_task(name=...)."""
    return f"echo:{value}"


@rl_task(name="tests.migration_target_task")
async def migration_target_task(order_id: str, region: str = "global") -> dict:
    """Task used by schema-migration e2e tests. v2 signature adds `region`."""
    return {"order_id": order_id, "region": region}
