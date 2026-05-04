import asyncio

from relier.storage.redis import get_relier_redis
from relier.tasks.decorator import rl_task


@rl_task(idempotent=True)
async def increment_task(key: str) -> int:
    """Increment a counter in Redis and return the new value."""
    redis = await get_relier_redis()
    # We use a test-specific counter key
    val = await redis.incr(f"test_counter:{key}")
    return int(val)


@rl_task(hard_timeout=2)
async def slow_task(duration: int, marker_key: str) -> str:
    """Wait for a duration, setting markers in Redis along the way."""
    redis = await get_relier_redis()

    await redis.set(f"test_marker:{marker_key}:started", "1")

    # Simulate work
    await asyncio.sleep(duration)

    await redis.set(f"test_marker:{marker_key}:finished", "1")
    return "done"


@rl_task()
async def resurrection_task(duration: int, marker_key: str) -> str:
    """A task specifically for testing worker kills without timing out."""
    redis = await get_relier_redis()

    await redis.set(f"test_marker:{marker_key}:started", "1")

    # Yield to the event loop so we can kill the worker mid-flight safely
    for _ in range(duration * 10):
        await asyncio.sleep(0.1)

    await redis.set(f"test_marker:{marker_key}:finished", "1")
    return "done"


@rl_task()
async def failing_task() -> None:
    """Always raises an error."""
    raise ValueError("Intentional failure")
