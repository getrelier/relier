"""
Relier Chaos — Internal target tasks.

The chaos suite needs tasks it can dispatch against in every environment
(including production), so it cannot depend on ``relier.tasks.debug``
which is only included by the Celery app outside production.

These tasks are deliberately minimal: they exist solely to exercise
Relier's reliability machinery (admission control, hard timeouts,
Phoenix resurrection, payload integrity) from the chaos CLI. They do no
useful application work and write only to a chaos-scoped Redis namespace.
"""

import asyncio

from relier.storage.redis import get_relier_redis
from relier.tasks.context import task_context
from relier.tasks.decorator import rl_task

# Namespaced so chaos markers never collide with application state.
_MARKER_PREFIX = "rl:chaos:marker"


@rl_task(idempotent=True)
async def chaos_counter(key: str) -> int:
    """Idempotent counter. Increments once per unique key regardless of how many
    times the task is dispatched with the same arguments."""
    redis = await get_relier_redis()
    val = await redis.incr(f"{_MARKER_PREFIX}:counter:{key}")
    return int(val)


@rl_task()
async def chaos_noop(tag: str = "chaos") -> str:
    """No-op target for ``load-spike`` and ``task-corrupt``.

    Increments a per-tag counter so operators can confirm dispatches landed
    on a worker (useful when validating that admission rejections were the
    actual choke point, not silent broker drops).
    """
    redis = await get_relier_redis()
    await redis.incr(f"{_MARKER_PREFIX}:noop:{tag}")
    return "ok"


@rl_task(hard_timeout=2)
async def chaos_slow(duration: int, marker_key: str) -> str:
    """Sleep for ``duration`` seconds, used by ``slow-task``.

    Decorated with ``hard_timeout=2`` so any ``duration >= 2`` forces the
    two-tier timeout machinery to fire and the task to land in the DLQ.
    """
    redis = await get_relier_redis()
    await redis.set(f"{_MARKER_PREFIX}:slow:{marker_key}:started", "1", ex=300)
    await asyncio.sleep(duration)
    await redis.set(f"{_MARKER_PREFIX}:slow:{marker_key}:finished", "1", ex=300)
    return "done"


@rl_task()
async def chaos_long_running(duration: int, marker_key: str) -> str:
    """Sleep for ``duration`` seconds without a hard timeout.

    Used as the seed task for ``worker-kill``: it must still be running when
    the SIGKILL lands so Phoenix has something orphaned to resurrect. We
    yield to the event loop in small steps so cancellation lands cleanly if
    the test ever needs to abort it.
    """
    redis = await get_relier_redis()
    await redis.set(f"{_MARKER_PREFIX}:longrun:{marker_key}:started", "1", ex=600)
    for _ in range(duration * 10):
        await asyncio.sleep(0.1)
    await redis.set(f"{_MARKER_PREFIX}:longrun:{marker_key}:finished", "1", ex=600)
    return "done"


@rl_task()
async def chaos_checkpoint(steps: int, marker: str) -> str:
    """Checkpointed task that saves progress at each step.

    Used to verify Phoenix resurrection resumes from the last persisted
    checkpoint rather than restarting from the beginning.
    """
    redis = await get_relier_redis()
    start_step = 0
    if task_context.partial_result:
        start_step = task_context.partial_result.get("last_step", 0)
    for i in range(start_step, steps):
        await asyncio.sleep(0.1)
        await task_context.set_partial({"last_step": i + 1})
    await redis.set(f"{_MARKER_PREFIX}:checkpoint:{marker}:finished", "1", ex=300)
    return "done"


@rl_task()
async def chaos_fail() -> None:
    """Always raises an error. Used to exercise the failure and DLQ path."""
    raise ValueError("Intentional failure")
