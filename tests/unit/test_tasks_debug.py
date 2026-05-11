import asyncio
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_debug_tasks() -> None:
    """Exercise debug tasks for coverage without nested loop errors."""
    import concurrent.futures

    from relier.tasks.debug import (
        failing_task,
        increment_task,
        resurrection_task,
        slow_task,
    )

    def mock_run_coroutine(coro, loop=None):
        """Manually mark the coroutine as exhausted properly and return a concurrent Future."""
        if asyncio.iscoroutine(coro):
            while True:
                try:
                    coro.send(None)
                except StopIteration as e:
                    f = concurrent.futures.Future()
                    f.set_result(e.value)
                    return f
                except Exception as e:
                    f = concurrent.futures.Future()
                    f.set_exception(e)
                    return f
        else:
            # Fallback for mock objects or non-coroutines
            f = concurrent.futures.Future()
            f.set_result(None)
            return f

    with (
        patch("relier.storage.redis.RedisManager.close", new_callable=AsyncMock),
        patch(
            "relier.core.shutdown.GracefulShutdownHandler.drain",
            new_callable=AsyncMock,
        ),
        patch("asyncio.run_coroutine_threadsafe", side_effect=mock_run_coroutine),
        patch(
            "relier.tasks.debug.get_relier_redis", new_callable=AsyncMock
        ) as mock_get_redis,
    ):
        # mock_get_redis is the mocked async function; its return_value is the Redis client
        mock_get_redis.return_value.incr.return_value = 1

        # Double-unwrap to bypass both Celery's and Relier's decorators
        res = await increment_task.__wrapped__.__wrapped__("key")
        assert res == 1

        with patch("asyncio.sleep", new_callable=AsyncMock):
            res = await slow_task.__wrapped__.__wrapped__(1, "key")
            assert res == "done"

            res = await resurrection_task.__wrapped__.__wrapped__(1, "key")
            assert res == "done"

        with pytest.raises(ValueError, match="Intentional failure"):
            await failing_task.__wrapped__.__wrapped__()

    # Allow background cleanup coroutines to finish
    await asyncio.sleep(0)
