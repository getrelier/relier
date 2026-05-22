import asyncio
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_chaos_tasks() -> None:
    """Exercise chaos tasks for coverage without nested loop errors."""
    import concurrent.futures

    from relier.chaos.tasks import (
        chaos_counter,
        chaos_fail,
        chaos_long_running,
        chaos_slow,
    )

    def mock_run_coroutine(coro, loop=None):
        """Manually mark the coroutine as exhausted properly and return a concurrent Future."""
        if asyncio.iscoroutine(coro):
            while True:
                try:
                    coro.send(None)
                except StopIteration as e:
                    f = concurrent.futures.Future()  # type: ignore[var-annotated]
                    f.set_result(e.value)
                    return f
                except Exception as e:
                    f = concurrent.futures.Future()
                    f.set_exception(e)
                    return f
        else:
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
            "relier.chaos.tasks.get_relier_redis", new_callable=AsyncMock
        ) as mock_get_redis,
    ):
        mock_get_redis.return_value.incr.return_value = 1

        res = await chaos_counter.__wrapped__.__wrapped__("key")
        assert res == 1

        with patch("asyncio.sleep", new_callable=AsyncMock):
            res = await chaos_slow.__wrapped__.__wrapped__(1, "key")
            assert res == "done"

            res = await chaos_long_running.__wrapped__.__wrapped__(1, "key")
            assert res == "done"

        with pytest.raises(ValueError, match="Intentional failure"):
            await chaos_fail.__wrapped__.__wrapped__()

    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_chaos_checkpoint_task() -> None:
    """Exercise chaos_checkpoint covering partial_result resume and set_partial paths."""
    from relier.chaos.tasks import chaos_checkpoint
    from relier.core.phoenix import PhoenixRegistry
    from relier.tasks.context import TaskContext, _task_context_var

    ctx = TaskContext(
        task_id="cp_test",
        task_name="chaos_checkpoint",
        args=(2, "marker"),
        kwargs={},
    )
    ctx.partial_result = {"last_step": 1}  # Exercises the resume-from-checkpoint branch
    token = _task_context_var.set(ctx)
    try:
        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch.object(
                PhoenixRegistry, "update_partial_state", new_callable=AsyncMock
            ),
            patch("relier.chaos.tasks.get_relier_redis") as mock_get_redis,
        ):
            mock_client = AsyncMock()
            mock_get_redis.return_value = mock_client
            result = await chaos_checkpoint.__wrapped__.__wrapped__(2, "marker")
    finally:
        _task_context_var.reset(token)
    assert result == "done"
    mock_client.set.assert_called_once_with(
        "rl:chaos:marker:checkpoint:marker:finished", "1", ex=300
    )
