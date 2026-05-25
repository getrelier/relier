import asyncio

import pytest

from relier.core.timeouts import TaskContext, TimeoutEnforcer


@pytest.mark.asyncio
class TestTimeoutEnforcer:
    async def test_soft_timeout_fires_cleanup_hook(self) -> None:
        """Verifies on_soft hook is called safely."""
        cleanup_called = False

        async def mock_cleanup(ctx: TaskContext):
            nonlocal cleanup_called
            cleanup_called = True

        async def slow_func(*args, **kwargs):
            await asyncio.sleep(5)
            return "done"

        # Set soft timeout to 0.1s, hard to 1s
        with pytest.raises(TimeoutError):
            await TimeoutEnforcer.run(
                func=slow_func,
                args=(),
                kwargs={},
                soft=0.1,
                hard=0.5,
                on_soft=mock_cleanup,
                task_id="test_soft_hook",
            )
        assert cleanup_called is True

    async def test_hard_timeout_terminates_task(self) -> None:
        """Verifies hard timeout raises TimeoutError and cancels task."""

        async def infinite_task():
            await asyncio.sleep(100)

        with pytest.raises(TimeoutError, match="exceeded hard timeout"):
            await TimeoutEnforcer.run(
                func=infinite_task,
                args=(),
                kwargs={},
                soft=None,
                hard=0.1,
                on_soft=None,
                task_id="test_hard_kill",
            )

    async def test_task_completes_normally(self) -> None:
        """Verifies successful path doesn't trigger timeouts."""

        async def fast_task(x):
            return x * 2

        result = await TimeoutEnforcer.run(
            func=fast_task,
            args=(10,),
            kwargs={},
            soft=1,
            hard=2,
            on_soft=None,
            task_id="test_success",
        )
        assert result == 20
