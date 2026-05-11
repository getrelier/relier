from unittest.mock import AsyncMock, patch

import pytest

from relier.tasks.context import TaskContext


@pytest.mark.asyncio
async def test_task_context_properties() -> None:
    """Verify TaskContext methods and properties."""
    ctx = TaskContext(
        task_id="123",
        task_name="my_task",
        args=(1,),
        kwargs={"a": 2},
        worker_id="worker-1",
    )

    assert ctx.full_name == "my_task[123]"

    with patch(
        "relier.core.phoenix.PhoenixRegistry.update_partial_state",
        new_callable=AsyncMock,
    ) as mock_update:
        await ctx.set_partial(data={"status": "working"})

        assert ctx.partial_result == {"status": "working"}

        mock_update.assert_called_once_with("123", {"status": "working"})
