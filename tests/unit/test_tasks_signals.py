import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relier.tasks.signals import on_task_failure, on_task_postrun


@pytest.mark.asyncio
async def test_on_task_postrun_success():
    """Verify that successful tasks trigger SLO metric recording."""
    mock_loop = MagicMock()
    mock_loop.is_running.return_value = True

    import concurrent.futures

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
        patch("relier.tasks.app.worker_loop", mock_loop),
        patch("relier.storage.redis.RedisManager.close", new_callable=AsyncMock),
        patch("relier.storage.database.DatabaseManager.close", new_callable=AsyncMock),
        patch("relier.core.slo.SLOMetrics.record_event", AsyncMock()) as mock_record,
        patch(
            "asyncio.run_coroutine_threadsafe",
            side_effect=mock_run_coroutine,
        ) as mock_run,
    ):
        on_task_postrun(state="SUCCESS", task_id="123")

        assert mock_run.called
        assert mock_record.called
        mock_record.assert_called_once_with("success")


@pytest.mark.asyncio
async def test_on_task_postrun_no_loop():
    """Verify that it handles cases where the worker loop is missing."""
    with (
        patch("relier.tasks.app.worker_loop", None),
        patch("relier.tasks.signals.logger") as mock_logger,
    ):
        on_task_postrun(state="SUCCESS", task_id="123")
        assert mock_logger.warning.called

    # Allow background cleanup to finish
    await asyncio.sleep(0)


def test_on_task_failure_logging():
    """Verify that task failures are logged with exception context."""
    with patch("relier.tasks.signals.logger") as mock_logger:
        on_task_failure(task_id="123", exception=ValueError("Boom"))
        assert mock_logger.error.called
        _, kwargs = mock_logger.error.call_args
        assert kwargs["extra"]["task_id"] == "123"
        assert kwargs["extra"]["exception_type"] == "ValueError"
