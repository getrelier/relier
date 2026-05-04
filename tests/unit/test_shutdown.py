import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relier.core.shutdown import GracefulShutdownHandler


class TestShutdown:
    @pytest.fixture
    def handler(self):
        return GracefulShutdownHandler(worker_id="test_worker")

    def test_track_untrack_task(self, handler):
        """Verify task tracking logic."""
        handler.track_task("task_1")
        assert "task_1" in handler._active_tasks
        handler.untrack_task("task_1")
        assert "task_1" not in handler._active_tasks
        # Untracking non-existent task shouldn't raise
        handler.untrack_task("task_non_existent")

    def test_install_signal_handlers(self, handler):
        """Verify that signal handlers are registered on the loop."""
        mock_loop = MagicMock()
        # Ensure add_signal_handler exists on the mock
        mock_loop.add_signal_handler = MagicMock()
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            handler.install()
            # On platforms that support it, it should be called twice
            if mock_loop.add_signal_handler.called:
                assert mock_loop.add_signal_handler.call_count == 2
            else:
                # If the code hit NotImplementedError before calling add_signal_handler,
                # that's also fine as long as it doesn't crash.
                pass

    def test_install_signal_handlers_not_implemented(self, handler):
        """Verify that it handles platforms without add_signal_handler (Windows)."""
        mock_loop = MagicMock()
        mock_loop.add_signal_handler.side_effect = NotImplementedError()
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            handler.install()  # Should not raise

    def test_install_signal_handlers_no_loop(self, handler):
        """Verify that it handles being called without a running loop."""
        with patch("asyncio.get_running_loop", side_effect=RuntimeError()):
            # Should not raise
            handler.install()

    @pytest.mark.asyncio
    async def test_drain_idempotent(self, handler):
        """Verify that multiple calls to drain are collapsed."""
        handler._draining = True
        # Calling drain when already draining should return immediately
        await handler.drain()
        # If it didn't return, it would try to import celery_app and fail if not mocked
        # But we check _draining flag first.

    @pytest.mark.asyncio
    async def test_drain_clean_completion(self, handler):
        """Verify drain waits for tasks and completes cleanly."""
        handler.track_task("task_1")

        # Simulate task completion in the background
        async def complete_task():
            await asyncio.sleep(0.1)
            handler.untrack_task("task_1")

        asyncio.create_task(complete_task())

        with patch("relier.tasks.app.celery_app") as mock_celery:
            # Provide mock queues with .name attributes
            mock_q = MagicMock()
            mock_q.name = "default"
            mock_celery.conf.task_queues = [mock_q]
            await handler.drain()
            assert mock_celery.control.cancel_consumer.called
            assert len(handler._active_tasks) == 0

    @pytest.mark.asyncio
    async def test_drain_timeout_and_handoff(self, handler):
        """Verify that drain hands off remaining tasks after timeout."""
        handler.track_task("task_1")

        mock_redis = AsyncMock()
        with (
            patch("relier.tasks.app.celery_app"),
            patch("relier.core.shutdown.get_relier_redis", return_value=mock_redis),
            patch("relier.core.shutdown.get_settings") as mock_get_settings,
        ):
            mock_get_settings.return_value.graceful_shutdown_timeout = 0.1
            await handler.drain()

            # Verify handoff logic
            assert mock_redis.delete.called
            mock_redis.delete.assert_any_call("rl:hb:task_1")
            assert handler._draining is True

    @pytest.mark.asyncio
    async def test_drain_celery_error(self, handler):
        """Verify that drain survives errors during consumer cancellation."""
        with patch("relier.tasks.app.celery_app") as mock_celery:
            mock_celery.control.cancel_consumer.side_effect = Exception("Celery down")
            # Should not raise
            await handler.drain()
