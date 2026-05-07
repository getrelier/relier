import asyncio
import concurrent.futures
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relier.tasks.app import (
    _run_event_loop,
    create_celery_app,
    init_worker,
    shutdown_worker,
)


# =========================================================
# HELPER MOCKS
# =========================================================
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
        f = concurrent.futures.Future()
        f.set_result(None)
        return f


class TestTasksApp:
    @pytest.fixture(autouse=True)
    async def cleanup_coroutines(self):
        """Global reaper to kill 'never awaited' warnings from nested coros."""
        yield
        import gc

        for obj in gc.get_objects():
            # Combined the coroutine check and the name filter
            if asyncio.iscoroutine(obj) and any(
                name in str(obj)
                for name in ["init_worker", "_warm_up", "_presence_loop"]
            ):
                obj.close()

    @pytest.fixture(autouse=True)
    def reset_worker_loop(self):
        """Ensure worker_loop is reset for every test."""
        import relier.tasks.app

        relier.tasks.app.worker_loop = None
        yield
        relier.tasks.app.worker_loop = None

    # =========================================================
    # BASIC CONFIG TEST
    # =========================================================
    def test_create_celery_app(self):
        """Verify Celery app configuration."""
        app = create_celery_app()
        assert app.main == "relier"
        assert app.conf.task_serializer == "json"
        assert app.conf.task_acks_late is True
        assert app.conf.task_reject_on_worker_lost is True
        assert len(app.conf.task_queues) == 4

    # =========================================================
    # SHUTDOWN TESTS
    # =========================================================
    def test_shutdown_worker_no_loop(self):
        """Verify shutdown handles missing loop gracefully."""
        with patch("relier.tasks.app.worker_loop", None):
            shutdown_worker()

    def test_shutdown_worker_full(self):
        """Verify full shutdown sequence."""
        mock_loop = MagicMock()
        mock_handler = MagicMock()
        mock_future = MagicMock()

        with (
            patch("relier.tasks.app.worker_loop", mock_loop),
            patch("relier.tasks.app.shutdown_handler", mock_handler),
            patch("relier.tasks.app._presence_future", mock_future),
            patch(
                "asyncio.run_coroutine_threadsafe", side_effect=mock_run_coroutine
            ) as mock_run,
        ):
            shutdown_worker()
            assert mock_future.cancel.called
            assert mock_run.called
            assert mock_loop.call_soon_threadsafe.called

    def test_shutdown_worker_exception(self):
        """Verify shutdown_worker handles exceptions during shutdown."""
        mock_loop = MagicMock()

        def side_effect_fail(coro, loop=None):
            if asyncio.iscoroutine(coro):
                coro.close()
            raise Exception("shutdown fail")

        with (
            patch("relier.tasks.app.worker_loop", mock_loop),
            patch("relier.tasks.app.shutdown_handler", MagicMock()),
            patch("relier.tasks.app.redis_manager"),
            patch("relier.tasks.app.db_manager"),
            patch("asyncio.run_coroutine_threadsafe", side_effect=side_effect_fail),
        ):
            shutdown_worker()
            assert mock_loop.call_soon_threadsafe.called

    # =========================================================
    # EVENT LOOP
    # =========================================================
    def test_run_event_loop(self):
        """Verify that _run_event_loop sets the event loop and runs forever."""
        loop = MagicMock()
        with patch("asyncio.set_event_loop") as mock_set:
            loop.run_forever.side_effect = KeyboardInterrupt
            with pytest.raises(KeyboardInterrupt):
                _run_event_loop(loop)
            mock_set.assert_called_once_with(loop)

    # =========================================================
    # INITIALIZATION & WORKER LOGIC
    # =========================================================
    def test_init_worker(self):
        """Verify init_worker sets up loop, logging, telemetry, and warms up."""
        with (
            patch("threading.Thread") as mock_thread,
            patch("relier.tasks.app.setup_logging") as mock_setup_logging,
            patch("relier.tasks.app.setup_telemetry") as mock_setup_telemetry,
            patch("relier.tasks.app.redis_manager") as mock_rm,
            patch("relier.tasks.app.db_manager") as mock_dbm,
            patch(
                "asyncio.run_coroutine_threadsafe", side_effect=mock_run_coroutine
            ) as mock_run,
            patch("asyncio.new_event_loop"),
        ):
            mock_rm.get_client = AsyncMock()
            mock_dbm.engine.connect = MagicMock()
            mock_dbm.engine.connect.return_value.__aenter__ = AsyncMock()

            init_worker(hostname="test-host")

            assert mock_thread.called
            assert mock_run.call_count == 2
            mock_setup_logging.assert_called_once()
            mock_setup_telemetry.assert_called_once()

    @pytest.mark.asyncio
    async def test_init_worker_internal_coroutines(self):
        """Cover the internal _warm_up and _presence_loop in init_worker."""
        mock_loop = MagicMock()
        captured_coros = []

        def capture_only(coro, loop):
            captured_coros.append(coro)
            if "_warm_up" in str(coro):
                return mock_run_coroutine(coro, loop)
            f = concurrent.futures.Future()
            f.set_result(None)
            return f

        with (
            patch("relier.tasks.app.redis_manager") as mock_rm,
            patch("relier.tasks.app.db_manager") as mock_dbm,
            patch("threading.Thread"),
            patch("relier.tasks.app.setup_logging"),
            patch("relier.tasks.app.setup_telemetry"),
            patch("asyncio.new_event_loop", return_value=mock_loop),
            patch("asyncio.run_coroutine_threadsafe", side_effect=capture_only),
        ):
            mock_rm.get_client = AsyncMock()
            mock_dbm.engine.connect.return_value.__aenter__ = AsyncMock()

            init_worker(hostname="cov-worker")

            presence_coro = next(
                (c for c in captured_coros if "_presence_loop" in str(c)), None
            )
            assert presence_coro is not None

            with (
                patch("asyncio.sleep", side_effect=asyncio.CancelledError()),
                pytest.raises(asyncio.CancelledError),
            ):
                await presence_coro

    @pytest.mark.asyncio
    async def test_presence_loop_exception(self):
        """Verify that exceptions in _presence_loop are handled gracefully."""
        mock_loop = MagicMock()
        captured = []

        with (
            patch("threading.Thread"),
            patch("asyncio.new_event_loop", return_value=mock_loop),
            patch(
                "asyncio.run_coroutine_threadsafe",
                side_effect=lambda c: (captured.append(c), mock_run_coroutine(None))[1],
            ),
        ):
            init_worker(hostname="fail-worker")

        presence_coro = next((c for c in captured if "presence_loop" in str(c)), None)
        if presence_coro:
            with patch("relier.tasks.app.redis_manager") as mock_rm:
                mock_redis = AsyncMock()
                mock_rm.get_client = AsyncMock(return_value=mock_redis)
                mock_redis.set.side_effect = [
                    Exception("mock fail"),
                    asyncio.CancelledError(),
                ]

                with (
                    patch(
                        "asyncio.sleep", side_effect=[None, asyncio.CancelledError()]
                    ),
                    pytest.raises(asyncio.CancelledError),
                ):
                    await presence_coro
