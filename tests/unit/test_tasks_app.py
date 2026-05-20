import asyncio
import concurrent.futures
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relier.tasks.app import (
    _run_event_loop,
    create_celery_app,
    init_worker_process,
    shutdown_worker,
    start_relier_identity,
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
                for name in [
                    "init_worker_process",
                    "_warm_up",
                    "_presence_loop",
                    "_cleanup_dead_workers",
                    "get_client",
                    "_execute_mock_call",
                ]
            ):
                obj.close()

    @pytest.fixture(autouse=True)
    def reset_worker_loop(self):
        """Ensure worker_loop is reset for every test."""
        import relier.tasks.app

        relier.tasks.app.worker_loop = None
        yield
        relier.tasks.app.worker_loop = None

    @pytest.fixture(autouse=True)
    def fake_sender(self):
        class FakeSender:
            def __init__(self, hostname):
                self.hostname = hostname

        return FakeSender

    # =========================================================
    # BASIC CONFIG TEST
    # =========================================================
    def test_create_celery_app(self) -> None:
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
    def test_shutdown_worker_no_loop(self) -> None:
        """Verify shutdown handles missing loop gracefully."""
        with patch("relier.tasks.app.worker_loop", None):
            shutdown_worker()

    def test_shutdown_worker_full(self) -> None:
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

    def test_shutdown_worker_exception(self) -> None:
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
            patch("asyncio.run_coroutine_threadsafe", side_effect=side_effect_fail),
        ):
            shutdown_worker()
            assert mock_loop.call_soon_threadsafe.called

    # =========================================================
    # EVENT LOOP
    # =========================================================
    def test_run_event_loop(self) -> None:
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
    def test_init_worker(self) -> None:
        """Verify init_worker sets up loop, logging, telemetry, and validation paths."""
        with (
            patch("threading.Thread") as mock_thread,
            patch("relier.tasks.app.setup_logging") as mock_setup_logging,
            patch("relier.tasks.app.setup_telemetry") as mock_setup_telemetry,
            patch("relier.tasks.app.redis_manager") as mock_rm,
            patch(
                "relier.core.validation.validate_redis_reachable",
                new_callable=AsyncMock,
            ) as mock_val_reachable,
            patch(
                "relier.core.validation.validate_redis_config", new_callable=AsyncMock
            ) as mock_val_redis,
            patch(
                "relier.core.validation.validate_connection_pool",
                new_callable=AsyncMock,
            ) as mock_val_pool,
            patch(
                "asyncio.run_coroutine_threadsafe", side_effect=mock_run_coroutine
            ) as mock_run,
            patch("asyncio.new_event_loop"),
        ):
            mock_rm.get_client = AsyncMock()

            init_worker_process(hostname="test-host")

            assert mock_thread.called

            assert mock_run.call_count == 2
            mock_setup_logging.assert_called_once()
            mock_setup_telemetry.assert_called_once()
            mock_val_reachable.assert_called_once()
            mock_val_redis.assert_called_once()
            mock_val_pool.assert_called_once()

    def test_init_worker_validation_failure(self) -> None:
        """Verify init_worker aborts initialization if validation fails inside inline imports."""

        async def mock_validation_crash(*args, **kwargs):
            raise RuntimeError("Validation blocked: Cannot verify Redis settings.")

        with (
            patch("threading.Thread"),
            patch("relier.tasks.app.setup_logging"),
            patch("relier.tasks.app.setup_telemetry"),
            patch("relier.tasks.app.redis_manager") as mock_rm,
            patch(
                "relier.core.validation.validate_redis_reachable",
                new_callable=AsyncMock,
            ),
            patch(
                "relier.core.validation.validate_redis_config",
                side_effect=mock_validation_crash,
            ),
            patch(
                "relier.core.validation.validate_connection_pool",
                new_callable=AsyncMock,
            ),
            patch("asyncio.new_event_loop"),
            patch("asyncio.run_coroutine_threadsafe", side_effect=mock_run_coroutine),
        ):
            # Ensure the patched redis_manager returns an awaitable client
            mock_rm.get_client = AsyncMock()

            with pytest.raises(RuntimeError, match="Validation blocked"):
                init_worker_process(hostname="fail-host")

    @pytest.mark.asyncio
    async def test_init_worker_internal_coroutines(self, fake_sender) -> None:
        """Cover the internal _warm_up and _presence_loop in init_worker."""
        mock_loop = MagicMock()
        captured_coros = []

        def capture_only(coro, loop):
            captured_coros.append(coro)
            if "_warm_up" in str(coro):
                return mock_run_coroutine(coro, loop)
            f = concurrent.futures.Future()  # type: ignore[var-annotated]
            f.set_result(None)
            return f

        with (
            patch("relier.tasks.app.redis_manager") as mock_rm,
            patch("threading.Thread"),
            patch("relier.tasks.app.setup_logging"),
            patch("relier.tasks.app.setup_telemetry"),
            patch("asyncio.new_event_loop", return_value=mock_loop),
            patch("asyncio.run_coroutine_threadsafe", side_effect=capture_only),
        ):
            mock_rm.get_client = AsyncMock()

            start_relier_identity(sender=fake_sender("cov-worker"))

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
    async def test_presence_loop_exception(self) -> None:
        """Verify that exceptions in _presence_loop are handled gracefully."""
        mock_loop = MagicMock()
        captured = []

        def _capture_coro(c, loop=None):
            captured.append(c)
            return mock_run_coroutine(None)

        with (
            patch("threading.Thread"),
            patch("asyncio.new_event_loop", return_value=mock_loop),
            patch(
                "asyncio.run_coroutine_threadsafe",
                side_effect=_capture_coro,
            ),
        ):
            init_worker_process(hostname="fail-worker")

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
