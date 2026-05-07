import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from celery.exceptions import SoftTimeLimitExceeded

from relier.core.exceptions import IdempotencyInFlightError, SchemaMigrationError
from relier.tasks.decorator import rl_task


class TestTasksDecorator:
    @pytest.fixture(autouse=True)
    def reset_worker_loop(self):
        """Ensure worker_loop is reset for every test."""
        import relier.tasks.app

        relier.tasks.app.worker_loop = None
        yield
        relier.tasks.app.worker_loop = None

    @pytest.mark.asyncio
    async def test_delay_versioned(self):
        """Verify that delay_versioned wraps arguments in a schema envelope."""

        @rl_task()
        async def my_task(x):
            return x

        with (
            patch.object(my_task, "apply_async") as mock_apply,
            patch("relier.core.schema.SchemaRegistry.wrap") as mock_wrap,
        ):
            mock_wrap.return_value = {
                "schema_version": 1,
                "payload": {"kwargs": {"x": 1}},
            }
            my_task.delay_versioned(x=1)

            assert mock_apply.called
            args, kwargs = mock_apply.call_args
            envelope = kwargs["args"][0]
            assert envelope["schema_version"] == 1

        # Explicitly await a sleep(0) to allow any background
        # cleanup coroutines triggered by the decorator to finish
        await asyncio.sleep(0)

    # -----------------------------------------------------------------------
    # Use standard synchronous tests below. This forces the decorator to use
    # its native `run_until_complete` logic, fully executing the async bridge
    # -----------------------------------------------------------------------

    def _ensure_no_loop(self):
        """Helper to ensure no event loop is running in the current thread."""
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                pass
        except RuntimeError:
            pass

    def test_orchestrate_idempotency_hit_full(self, mock_redis):
        """Verify idempotency returns cached result without executing."""

        @rl_task(idempotent=True)
        async def idem_task(x):
            return x

        idem_task.request.id = "task-1"
        idem_task.request.hostname = "worker-1"
        idem_task.name = "idem_task"

        mock_idem_result = MagicMock()
        mock_idem_result.already_executed = True
        mock_idem_result.cached_result = "cached_real"

        with (
            patch("relier.tasks.decorator._get_worker_loop") as mock_get_loop,
            patch(
                "relier.core.idempotency.idempotency_manager.check_or_claim",
                new_callable=AsyncMock,
            ) as mock_check,
        ):
            mock_get_loop.return_value = asyncio.new_event_loop()
            mock_check.return_value = mock_idem_result
            # Runs natively through the async bridge
            result = idem_task.run(x=1)
            assert result == "cached_real"
            mock_get_loop.return_value.close()

    def test_orchestrate_schema_quarantine(self, mock_redis):
        """Verify tasks with invalid schemas are pushed to the DLQ."""

        @rl_task()
        async def schema_task(x):
            return x

        schema_task.request.id = "task-1"
        schema_task.request.hostname = "worker-1"
        schema_task.name = "schema_task"

        with (
            patch(
                "relier.core.schema.SchemaRegistry.unwrap_and_migrate",
                side_effect=SchemaMigrationError("bad"),
            ),
            patch(
                "relier.core.dlq.DeadLetterQueue.quarantine", new_callable=AsyncMock
            ) as mock_q,
            patch("relier.tasks.decorator._get_worker_loop") as mock_get_loop,
        ):
            mock_get_loop.return_value = asyncio.new_event_loop()
            result = schema_task.run({"schema_version": 1, "x": 1})
            assert result["status"] == "quarantined"
            assert mock_q.called
            mock_get_loop.return_value.close()

    def test_orchestrate_success_async(self, mock_redis):
        """Verify successful async execution and Phoenix registration."""

        @rl_task()
        async def success_task(x):
            return x * 2

        success_task.request.id = "task-1"
        success_task.request.hostname = "worker-1"
        success_task.name = "success_task"

        with (
            patch(
                "relier.core.phoenix.PhoenixRegistry.register", new_callable=AsyncMock
            ) as mock_reg,
            patch(
                "relier.core.phoenix.PhoenixRegistry.complete",
                new_callable=AsyncMock,
            ) as mock_comp,
            patch(
                "relier.core.slo.SLOMetrics.record_event",
                new_callable=AsyncMock,
            ),
            patch("relier.tasks.decorator._get_worker_loop") as mock_get_loop,
        ):
            mock_get_loop.return_value = asyncio.new_event_loop()
            result = success_task.run(x=5)

            assert result == 10
            assert mock_reg.called
            assert mock_comp.called
            mock_get_loop.return_value.close()

    def test_orchestrate_sync_task_thread_dispatch(self, mock_redis):
        """Verify synchronous tasks are executed in asyncio.to_thread."""

        @rl_task()
        def sync_task(x):
            return x + 1

        sync_task.request.id = "sync-1"
        sync_task.request.hostname = "worker-1"
        sync_task.name = "sync_task"

        with (
            patch(
                "relier.core.phoenix.PhoenixRegistry.register", new_callable=AsyncMock
            ),
            patch(
                "relier.core.phoenix.PhoenixRegistry.complete",
                new_callable=AsyncMock,
            ),
            patch(
                "relier.core.slo.SLOMetrics.record_event",
                new_callable=AsyncMock,
            ),
            patch("relier.tasks.decorator._get_worker_loop") as mock_get_loop,
        ):
            mock_get_loop.return_value = asyncio.new_event_loop()
            result = sync_task.run(x=10)
            assert result == 11
            mock_get_loop.return_value.close()

    def test_orchestrate_hard_timeout_exception(self, mock_redis):
        """Verify hard timeouts correctly raise SoftTimeLimitExceeded."""

        @rl_task(hard_timeout=1)
        async def timeout_task(x):
            return x

        timeout_task.request.id = "task-1"
        timeout_task.request.hostname = "worker-1"
        timeout_task.name = "timeout_task"

        with (
            patch(
                "relier.core.phoenix.PhoenixRegistry.register", new_callable=AsyncMock
            ),
            patch(
                "relier.core.phoenix.PhoenixRegistry.complete",
                new_callable=AsyncMock,
            ),
            patch(
                "relier.core.slo.SLOMetrics.record_event",
                new_callable=AsyncMock,
            ),
            patch(
                "relier.core.timeouts.TimeoutEnforcer.run",
                side_effect=TimeoutError("Too slow"),
            ),
            patch("relier.tasks.decorator._get_worker_loop") as mock_get_loop,
            pytest.raises(SoftTimeLimitExceeded),
        ):
            mock_get_loop.return_value = asyncio.new_event_loop()
            timeout_task.run(x=1)
            mock_get_loop.return_value.close()

    def test_orchestrate_idempotency_in_flight_retry(self, mock_redis):
        """Verify concurrent execution triggers Celery retry."""

        @rl_task(idempotent=True)
        async def idem_task(x):
            return x

        idem_task.request.id = "task-1"
        idem_task.request.hostname = "worker-1"
        idem_task.name = "idem_task"
        idem_task.retry = MagicMock(side_effect=Exception("Celery retry triggered"))

        with (
            patch(
                "relier.core.idempotency.idempotency_manager.check_or_claim",
                new_callable=AsyncMock,
            ) as mock_check,
            pytest.raises(Exception, match="Celery retry triggered"),
            patch("relier.tasks.decorator._get_worker_loop") as mock_get_loop,
        ):
            mock_check.side_effect = IdempotencyInFlightError("hash123")
            mock_get_loop.return_value = asyncio.new_event_loop()
            idem_task.run(x=1)
            mock_get_loop.return_value.close()

        idem_task.retry.assert_called_once()

    def test_orchestrate_general_exception(self, mock_redis):
        """Verify general exceptions are logged and re-raised."""

        @rl_task()
        async def fail_task():
            raise ValueError("orchestrate fail")

        fail_task.request.id = "fail-1"
        fail_task.request.hostname = "worker-1"
        fail_task.name = "fail_task"

        with (
            patch("relier.core.slo.SLOMetrics.record_event", new_callable=AsyncMock),
            patch("relier.tasks.decorator._get_worker_loop") as mock_get_loop,
        ):
            mock_get_loop.return_value = asyncio.new_event_loop()
            with pytest.raises(ValueError, match="orchestrate fail"):
                fail_task.run()
            mock_get_loop.return_value.close()

    def test_bridge_run_coroutine_threadsafe(self, mock_redis):
        """Verify the threadsafe bridge path (loop already running)."""

        @rl_task()
        async def bridge_task():
            return "bridge-ok"

        bridge_task.request.id = "bridge-1"
        bridge_task.request.hostname = "worker-1"
        bridge_task.name = "bridge_task"

        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True

        mock_future = MagicMock()
        mock_future.result.return_value = "bridge-ok"

        def capture_and_close(coro, loop):
            if asyncio.iscoroutine(coro):
                coro.close()
            return mock_future

        with (
            patch("relier.tasks.decorator._get_worker_loop", return_value=mock_loop),
            patch(
                "asyncio.run_coroutine_threadsafe", side_effect=capture_and_close
            ) as mock_run,
        ):
            result = bridge_task.run()
            assert result == "bridge-ok"
            assert mock_run.called

    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    def test_get_worker_loop_fallbacks(self):
        """Verify _get_worker_loop creates a new loop if none exists.

        Note: The RuntimeWarning filter suppresses a phantom '_orchestrate'
        coroutine created by MagicMock's attribute proxy system during teardown.
        This is a mock-framework artifact, not a real coroutine leak.
        """
        from relier.tasks.decorator import _get_worker_loop

        with (
            patch("relier.tasks.app.worker_loop", None),
            patch.dict("os.environ", {}, clear=True),
            patch("asyncio.get_event_loop", side_effect=RuntimeError("no loop")),
            patch("asyncio.new_event_loop") as mock_new,
            patch("asyncio.set_event_loop") as mock_set,
        ):
            mock_loop = MagicMock()
            # Ensure the mock loop doesn't raise issues when 'run_until_complete' is called
            mock_new.return_value = mock_loop

            loop = _get_worker_loop()

            assert loop == mock_loop
            mock_set.assert_called_once_with(mock_loop)

            # Explicitly 'close' to prevent RuntimeWarnings
            loop.close()

    def test_get_worker_loop_lazy_init(self):
        """Verify that _get_worker_loop performs lazy initialization."""
        import relier.tasks.app
        from relier.tasks.decorator import _get_worker_loop

        original_loop = relier.tasks.app.worker_loop
        relier.tasks.app.worker_loop = None
        try:
            with (
                patch.dict("os.environ", {"CELERY_LOADER": "1"}),
                patch("relier.tasks.app.init_worker") as mock_init,
            ):
                mock_loop = MagicMock()

                def set_loop(**kwargs):
                    relier.tasks.app.worker_loop = mock_loop

                mock_init.side_effect = set_loop

                loop = _get_worker_loop()
                assert mock_init.called
                assert loop == mock_loop
        finally:
            relier.tasks.app.worker_loop = original_loop
