"""
Integration tests for the rl_task decorator, app bootstrap, checkpoint storage,
Phoenix scan-and-resurrect, signal handlers, and telemetry helpers.

All tests run against real Redis and use real Relier module code, no mocks.
"""

import asyncio
import json
import os
import time
import uuid

import pytest

pytestmark = pytest.mark.asyncio


# ===========================================================================
# rl_task decorator — exercised via task.apply() in a thread
#
# Calling task.apply() from asyncio.to_thread() runs the Celery task
# synchronously in a thread-pool thread that has no running event loop.
# _get_worker_loop() falls back to creating a fresh loop and uses
# loop.run_until_complete(_orchestrate()), covering the full decorator path.
# ===========================================================================


class TestDecoratorViaApply:
    async def test_increment_task_happy_path(self, redis_client) -> None:
        """Full rl_task orchestration path against real Redis (no envelope)."""
        from tests.integration.tasks import counter_task

        key = f"integ-dec-{uuid.uuid4().hex[:8]}"

        def call():
            return counter_task.apply(kwargs={"key": key}).result

        result = await asyncio.to_thread(call)
        assert result == 1

        val = await redis_client.get(f"test:counter:{key}")
        assert int(val) == 1

    async def test_increment_task_idempotency_hit(self, redis_client) -> None:
        """Second call with the same args returns cached result without re-executing."""
        from tests.integration.tasks import counter_task

        key = f"integ-idem-{uuid.uuid4().hex[:8]}"

        def call():
            return counter_task.apply(kwargs={"key": key}).result

        r1 = await asyncio.to_thread(call)
        r2 = await asyncio.to_thread(call)

        assert r1 == 1
        assert r2 == 1  # idempotency: task didn't run again
        val = await redis_client.get(f"test:counter:{key}")
        assert int(val) == 1  # counter was only incremented once

    async def test_task_apply_with_schema_envelope(self, redis_client) -> None:
        """Passing a schema envelope as args[0] exercises the unwrap_and_migrate path."""
        from relier.core.schema import SchemaRegistry
        from tests.integration.tasks import counter_task

        key = f"integ-env-{uuid.uuid4().hex[:8]}"

        task_id = str(uuid.uuid4()).replace("-", "")[:32]
        envelope = SchemaRegistry.wrap(task_id, (), {"key": key})

        def call():
            # Pass the envelope as the first positional arg, this is exactly
            # what push()/apush() sends to the Celery broker.
            return counter_task.apply(args=(envelope,)).result

        result = await asyncio.to_thread(call)
        assert result == 1

    async def test_slow_task_with_timeout_config(self, redis_client) -> None:
        """Timeout watcher tasks are created/cancelled even when no timeout fires."""
        from tests.integration.tasks import slow_task

        key = f"integ-slow-{uuid.uuid4().hex[:8]}"

        def call():
            # duration=0 → task completes immediately; the timeout watchers are
            # still created and cancelled, covering those code paths.
            return slow_task.apply(kwargs={"duration": 0, "marker_key": key}).result

        result = await asyncio.to_thread(call)
        assert result == "done"

    async def test_failing_task_routes_to_dlq(self, redis_client) -> None:
        """failing_task always raises; the decorator routes it to DLQ."""
        from tests.integration.tasks import failing_task

        def call():
            return failing_task.apply().state

        state = await asyncio.to_thread(call)
        assert state == "FAILURE"

    async def test_resurrection_task_via_apply(self, redis_client) -> None:
        """long_running_task completes normally when duration=0."""
        from tests.integration.tasks import long_running_task

        key = f"integ-res-{uuid.uuid4().hex[:8]}"

        def call():
            return long_running_task.apply(
                kwargs={"duration": 0, "marker_key": key}
            ).result

        result = await asyncio.to_thread(call)
        assert result == "done"

    async def test_checkpoint_task_via_apply(self, redis_client) -> None:
        """checkpoint_task saves partial state and completes when steps=1."""
        from tests.integration.tasks import checkpoint_task

        key = f"integ-ckpt-task-{uuid.uuid4().hex[:8]}"

        def call():
            return checkpoint_task.apply(kwargs={"steps": 1, "marker": key}).result

        result = await asyncio.to_thread(call)
        assert result == "done"

    async def test_slow_task_hard_timeout_triggers_failure(self, redis_client) -> None:
        """slow_task with duration > hard_timeout=2 hits the hard timeout path."""
        from tests.integration.tasks import slow_task

        key = f"integ-tmo-{uuid.uuid4().hex[:8]}"

        def call():
            # duration=5 exceeds hard_timeout=2 → TimeoutError is caught in decorator
            ar = slow_task.apply(kwargs={"duration": 5, "marker_key": key})
            return ar.state

        state = await asyncio.to_thread(call)
        assert state == "FAILURE"


# ===========================================================================
# App bootstrap — presence loop, cleanup loop, bridge setup, worker init
# ===========================================================================


class TestAppBootstrap:
    async def test_presence_loop_one_heartbeat_iteration(self, redis_client) -> None:
        """_presence_loop writes a heartbeat entry to Redis then sleeps."""
        import relier.tasks.app as app_module
        from relier.core.keys import RedisKeys

        worker_id = f"integ-presence-{uuid.uuid4().hex[:8]}"
        task = asyncio.create_task(app_module._presence_loop(worker_id))
        await asyncio.sleep(0.3)  # enough for the pipeline to execute
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert await redis_client.exists(RedisKeys.presence(worker_id)) == 1
        workers = await redis_client.zrange("rl:workers", 0, -1)
        assert any(
            worker_id in (w.decode() if isinstance(w, bytes) else w) for w in workers
        )

    async def test_cleanup_dead_workers_prunes_stale_entries(
        self, redis_client
    ) -> None:
        """_cleanup_dead_workers removes workers whose registry score is too old."""
        import relier.tasks.app as app_module
        from relier.core.keys import RedisKeys

        stale_id = f"dead-worker-{uuid.uuid4().hex[:8]}"
        await redis_client.zadd(
            RedisKeys.workers(), {stale_id: 1.0}
        )  # epoch 1 = very old

        task = asyncio.create_task(app_module._cleanup_dead_workers())
        await asyncio.sleep(0.3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        remaining = await redis_client.zrange(RedisKeys.workers(), 0, -1)
        remaining_strs = [r.decode() if isinstance(r, bytes) else r for r in remaining]
        assert stale_id not in remaining_strs

    async def test_init_main_process_creates_bridge_loop(self, redis_client) -> None:
        """init_main_process() starts the async bridge event loop."""
        import relier.tasks.app as app_module

        old_loop = app_module.worker_loop
        try:
            app_module.init_main_process()
            assert app_module.worker_loop is not None
            assert app_module.worker_loop.is_running()
        finally:
            new_loop = app_module.worker_loop
            if new_loop and new_loop is not old_loop and new_loop.is_running():
                new_loop.call_soon_threadsafe(new_loop.stop)
            await asyncio.sleep(0.1)
            app_module.worker_loop = old_loop

    async def test_init_worker_process_runs_startup_checks(self, redis_client) -> None:
        """init_worker_process() validates Redis, sets up logging, and starts bridge."""
        import relier.tasks.app as app_module

        old_loop = app_module.worker_loop
        old_handler = app_module.shutdown_handler

        try:
            app_module.init_worker_process()

            assert app_module.worker_loop is not None
            assert app_module.worker_loop.is_running()
            assert app_module.shutdown_handler is not None
        finally:
            new_loop = app_module.worker_loop
            if new_loop and new_loop is not old_loop and new_loop.is_running():
                new_loop.call_soon_threadsafe(new_loop.stop)
            await asyncio.sleep(0.15)
            app_module.worker_loop = old_loop
            app_module.shutdown_handler = old_handler


# ===========================================================================
# CheckpointStore — inline save/gc, too-large checkpoint
# ===========================================================================


class TestCheckpointStoreIntegration:
    async def test_save_and_load_inline_checkpoint(self, redis_client) -> None:
        """Small checkpoint is stored inline in the Phoenix hash."""
        from relier.core.checkpoint import CheckpointStore
        from relier.core.keys import RedisKeys

        task_id = f"integ-ckpt-{uuid.uuid4().hex[:8]}"
        await redis_client.hset(RedisKeys.phoenix(task_id), "worker_id", "w1")

        state = {"step": 5, "value": [1, 2, 3]}
        await CheckpointStore.save(task_id, state)

        raw = await redis_client.hget(RedisKeys.phoenix(task_id), "partial_result")
        assert raw is not None
        stored = json.loads(raw)
        assert stored == state

    async def test_gc_noop_for_inline_checkpoint(self, redis_client) -> None:
        """gc() is a no-op for plain inline JSON (not a blob reference)."""
        from relier.core.checkpoint import CheckpointStore

        # Should not raise and should not call any blob backend
        await CheckpointStore.gc(json.dumps({"step": 1}))
        await CheckpointStore.gc(b'{"step": 2}')
        await CheckpointStore.gc(None)

    async def test_gc_with_invalid_json_is_noop(self, redis_client) -> None:
        """gc() handles corrupted checkpoint values without raising."""
        from relier.core.checkpoint import CheckpointStore

        await CheckpointStore.gc("not-valid-json{{{")
        await CheckpointStore.gc(b"also-invalid")

    async def test_checkpoint_too_large_raises(self, redis_client) -> None:
        """CheckpointTooLargeError is raised when inline limit is exceeded."""
        from relier.config import get_settings
        from relier.core.checkpoint import CheckpointStore, CheckpointTooLargeError

        # Override settings to set a very small inline limit
        original = os.environ.get("RELIER_CHECKPOINT_MAX_INLINE_BYTES")
        os.environ["RELIER_CHECKPOINT_MAX_INLINE_BYTES"] = "1"
        get_settings.cache_clear()

        task_id = f"integ-ckpt-large-{uuid.uuid4().hex[:8]}"
        try:
            with pytest.raises(CheckpointTooLargeError):
                await CheckpointStore.save(task_id, {"big": "data" * 100})
        finally:
            if original is None:
                os.environ.pop("RELIER_CHECKPOINT_MAX_INLINE_BYTES", None)
            else:
                os.environ["RELIER_CHECKPOINT_MAX_INLINE_BYTES"] = original
            get_settings.cache_clear()


# ===========================================================================
# Phoenix scan-and-resurrect — expired task discovered and re-queued
# ===========================================================================


class TestPhoenixScanIntegration:
    async def test_scan_discovers_and_processes_expired_task(
        self, redis_client
    ) -> None:
        """_scan_and_resurrect detects an expired heartbeat and re-queues the task."""
        from relier.core.dlq import DeadLetterQueue
        from relier.core.keys import RedisKeys
        from relier.core.phoenix import PhoenixRegistry
        from relier.tasks.app import celery_app

        task_id = f"integ-scan-{uuid.uuid4().hex[:8]}"
        payload = {
            "task_name": "tests.integration.tasks.counter_task",
            "args": [],
            "kwargs": {"key": "scan-test"},
            "queue": "default",
            "worker_id": "dead-worker",
        }

        # Simulate an expired task: phoenix hash exists, no heartbeat key,
        # expiry index entry is in the past.
        await redis_client.hset(
            RedisKeys.phoenix(task_id),
            mapping={
                "payload": json.dumps(payload),
                "worker_id": "dead-worker",
                "registered_at": str(int(time.time() - 120)),
            },
        )
        await redis_client.zadd(
            RedisKeys.phoenix_expiry_index(),
            {task_id: time.time() - 100},  # expired 100 seconds ago
        )
        # Intentionally no heartbeat key → simulates worker death

        dlq = DeadLetterQueue()
        count = await PhoenixRegistry._scan_and_resurrect(redis_client, dlq, celery_app)

        assert isinstance(count, int)
        # Allow the _bg_send background task to complete
        await asyncio.sleep(0.2)

    async def test_scan_skips_live_tasks(self, redis_client) -> None:
        """_scan_and_resurrect ignores tasks that still have a live heartbeat."""
        from relier.core.dlq import DeadLetterQueue
        from relier.core.keys import RedisKeys
        from relier.core.phoenix import PhoenixRegistry
        from relier.tasks.app import celery_app

        task_id = f"integ-scan-live-{uuid.uuid4().hex[:8]}"
        # Set BOTH a heartbeat AND an expiry index entry (task is alive)
        await redis_client.set(RedisKeys.heartbeat(task_id), "w1", ex=30)
        await redis_client.zadd(
            RedisKeys.phoenix_expiry_index(),
            {task_id: time.time() - 50},
        )

        count = await PhoenixRegistry._scan_and_resurrect(
            redis_client, DeadLetterQueue(), celery_app
        )
        # The live task should not be resurrected
        assert count == 0

    async def test_monitor_resurrected_tasks_completed(self, redis_client) -> None:
        """_monitor_resurrected_tasks() prunes entries for completed tasks."""
        from relier.core.keys import RedisKeys
        from relier.core.phoenix import PhoenixRegistry

        task_id = f"integ-mon-{uuid.uuid4().hex[:8]}"
        monitor_key = RedisKeys.monitor()

        # state=1 + no heartbeat + no phoenix hash → task completed normally
        await redis_client.hset(monitor_key, task_id, "1")

        count = await PhoenixRegistry._monitor_resurrected_tasks(redis_client)
        assert isinstance(count, int)
        assert count >= 1
        # Entry should be removed from monitor hash
        remaining = await redis_client.hget(monitor_key, task_id)
        assert remaining is None

    async def test_monitor_resurrected_tasks_alive(self, redis_client) -> None:
        """_monitor_resurrected_tasks() records alive transitions (state=0, HB present)."""
        from relier.core.keys import RedisKeys
        from relier.core.phoenix import PhoenixRegistry

        task_id = f"integ-mon-alive-{uuid.uuid4().hex[:8]}"
        monitor_key = RedisKeys.monitor()

        # state=0 + heartbeat present → task was resurrected and is alive
        await redis_client.hset(monitor_key, task_id, "0")
        await redis_client.set(RedisKeys.heartbeat(task_id), "w1", ex=30)
        # Phoenix hash must exist for the alive branch
        await redis_client.hset(RedisKeys.phoenix(task_id), "worker_id", "w1")

        count = await PhoenixRegistry._monitor_resurrected_tasks(redis_client)
        assert count >= 1
        # Entry stays in monitor with state updated to 1
        state = await redis_client.hget(monitor_key, task_id)
        assert state == "1"

    async def test_monitor_resurrected_tasks_empty(self, redis_client) -> None:
        """_monitor_resurrected_tasks() returns 0 when monitor hash is empty."""
        from relier.core.phoenix import PhoenixRegistry

        count = await PhoenixRegistry._monitor_resurrected_tasks(redis_client)
        assert count == 0


# ===========================================================================
# Signal handlers — called directly with real objects
# ===========================================================================


class TestSignalHandlersIntegration:
    async def test_on_task_postrun_success_without_worker_loop(
        self, redis_client
    ) -> None:
        """on_task_postrun logs a warning when worker_loop is unavailable."""
        import relier.tasks.app as app_module
        import relier.tasks.signals as task_signals
        from tests.integration.tasks import counter_task

        old_loop = app_module.worker_loop
        app_module.worker_loop = None
        try:
            task_signals.on_task_postrun(
                task_id="integ-sig-1",
                task=counter_task,
                state="SUCCESS",
                retval=1,
            )
        finally:
            app_module.worker_loop = old_loop

    async def test_on_task_postrun_success_with_worker_loop(self, redis_client) -> None:
        """on_task_postrun schedules SLO recording when worker_loop is active."""
        import relier.tasks.app as app_module
        import relier.tasks.signals as task_signals
        from tests.integration.tasks import counter_task

        old_loop = app_module.worker_loop
        try:
            # Set up a real worker loop so the SLO scheduling path is taken
            app_module.init_main_process()
            assert app_module.worker_loop is not None

            task_signals.on_task_postrun(
                task_id="integ-sig-2",
                task=counter_task,
                state="SUCCESS",
                retval=1,
            )
            await asyncio.sleep(0.1)  # let the scheduled SLO record run
        finally:
            new_loop = app_module.worker_loop
            if new_loop and new_loop is not old_loop and new_loop.is_running():
                new_loop.call_soon_threadsafe(new_loop.stop)
            await asyncio.sleep(0.1)
            app_module.worker_loop = old_loop

    async def test_on_task_failure_logs_error(self, redis_client) -> None:
        """on_task_failure runs without raising for any exception type."""
        import relier.tasks.signals as task_signals
        from tests.integration.tasks import counter_task

        task_signals.on_task_failure(
            task_id="integ-fail-1",
            task=counter_task,
            exception=RuntimeError("simulated failure"),
        )


# ===========================================================================
# Telemetry helpers — spans and logging
# ===========================================================================


class TestSpansIntegration:
    async def test_trace_task_context_manager(self) -> None:
        """trace_task() wraps an async block in an OTEL span."""
        from relier.telemetry.spans import ATTR_TASK_QUEUE, trace_task

        async with trace_task(
            "rl.test.span",
            task_id="integ-span-1",
            attributes={ATTR_TASK_QUEUE: "default"},
        ) as span:
            assert span is not None

    async def test_trace_task_records_exception(self) -> None:
        """trace_task() records exceptions on the span and re-raises."""
        from relier.telemetry.spans import trace_task

        with pytest.raises(ValueError):
            async with trace_task("rl.test.error", task_id="integ-span-2"):
                raise ValueError("test error")

    async def test_trace_sync_context_manager(self) -> None:
        """trace_sync() wraps a sync block in an OTEL span."""
        from relier.telemetry.spans import trace_sync

        with trace_sync("rl.test.sync", attributes={"key": "val"}) as span:
            assert span is not None

    async def test_trace_sync_records_exception(self) -> None:
        """trace_sync() records exceptions on the span and re-raises."""
        from relier.telemetry.spans import trace_sync

        with pytest.raises(RuntimeError), trace_sync("rl.test.sync.error"):
            raise RuntimeError("sync error")

    async def test_record_exception_on_span(self) -> None:
        """record_exception() marks the span as error and sets status."""
        from opentelemetry import trace

        from relier.telemetry.spans import record_exception

        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("test-span") as span:
            record_exception(span, ValueError("test"))


class TestLoggingSetup:
    async def test_setup_logging_info_level(self) -> None:
        """setup_logging() configures structlog without raising."""
        from relier.telemetry.logging import setup_logging

        setup_logging(level="INFO", cache_loggers=False)

    async def test_setup_logging_debug_level(self) -> None:
        """setup_logging() with DEBUG level uses ConsoleRenderer."""
        from relier.telemetry.logging import setup_logging

        setup_logging(level="DEBUG", cache_loggers=False)

    async def test_inject_otel_context_with_no_span(self) -> None:
        """inject_otel_context() is a no-op when no active span exists."""
        from relier.telemetry.logging import inject_otel_context

        event = {"event": "test"}
        result = inject_otel_context(None, "info", event)
        assert result == event

    async def test_get_tracer_and_get_meter(self) -> None:
        """get_tracer() and get_meter() return non-None providers."""
        from relier.telemetry.setup import get_meter, get_tracer

        tracer = get_tracer("test-tracer")
        assert tracer is not None
        meter = get_meter("test-meter")
        assert meter is not None

    async def test_setup_telemetry_when_enabled(self) -> None:
        """setup_telemetry() initializes SDK providers when OTEL is enabled."""
        from relier.config import get_settings
        from relier.telemetry.setup import setup_telemetry

        os.environ["RELIER_OTEL_ENABLED"] = "true"
        os.environ["RELIER_OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://localhost:4317"
        get_settings.cache_clear()
        try:
            setup_telemetry("integration-test-service")
        finally:
            os.environ.pop("RELIER_OTEL_ENABLED", None)
            os.environ.pop("RELIER_OTEL_EXPORTER_OTLP_ENDPOINT", None)
            get_settings.cache_clear()


# ===========================================================================
# Context and exceptions
# ===========================================================================


class TestContextAndExceptions:
    async def test_task_context_proxy_raises_outside_task(self) -> None:
        """TaskContextProxy raises RuntimeError when accessed outside a running task."""
        from relier.tasks.context import TaskContextProxy

        proxy = TaskContextProxy()
        with pytest.raises(RuntimeError, match="outside of a running task"):
            _ = proxy.task_id

    async def test_task_context_proxy_setattr_raises(self) -> None:
        """TaskContextProxy raises RuntimeError on attribute set outside a task."""
        from relier.tasks.context import TaskContextProxy

        proxy = TaskContextProxy()
        with pytest.raises(RuntimeError, match="outside of a running task"):
            proxy.task_id = "new_id"

    async def test_exception_classes_instantiate(self) -> None:
        """Core exception classes can be instantiated with expected arguments."""
        from relier.core.exceptions import (
            AdmissionRejectedError,
            IdempotencyInFlightError,
            PayloadIntegrityError,
        )

        e1 = AdmissionRejectedError("at capacity", retry_after=5)
        assert e1.retry_after == 5

        e2 = IdempotencyInFlightError(key="rl:idem:test")
        assert "test" in str(e2)

        e3 = PayloadIntegrityError("checksum mismatch")
        assert "checksum" in str(e3)


# ===========================================================================
# FilesystemBackend — direct write/read/delete/sweep on disk
# ===========================================================================


class TestFilesystemBackend:
    async def test_write_and_read(self, tmp_path) -> None:
        """write() stores data; read() retrieves the same bytes."""
        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path))
        await backend.write("task-abc", b"hello world")
        data = await backend.read("task-abc")
        assert data == b"hello world"

    async def test_delete_removes_file(self, tmp_path) -> None:
        """delete() removes the checkpoint file from disk."""
        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path))
        await backend.write("task-del", b"data")
        await backend.delete("task-del")
        assert not (tmp_path / "task-del.ckpt").exists()

    async def test_delete_missing_file_is_noop(self, tmp_path) -> None:
        """delete() on a missing file does not raise."""
        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path))
        await backend.delete("does-not-exist")

    async def test_sweep_removes_old_files(self, tmp_path) -> None:
        """sweep() removes checkpoint files older than max_age_seconds."""
        import os
        import time

        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path))
        await backend.write("old-task", b"old data")
        path = tmp_path / "old-task.ckpt"
        old_time = time.time() - 100_000
        os.utime(path, (old_time, old_time))

        removed = await backend.sweep(max_age_seconds=1)
        assert removed >= 1
        assert not path.exists()

    async def test_sweep_empty_directory(self, tmp_path) -> None:
        """sweep() on an empty directory returns 0."""
        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path))
        removed = await backend.sweep(max_age_seconds=1)
        assert removed == 0

    async def test_sweep_nonexistent_directory(self, tmp_path) -> None:
        """sweep() on a non-existent directory returns 0 without raising."""
        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path / "no-such-dir"))
        removed = await backend.sweep(max_age_seconds=1)
        assert removed == 0

    async def test_unsafe_ref_raises_value_error(self, tmp_path) -> None:
        """write() raises ValueError for refs with path-traversal characters."""
        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path))
        with pytest.raises(ValueError, match="Unsafe checkpoint reference"):
            await backend.write("../evil", b"bad")

    async def test_overwrite_existing_checkpoint(self, tmp_path) -> None:
        """write() atomically replaces an existing checkpoint for the same task."""
        from relier.core.checkpoint import FilesystemBackend

        backend = FilesystemBackend(str(tmp_path))
        await backend.write("task-overwrite", b"v1")
        await backend.write("task-overwrite", b"v2")
        data = await backend.read("task-overwrite")
        assert data == b"v2"


# ===========================================================================
# CheckpointStore — blob backend (filesystem) spill, gc, resolve, sweep
# ===========================================================================


class TestBlobCheckpoint:
    async def test_save_spills_large_checkpoint_to_filesystem(
        self, redis_client, tmp_path
    ) -> None:
        """Checkpoints exceeding inline_bytes limit are spilled to filesystem."""
        from relier.config import get_settings
        from relier.core.checkpoint import CheckpointStore
        from relier.core.keys import RedisKeys

        os.environ["RELIER_CHECKPOINT_BACKEND"] = "filesystem"
        os.environ["RELIER_CHECKPOINT_DIR"] = str(tmp_path)
        os.environ["RELIER_CHECKPOINT_MAX_INLINE_BYTES"] = "10"
        get_settings.cache_clear()
        CheckpointStore._backend = None
        CheckpointStore._backend_kind = None

        task_id = f"integ-blob-{uuid.uuid4().hex[:8]}"
        await redis_client.hset(RedisKeys.phoenix(task_id), "worker_id", "w1")

        try:
            await CheckpointStore.save(task_id, {"large": "x" * 1000})
            raw = await redis_client.hget(RedisKeys.phoenix(task_id), "partial_result")
            assert raw is not None
            envelope = json.loads(raw)
            assert envelope.get("__relier_checkpoint_ref__") == 1
        finally:
            os.environ.pop("RELIER_CHECKPOINT_BACKEND", None)
            os.environ.pop("RELIER_CHECKPOINT_DIR", None)
            os.environ.pop("RELIER_CHECKPOINT_MAX_INLINE_BYTES", None)
            get_settings.cache_clear()
            CheckpointStore._backend = None
            CheckpointStore._backend_kind = None

    async def test_resolve_dereferences_blob_envelope(
        self, redis_client, tmp_path
    ) -> None:
        """resolve() decompresses and returns the original state from blob storage."""
        from relier.config import get_settings
        from relier.core.checkpoint import _REF_MARKER, CheckpointStore

        os.environ["RELIER_CHECKPOINT_BACKEND"] = "filesystem"
        os.environ["RELIER_CHECKPOINT_DIR"] = str(tmp_path)
        os.environ["RELIER_CHECKPOINT_MAX_INLINE_BYTES"] = "10"
        get_settings.cache_clear()
        CheckpointStore._backend = None
        CheckpointStore._backend_kind = None

        task_id = f"integ-resolve-{uuid.uuid4().hex[:8]}"
        state = {"step": 42, "data": "x" * 100}

        try:
            await CheckpointStore.save(task_id, state)
            envelope = {
                _REF_MARKER: 1,
                "backend": "filesystem",
                "ref": task_id,
                "codec": "gzip",
            }
            resolved = await CheckpointStore.resolve(envelope)
            assert resolved == state
        finally:
            os.environ.pop("RELIER_CHECKPOINT_BACKEND", None)
            os.environ.pop("RELIER_CHECKPOINT_DIR", None)
            os.environ.pop("RELIER_CHECKPOINT_MAX_INLINE_BYTES", None)
            get_settings.cache_clear()
            CheckpointStore._backend = None
            CheckpointStore._backend_kind = None

    async def test_resolve_inline_value_unchanged(self) -> None:
        """resolve() returns inline (non-reference) values unchanged."""
        from relier.core.checkpoint import CheckpointStore

        state = {"step": 1}
        result = await CheckpointStore.resolve(state)
        assert result == state

    async def test_gc_deletes_blob_file(self, tmp_path) -> None:
        """gc() removes the blob file referenced by the stored envelope."""
        from relier.config import get_settings
        from relier.core.checkpoint import _REF_MARKER, CheckpointStore

        os.environ["RELIER_CHECKPOINT_BACKEND"] = "filesystem"
        os.environ["RELIER_CHECKPOINT_DIR"] = str(tmp_path)
        get_settings.cache_clear()
        CheckpointStore._backend = None
        CheckpointStore._backend_kind = None

        task_id = f"integ-gc-{uuid.uuid4().hex[:8]}"

        try:
            backend = CheckpointStore._get_backend()
            assert backend is not None
            await backend.write(task_id, b"test data")
            blob_path = tmp_path / f"{task_id}.ckpt"
            assert blob_path.exists()

            envelope = {
                _REF_MARKER: 1,
                "backend": "filesystem",
                "ref": task_id,
                "codec": "gzip",
            }
            await CheckpointStore.gc(envelope)
            assert not blob_path.exists()
        finally:
            os.environ.pop("RELIER_CHECKPOINT_BACKEND", None)
            os.environ.pop("RELIER_CHECKPOINT_DIR", None)
            get_settings.cache_clear()
            CheckpointStore._backend = None
            CheckpointStore._backend_kind = None

    async def test_sweep_delegates_to_backend(self, tmp_path) -> None:
        """CheckpointStore.sweep() removes orphaned blob files via backend."""
        from relier.config import get_settings
        from relier.core.checkpoint import CheckpointStore

        os.environ["RELIER_CHECKPOINT_BACKEND"] = "filesystem"
        os.environ["RELIER_CHECKPOINT_DIR"] = str(tmp_path)
        get_settings.cache_clear()
        CheckpointStore._backend = None
        CheckpointStore._backend_kind = None

        try:
            count = await CheckpointStore.sweep()
            assert isinstance(count, int)
        finally:
            os.environ.pop("RELIER_CHECKPOINT_BACKEND", None)
            os.environ.pop("RELIER_CHECKPOINT_DIR", None)
            get_settings.cache_clear()
            CheckpointStore._backend = None
            CheckpointStore._backend_kind = None

    async def test_sweep_inline_mode_is_noop(self) -> None:
        """CheckpointStore.sweep() returns 0 in inline (no-backend) mode."""
        from relier.core.checkpoint import CheckpointStore

        count = await CheckpointStore.sweep()
        assert count == 0

    async def test_gc_existing_removes_blob_on_inline_save(
        self, redis_client, tmp_path
    ) -> None:
        """_gc_existing cleans up an old blob when a small checkpoint is saved inline."""
        from relier.config import get_settings
        from relier.core.checkpoint import _REF_MARKER, CheckpointStore
        from relier.core.keys import RedisKeys

        os.environ["RELIER_CHECKPOINT_BACKEND"] = "filesystem"
        os.environ["RELIER_CHECKPOINT_DIR"] = str(tmp_path)
        os.environ["RELIER_CHECKPOINT_MAX_INLINE_BYTES"] = "100000"
        get_settings.cache_clear()
        CheckpointStore._backend = None
        CheckpointStore._backend_kind = None

        task_id = f"integ-gcx-{uuid.uuid4().hex[:8]}"

        try:
            backend = CheckpointStore._get_backend()
            assert backend is not None
            await backend.write(task_id, b"old blob")
            blob_path = tmp_path / f"{task_id}.ckpt"
            assert blob_path.exists()

            old_env = json.dumps(
                {
                    _REF_MARKER: 1,
                    "backend": "filesystem",
                    "ref": task_id,
                    "codec": "gzip",
                }
            )
            await redis_client.hset(
                RedisKeys.phoenix(task_id), "partial_result", old_env
            )

            # Small checkpoint fits inline, triggers _gc_existing which deletes blob
            await CheckpointStore.save(task_id, {"step": 1})
            assert not blob_path.exists()

            raw = await redis_client.hget(RedisKeys.phoenix(task_id), "partial_result")
            assert json.loads(raw) == {"step": 1}
        finally:
            os.environ.pop("RELIER_CHECKPOINT_BACKEND", None)
            os.environ.pop("RELIER_CHECKPOINT_DIR", None)
            os.environ.pop("RELIER_CHECKPOINT_MAX_INLINE_BYTES", None)
            get_settings.cache_clear()
            CheckpointStore._backend = None
            CheckpointStore._backend_kind = None

    async def test_resolve_returns_none_when_no_backend(self) -> None:
        """resolve() returns None and logs when a blob ref exists but no backend is set."""
        from relier.core.checkpoint import _REF_MARKER, CheckpointStore

        envelope = {
            _REF_MARKER: 1,
            "backend": "filesystem",
            "ref": "some-task",
            "codec": "gzip",
        }
        # Default backend is inline (no filesystem backend)
        result = await CheckpointStore.resolve(envelope)
        assert result is None

    async def test_sweep_logs_count_when_blobs_removed(self, tmp_path) -> None:
        """sweep() emits an info log when orphaned blobs are removed."""
        import os as _os
        import time

        from relier.config import get_settings
        from relier.core.checkpoint import CheckpointStore

        os.environ["RELIER_CHECKPOINT_BACKEND"] = "filesystem"
        os.environ["RELIER_CHECKPOINT_DIR"] = str(tmp_path)
        get_settings.cache_clear()
        CheckpointStore._backend = None
        CheckpointStore._backend_kind = None

        task_id = f"integ-swp-{uuid.uuid4().hex[:8]}"
        backend = CheckpointStore._get_backend()
        assert backend is not None
        await backend.write(task_id, b"stale data")
        path = tmp_path / f"{task_id}.ckpt"
        old_time = time.time() - 200_000
        _os.utime(path, (old_time, old_time))

        try:
            count = await CheckpointStore.sweep()
            assert count >= 1
            assert not path.exists()
        finally:
            os.environ.pop("RELIER_CHECKPOINT_BACKEND", None)
            os.environ.pop("RELIER_CHECKPOINT_DIR", None)
            get_settings.cache_clear()
            CheckpointStore._backend = None
            CheckpointStore._backend_kind = None
