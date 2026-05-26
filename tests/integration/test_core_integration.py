"""
Integration tests for core Relier modules against a real Redis instance.

These tests exercise the full stack, real Redis, real Lua scripts, real module
logic without spawning Celery workers, keeping the suite fast (<2s per test).
No internal mocking: if a function needs Redis, it gets the real one.
"""

import asyncio
import json
import os
from typing import Any

import pytest

pytestmark = pytest.mark.asyncio


# ===========================================================================
# Phoenix Registry — tests are async, fixture provides live Redis
# ===========================================================================


class TestPhoenixRegistryIntegration:
    async def test_register_writes_heartbeat_and_hash(self, redis_client) -> None:
        """register() creates both the heartbeat key and the phoenix hash in Redis."""
        from relier.core.keys import RedisKeys
        from relier.core.phoenix import PhoenixRegistry

        task_id = "integ-reg-1"
        payload = {
            "task_name": "my_task",
            "args": [],
            "kwargs": {},
            "queue": "default",
        }

        await PhoenixRegistry.register(task_id, "worker-1", payload)

        try:
            assert await redis_client.exists(RedisKeys.heartbeat(task_id)) == 1
            assert await redis_client.exists(RedisKeys.phoenix(task_id)) == 1
        finally:
            # complete() cancels the background refresh loop cleanly
            await PhoenixRegistry.complete(task_id)

    async def test_is_active_reflects_heartbeat(self, redis_client) -> None:
        """is_active() returns True with a live heartbeat, False after completion."""
        from relier.core.phoenix import PhoenixRegistry

        task_id = "integ-active-1"
        payload = {"task_name": "t", "args": [], "kwargs": {}, "queue": "default"}

        await PhoenixRegistry.register(task_id, "w1", payload)
        assert await PhoenixRegistry._is_active(task_id) is True

        await PhoenixRegistry.complete(task_id)
        assert await PhoenixRegistry._is_active(task_id) is False

    async def test_complete_removes_all_phoenix_keys(self, redis_client) -> None:
        """complete() deletes heartbeat, phoenix hash, and expiry-index entry."""
        from relier.core.keys import RedisKeys
        from relier.core.phoenix import PhoenixRegistry

        task_id = "integ-complete-1"
        payload = {"task_name": "t", "args": [], "kwargs": {}, "queue": "default"}

        await PhoenixRegistry.register(task_id, "w1", payload)
        await PhoenixRegistry.complete(task_id)

        assert await redis_client.exists(RedisKeys.heartbeat(task_id)) == 0
        assert await redis_client.exists(RedisKeys.phoenix(task_id)) == 0

    async def test_update_partial_state_stores_checkpoint(self, redis_client) -> None:
        """update_partial_state() persists checkpoint data on the phoenix hash."""
        from relier.core.keys import RedisKeys
        from relier.core.phoenix import PhoenixRegistry

        task_id = "integ-partial-1"
        payload = {"task_name": "t", "args": [], "kwargs": {}, "queue": "default"}

        await PhoenixRegistry.register(task_id, "w1", payload)
        try:
            await PhoenixRegistry.update_partial_state(task_id, {"step": 7})

            raw = await redis_client.hget(RedisKeys.phoenix(task_id), "partial_result")
            assert raw is not None
            data = json.loads(raw)
            # Small checkpoints are stored inline; large ones emit a blob-ref envelope
            assert data.get("step") == 7 or "ref" in data
        finally:
            await PhoenixRegistry.complete(task_id)

    async def test_is_active_false_without_registration(self, redis_client) -> None:
        """is_active() returns False for a task that was never registered."""
        from relier.core.phoenix import PhoenixRegistry

        assert await PhoenixRegistry._is_active("never-registered-task") is False


# ===========================================================================
# Dead Letter Queue
# ===========================================================================


class TestDLQIntegration:
    async def test_quarantine_creates_entry_with_correct_fields(
        self, redis_client
    ) -> None:
        """quarantine() writes a complete DLQ entry to the Redis hash."""
        from relier.core.dlq import DeadLetterQueue

        task_id = "integ-dlq-1"
        payload = {
            "task_name": "bad_task",
            "args": [1, 2],
            "kwargs": {"x": "y"},
            "queue": "default",
        }

        await DeadLetterQueue.quarantine(
            task_id, reason="exceeded retries", payload=payload
        )

        raw = await redis_client.hget(DeadLetterQueue.DLQ_HASH_KEY, task_id)
        assert raw is not None
        entry = json.loads(raw)
        assert entry["task_name"] == "bad_task"
        assert entry["reason"] == "exceeded retries"
        assert entry["args"] == [1, 2]
        assert entry["kwargs"] == {"x": "y"}

    async def test_list_tasks_returns_quarantined_entries(self, redis_client) -> None:
        """list_tasks() returns all entries that were quarantined."""
        from relier.core.dlq import DeadLetterQueue

        payload = {
            "task_name": "list_task",
            "args": [],
            "kwargs": {},
            "queue": "default",
        }
        await DeadLetterQueue.quarantine("integ-list-1", reason="r1", payload=payload)
        await DeadLetterQueue.quarantine("integ-list-2", reason="r2", payload=payload)

        entries = await DeadLetterQueue.list_tasks()
        ids = {e["task_id"] for e in entries}
        assert "integ-list-1" in ids
        assert "integ-list-2" in ids

    async def test_inspect_retrieves_single_entry(self, redis_client) -> None:
        """inspect() returns the DLQ entry for a specific task_id."""
        from relier.core.dlq import DeadLetterQueue

        task_id = "integ-inspect-1"
        payload = {
            "task_name": "inspect_task",
            "args": [],
            "kwargs": {},
            "queue": "high",
        }
        await DeadLetterQueue.quarantine(task_id, reason="test", payload=payload)

        entry = await DeadLetterQueue.inspect(task_id)
        assert entry is not None
        assert entry["task_name"] == "inspect_task"
        assert entry["queue"] == "high"

    async def test_inspect_returns_none_for_missing_task(self, redis_client) -> None:
        """inspect() returns None when the task_id is not in the DLQ."""
        from relier.core.dlq import DeadLetterQueue

        result = await DeadLetterQueue.inspect("does-not-exist-integ")
        assert result is None

    async def test_quarantine_clears_active_phoenix_keys(self, redis_client) -> None:
        """quarantine() removes the heartbeat and phoenix hash for the task."""
        from relier.core.dlq import DeadLetterQueue
        from relier.core.keys import RedisKeys

        task_id = "integ-dlq-cleanup-1"
        # Manually plant active-task state
        await redis_client.set(RedisKeys.heartbeat(task_id), "w1", ex=30)
        await redis_client.hset(RedisKeys.phoenix(task_id), "worker_id", "w1")

        payload = {"task_name": "t", "args": [], "kwargs": {}, "queue": "default"}
        await DeadLetterQueue.quarantine(task_id, reason="test", payload=payload)

        assert await redis_client.exists(RedisKeys.heartbeat(task_id)) == 0
        assert await redis_client.exists(RedisKeys.phoenix(task_id)) == 0

    async def test_release_re_enqueues_and_removes_from_dlq(self, redis_client) -> None:
        """release() sends the task to the Celery broker and removes the DLQ entry."""
        from relier.core.dlq import DeadLetterQueue

        task_id = "integ-release-1"
        payload = {
            "task_name": "tests.integration.tasks.counter_task",
            "args": [],
            "kwargs": {"key": "integ-release-key"},
            "queue": "default",
        }
        await DeadLetterQueue.quarantine(
            task_id, reason="test-release", payload=payload
        )

        result = await DeadLetterQueue.release(task_id)

        assert result is True
        # Entry must be gone from the DLQ hash
        assert await DeadLetterQueue.inspect(task_id) is None

    async def test_purge_returns_integer_count(self, redis_client) -> None:
        """purge() returns a non-negative integer regardless of DLQ contents."""
        from relier.core.dlq import DeadLetterQueue

        payload = {"task_name": "purge_t", "args": [], "kwargs": {}, "queue": "default"}
        await DeadLetterQueue.quarantine("integ-purge-1", reason="old", payload=payload)

        count = await DeadLetterQueue.purge()
        assert isinstance(count, int)
        assert count >= 0


# ===========================================================================
# Idempotency
# ===========================================================================


class TestIdempotencyIntegration:
    async def test_first_claim_grants_ownership(self, redis_client) -> None:
        """First check_or_claim on a fresh key returns already_executed=False."""
        from relier.core.idempotency import idempotency_manager

        result = await idempotency_manager.check_or_claim("integ-idem-1", ttl=60)
        assert result.already_executed is False

    async def test_cached_result_returned_after_record(self, redis_client) -> None:
        """After record_result(), subsequent claims return the cached value."""
        from relier.core.idempotency import idempotency_manager

        key = "integ-idem-2"
        result = await idempotency_manager.check_or_claim(key, ttl=60)
        assert not result.already_executed
        await result.record_result({"answer": 42})

        result2 = await idempotency_manager.check_or_claim(key, ttl=60)
        assert result2.already_executed is True
        assert result2.cached_result == {"answer": 42}

    async def test_idempotency_lock_commits_on_clean_exit(self, redis_client) -> None:
        """idempotency_lock context manager persists the result after the block."""
        from relier.core.idempotency import idempotency_lock

        key = "integ-lock-1"
        async with idempotency_lock(key, ttl=60) as res:
            assert not res.already_executed
            await res.record_result("committed")

        async with idempotency_lock(key, ttl=60) as res2:
            assert res2.already_executed
            assert res2.cached_result == "committed"

    async def test_idempotency_lock_releases_on_exception(self, redis_client) -> None:
        """idempotency_lock releases ownership when the body raises, allowing retry."""
        from relier.core.idempotency import idempotency_lock

        key = "integ-lock-err-1"
        with pytest.raises(ValueError):
            async with idempotency_lock(key, ttl=60):
                raise ValueError("intentional")

        # After the failed attempt the lock should be released, so the next
        # caller can claim ownership rather than seeing a stale in-flight sentinel.
        async with idempotency_lock(key, ttl=60) as retry:
            assert not retry.already_executed

    async def test_concurrent_claims_one_wins(self, redis_client) -> None:
        """Only one concurrent check_or_claim succeeds; the other gets cached result."""
        from relier.core.exceptions import IdempotencyInFlightError
        from relier.core.idempotency import idempotency_manager

        key = "integ-concurrent-1"
        first = await idempotency_manager.check_or_claim(key, ttl=60)
        assert not first.already_executed

        # Second claim while the first is in-flight raises IdempotencyInFlightError
        with pytest.raises(IdempotencyInFlightError):
            await idempotency_manager.check_or_claim(key, ttl=60)

        # After recording, a third claim gets the cached result
        await first.record_result("result")
        third = await idempotency_manager.check_or_claim(key, ttl=60)
        assert third.already_executed


# ===========================================================================
# Schema Registry — pure Python, no Redis
# ===========================================================================


class TestSchemaIntegration:
    @pytest.fixture(autouse=True)
    def _reset_registry(self):
        from relier.core.schema import SchemaRegistry

        original_version = SchemaRegistry.CURRENT_VERSION
        original_migrations = dict(SchemaRegistry._migrations)
        SchemaRegistry.CURRENT_VERSION = 1
        SchemaRegistry._migrations = {}
        yield
        SchemaRegistry.CURRENT_VERSION = original_version
        SchemaRegistry._migrations = original_migrations

    async def test_wrap_unwrap_roundtrip(self) -> None:
        """wrap() then unwrap_and_migrate() recovers the original args and kwargs."""
        from relier.core.schema import SchemaRegistry

        args = (1, "hello", True)
        kwargs = {"flag": True, "count": 5}
        envelope = SchemaRegistry.wrap("task-abc-123", args, kwargs)

        recovered_args, recovered_kwargs = SchemaRegistry.unwrap_and_migrate(
            "my_task", envelope
        )
        assert recovered_args == args
        assert recovered_kwargs == kwargs

    async def test_checksum_mismatch_raises_integrity_error(self) -> None:
        """Tampered checksum causes unwrap_and_migrate to raise PayloadIntegrityError."""
        from relier.core.exceptions import PayloadIntegrityError
        from relier.core.schema import SchemaRegistry

        envelope = SchemaRegistry.wrap("task-xyz-456", (1,), {})
        envelope["checksum"] = "sha256:deadbeef"

        with pytest.raises(PayloadIntegrityError, match="checksum"):
            SchemaRegistry.unwrap_and_migrate("my_task", envelope)

    async def test_malformed_envelope_raises_integrity_error(self) -> None:
        """An envelope missing required fields is rejected immediately."""
        from relier.core.exceptions import PayloadIntegrityError
        from relier.core.schema import SchemaRegistry

        with pytest.raises(PayloadIntegrityError):
            SchemaRegistry.unwrap_and_migrate("my_task", {"not": "valid"})

    async def test_wrap_envelope_contains_all_required_fields(self) -> None:
        """Every envelope produced by wrap() has the fields TaskEnvelope requires."""
        from relier.core.schema import SchemaRegistry

        envelope = SchemaRegistry.wrap("task-fields-1", (), {})
        for field in (
            "task_id",
            "schema_version",
            "payload",
            "checksum",
            "enqueued_at",
        ):
            assert field in envelope, f"Missing field: {field}"

    async def test_registered_migration_is_applied(self) -> None:
        """A migration registered for a task name transforms the payload."""
        from relier.core.schema import SchemaRegistry

        task_name = "schema_integ_migration_task"

        @SchemaRegistry.register_migration(task_name, from_version=1)
        def add_new_field(args, kwargs):
            kwargs["migrated"] = True
            return args, kwargs

        # Build a v1 envelope manually with correct checksum
        payload: dict[str, Any] = {"args": [], "kwargs": {}}
        envelope = {
            "task_id": "mig-test-1",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": "2024-01-01T00:00:00+00:00",
            "checksum": SchemaRegistry._generate_checksum(payload),
        }

        # Temporarily bump CURRENT_VERSION so migration is triggered
        original = SchemaRegistry.CURRENT_VERSION
        SchemaRegistry.CURRENT_VERSION = 2
        try:
            _, kwargs = SchemaRegistry.unwrap_and_migrate(task_name, envelope)
        finally:
            SchemaRegistry.CURRENT_VERSION = original

        assert kwargs.get("migrated") is True


# ===========================================================================
# Admission Control
# ===========================================================================


class TestAdmissionIntegration:
    async def test_first_request_is_admitted(self, redis_client) -> None:
        """The first request to check_capacity is always admitted."""
        from relier.core.admission import AdmissionController

        controller = AdmissionController()
        is_admitted, retry_after = await controller.check_capacity("integ-global")
        assert is_admitted is True
        assert retry_after == 0

    async def test_requests_rejected_after_limit_exceeded(self, redis_client) -> None:
        """Requests beyond the configured limit are rejected with a retry_after delay."""
        from relier.config import get_settings
        from relier.core.admission import AdmissionController

        # Configure a tiny limit via env so the real Lua script gets tested
        original_limit = os.environ.get("RELIER_ADMISSION_LIMIT")
        original_window = os.environ.get("RELIER_ADMISSION_WINDOW")
        os.environ["RELIER_ADMISSION_LIMIT"] = "2"
        os.environ["RELIER_ADMISSION_WINDOW"] = "60"
        get_settings.cache_clear()

        controller = AdmissionController()
        resource = "integ-over-limit"

        try:
            # Exhaust the 2-request limit
            r1 = await controller.check_capacity(resource)
            r2 = await controller.check_capacity(resource)
            r3 = await controller.check_capacity(resource)

            assert r1[0] is True
            assert r2[0] is True
            assert r3[0] is False
            assert r3[1] > 0
        finally:
            # Restore env state
            if original_limit is None:
                os.environ.pop("RELIER_ADMISSION_LIMIT", None)
            else:
                os.environ["RELIER_ADMISSION_LIMIT"] = original_limit
            if original_window is None:
                os.environ.pop("RELIER_ADMISSION_WINDOW", None)
            else:
                os.environ["RELIER_ADMISSION_WINDOW"] = original_window
            get_settings.cache_clear()

    async def test_noscript_recovery_reloads_lua(self, redis_client) -> None:
        """_evalsha_with_fallback reloads and retries when the Lua SHA is stale."""
        from relier.core.admission import AdmissionController

        controller = AdmissionController()
        # Force a load so we have a SHA cached
        await controller.check_capacity("integ-noscript")
        # Flush script cache to simulate a Redis restart
        await redis_client.execute_command("SCRIPT", "FLUSH")
        controller._script_sha = "0000000000000000000000000000000000000000"

        # Should transparently recover and succeed
        is_admitted, _ = await controller.check_capacity("integ-noscript-retry")
        assert is_admitted is True


# ===========================================================================
# SLO Metrics
# ===========================================================================


class TestSLOIntegration:
    async def test_record_events_visible_in_burn_rate(self, redis_client) -> None:
        """Events recorded with record_event() appear in get_burn_rate()."""
        from relier.core.slo import SLOMetrics

        await SLOMetrics.record_event("success")
        await SLOMetrics.record_event("success")
        await SLOMetrics.record_event("failure")

        burn = await SLOMetrics.get_burn_rate(window="1h", target_slo=0.9)
        assert burn > 0.0

    async def test_burn_rate_zero_with_no_traffic(self, redis_client) -> None:
        """get_burn_rate() returns 0.0 when no events have been recorded."""
        from relier.core.slo import SLOMetrics

        burn = await SLOMetrics.get_burn_rate(window="1h")
        assert burn == 0.0

    async def test_all_successes_give_zero_burn(self, redis_client) -> None:
        """All-success traffic produces a burn rate of 0.0."""
        from relier.core.slo import SLOMetrics

        for _ in range(10):
            await SLOMetrics.record_event("success")

        burn = await SLOMetrics.get_burn_rate(window="1h", target_slo=0.999)
        assert burn == 0.0

    async def test_get_report_covers_all_windows(self, redis_client) -> None:
        """get_report() returns a burn-rate entry for every configured window."""
        from relier.core.slo import SLOMetrics

        await SLOMetrics.record_event("success")
        report = await SLOMetrics.get_report()
        assert set(report.keys()) == {"1h", "6h", "3d"}
        for val in report.values():
            assert isinstance(val, float)


# ===========================================================================
# Validation
# ===========================================================================


class TestValidationIntegration:
    async def test_validate_redis_reachable_passes_against_live_redis(
        self, redis_client
    ) -> None:
        """validate_redis_reachable() succeeds when Redis is alive."""
        from relier.config import get_settings
        from relier.core.validation import validate_redis_reachable

        await validate_redis_reachable(redis_client, get_settings())  # must not raise

    async def test_validate_redis_config_against_live_redis(self, redis_client) -> None:
        """validate_redis_config() runs against the real Redis without raising."""
        from relier.config import get_settings
        from relier.core.validation import validate_redis_config

        # The testcontainer Redis starts with noeviction (no maxmemory set).
        # If this raises it means the container config is wrong, useful signal.
        await validate_redis_config(redis_client, get_settings())

    async def test_validate_connection_pool_within_safe_limits(self, setup_env) -> None:
        """validate_connection_pool() completes without warning for modest counts."""

        from relier.config import Settings
        from relier.core.validation import validate_connection_pool

        settings = Settings(
            celery_worker_count=1,
            celery_worker_concurrency=4,
            redis_max_connections=10,
        )
        # Just verifying no exception is raised
        await validate_connection_pool(settings)

    async def test_validate_connection_pool_high_pressure(self, setup_env) -> None:
        """validate_connection_pool() completes without raising for high connection counts."""
        from relier.config import Settings
        from relier.core.validation import validate_connection_pool

        settings = Settings(
            celery_worker_count=10,
            celery_worker_concurrency=10,
            redis_max_connections=100,
        )
        await validate_connection_pool(settings)  # must not raise


# ===========================================================================
# Timeout Enforcer
# ===========================================================================


class TestTimeoutsIntegration:
    async def test_function_completes_normally(self) -> None:
        """TimeoutEnforcer.run() returns the result when the function finishes in time."""
        from relier.core.timeouts import TimeoutEnforcer

        async def fast():
            return "result"

        result = await TimeoutEnforcer.run(fast, (), {}, None, None, None, "t-fast")
        assert result == "result"

    async def test_hard_timeout_raises(self) -> None:
        """TimeoutEnforcer.run() raises TimeoutError when the hard limit is exceeded."""
        from relier.core.timeouts import TimeoutEnforcer

        async def slow():
            await asyncio.sleep(10)

        with pytest.raises(TimeoutError):
            await TimeoutEnforcer.run(slow, (), {}, None, 0.05, None, "t-hard")

    async def test_soft_timeout_fires_callback(self) -> None:
        """The on_soft callback is invoked before the hard deadline cancels execution."""
        from relier.core.timeouts import TimeoutEnforcer

        soft_fired: list[bool] = []

        async def on_soft(ctx):
            soft_fired.append(True)

        async def slow():
            await asyncio.sleep(10)

        with pytest.raises(TimeoutError):
            await TimeoutEnforcer.run(slow, (), {}, 0.02, 0.1, on_soft, "t-soft")

        assert soft_fired, "Soft-timeout callback was never invoked"

    async def test_function_with_args_and_kwargs(self) -> None:
        """TimeoutEnforcer.run() correctly threads args and kwargs into the function."""
        from relier.core.timeouts import TimeoutEnforcer

        async def add(x, y, *, multiplier=1):
            return (x + y) * multiplier

        result = await TimeoutEnforcer.run(
            add, (3, 4), {"multiplier": 2}, None, None, None, "t-args"
        )
        assert result == 14
