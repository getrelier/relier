import json
import pytest
import asyncio
from relier.core.idempotency import idempotency_manager, idempotency_lock
from relier.core.exceptions import IdempotencyInFlightError

pytestmark = pytest.mark.asyncio


class TestIdempotencyManager:
    async def test_claim_and_record_lifecycle(self, mock_redis):
        """Test that a key can be claimed, executed, and cached."""
        key = "test_task:123"
        ttl = 60

        # 1. First call should claim the lock
        result = await idempotency_manager.check_or_claim(key, ttl)
        assert result.already_executed is False
        assert "IN_FLIGHT" in result._lock_id

        # Verify Redis state is indeed IN_FLIGHT
        val = await mock_redis.get(f"rl:idem:{key}")
        assert val == result._lock_id

        # 2. Record the result
        task_output = {"status": "success", "data": 42}
        await result.record_result(task_output)

        # 3. Subsequent call should return the cached result
        next_result = await idempotency_manager.check_or_claim(key, ttl)
        assert next_result.already_executed is True
        assert next_result.cached_result == task_output

    async def test_concurrent_execution_raises_inflight_error(self, mock_redis):
        """Test that while one worker has a lock, others receive InFlightError."""
        key = "concurrent_task:999"

        # Worker A claims it
        await idempotency_manager.check_or_claim(key, 60)

        # Worker B tries to claim the same key
        with pytest.raises(IdempotencyInFlightError) as exc:
            await idempotency_manager.check_or_claim(key, 60)

        assert key in str(exc.value)

    async def test_clear_lock_compare_and_delete(self, mock_redis):
        """Test that we only delete a lock if we own the specific lock_id."""
        key = "lock_test"
        full_key = f"rl:idem:{key}"

        # Set a fake lock manually
        await mock_redis.set(full_key, "IN_FLIGHT:owner_1")

        # owner_2 tries to clear it (should fail/do nothing)
        await idempotency_manager.clear_lock(key, "IN_FLIGHT:owner_2")
        assert await mock_redis.get(full_key) == "IN_FLIGHT:owner_1"

        # owner_1 clears it (should succeed)
        await idempotency_manager.clear_lock(key, "IN_FLIGHT:owner_1")
        assert await mock_redis.exists(full_key) == 0


class TestIdempotencyContextManager:
    async def test_lock_happy_path(self, mock_redis):
        """Test that the context manager records results on success."""
        key = "ctx_key"

        async with idempotency_lock(key=key, ttl=30) as result:
            assert result.already_executed is False
            output = "processed_data"
            await result.record_result(output)

        final = await mock_redis.get(f"rl:idem:{key}")
        assert json.loads(final) == output

    async def test_lock_failure_releases_sentinel(self, mock_redis):
        """Test that the IN_FLIGHT sentinel is deleted if the task crashes."""
        key = "fail_key"
        full_key = f"rl:idem:{key}"

        try:
            async with idempotency_lock(key=key, ttl=30) as result:
                assert await mock_redis.exists(full_key) == 1
                raise ValueError("Worker crashed mid-task!")
        except ValueError:
            pass

        # The lock should have been released automatically by the context manager
        assert await mock_redis.exists(full_key) == 0

    async def test_lock_returns_cached_immediately(self, mock_redis):
        """Test that context manager bypasses logic if already executed."""
        key = "early_return"
        await mock_redis.set(f"rl:idem:{key}", json.dumps("old_result"))

        async with idempotency_lock(key=key) as result:
            assert result.already_executed is True
            assert result.cached_result == "old_result"
