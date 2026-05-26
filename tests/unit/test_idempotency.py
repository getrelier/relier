import json
import logging

import pytest

from relier.core.exceptions import IdempotencyInFlightError
from relier.core.idempotency import idempotency_lock, idempotency_manager
from relier.core.keys import RedisKeys

pytestmark = pytest.mark.asyncio


class TestIdempotencyManager:
    async def test_claim_and_record_lifecycle(self, mock_redis) -> None:
        """Test that a key can be claimed, executed, and cached."""
        key = "test_task:123"
        ttl = 60

        # First call should claim the lock
        result = await idempotency_manager.check_or_claim(key, ttl)
        assert result.already_executed is False
        assert result._lock_id.startswith(f"{RedisKeys.PREFIX}:inflight:")

        # Verify Redis state is indeed IN_FLIGHT
        val = await mock_redis.get(RedisKeys.idempotency(key))
        assert val.decode() == result._lock_id

        # Record the result
        task_output = {"status": "success", "data": 42}
        await result.record_result(task_output)

        # Subsequent call should return the cached result
        next_result = await idempotency_manager.check_or_claim(key, ttl)
        assert next_result.already_executed is True
        assert next_result.cached_result == task_output

    async def test_concurrent_execution_raises_inflight_error(self, mock_redis) -> None:
        """Test that while one worker has a lock, others receive InFlightError."""
        key = "concurrent_task:999"

        # Worker A claims it
        await idempotency_manager.check_or_claim(key, 60)

        # Worker B tries to claim the same key
        with pytest.raises(IdempotencyInFlightError) as exc:
            await idempotency_manager.check_or_claim(key, 60)

        assert key in str(exc.value)

    async def test_clear_lock_compare_and_delete(self, mock_redis) -> None:
        """Test that we only delete a lock if we own the specific lock_id."""
        key = "lock_test"
        full_key = RedisKeys.idempotency(key)

        # Set a fake lock manually
        await mock_redis.set(full_key, "IN_FLIGHT:owner_1")

        # owner_2 tries to clear it (should fail/do nothing)
        await idempotency_manager.clear_lock(key, "IN_FLIGHT:owner_2")

        assert await mock_redis.exists(full_key) == 1

        result = await mock_redis.get(full_key)
        assert result.decode() == "IN_FLIGHT:owner_1"

        # owner_1 clears it (should succeed)
        await idempotency_manager.clear_lock(key, "IN_FLIGHT:owner_1")
        assert await mock_redis.exists(full_key) == 0


class TestIdempotencyContextManager:
    async def test_lock_happy_path(self, mock_redis) -> None:
        """Test that the context manager records results on success."""
        key = "ctx_key"

        async with idempotency_lock(key=key, ttl=30) as result:
            assert result.already_executed is False
            output = "processed_data"
            await result.record_result(output)

        final = await mock_redis.get(RedisKeys.idempotency(key))
        assert json.loads(final.decode()) == output

    async def test_lock_failure_releases_sentinel(self, mock_redis) -> None:
        """Test that the IN_FLIGHT sentinel is deleted if the task crashes."""
        key = "fail_key"
        full_key = RedisKeys.idempotency(key)

        try:
            async with idempotency_lock(key=key, ttl=30):
                assert await mock_redis.exists(full_key) == 1
                raise ValueError("Worker crashed mid-task!")
        except ValueError:
            pass

        # The lock should have been released automatically by the context manager
        assert await mock_redis.exists(full_key) == 0

    async def test_lock_returns_cached_immediately(self, mock_redis) -> None:
        """Test that context manager bypasses logic if already executed."""
        key = "early_return"
        await mock_redis.set(RedisKeys.idempotency(key), json.dumps("old_result"))

        async with idempotency_lock(key=key) as result:
            assert result.already_executed is True
            assert result.cached_result == "old_result"


class TestIdempotencyLockSetResult:
    async def test_set_result_commits_on_exit(self, mock_redis) -> None:
        """set_result() stages a value that is committed to Redis on clean exit."""
        key = "set_result_key"

        async with idempotency_lock(key=key, ttl=60) as lock:
            assert lock.already_executed is False
            lock.set_result({"status": "done", "count": 7})

        stored = await mock_redis.get(RedisKeys.idempotency(key))
        assert json.loads(stored.decode()) == {"status": "done", "count": 7}

    async def test_set_result_dedup_blocks_second_call(self, mock_redis) -> None:
        """After a clean exit with set_result(), a second call gets the cached result."""
        key = "dedup_set_result"

        async with idempotency_lock(key=key, ttl=60) as lock:
            lock.set_result("first_run")

        async with idempotency_lock(key=key, ttl=60) as lock2:
            assert lock2.already_executed is True
            assert lock2.cached_result == "first_run"

    async def test_forgot_set_result_still_deduplicates(self, mock_redis) -> None:
        """Omitting set_result() commits a None sentinel — second call is still blocked."""
        key = "forgot_set_result"

        async with idempotency_lock(key=key, ttl=60):
            pass  # no set_result call

        # Lock should have been committed with None sentinel.
        stored = await mock_redis.get(RedisKeys.idempotency(key))
        assert json.loads(stored.decode()) is None

        # Second call is blocked — task does not re-run.
        async with idempotency_lock(key=key, ttl=60) as lock2:
            assert lock2.already_executed is True
            assert lock2.cached_result is None

    async def test_forgot_set_result_logs_warning(self, mock_redis, caplog) -> None:
        """Omitting set_result() emits a warning identifying the key."""
        key = "warn_no_set_result"

        with caplog.at_level(logging.WARNING, logger="relier.core.idempotency"):
            async with idempotency_lock(key=key, ttl=60):
                pass

        assert any("staged result" in r.message for r in caplog.records)

    async def test_exception_releases_lock_not_committed(self, mock_redis) -> None:
        """An exception in the context releases the lock without committing."""
        key = "exception_no_commit"
        full_key = RedisKeys.idempotency(key)

        try:
            async with idempotency_lock(key=key, ttl=60) as lock:
                lock.set_result("partial")
                raise RuntimeError("task failed mid-way")
        except RuntimeError:
            pass

        # Lock must be gone — not committed, not blocked.
        assert await mock_redis.exists(full_key) == 0

    async def test_exception_allows_retry(self, mock_redis) -> None:
        """After an exception exit, a fresh call can acquire the lock and run."""
        key = "retry_after_exception"

        try:
            async with idempotency_lock(key=key, ttl=60):
                raise ValueError("first attempt failed")
        except ValueError:
            pass

        # Second attempt should get a fresh lock, not a cached hit.
        async with idempotency_lock(key=key, ttl=60) as lock:
            assert lock.already_executed is False
            lock.set_result("second_attempt_result")

        stored = await mock_redis.get(RedisKeys.idempotency(key))
        assert json.loads(stored.decode()) == "second_attempt_result"

    async def test_set_result_none_is_distinct_from_not_called(
        self, mock_redis, caplog
    ) -> None:
        """set_result(None) is an explicit commit — no warning should be logged."""
        key = "explicit_none"

        with caplog.at_level(logging.WARNING, logger="relier.core.idempotency"):
            async with idempotency_lock(key=key, ttl=60) as lock:
                lock.set_result(None)

        # No warning — None was explicitly staged.
        assert not any("staged result" in r.message for r in caplog.records)

        stored = await mock_redis.get(RedisKeys.idempotency(key))
        assert json.loads(stored.decode()) is None
