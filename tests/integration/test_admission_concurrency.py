"""
Integration test: admission control under concurrent load.

The admission Lua script must atomically increment + check the fixed-window
counter. If the increment and the limit check were not atomic, concurrent
dispatches could each read the count before any of them write, and all of
them would be admitted past the configured ceiling.

This test fires N concurrent calls at a single window with a tight limit
and asserts that exactly `limit` calls are admitted — no more, no fewer.
"""

import asyncio

import pytest

from relier.config import get_settings
from relier.core.admission import AdmissionController
from relier.core.keys import RedisKeys


@pytest.mark.asyncio
async def test_admission_atomic_under_concurrent_dispatch(redis_client) -> None:
    """Concurrent dispatches must respect the configured admission limit."""
    settings = get_settings()
    original_limit = settings.admission_limit
    original_window = settings.admission_window
    resource_key = "concurrency-test"

    # Use a tight limit with a long window so timing jitter cannot let a
    # second window open mid-test. `object.__setattr__` bypasses the
    # frozen-model guard since Settings is constructed once per process.
    object.__setattr__(settings, "admission_limit", 50)
    object.__setattr__(settings, "admission_window", 60)

    try:
        # Wipe any prior counter so the test starts from zero.
        await redis_client.delete(RedisKeys.admission(resource_key))

        controller = AdmissionController()
        attempts = 200

        async def attempt() -> bool:
            admitted, _ = await controller.check_capacity(resource_key)
            return admitted

        results = await asyncio.gather(*(attempt() for _ in range(attempts)))

        admitted = sum(1 for r in results if r)
        rejected = attempts - admitted

        # Exactly `limit` admits, no more, no fewer. Anything else means the
        # increment + check is not atomic.
        assert admitted == 50, (
            f"expected exactly 50 admits, got {admitted} "
            f"(rejected={rejected}). The admission Lua script lost atomicity."
        )
        assert rejected == 150

        # Counter should reflect every attempt that touched the window.
        counter = await redis_client.get(RedisKeys.admission(resource_key))
        assert int(counter) == attempts
    finally:
        object.__setattr__(settings, "admission_limit", original_limit)
        object.__setattr__(settings, "admission_window", original_window)
