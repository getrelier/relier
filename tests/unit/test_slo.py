import pytest
import time
from unittest.mock import patch
from relier.core.slo import SLOMetrics

pytestmark = pytest.mark.asyncio


class TestSLOMetrics:
    async def test_record_event_updates_all_windows(self, mock_redis):
        """Test that recording a success/failure adds entries to all window sorted sets."""
        await SLOMetrics.record_event("success")

        # SLOMetrics has 3 windows: 1h, 6h, 3d
        for window in SLOMetrics.WINDOW_SIZES:
            key = f"rl:slo:{window}:success"
            # Check if something was added to the sorted set
            assert await mock_redis.zcount(key, 0, time.time() + 10) == 1

    async def test_get_burn_rate_empty_returns_zero(self, mock_redis):
        """Test that burn rate is 0.0 when no events have occurred."""
        rate = await SLOMetrics.get_burn_rate("1h")
        assert rate == 0.0

    async def test_get_burn_rate_calculation(self, mock_redis):
        """Test burn rate calculation with specific event counts.

        SLO Target: 99.9% (allowed error rate: 0.1%)
        Burn Rate = actual_error_rate / allowed_error_rate
        """
        now = time.time()
        window = "1h"

        # Case: 1 failure, 99 successes -> 1% error rate
        # Target: 0.1% error rate
        # Expected Burn Rate = 0.01 / 0.001 = 10.0

        await mock_redis.zadd(f"rl:slo:{window}:failure", {"f1": now})
        for i in range(99):
            await mock_redis.zadd(f"rl:slo:{window}:success", {f"s{i}": now})

        rate = await SLOMetrics.get_burn_rate(window, target_slo=0.999)
        assert rate == pytest.approx(10.0)

    async def test_get_report_returns_all_windows(self, mock_redis):
        """Test that the report contains data for all configured windows."""
        # Record some events to ensure the report has data
        await SLOMetrics.record_event("success")
        await SLOMetrics.record_event("failure")

        report = await SLOMetrics.get_report()
        assert set(report.keys()) == set(SLOMetrics.WINDOW_SIZES.keys())
        for val in report.values():
            assert isinstance(val, float)

    async def test_zremrangebyscore_cleanup(self, mock_redis):
        """Test that recording an event cleans up old entries outside the window."""
        now = time.time()
        window = "1h"
        seconds = SLOMetrics.WINDOW_SIZES[window]
        key = f"rl:slo:{window}:success"

        # Add an old event (2 hours ago)
        await mock_redis.zadd(key, {"old": now - (seconds + 100)})
        assert await mock_redis.zcount(key, 0, now) == 1

        # Record a new event, which triggers cleanup
        await SLOMetrics.record_event("success")

        # The old event should be gone from the 1h window
        assert await mock_redis.zcount(key, 0, now - seconds) == 0
        # But the new one should be there
        assert await mock_redis.zcount(key, now - seconds, now + 10) == 1
