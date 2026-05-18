import time

import pytest

from relier.core.keys import RedisKeys
from relier.core.slo import SLOMetrics

pytestmark = pytest.mark.asyncio


class TestSLOMetrics:
    async def test_record_event_increments_window_count(self, mock_redis) -> None:
        """Recording an outcome makes it visible inside the rolling window."""
        await SLOMetrics.record_event("success")

        total = await SLOMetrics._count_window(mock_redis, "success", 3600)
        assert total == 1

    async def test_record_event_writes_current_bucket(self, mock_redis) -> None:
        """Recording an outcome increments the counter for a time bucket."""
        await SLOMetrics.record_event("failure")

        bucket = SLOMetrics._bucket(time.time())
        val = await mock_redis.get(RedisKeys.slo_bucket("failure", bucket))
        assert val is not None and int(val) == 1

    async def test_get_burn_rate_empty_returns_zero(self, mock_redis) -> None:
        """Burn rate is 0.0 when no events have occurred."""
        rate = await SLOMetrics.get_burn_rate("1h")
        assert rate == 0.0

    async def test_get_burn_rate_calculation(self, mock_redis) -> None:
        """Burn rate = actual_error_rate / allowed_error_rate.

        1 failure + 99 successes -> 1% error rate.
        Target 99.9% -> allowed error rate 0.1%.
        Expected burn rate = 0.01 / 0.001 = 10.0
        """
        bucket = SLOMetrics._bucket(time.time())
        await mock_redis.set(RedisKeys.slo_bucket("failure", bucket), "1")
        await mock_redis.set(RedisKeys.slo_bucket("success", bucket), "99")

        rate = await SLOMetrics.get_burn_rate("1h", target_slo=0.999)
        assert rate == pytest.approx(10.0)

    async def test_get_report_returns_all_windows(self, mock_redis) -> None:
        """The report contains a float burn rate for every configured window."""
        await SLOMetrics.record_event("success")
        await SLOMetrics.record_event("failure")

        report = await SLOMetrics.get_report()
        assert set(report.keys()) == set(SLOMetrics.WINDOW_SIZES.keys())
        for val in report.values():
            assert isinstance(val, float)

    async def test_old_buckets_excluded_from_window(self, mock_redis) -> None:
        """Counters outside the rolling window are not included in the count."""
        now = time.time()
        window_secs = SLOMetrics.WINDOW_SIZES["1h"]

        # A bucket well outside the 1h window.
        old_bucket = SLOMetrics._bucket(now - window_secs - 600)
        await mock_redis.set(RedisKeys.slo_bucket("success", old_bucket), "5")

        # A bucket inside the window.
        current_bucket = SLOMetrics._bucket(now)
        await mock_redis.set(RedisKeys.slo_bucket("success", current_bucket), "3")

        successes = await SLOMetrics._count_window(mock_redis, "success", window_secs)
        assert successes == 3
