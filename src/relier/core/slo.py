"""
Relier Core — SLO & Error Budget Tracking.

Implements SRE multi-window burn rate alerting.
Tracks task success/failure rates in Redis to calculate real-time reliability.
"""

import logging
import os
import time

from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class SLOMetrics:
    """
    Tracks task completions and failures to calculate budget burn rates.
    """

    WINDOW_SIZES = {"1h": 3600, "6h": 21600, "3d": 259200}

    @classmethod
    async def record_event(cls, status: str) -> None:
        """
        Record a task completion or failure.
        status: 'success' or 'failure'
        """
        redis = await get_relier_redis()
        now = time.time()
        # Use a unique member key to prevent silent overwrites when multiple
        # events land in the exact same microsecond under load.
        unique_member = f"{now}:{os.urandom(4).hex()}"

        pipe = redis.pipeline()
        for label, seconds in cls.WINDOW_SIZES.items():
            # Use sorted sets to track events with timestamps
            key = f"rl:slo:{label}:{status}"
            pipe.zadd(key, {unique_member: now})
            pipe.zremrangebyscore(key, 0, now - seconds)

        await pipe.execute()

    @classmethod
    async def get_burn_rate(
        cls, window: str = "1h", target_slo: float = 0.999
    ) -> float:
        """
        Calculate the burn rate for a given window.
        1.0 means consuming budget at exactly the target rate.
        > 1.0 means budget is being exhausted too fast.
        """
        redis = await get_relier_redis()
        now = time.time()
        start = now - cls.WINDOW_SIZES.get(window, 3600)

        # Count successes and failures in the window
        successes = await redis.zcount(f"rl:slo:{window}:success", start, now)
        failures = await redis.zcount(f"rl:slo:{window}:failure", start, now)

        total = successes + failures
        if total == 0:
            return 0.0

        actual_error_rate = failures / total
        allowed_error_rate = 1.0 - target_slo

        if allowed_error_rate == 0:
            return 100.0  # Division by zero protection

        return float(actual_error_rate / allowed_error_rate)

    @classmethod
    async def get_report(cls) -> dict[str, float]:
        """
        Return a full burn rate report across all windows.
        """
        report = {}
        for window in cls.WINDOW_SIZES:
            report[window] = await cls.get_burn_rate(window)
        return report


# Global instance
slo_metrics = SLOMetrics()
