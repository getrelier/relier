"""
Relier Core — SLO & Error Budget Tracking.

Implements rolling SLO telemetry and multi-window error-budget burn-rate
calculation for task execution reliability.

Task outcomes are aggregated into time-windowed Redis datasets to support:
- real-time reliability monitoring
- SRE burn-rate alerting
- operational dashboards
- automated incident detection

The burn-rate model follows standard Site Reliability Engineering (SRE)
practices where sustained elevated failure rates consume the service's
allowed error budget faster than intended.
"""

import logging
import time

from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class SLOMetrics:
    """
    Records task outcome telemetry and computes rolling SLO burn rates.

    Outcomes are aggregated into fixed-size time buckets (plain Redis integer
    counters with a TTL). This keeps SLO storage and per-event cost O(1)
    regardless of throughput, unlike a per-event sorted set, whose member
    count grows linearly with traffic and can reach hundreds of millions of
    entries over a multi-day window.
    """

    # Rolling observation windows used for burn-rate calculations.
    #
    # Short windows detect fast regressions quickly, while longer windows
    # smooth transient spikes and identify sustained reliability degradation.
    WINDOW_SIZES = {"1h": 3600, "6h": 21600, "3d": 259200}

    # Width of a single counter bucket, in seconds.
    BUCKET_SECONDS = 60

    # Buckets are retained slightly beyond the largest window so a burn-rate
    # query never races a just-expired bucket out of existence.
    _RETENTION_SECONDS = max(WINDOW_SIZES.values()) + BUCKET_SECONDS

    @classmethod
    def _bucket(cls, ts: float) -> int:
        """Return the bucket epoch (floored to ``BUCKET_SECONDS``) for ``ts``."""
        return int(ts) - (int(ts) % cls.BUCKET_SECONDS)

    @classmethod
    async def record_event(cls, status: str) -> None:
        """
        Record a task execution outcome into the current time bucket.

        Args:
            status:
                Execution outcome classification. Expected values:
                ``"success"`` or ``"failure"``.
        """
        redis = await get_relier_redis()
        key = RedisKeys.slo_bucket(status, cls._bucket(time.time()))

        # A single INCR per event; the TTL bounds total key count so old
        # buckets expire on their own without an explicit trim pass.
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, cls._RETENTION_SECONDS)
        await pipe.execute()

    @classmethod
    async def _count_window(cls, redis: object, status: str, window_secs: int) -> int:
        """Sum the outcome counters for ``status`` across the rolling window."""
        end = cls._bucket(time.time())
        start = end - window_secs + cls.BUCKET_SECONDS
        keys = [
            RedisKeys.slo_bucket(status, b)
            for b in range(start, end + 1, cls.BUCKET_SECONDS)
        ]
        if not keys:
            return 0
        # One MGET round-trip regardless of window size.
        values = await redis.mget(keys)  # type: ignore[attr-defined]
        return sum(int(v) for v in values if v)

    @classmethod
    async def get_burn_rate(
        cls, window: str = "1h", target_slo: float = 0.999
    ) -> float:
        """
        Calculate the SLO error-budget burn rate for a rolling time window.

        Burn-rate interpretation:
        - ``1.0``  → consuming error budget at the expected rate
        - ``>1.0`` → reliability degradation is exhausting budget too quickly
        - ``<1.0`` → operating within reliability targets
        """
        redis = await get_relier_redis()
        window_secs = cls.WINDOW_SIZES.get(window, 3600)

        # Count execution outcomes that fall within the active observation window.
        successes = await cls._count_window(redis, "success", window_secs)
        failures = await cls._count_window(redis, "failure", window_secs)

        total = successes + failures
        # Avoid reporting burn when no execution traffic exists in the window.
        if total == 0:
            return 0.0

        actual_error_rate = failures / total

        # Convert the target SLO into its corresponding allowable failure budget.
        allowed_error_rate = 1.0 - target_slo

        if allowed_error_rate == 0:
            logger.warning(
                "Burn rate undefined because target SLO permits zero failures.",
                extra={"window": window, "failures": failures},
            )
            return (
                100.0  # Cap the value to avoid propagating infinite metrics downstream.
            )

        # Burn rate expresses how quickly the observed error rate is consuming
        # the allowable reliability budget.
        return float(actual_error_rate / allowed_error_rate)

    @classmethod
    async def get_report(cls) -> dict[str, float]:
        """
        Generate a multi-window burn-rate snapshot across all configured SLO
        observation windows.
        """

        # Aggregate burn rates across all configured rolling windows.
        report = {}
        for window in cls.WINDOW_SIZES:
            report[window] = await cls.get_burn_rate(window)
        return report
