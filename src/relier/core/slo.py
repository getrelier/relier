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
import os
import time

from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)


class SLOMetrics:
    """
    Records task outcome telemetry and computes rolling SLO burn rates.

    Metrics are maintained in Redis-backed time windows to enable
    low-latency reliability calculations without requiring a dedicated
    metrics backend.
    """

    # Rolling observation windows used for burn-rate calculations.
    #
    # Short windows detect fast regressions quickly, while longer windows
    # smooth transient spikes and identify sustained reliability degradation.
    WINDOW_SIZES = {"1h": 3600, "6h": 21600, "3d": 259200}

    @classmethod
    async def record_event(cls, status: str) -> None:
        """
        Record a task execution outcome into all active SLO windows.

        Args:
            status:
                Execution outcome classification. Expected values:
                ``"success"`` or ``"failure"``.
        """
        redis = await get_relier_redis()
        now = time.time()
        # Use a unique sorted-set member to avoid timestamp collisions when
        # multiple events are recorded within the same clock tick under load.
        unique_member = f"{now}:{os.urandom(4).hex()}"

        pipe = redis.pipeline()
        for label, seconds in cls.WINDOW_SIZES.items():
            # Store outcome events as timestamp-scored sorted-set entries so old
            # observations can be trimmed efficiently per rolling window.
            key = RedisKeys.slo(label, status)
            pipe.zadd(key, {unique_member: now})
            pipe.zremrangebyscore(key, 0, now - seconds)

        await pipe.execute()

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
        now = time.time()
        start = now - cls.WINDOW_SIZES.get(window, 3600)

        # Count execution outcomes that fall within the active observation window.
        successes = await redis.zcount(RedisKeys.slo(window, "success"), start, now)
        failures = await redis.zcount(RedisKeys.slo(window, "failure"), start, now)

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
