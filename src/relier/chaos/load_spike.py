"""
Relier Chaos,  Load Spike Scenario.

Drives a sustained burst of task dispatches through the Relier admission
controller. Each dispatch goes through ``apush()``, which means the
fixed-window admission limiter (``admission_limit`` / ``admission_window``)
is the actual choke point, not a fake HTTP layer.

When the cluster is at capacity, ``apush`` raises ``AdmissionRejectedError``.
We tally accepts vs. rejects so the operator can confirm that admission
control engages and that workers are not flooded with unbounded backlog.
"""

import asyncio
import logging

from relier.chaos.engine import chaos_engine
from relier.core.exceptions import AdmissionRejectedError

logger = logging.getLogger(__name__)


@chaos_engine.register("load-spike")
class LoadSpikeScenario:
    """Flood Relier with task dispatches to trigger admission rejections."""

    def __init__(self, rps: int = 100, duration: int = 10) -> None:
        self.rps = rps
        self.duration = duration

    async def execute(self) -> dict[str, int]:
        """Burst ``rps`` dispatches per second for ``duration`` seconds."""
        # Imported lazily so the chaos package does not pull in the Celery
        # runtime at import time (matters for `rl chaos --help`).
        from relier.chaos.tasks import chaos_noop

        logger.critical(
            "Triggering load spike.",
            extra={"rps": self.rps, "duration_s": self.duration},
        )

        accepted = 0
        rejected = 0
        errored = 0

        async def _dispatch_one() -> str:
            try:
                await chaos_noop.apush("chaos-load-spike")
                return "ok"
            except AdmissionRejectedError:
                return "rejected"
            except Exception as exc:
                logger.debug(
                    "Dispatch failed unexpectedly.",
                    extra={"error_type": type(exc).__name__},
                )
                return "error"

        loop = asyncio.get_running_loop()
        start = loop.time()
        # Slice into 10 sub-intervals per second to approximate the target RPS
        # without spawning all dispatches in a single huge gather().
        batch_size = max(1, self.rps // 10)

        while (loop.time() - start) < self.duration:
            tick_start = loop.time()
            results = await asyncio.gather(
                *(_dispatch_one() for _ in range(batch_size))
            )
            for r in results:
                if r == "ok":
                    accepted += 1
                elif r == "rejected":
                    rejected += 1
                else:
                    errored += 1

            elapsed = loop.time() - tick_start
            sleep_for = max(0.0, 0.1 - elapsed)
            await asyncio.sleep(sleep_for)

        logger.info(
            "Load spike simulation complete.",
            extra={
                "accepted": accepted,
                "rejected": rejected,
                "errored": errored,
                "duration_s": self.duration,
                "target_rps": self.rps,
            },
        )
        return {"accepted": accepted, "rejected": rejected, "errored": errored}
