"""
Relier Chaos — Slow Task Scenario.

Dispatches a task that intentionally sleeps for longer than its configured
``hard_timeout`` so the two-tier (soft/hard) timeout machinery in
``relier.core.timeouts`` is forced to fire.

We use the internal ``relier.chaos.tasks.chaos_slow`` task, which is
decorated with ``hard_timeout=2``. Any ``duration`` >= 2s exercises:

    1. Soft-timeout cleanup hook (if registered)
    2. Hard-timeout cancellation
    3. DLQ quarantine for ``TimeoutError`` in the decorator
"""

import logging
import uuid

from relier.chaos.engine import chaos_engine

logger = logging.getLogger(__name__)


@chaos_engine.register("slow-task")
class SlowTaskScenario:
    """Dispatch a deliberately-slow task to force a hard-timeout cancellation."""

    def __init__(self, duration: int = 60) -> None:
        self.duration = duration

    async def execute(self) -> str:
        """Dispatch a slow task whose runtime exceeds its decorator hard_timeout."""
        # Lazy import: avoids pulling Celery into `rl chaos --help`.
        from relier.chaos.tasks import chaos_slow

        marker = f"chaos-slow-{uuid.uuid4().hex[:8]}"
        logger.critical(
            "Dispatching slow task to provoke timeout enforcement.",
            extra={"duration_s": self.duration, "marker": marker},
        )

        await chaos_slow.apush(duration=self.duration, marker_key=marker)

        logger.info(
            "Slow task dispatched; expect a hard-timeout + DLQ quarantine.",
            extra={"marker": marker},
        )
        return marker
