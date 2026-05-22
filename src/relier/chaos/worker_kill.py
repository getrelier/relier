"""
Relier Chaos — Worker Kill Scenario.

Simulates a catastrophic worker failure (OOM kill or hard process crash)
by sending a SIGKILL to a randomly selected Celery worker container.

The recovery worker is deliberately excluded from the selection pool, it
drains the ``re-queue`` queue that Phoenix resurrects orphaned tasks onto,
so killing it would defeat the very recovery path this scenario is meant
to exercise.
"""

import logging
import random
import subprocess

from relier.chaos.engine import chaos_engine

logger = logging.getLogger(__name__)


@chaos_engine.register("worker-kill")
class WorkerKillScenario:
    """Hard-kill a worker container to trigger Phoenix resurrection."""

    def __init__(self, worker_id: str | None = None) -> None:
        self.worker_id = worker_id

    async def execute(self) -> None:
        """Find and terminate a worker container via the Docker CLI."""
        if not self.worker_id:
            try:
                cmd = [
                    "docker",
                    "ps",
                    "--filter",
                    "name=worker",
                    "--format",
                    "{{.Names}}",
                ]
                output = subprocess.check_output(cmd).decode().strip()
                # Exclude the recovery worker, it drains the re-queue queue
                # that Phoenix replays orphaned tasks onto.
                workers = [w for w in output.splitlines() if w and "recovery" not in w]

                if not workers:
                    logger.error(
                        "No eligible worker containers found to kill "
                        "(recovery workers are excluded)."
                    )
                    return

                self.worker_id = random.choice(workers)
            except Exception as exc:
                logger.error("Failed to list workers.", extra={"error": str(exc)})
                return

        logger.critical(
            "Executing worker kill.",
            extra={"worker_id": self.worker_id},
        )

        try:
            subprocess.run(["docker", "kill", self.worker_id], check=True)
            logger.info(
                "Worker container terminated.", extra={"worker_id": self.worker_id}
            )
        except subprocess.CalledProcessError as exc:
            logger.error(
                "Failed to kill worker container.",
                extra={"worker_id": self.worker_id, "error": str(exc)},
            )
