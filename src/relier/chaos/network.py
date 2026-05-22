"""
Relier Chaos — Network Partition Scenario.

Simulates a network disconnect between the Relier cluster and its Redis
broker, exercising worker resilience, "fail-open" admission control, and
Phoenix's behaviour when heartbeat refreshes cannot land.

The Compose project does not declare a custom network, so the Redis
container is attached to ``<project>_default`` (typically
``relier-cluster_default``). Rather than hard-coding that name, we discover
every network the ``relier-redis`` container is attached to at runtime and
detach/reattach all of them.
"""

import asyncio
import json
import logging
import subprocess

from relier.chaos.engine import chaos_engine

logger = logging.getLogger(__name__)

REDIS_CONTAINER = "relier-redis"


def _discover_redis_networks(
    container: str = REDIS_CONTAINER,
) -> dict[str, list[str]]:
    """Return a mapping of network-name -> list of DNS aliases for the container.

    Aliases must be captured *before* detaching so we can restore them on
    reconnect. ``docker network connect`` does not preserve the original
    compose service-name alias (e.g. ``redis``) without it, other
    containers can still reach Redis by container name (``relier-redis``)
    but DNS lookups for the service name fail.
    """
    try:
        raw = (
            subprocess.check_output(
                [
                    "docker",
                    "inspect",
                    container,
                    "--format",
                    "{{json .NetworkSettings.Networks}}",
                ]
            )
            .decode()
            .strip()
        )
        if not raw or raw == "null":
            return {}
        data = json.loads(raw)
        return {net: (cfg.get("Aliases") or []) for net, cfg in data.items()}
    except subprocess.CalledProcessError as exc:
        logger.error(
            "Failed to inspect Redis container networks.",
            extra={"container": container, "error": str(exc)},
        )
        return {}


@chaos_engine.register("network-partition")
class NetworkPartitionScenario:
    """Isolate the Redis broker from every network it is attached to."""

    def __init__(self, duration: int = 15) -> None:
        self.duration = duration

    async def execute(self) -> None:
        """Disconnect Redis from all its networks, then reconnect after `duration`."""
        net_aliases = _discover_redis_networks()
        if not net_aliases:
            logger.error(
                "No docker networks discovered for Redis; cannot partition.",
                extra={"container": REDIS_CONTAINER},
            )
            return

        logger.critical(
            "Executing network partition (isolating Redis).",
            extra={
                "duration_s": self.duration,
                "networks": list(net_aliases.keys()),
            },
        )

        detached: dict[str, list[str]] = {}
        try:
            for net, aliases in net_aliases.items():
                try:
                    subprocess.run(
                        ["docker", "network", "disconnect", net, REDIS_CONTAINER],
                        check=True,
                    )
                    detached[net] = aliases
                except subprocess.CalledProcessError as exc:
                    logger.error(
                        "Failed to disconnect Redis from network.",
                        extra={"network": net, "error": str(exc)},
                    )

            await asyncio.sleep(self.duration)

        finally:
            # Reattach every network we successfully removed, restoring the
            # original DNS aliases (in particular the compose service name
            # without it, other containers cannot resolve ``redis``).
            for net, aliases in detached.items():
                cmd = ["docker", "network", "connect"]
                for alias in aliases:
                    # Skip the auto-assigned container-ID-prefix alias; it is
                    # re-added automatically on connect and rejecting it with
                    # --alias is harmless but noisy.
                    if not alias or alias == REDIS_CONTAINER:
                        continue
                    cmd.extend(["--alias", alias])
                cmd.extend([net, REDIS_CONTAINER])
                try:
                    subprocess.run(cmd, check=True)
                except Exception as exc:
                    logger.critical(
                        "FAILED TO HEAL NETWORK PARTITION.",
                        extra={"network": net, "error": str(exc)},
                    )
            logger.info(
                "Network partition healed.",
                extra={"networks": list(detached.keys())},
            )
