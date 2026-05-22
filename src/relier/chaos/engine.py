"""
Relier Chaos, The Engine of Destruction.

Registry and orchestration for chaos experiments designed to validate
Relier's reliability mechanisms (Phoenix, timeouts, circuit breakers).
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ChaosEngine:
    """Registry and runner for all chaos scenarios."""

    _scenarios: dict[str, type] = {}

    @classmethod
    def register(cls, name: str) -> Any:
        """Decorator for registering a chaos scenario class."""

        def decorator(scenario_class: type) -> type:
            cls._scenarios[name] = scenario_class
            return scenario_class

        return decorator

    @classmethod
    async def run(cls, name: str, **kwargs: Any) -> Any:
        """Instantiate and execute a named chaos scenario.

        Args:
            name:   Name of the scenario (e.g. "worker-kill").
            kwargs: Parameters passed to the scenario constructor.
        """
        if name not in cls._scenarios:
            raise ValueError(f"Chaos scenario {name!r} not found.")

        scenario = cls._scenarios[name](**kwargs)
        logger.warning(
            "Unleashing chaos scenario.",
            extra={"scenario": name, "params": kwargs},
        )
        return await scenario.execute()


# Global Registry Singleton
chaos_engine = ChaosEngine()
