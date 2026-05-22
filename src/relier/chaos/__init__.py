"""
Relier Chaos, Reliability validation suite.

Importing this package registers every chaos scenario with the
``ChaosEngine`` singleton via side effects. Without this, the CLI would
resolve ``chaos_engine.run("worker-kill", ...)`` against an empty registry
and raise ``ValueError``.
"""

from relier.chaos import (  # noqa: F401
    load_spike,
    network,
    slow_task,
    task_corrupt,
    worker_kill,
)
from relier.chaos.engine import chaos_engine

__all__ = ["chaos_engine"]
