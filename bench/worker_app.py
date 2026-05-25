"""
Relier worker app entry point for the benchmark.

Start with:
    celery -A bench.worker_app worker -Q default,high_priority,low_priority,re-queue -l warning

This module imports the Relier Celery app (which owns the event loop bridge,
Phoenix, graceful shutdown, etc.) and then imports bench tasks so they
register as shared_tasks on that same app: exactly the pattern the
Integration Recipes doc recommends for adding your tasks to a Relier worker.
"""

# 1. Boot the Relier Celery app (event loop bridge, signals, queues, …)
# 2. Register benchmark tasks so the worker can execute them
import bench.relier_tasks  # noqa: F401
from relier.tasks.app import celery_app  # noqa: F401
