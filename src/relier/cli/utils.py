"""
Relier CLI — Utilities.

Common decorators and helpers for Typer-based async command execution.
"""

import asyncio
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from relier.storage.redis import redis_manager

logger = logging.getLogger(__name__)

PRIMARY_COLOR = "#6366F1"


def coro(f: Callable) -> Callable:
    """Decorator to run async Typer commands safely."""

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        async def _run_and_teardown() -> Any:
            try:
                return await f(*args, **kwargs)
            finally:
                # Idempotent cleanup of persistent connection pools.
                await redis_manager.close()

        try:
            # Check if a loop is already running in this thread
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        try:
            if loop and loop.is_running():
                # If a loop is running, we return the coroutine for
                # the caller to handle or create a task.
                return asyncio.ensure_future(_run_and_teardown())
            else:
                # Standard entry point: no loop exists, start a new one.
                return asyncio.run(_run_and_teardown())
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            logger.error("CLI command failed.", extra={"error": str(exc)})
            raise

    return wrapper


async_command = coro
