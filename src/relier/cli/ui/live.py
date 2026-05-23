"""
Relier CLI — Live UI Components.

Helpers for real-time refreshing terminal displays.
"""

import asyncio
from collections.abc import Callable

from rich.live import Live


async def run_live_display(render_fn: Callable, refresh_rate: float = 2.0) -> None:
    """
    Standard loop for refreshing a Rich Live display.
    """
    with Live(await render_fn(), refresh_per_second=1 / refresh_rate) as live:
        while True:
            await asyncio.sleep(refresh_rate)
            live.update(await render_fn())
