"""
Relier CLI — Admission Control.

Commands for monitoring and managing admission control.
"""

import typer
from rich.console import Console

from relier.cli.utils import PRIMARY_COLOR, coro
from relier.core.admission import admission_control
from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

app = typer.Typer(help="Manage admission control and load shedding.")
console = Console()


@app.command(name="status")
@coro
async def status() -> None:
    """Show current admission control status and load shedding."""
    redis = await get_relier_redis()

    # Get current count
    window_key = RedisKeys.admission("global")
    current_count = await redis.get(window_key)
    current_count = int(current_count) if current_count else 0

    limit = admission_control.settings.admission_limit
    window = admission_control.settings.admission_window

    if current_count >= limit:
        status = f"[bold red]SHEDDING[/bold red] ({current_count}/{limit})"
    else:
        pct = (current_count / limit) * 100 if limit > 0 else 0
        status = (
            f"[bold green]ALLOWING[/bold green] ({current_count}/{limit}, {pct:.1f}%)"
        )

    console.print(
        f"\n[bold {PRIMARY_COLOR}]Admission Control Status[/bold {PRIMARY_COLOR}]\n"
    )
    console.print(f"Status: {status}")
    console.print(f"Window: {window}s")
    console.print("")
