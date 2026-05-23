"""
Relier CLI — Cluster Administration.

Advanced tools for cluster-wide configuration management and data cleanup.
"""

import typer
from rich.console import Console
from rich.table import Table

from relier.cli.utils import PRIMARY_COLOR, coro
from relier.config import get_settings
from relier.core.keys import RedisKeys

app = typer.Typer(help="Administrative tools for cluster maintenance.")
console = Console()


@app.command(name="config")
def show_config() -> None:
    """Display the active cluster configuration and environment settings."""
    settings = get_settings()

    table = Table(box=None, header_style=f"bold {PRIMARY_COLOR}")
    table.add_column("Setting")
    table.add_column("Value")

    # Filter out sensitive strings like passwords/tokens if they were here.
    # Pydantic-settings RedisUrl masks passwords by default in its __repr__.
    table.add_row("Environment", settings.env)
    table.add_row("Log Level", settings.log_level)
    table.add_row("Admission Limit", str(settings.admission_limit))
    table.add_row("Max Resurrections", str(settings.max_resurrections))
    table.add_row("Heartbeat TTL", f"{settings.heartbeat_ttl}s")
    table.add_row("OTEL Enabled", str(settings.otel_enabled))

    console.print(
        f"\n[bold {PRIMARY_COLOR}]Active Configuration[/bold {PRIMARY_COLOR}]\n"
    )
    console.print(table)
    console.print("")


@app.command(name="purge-locks")
@coro
async def purge_locks() -> None:
    """Force-delete all idempotency locks and sentinels in Redis."""
    from relier.storage.redis import get_relier_redis

    redis = await get_relier_redis()
    keys_to_delete = []

    async for key in redis.scan_iter(match=f"{RedisKeys.PREFIX}:idem:*"):
        keys_to_delete.append(key)

    if not keys_to_delete:
        console.print("[dim]No idempotency locks found.[/dim]")
        return

    await redis.delete(*keys_to_delete)
    console.print(
        f"[bold red]Purged {len(keys_to_delete)} idempotency locks.[/bold red]"
    )


@app.command(name="reset-admission")
@coro
async def reset_admission() -> None:
    """Reset the cluster admission control counters."""
    from relier.storage.redis import get_relier_redis

    redis = await get_relier_redis()
    keys = await redis.keys(f"{RedisKeys.PREFIX}:admission:*")

    if not keys:
        console.print("[dim]No admission counters found.[/dim]")
        return

    await redis.delete(*keys)
    console.print("[bold green]Admission control counters reset.[/bold green]")
