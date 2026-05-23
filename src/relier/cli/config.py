"""
Relier CLI — Configuration Management.
"""

import re
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from relier.cli.utils import PRIMARY_COLOR, coro
from relier.config import get_settings

app = typer.Typer(help="Manage and validate Relier configuration.")
console = Console()


@app.command(name="show")
def show() -> None:
    """Print the active configuration settings."""
    settings = get_settings()

    table = Table(title="Active Relier Configuration")
    table.add_column("SETTING", style=f"{PRIMARY_COLOR}")
    table.add_column("VALUE", style="green")

    data = settings.model_dump()
    for k, v in data.items():
        val = str(v)
        if "pass" in k.lower() or "secret" in k.lower() or "url" in k.lower():
            val = "[dim]********[/dim]"
        table.add_row(k, val)

    console.print(table)


@app.command(name="validate")
@coro
async def validate() -> None:
    """
    Validate Relier configuration and Redis setup.

    Checks:
    - Redis noeviction policy (CRITICAL)
    - Connection pool pressure estimate
    - All RELIER_* environment variables
    """
    from relier.core.validation import validate_connection_pool, validate_redis_config
    from relier.storage.redis import get_relier_redis

    settings = get_settings()
    redis = await get_relier_redis()

    console.print("\nValidating Relier configuration...\n")

    try:
        await validate_redis_config(redis, settings)
        console.print("[green]OK[/green]  Redis noeviction policy")
    except RuntimeError as exc:
        console.print(f"[red]FAIL[/red] Redis policy check failed:\n{exc}")
        raise typer.Exit(code=1) from exc

    await validate_connection_pool(settings)

    console.print(
        Panel(
            f"Redis URL: {settings.redis_url}\n"
            f"Heartbeat TTL: {settings.heartbeat_ttl}s\n"
            f"IN_FLIGHT TTL: {settings.idempotency_inflight_ttl}s\n"
            f"Max Resurrections: {settings.max_resurrections}\n"
            f"Workers: {settings.celery_worker_count}\n"
            f"Concurrency: {settings.celery_worker_concurrency}\n"
            f"Pool Size: {settings.redis_max_connections}",
            title="Active Configuration",
            border_style="blue",
        )
    )

    console.print("\n[bold green]All validation checks passed.[/bold green]\n")


@app.command(name="set")
def set_config(
    key: str = typer.Argument(..., help="Config key (e.g. RELIER_HEARTBEAT_TTL)."),
    value: str = typer.Argument(..., help="New value to set."),
) -> None:
    """Update a configuration value in the local .env file."""
    # Search for .env starting from CWD, then walk up to root.
    env_path: Path | None = None
    for directory in [Path.cwd(), *Path.cwd().parents]:
        candidate = directory / ".env"
        if candidate.exists():
            env_path = candidate
            break

    if env_path is None:
        console.print(
            "[bold red]Error:[/bold red] No .env file found. "
            "Create one or run from the project root."
        )
        raise typer.Exit(code=1)

    content = env_path.read_text(encoding="utf-8")
    new_line = f"{key}={value}"
    pattern = rf"^({re.escape(key)}\s*=).*$"

    if re.search(pattern, content, flags=re.MULTILINE):
        content = re.sub(pattern, new_line, content, flags=re.MULTILINE)
    else:
        # Append with a trailing newline.
        content = content.rstrip("\n") + f"\n{new_line}\n"

    env_path.write_text(content, encoding="utf-8")
    console.print(f"[bold green]Set[/bold green] {key}={value} in {env_path}")
    console.print("[dim]Restart workers for the change to take effect.[/dim]")
