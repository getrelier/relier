"""
Relier CLI — Cluster Orchestration.

Tools for managing the underlying Docker infrastructure and scaling the
worker pool.
"""

import subprocess

import typer
from rich.console import Console

from relier.cli.utils import PRIMARY_COLOR, coro
from relier.storage.redis import get_relier_redis

app = typer.Typer(help="Manage the Relier infrastructure stack.")
console = Console()


@app.command(name="up")
def up(
    detach: bool = typer.Option(
        True, "--detach/--no-detach", "-d", help="Run in detached mode."
    ),
) -> None:
    """Start the full Relier stack via Docker Compose."""
    cmd = ["docker", "compose", "up", "--build"]
    if detach:
        cmd.append("-d")
    try:
        subprocess.run(cmd, check=True)
        if detach:
            console.print("[bold green]Cluster started.[/bold green]")
    except subprocess.CalledProcessError as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    except FileNotFoundError:
        console.print(
            "[bold red]Error:[/bold red] Docker not found. "
            "Ensure Docker is installed and running."
        )
        raise typer.Exit(code=1) from None


@app.command(name="down")
def down() -> None:
    """Gracefully shut down the Relier stack."""
    try:
        subprocess.run(["docker", "compose", "down"], check=True)
        console.print("[bold green]Cluster stopped.[/bold green]")
    except subprocess.CalledProcessError as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    except FileNotFoundError:
        console.print(
            "[bold red]Error:[/bold red] Docker not found. "
            "Ensure Docker is installed and running."
        )
        raise typer.Exit(code=1) from None


@app.command(name="status")
@coro
async def status() -> None:
    """Check the health of the underlying Docker infrastructure."""
    console.print(
        f"\n[bold {PRIMARY_COLOR}]Cluster Infrastructure[/bold {PRIMARY_COLOR}]\n"
    )

    # 1. Check Docker Compose Status
    try:
        result = subprocess.run(
            ["docker", "compose", "ps"],
            check=True,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            console.print(result.stdout)
        console.print("[bold green]DOCKER[/bold green] Compose stack is active.")
    except subprocess.CalledProcessError:
        console.print(
            "[bold red]DOCKER[/bold red] Compose stack is unreachable or not running."
        )
    except FileNotFoundError:
        console.print("[bold red]DOCKER[/bold red] Docker not found.")

    # 2. Redis Connectivity
    redis = await get_relier_redis()
    try:
        await redis.ping()
        console.print("[bold green]REDIS[/bold green] Connection established.")
    except Exception:
        console.print("[bold red]REDIS[/bold red] Connection failed.")


@app.command(name="scale")
def scale(
    workers: int = typer.Argument(..., help="Number of worker replicas to maintain."),
) -> None:
    """Scale the worker pool to the specified number of replicas."""
    console.print(
        f"[bold {PRIMARY_COLOR}]Scaling worker pool:[/bold {PRIMARY_COLOR}] {workers} units"
    )

    try:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", f"worker={workers}"],
            check=True,
        )
        console.print("[bold green]Scale operation successful.[/bold green]")
    except subprocess.CalledProcessError as exc:
        console.print(f"[bold red]Error:[/bold red] Failed to scale cluster: {exc}")
        raise typer.Exit(code=1) from exc
    except FileNotFoundError:
        console.print(
            "[bold red]Error:[/bold red] Docker not found. "
            "Ensure Docker is installed and running."
        )
        raise typer.Exit(code=1) from None


@app.command(name="logs")
def logs(
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Stream logs continuously."
    ),
    service: str | None = typer.Argument(
        None, help="Service name to filter (e.g. worker, redis)."
    ),
) -> None:
    """Tail logs from the Relier stack."""
    cmd = ["docker", "compose", "logs"]
    if follow:
        cmd.append("-f")
    if service:
        cmd.append(service)
    try:
        subprocess.run(cmd, check=False)
    except FileNotFoundError:
        console.print(
            "[bold red]Error:[/bold red] Docker not found. "
            "Ensure Docker is installed and running."
        )
        raise typer.Exit(code=1) from None
