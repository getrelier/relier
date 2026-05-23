"""
Relier CLI — Worker Management (UPDATED VERSION)

Commands for monitoring active workers and managing graceful shutdowns.
Now includes per-worker success/failed task metrics.
"""

import typer
from rich import box
from rich.console import Console
from rich.table import Table

from relier.cli.utils import PRIMARY_COLOR, coro
from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

app = typer.Typer(help="Manage and monitor Relier worker nodes.")
console = Console()


def create_enhanced_worker_table() -> Table:
    """Enhanced worker table with session metrics."""
    table = Table(box=box.SIMPLE, header_style=f"bold {PRIMARY_COLOR}")
    table.add_column("Worker ID", style="bold")
    table.add_column("Status", justify="center")
    table.add_column("Active", justify="right", style="magenta")
    table.add_column("✓ Success", justify="right", style="green")
    table.add_column("✗ Failed", justify="right", style="red")
    table.add_column("Success Rate", justify="right", style="cyan")
    return table


@app.command(name="status")
@coro
async def status() -> None:
    """
    List all registered workers with their execution metrics.

    Shows per-worker session statistics including:
    - Active tasks currently executing
    - Total successful task completions
    - Total failed tasks
    - Success rate percentage
    """
    redis = await get_relier_redis()
    raw_worker_ids = await redis.zrange(RedisKeys.workers(), 0, -1)

    if not raw_worker_ids:
        console.print("\n[bold yellow]No workers found in registry.[/bold yellow]\n")
        return

    table = create_enhanced_worker_table()

    # Sort workers by ID for stable output.
    active_workers = []
    for w_id_bytes in sorted(raw_worker_ids):
        w_id = (
            w_id_bytes.decode("utf-8")
            if isinstance(w_id_bytes, bytes)
            else str(w_id_bytes)
        )

        # 1. Check presence (emitted by worker_loop heartbeat).
        is_alive = await redis.exists(RedisKeys.presence(w_id))
        if not is_alive:
            # Cleanup stale worker IDs from the set.
            await redis.zrem(RedisKeys.workers(), w_id_bytes)
            continue

        active_workers.append(w_id)

    if not active_workers:
        console.print("\n[bold yellow]No active workers found.[/bold yellow]\n")
        return

    # Aggregate totals for summary footer
    total_completed = 0
    total_failed = 0
    total_active = 0

    for w_id in active_workers:
        status_str = "[bold green]● ONLINE[/bold green]"

        # 2. Count in-flight tasks from the sorted set.
        inflight_count = await redis.zcard(RedisKeys.inflight(w_id))
        total_active += inflight_count

        # 3. Fetch per-worker session metrics
        completed_raw = await redis.get(RedisKeys.metric_worker(w_id, "success"))
        failed_raw = await redis.get(RedisKeys.metric_worker(w_id, "failed"))

        completed = int(completed_raw) if completed_raw else 0
        failed = int(failed_raw) if failed_raw else 0

        total_completed += completed
        total_failed += failed

        # Calculate success rate
        total_tasks = completed + failed
        if total_tasks > 0:
            success_rate = (completed / total_tasks) * 100
            rate_str = f"{success_rate:.1f}%"

            # Color code the success rate
            if success_rate >= 95:
                rate_style = "bold green"
            elif success_rate >= 80:
                rate_style = "yellow"
            else:
                rate_style = "bold red"
            rate_display = f"[{rate_style}]{rate_str}[/{rate_style}]"
        else:
            rate_display = "[dim]N/A[/dim]"

        table.add_row(
            w_id,
            status_str,
            str(inflight_count),
            str(completed),
            str(failed) if failed > 0 else "[dim]0[/dim]",
            rate_display,
        )

    console.print(table)

    # Summary footer
    console.print()
    if total_completed + total_failed > 0:
        overall_rate = (total_completed / (total_completed + total_failed)) * 100
        console.print(
            f"[dim]Cluster Summary:[/dim] "
            f"[green]{total_completed} completed[/green] | "
            f"[red]{total_failed} failed[/red] | "
            f"[magenta]{total_active} active[/magenta] | "
            f"[cyan]{overall_rate:.1f}% success rate[/cyan]"
        )
    console.print()


@app.command(name="drain")
@coro
async def drain(
    worker_id: str = typer.Argument(..., help="Hostname of the worker to drain."),
) -> None:
    """
    Trigger a graceful shutdown sequence for a specific worker.

    The worker will stop accepting new tasks and either finish its current
    work or hand it off to Phoenix before exiting.
    """
    from relier.tasks.app import celery_app

    console.print(
        f"[bold {PRIMARY_COLOR}]Initiating drain sequence:[/bold {PRIMARY_COLOR}] {worker_id}"
    )

    try:
        # Broadcast a remote control command to the target worker.
        celery_app.control.broadcast("shutdown", destination=[worker_id])
        console.print("[bold green]Signal broadcasted.[/bold green]")
        console.print(
            "[dim]Worker will now enter the drain phase and exit cleanly.[/dim]"
        )
    except Exception as exc:
        console.print(f"[bold red]Error:[/bold red] Failed to signal worker: {exc}")
        raise typer.Exit(code=1) from exc


@app.command(name="restart")
def restart(
    worker_id: str = typer.Argument(..., help="Worker hostname to restart."),
) -> None:
    """Send a graceful shutdown signal to a specific worker for rolling restart.

    The process manager (Docker, systemd, Kubernetes) is responsible for
    restarting the worker after it exits cleanly.
    """
    from relier.tasks.app import celery_app

    try:
        celery_app.control.broadcast("shutdown", destination=[worker_id])
        console.print(f"[bold green]Restart signal sent to[/bold green] {worker_id}.")
        console.print(
            "[dim]The process manager will restart the worker automatically.[/dim]"
        )
    except Exception as exc:
        console.print(f"[bold red]Error:[/bold red] Failed to signal worker: {exc}")
        raise typer.Exit(code=1) from exc


@app.command(name="reset")
@coro
async def reset_metrics(
    worker_id: str = typer.Argument(..., help="Worker ID to reset metrics for"),
) -> None:
    """
    Reset the session metrics (success/failed counts) for a specific worker.

    Useful when restarting workers or clearing stale metrics.
    """
    redis = await get_relier_redis()

    # Delete the per-worker metric keys
    await redis.delete(RedisKeys.metric_worker(worker_id, "success"))
    await redis.delete(RedisKeys.metric_worker(worker_id, "failed"))

    console.print(
        f"[bold green]✓[/bold green] Reset metrics for worker: [bold]{worker_id}[/bold]"
    )
