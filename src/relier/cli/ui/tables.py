"""
Relier CLI UI — Table Renderers.

Provides specialized Rich table formatters for cluster monitoring.
"""

import time
from datetime import datetime
from typing import Any

from rich import box
from rich.columns import Columns
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from relier.cli.utils import PRIMARY_COLOR
from relier.config import get_settings


def create_worker_table() -> Table:
    """Standard table for worker status overview."""
    table = Table(box=box.SIMPLE, header_style=f"bold {PRIMARY_COLOR}")
    table.add_column("Worker ID", style="bold")
    table.add_column("Status", justify="center")
    table.add_column("In-Flight", justify="right", style="magenta")
    table.add_column("Uptime", justify="right", style="dim")
    return table


def format_duration(seconds: float) -> str:
    """Turn raw seconds into a concise human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def render_inflight_table(
    tasks: list[dict[str, Any]],
    active_workers: list[str],
    stats: dict[str, int],
    queue_depth: int | None = None,
    p95: float | None = None,
    worker_metrics: dict[str, dict[str, int]] | None = None,
) -> Group:
    """
    Worker-centric view: one row per worker + sub-rows for each running task.
    """
    now = time.time()

    # Worker Status Table (one row per worker, sub-rows per task)
    table = Table(box=box.SIMPLE, header_style=f"bold {PRIMARY_COLOR}", expand=True)
    table.add_column("Worker", style="bold", no_wrap=True)
    table.add_column("Status", justify="center")
    table.add_column("In-Flight", justify="right", style="magenta")
    table.add_column("✓ Completed", justify="right", style="green")
    table.add_column("✗ Failed", justify="right", style="red")
    table.add_column("Success Rate", justify="right", style="cyan")

    # Group running tasks by worker
    tasks_by_worker: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        if task.get("status") == "RUNNING":
            w_id = task.get("worker_id", "unknown")
            tasks_by_worker.setdefault(w_id, []).append(task)

    # Render one row per active worker, followed by sub-rows for each task
    for w_id in sorted(active_workers):
        worker_tasks = tasks_by_worker.get(w_id, [])
        inflight_count = len(worker_tasks)

        # Get worker metrics
        metrics = worker_metrics.get(w_id, {}) if worker_metrics else {}
        completed = metrics.get("completed", 0)
        failed = metrics.get("failed", 0)

        # Calculate success rate
        total = completed + failed
        if total > 0:
            rate = (completed / total) * 100
            rate_style = "green" if rate >= 95 else "yellow" if rate >= 85 else "red"
            rate_display = f"[{rate_style}]{rate:.1f}%[/{rate_style}]"
        else:
            rate_display = "[dim]N/A[/dim]"

        # Status indicator
        if inflight_count > 0:
            status = "[bold green]● BUSY[/bold green]"
        else:
            status = "[dim]○ IDLE[/dim]"

        table.add_row(
            w_id,
            status,
            str(inflight_count) if inflight_count > 0 else "[dim]0[/dim]",
            str(completed) if completed > 0 else "[dim]0[/dim]",
            str(failed) if failed > 0 else "[dim]0[/dim]",
            rate_display,
        )

        # Sub-rows: one per running task on this worker
        for i, task in enumerate(worker_tasks):
            task_name = task.get("task_name", "unknown")
            # Show only the function name (strip module path)
            short_name = task_name.rsplit(".", 1)[-1] if "." in task_name else task_name
            task_id = task.get("task_id", "")
            short_id = (task_id[:8] + "…") if len(task_id) > 8 else task_id
            elapsed = now - float(task.get("started_at", now))
            elapsed_str = format_duration(elapsed)
            connector = "└─" if i == len(worker_tasks) - 1 else "├─"
            table.add_row(
                f"  [dim]{connector}[/dim] [italic]{short_name}[/italic]",
                f"[dim]{short_id}[/dim]",
                f"[dim]{elapsed_str}[/dim]",
                "",
                "",
                "",
            )

    # Footer with cluster-wide stats
    cluster_completed = stats.get("completed", 0)
    cluster_failed = stats.get("failed", 0)
    cluster_quarantined = stats.get("quarantined", 0)
    total_inflight = sum(len(v) for v in tasks_by_worker.values())
    cluster_resurrected = stats.get("resurrected", 0)

    session_completed = stats.get("session_completed", 0)

    stats_line = Columns(
        [
            Text(f"● {total_inflight} Active", style="bold green"),
            Text(f"✔ {session_completed} Session (24h)", style="bold green"),
            Text(f"✔ {cluster_completed} Lifetime", style="bold blue"),
            Text(f"✗ {cluster_failed} Failed", style="bold yellow"),
            Text(f"♻ {cluster_resurrected} Resurrected", style="bold cyan"),  # ← new
            Text(f"☢ {cluster_quarantined} Quarantined", style="bold red"),
            Text(
                f"Depth: {queue_depth if queue_depth is not None else 'N/A'}",
                style="dim",
            ),
            Text(f"p95: {format_duration(p95) if p95 else 'N/A'}", style="dim"),
        ],
        expand=True,
    )

    footer_panel = Panel(
        stats_line,
        border_style="dim",
        title="[dim]Cluster Health[/dim]",
        title_align="left",
    )

    return Group(table, footer_panel)


def render_dlq_table(tasks: list[dict[str, Any]]) -> Table:
    """Render a table of quarantined tasks with detailed failure context."""
    settings = get_settings()
    table = Table(
        box=box.SIMPLE,
        header_style="bold red",
        expand=True,
    )

    table.add_column("ID", style="dim", no_wrap=True)
    table.add_column("TASK", style="bold yellow")
    table.add_column("RESURRECTIONS", justify="center")
    table.add_column("QUARANTINED_AT", justify="right", style=f"{PRIMARY_COLOR}")
    table.add_column("LAST_ERROR", style="red")

    for task in tasks:
        raw_time = task.get("quarantined_at", "")
        try:
            # Handle 'Z' suffix for Python versions < 3.11.
            dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            time_str = str(raw_time)

        t_id = task.get("task_id", "unknown")
        task_name = task.get("task_name", "unknown")
        if task_name:
            task_name = str(task_name).split(".")[-1]
        res = int(task.get("resurrections", 0))
        max_res = settings.max_resurrections or 0
        last_err = task.get("error") or task.get("reason") or "unknown"

        table.add_row(
            t_id,
            task_name,
            f"{res}/{max_res}",
            time_str,
            last_err,
        )

    return table
