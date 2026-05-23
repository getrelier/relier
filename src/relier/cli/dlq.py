"""
Relier CLI — Dead Letter Queue Management.

Tools for inspecting, releasing, and purging quarantined tasks.
"""

import json

import typer
from rich.console import Console
from rich.syntax import Syntax

from relier.cli.utils import PRIMARY_COLOR, coro
from relier.core.dlq import DeadLetterQueue

app = typer.Typer(help="Manage quarantined tasks in the DLQ.")
console = Console()


@app.command(name="list")
@coro
async def list_dlq() -> None:
    """Show all quarantined tasks currently in the DLQ."""
    tasks = await DeadLetterQueue.list_tasks()

    if not tasks:
        console.print(
            "\n[bold green]DLQ is empty.[/bold green] All tasks processing normally.\n"
        )
        return

    from relier.cli.ui.tables import render_dlq_table

    console.print(render_dlq_table(tasks))


@app.command(name="inspect")
@coro
async def inspect(
    task_id: str = typer.Argument(..., help="Task ID to inspect."),
) -> None:
    """View the full JSON payload and error context of a quarantined task."""
    task = await DeadLetterQueue.inspect(task_id)
    if not task:
        console.print(f"[bold red]Error:[/bold red] Task {task_id!r} not found in DLQ.")
        raise typer.Exit(code=1)

    console.print(
        f"\n[bold {PRIMARY_COLOR}]Task Details:[/bold {PRIMARY_COLOR}] {task_id}"
    )
    payload_json = json.dumps(task, indent=2)
    syntax = Syntax(payload_json, "json", theme="ansi_dark", line_numbers=True)
    console.print(syntax)
    console.print("")


@app.command(name="release")
@coro
async def release(
    task_id: str = typer.Argument(..., help="Task ID to un-quarantine."),
) -> None:
    """Un-quarantine a task and re-submit it to its original queue."""
    success = await DeadLetterQueue.release(task_id)
    if success:
        console.print(f"[bold green]Task {task_id} released.[/bold green]")
    else:
        console.print(f"[bold red]Task {task_id!r} not found in DLQ.[/bold red]")
        raise typer.Exit(code=1)


@app.command(name="retry-all")
@coro
async def retry_all() -> None:
    """Re-submit all quarantined tasks to their original queues."""
    tasks = await DeadLetterQueue.list_tasks()

    if not tasks:
        console.print("[bold green]DLQ is empty.[/bold green]")
        return

    released = 0
    failed = 0
    for task in tasks:
        task_id = task.get("task_id", "")
        if not task_id:
            continue
        ok = await DeadLetterQueue.release(task_id)
        if ok:
            released += 1
            console.print(f"  [green]released[/green] {task_id}")
        else:
            failed += 1
            console.print(f"  [red]failed[/red]   {task_id}")

    console.print(f"\n[bold green]{released} task(s) released.[/bold green]")
    if failed:
        console.print(f"[yellow]{failed} task(s) could not be released.[/yellow]")


@app.command(name="purge")
@coro
async def purge(
    confirm: bool = typer.Option(
        False,
        "--confirm",
        help="Explicitly confirm the deletion of all DLQ entries.",
    ),
) -> None:
    """Permanently delete all tasks in the DLQ."""
    if not confirm:
        console.print(
            "[bold yellow]Warning:[/bold yellow] This operation is irreversible. "
            "Pass [bold]--confirm[/bold] to proceed."
        )
        raise typer.Exit(code=1)

    count = await DeadLetterQueue.purge()
    console.print(f"[bold red]Purged {count} tasks from the DLQ.[/bold red]")
