"""
Relier CLI — SLO & Burn Rate Monitoring.

Displays real-time reliability metrics against configured SLO targets.
"""

import json as json_module

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from relier.cli.utils import PRIMARY_COLOR, coro
from relier.core.slo import SLOMetrics

app = typer.Typer(help="Monitor cluster SLOs and error budgets.")
console = Console()


@app.command(name="status")
@coro
async def status(
    target: float = typer.Option(
        0.999, "--target", "-t", help="SLO target as a fraction, e.g. 0.999 for 99.9%"
    ),
) -> None:
    """Show the current error budget burn rates across all time windows.

    Optionally pass `--target` to override the configured SLO target.
    """
    report = await SLOMetrics.get_report()

    # Monthly budget (30-day month) expressed in minutes.
    month_minutes = 60 * 24 * 30
    allowed_month_minutes = (1.0 - target) * month_minutes

    console.print(
        Panel.fit(
            f"[bold {PRIMARY_COLOR}]Relier Reliability Dashboard[/bold {PRIMARY_COLOR}]\n"
            f"[dim]SLO target: {target * 100:.3g}% ({allowed_month_minutes:.1f} min/month budget)[/dim]",
            border_style=PRIMARY_COLOR,
        )
    )

    table = Table(box=None, header_style=f"bold {PRIMARY_COLOR}", padding=(0, 2))
    table.add_column("Window", style="dim", width=12)
    table.add_column("Burn Rate", justify="right")
    table.add_column("Status", justify="center")

    for window, rate in report.items():
        # A rate of 1.0 means exactly on budget.
        # > 1.0 means burning budget too fast.
        if rate == 0:
            status_str = "[bold green]PRISTINE[/bold green]"
            rate_color = "green"
        elif rate < 1.0:
            status_str = "[green]HEALTHY[/green]"
            rate_color = "green"
        elif rate < 14.4:
            status_str = "[yellow]WARNING[/yellow]"
            rate_color = "yellow"
        else:
            status_str = "[bold red]CRITICAL[/bold red]"
            rate_color = "bold red"

        table.add_row(
            window,
            f"[{rate_color}]{rate:.2f}x[/{rate_color}]",
            status_str,
        )

    console.print(table)

    # Compute an approximate monthly budget usage based on the 1h burn rate.
    primary_rate = report.get("1h", 0.0)
    budget_used_minutes = primary_rate * allowed_month_minutes
    pct_of_month = (
        (budget_used_minutes / allowed_month_minutes * 100.0)
        if allowed_month_minutes > 0
        else 0.0
    )

    console.print(
        f"\nBudget used: {budget_used_minutes:.1f} min ({pct_of_month:.1f}% of monthly)"
    )

    if any(r > 1.0 for r in report.values()):
        console.print(
            "\n[bold yellow]Attention:[/bold yellow] Error budget is being consumed "
            "faster than the target SLO."
        )
    else:
        console.print(
            "\n[bold green]Success:[/bold green] All reliability targets are being met."
        )


@app.command(name="report")
@coro
async def report(
    period: str = typer.Option(
        "3d",
        "--period",
        help="Reporting period: 1h, 6h, or 3d.",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table or json.",
    ),
) -> None:
    """Generate a historical SLO burn-rate report across all time windows."""
    valid_periods = set(SLOMetrics.WINDOW_SIZES.keys())
    if period not in valid_periods:
        console.print(
            f"[yellow]Note:[/yellow] Period {period!r} is not a supported window. "
            f"Valid values: {', '.join(sorted(valid_periods))}. Showing all windows."
        )

    report_data = await SLOMetrics.get_report()

    if format == "json":
        console.print(json_module.dumps(report_data, indent=2))
        return

    target = 0.999
    month_budget_minutes = (1.0 - target) * 60 * 24 * 30  # 43.2 min

    console.print(
        Panel.fit(
            f"[bold {PRIMARY_COLOR}]SLO Burn Rate Report[/bold {PRIMARY_COLOR}]\n"
            f"[dim]Target: {target * 100:.3g}%  "
            f"Budget: {month_budget_minutes:.1f} min/month[/dim]",
            border_style=PRIMARY_COLOR,
        )
    )

    table = Table(box=None, header_style=f"bold {PRIMARY_COLOR}", padding=(0, 2))
    table.add_column("Window", style="dim", width=10)
    table.add_column("Burn Rate", justify="right")
    table.add_column("Status", justify="center")
    table.add_column("Projected consumption", justify="right", style="dim")

    for window_name, rate in report_data.items():
        if rate == 0:
            status_str = "[bold green]PRISTINE[/bold green]"
            rate_color = "green"
        elif rate < 1.0:
            status_str = "[green]HEALTHY[/green]"
            rate_color = "green"
        elif rate < 6.0:
            status_str = "[yellow]WARNING[/yellow]"
            rate_color = "yellow"
        elif rate < 14.4:
            status_str = "[bold yellow]HIGH[/bold yellow]"
            rate_color = "yellow"
        else:
            status_str = "[bold red]CRITICAL[/bold red]"
            rate_color = "bold red"

        projected = rate * month_budget_minutes
        projected_str = f"{projected:.1f} min/month" if rate > 0 else "0 min/month"

        table.add_row(
            window_name,
            f"[{rate_color}]{rate:.2f}x[/{rate_color}]",
            status_str,
            projected_str,
        )

    console.print(table)
