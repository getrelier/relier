"""
Relier CLI UI — Panel Components.

Standardized UI panels for dashboard-style information display.
"""

from rich.panel import Panel
from rich.text import Text

from relier.cli.utils import PRIMARY_COLOR


def create_status_panel(
    title: str, message: str, style: str = f"{PRIMARY_COLOR}"
) -> Panel:
    """Create a standardized dashboard panel."""
    return Panel(
        Text(message, justify="center"),
        title=f"[bold]{title}[/bold]",
        border_style=style,
        padding=(1, 2),
    )


def create_burn_rate_panel(rate: float) -> Panel:
    """Create a specialized panel for SLO burn rate monitoring."""
    if rate == 0 or rate < 1.0:
        color = "green"
    elif rate < 14.4:
        color = "yellow"
    else:
        color = "red"

    return Panel(
        Text(f"{rate:.2f}x", style=f"bold {color}", justify="center"),
        title="Current Burn Rate",
        subtitle=f"Budget Status: {color.upper()}",
        border_style=color,
    )
