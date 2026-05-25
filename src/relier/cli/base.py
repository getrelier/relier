"""
Relier CLI — Base Components.

Centralises the shared Rich Console and the root Typer application so every
sub-app can import them without circular dependencies. Help text is
deliberately ASCII-only so it renders correctly on Windows cp1252 consoles
(em-dashes show as replacement glyphs there).
"""

import typer
from rich.console import Console

from relier.cli.utils import PRIMARY_COLOR

console = Console()

# ---------------------------------------------------------------------------
# Root help text. Indigo highlight on group names; everything else stays plain
# so the output reads more like a polished man page than a colourful demo.
# ---------------------------------------------------------------------------
_HELP = (
    "[bold]Relier[/bold] - reliability layer for Celery. Zero job loss.\n\n"
    "Every task completes, hands off, or lands in the DLQ with a traceable "
    "reason.\n\n"
    "[bold]Common commands[/bold]\n\n"
    "  [bold]rl doctor[/bold]              Check Redis + dependency health\n"
    "  [bold]rl run-resurrector[/bold]     Start the Phoenix resurrector loop\n"
    "  [bold]rl tasks inflight[/bold] -f   Live view of running tasks\n"
    "  [bold]rl dlq list[/bold]            List quarantined tasks\n"
    "  [bold]rl man[/bold]                 Print the full CLI manual\n\n"
    "[bold]Command groups[/bold]\n\n"
    f"  [bold {PRIMARY_COLOR}]cluster[/]      Start, stop, scale, and tail the Docker stack\n"
    f"  [bold {PRIMARY_COLOR}]tasks[/]        Monitor in-flight tasks; retry, cancel, inspect\n"
    f"  [bold {PRIMARY_COLOR}]worker[/]       Drain, restart, and view per-worker metrics\n"
    f"  [bold {PRIMARY_COLOR}]dlq[/]          Inspect and release quarantined tasks\n"
    f"  [bold {PRIMARY_COLOR}]slo[/]          Error-budget burn rates and reports\n"
    f"  [bold {PRIMARY_COLOR}]chaos[/]        Trigger failures to verify reliability\n"
    f"  [bold {PRIMARY_COLOR}]config[/]       Show, validate, and update configuration\n"
    f"  [bold {PRIMARY_COLOR}]admission[/]    Admission control status\n"
    f"  [bold {PRIMARY_COLOR}]admin[/]        Low-level Redis maintenance\n\n"
    "Run [bold]rl <group> --help[/bold] to see commands in a group.\n"
    "Run [bold]rl man[/bold] for the full manual."
)

app = typer.Typer(
    name="rl",
    help=_HELP,
    no_args_is_help=True,
    rich_markup_mode="rich",
    add_completion=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
