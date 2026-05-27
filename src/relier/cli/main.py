"""
Relier Command Line Interface.

The primary entry point for managing and monitoring the Relier reliability
cluster.  Scales via Typer sub-apps registered from the ``relier.cli`` package.
"""

import logging

import typer
from rich.logging import RichHandler
from rich.markup import escape as _markup_escape

from relier import __version__

# --- Register Sub-Apps ---
from relier.cli import (
    admin,
    admission,
    chaos,
    cluster,
    config,
    dlq,
    slo,
    tasks,
    workers,
)
from relier.cli.base import app, console
from relier.cli.utils import PRIMARY_COLOR, coro
from relier.storage.redis import redis_manager  # noqa: F401

# Attributes present on every LogRecord plus the ones added by Formatter.format().
_RECORD_ATTRS: frozenset[str] = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | frozenset({"message", "asctime"})

_PRIMARY = "#6366F1"  # Relier brand purple


class _StructuredRichFormatter(logging.Formatter):
    """Append extra= fields to the Rich terminal message so they are visible."""

    def format(self, record: logging.LogRecord) -> str:
        msg = _markup_escape(super().format(record))
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _RECORD_ATTRS and not k.startswith("_")
        }
        if extras:
            parts = "  ".join(
                f"[{_PRIMARY}]{k}[/{_PRIMARY}]={_markup_escape(repr(v))}"
                for k, v in extras.items()
            )
            msg = f"{msg}  {parts}"
        return msg


app.add_typer(tasks.tasks_app, name="tasks")
app.add_typer(cluster.app, name="cluster")
app.add_typer(workers.app, name="worker")
app.add_typer(dlq.app, name="dlq")
app.add_typer(slo.app, name="slo")
app.add_typer(admission.app, name="admission")
app.add_typer(chaos.app, name="chaos")
app.add_typer(admin.app, name="admin")
app.add_typer(config.app, name="config")


@app.command(name="man")
def man(
    plain: bool = typer.Option(
        False,
        "--plain",
        help="Print as raw Markdown instead of rendering with Rich.",
    ),
) -> None:
    """Print the Relier CLI manual.

    The manual is bundled with the package and rendered in-terminal with
    Rich-formatted Markdown, so it works the same way on Windows, macOS, and
    Linux. No Unix ``man`` command needed.
    """
    from rich.markdown import Markdown

    from relier.cli.manual import MANUAL

    if plain:
        print(MANUAL)
        return

    # Render with Rich's pager so long output is navigable on every platform.
    md = Markdown(MANUAL, code_theme="monokai")
    try:
        with console.pager(styles=True):
            console.print(md)
    except Exception:
        # Fallback for environments without a usable pager (e.g., piped output).
        console.print(md)


def version_callback(value: bool) -> None:
    """Callback for the global --version flag."""
    if value:
        console.print(
            f"Relier [bold {PRIMARY_COLOR}]v{__version__}[/bold {PRIMARY_COLOR}]"
        )
        raise typer.Exit()


@app.callback()
def main(
    version: bool | None = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show the application's version and exit.",
    ),
) -> None:
    """
    Zero job loss. Every task completes, hands off, or lands in the DLQ with a traceable reason.

    Run [bold]rl man[/bold] for the full manual, or [bold]rl <group> --help[/bold] for command details.
    """


async def _run_resurrector_impl(loglevel: str, interval: int | None) -> None:
    """Shared body for the resurrector entry point."""
    numeric_level = getattr(logging, loglevel.upper(), logging.INFO)

    # Use Rich logging for beautiful terminal logs in development.
    handler = RichHandler(rich_tracebacks=True, show_path=False, markup=True)
    handler.setFormatter(_StructuredRichFormatter(fmt="%(message)s", datefmt="[%X]"))
    logging.basicConfig(
        level=numeric_level,
        datefmt="[%X]",
        handlers=[handler],
    )

    from relier.core.phoenix import PhoenixRegistry

    if interval:
        # Override the interval
        from relier.config import get_settings

        settings = get_settings()
        settings.resurrection_check_interval = interval

    console.print(
        f"[bold {PRIMARY_COLOR}]PHOENIX[/bold {PRIMARY_COLOR}] Resurrector initializing..."
    )
    await PhoenixRegistry.resurrection_loop()


@app.command(name="run-resurrector")
@coro
async def run_resurrector(
    loglevel: str = typer.Option(
        "info", help="Logging level (debug, info, warning, error)."
    ),
    interval: int = typer.Option(
        None, help="Resurrection check interval in seconds (overrides config)."
    ),
) -> None:
    """
    Start the Phoenix resurrector engine.

    Monitors the cluster for dead workers and resurrects orphaned tasks.
    Typically runs as a dedicated 'guardian' process or container (see
    docker-compose.yml's resurrector service, or `make resurrector` for
    bare metal).
    """
    await _run_resurrector_impl(loglevel, interval)


@app.command(name="doctor")
@coro
async def doctor() -> None:
    """
    Check the health of Relier infrastructure and dependencies.
    """
    from rich.table import Table

    console.print(
        f"\n[bold {PRIMARY_COLOR}]Relier Infrastructure Status[/bold {PRIMARY_COLOR}]\n"
    )

    table = Table(box=None, show_header=False, padding=(0, 2))
    table.add_column("Component", style="bold")
    table.add_column("Status")
    table.add_column("Details", style="dim")

    # 1. Redis Check
    redis_alive = await redis_manager.ping()
    r_status = (
        "[bold green]ONLINE[/bold green]"
        if redis_alive
        else "[bold red]OFFLINE[/bold red]"
    )
    r_url = redis_manager.settings.redis_url
    r_details = f"{r_url.host}:{r_url.port}"
    table.add_row("Redis", r_status, r_details)

    console.print(table)

    if not redis_alive:
        console.print(
            "\n[bold red]CRITICAL:[/bold red] Redis is unreachable. "
            "Task persistence and locking are disabled."
        )
        raise typer.Exit(code=1)


@app.command(name="bench")
@coro
async def bench(
    iterations: int = typer.Option(
        1000, help="Number of dispatches per benchmark run."
    ),
    warmup: int = typer.Option(
        100,
        help="Warmup iterations discarded before measuring (covers Redis "
        "SCRIPT LOAD, pool warmup, JIT-ish effects).",
    ),
) -> None:
    """
    Measure Relier's dispatch overhead vs raw Celery dispatch.

    Compares ``task.delay()`` (Celery's native producer-side path) against
    ``task.apush()`` (Relier's path: admission control + schema envelope +
    OTel context injection). Both run against the always-loaded
    ``chaos_noop`` target so this works in any environment.

    Reports p50 / p95 / p99 / mean per-op timings so you can see the
    steady-state cost and the tail separately. Microbenchmarks like this
    have real noise; one run is not authoritative, re-run a few times and
    trust the p50.
    """
    import platform
    import sys
    import time

    from relier.chaos.tasks import chaos_noop

    # Import the Relier Celery app first so shared_task instances bind to
    # the Redis-backed broker. Without this, .delay() falls back to Celery's
    # default amqp:// transport and tries to connect to RabbitMQ.
    from relier.tasks.app import celery_app  # noqa: F401

    # ------------------------------------------------------------------
    # Environment line, context for whoever reads the output.
    # ------------------------------------------------------------------
    plat = f"{platform.system()} {platform.release()} ({platform.machine()})"
    py = f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    console.print(
        f"\n[bold {PRIMARY_COLOR}]Benchmarking Relier Overhead[/bold {PRIMARY_COLOR}]"
    )
    console.print(
        f"[dim]{plat} | {py} | {iterations} measured + {warmup} warmup[/dim]\n"
    )

    # ------------------------------------------------------------------
    # Warmup — discard. Redis Lua scripts load on first call (then EVALSHA
    # caches them), connection pools establish, the asyncio loop settles.
    # ------------------------------------------------------------------
    for _ in range(warmup):
        chaos_noop.delay("warmup")
    for _ in range(warmup):
        await chaos_noop.apush("warmup")

    # ------------------------------------------------------------------
    # Plain Celery dispatch — no admission, no envelope, no OTel.
    # Per-op timings give us a real distribution instead of one total.
    # ------------------------------------------------------------------
    plain_samples: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        chaos_noop.delay("bench")
        plain_samples.append(time.perf_counter() - t0)

    # ------------------------------------------------------------------
    # Relier dispatch — full producer path through admission + schema wrap.
    # ------------------------------------------------------------------
    reli_samples: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        await chaos_noop.apush("bench")
        reli_samples.append(time.perf_counter() - t0)

    # ------------------------------------------------------------------
    # Percentile + mean — trim the top 1% as an outlier guard so a single
    # AOF-fsync hiccup doesn't dominate the mean.
    # ------------------------------------------------------------------
    def stats(samples: list[float]) -> dict[str, float]:
        s = sorted(samples)
        n = len(s)
        return {
            "p50": s[n // 2] * 1000,
            "p95": s[int(n * 0.95)] * 1000,
            "p99": s[int(n * 0.99)] * 1000,
            "mean": (sum(s[: int(n * 0.99)]) / int(n * 0.99)) * 1000,
        }

    p = stats(plain_samples)
    r = stats(reli_samples)

    # ------------------------------------------------------------------
    # Output — table-shaped so columns line up across both paths.
    # ------------------------------------------------------------------
    header = f"{'':<22} {'p50':>9} {'p95':>9} {'p99':>9} {'mean*':>9}"
    console.print(header)
    console.print("-" * len(header))
    console.print(
        f"{'Plain Celery (.delay)':<22} "
        f"{p['p50']:>7.2f}ms {p['p95']:>7.2f}ms {p['p99']:>7.2f}ms {p['mean']:>7.2f}ms"
    )
    console.print(
        f"{'Relier        (.apush)':<22} "
        f"{r['p50']:>7.2f}ms {r['p95']:>7.2f}ms {r['p99']:>7.2f}ms {r['mean']:>7.2f}ms"
    )
    console.print("-" * len(header))

    over_p50 = r["p50"] - p["p50"]
    over_p99 = r["p99"] - p["p99"]
    pct_p50 = (over_p50 / p["p50"] * 100) if p["p50"] else 0.0

    console.print(
        f"\nOverhead at p50: [bold]{over_p50:+.2f} ms[/bold] "
        f"({pct_p50:+.1f}%) - the steady-state cost."
    )
    console.print(
        f"Overhead at p99: [bold]{over_p99:+.2f} ms[/bold] "
        f"- the tail, dominated by GC + AOF fsync."
    )
    console.print("[dim]*mean trimmed of top 1% outliers[/dim]\n")

    # ------------------------------------------------------------------
    # Honest framing — Windows + localhost Redis is the slowest environment
    # we benchmark on. Linux + uvloop is typically 30-50% faster baseline.
    # ------------------------------------------------------------------
    if platform.system() == "Windows":
        console.print(
            "[dim]Note: Windows + localhost Redis is the slowest path we "
            "measure. Linux with uvloop typically sees lower baselines "
            "(~1.5-2ms p50 plain, ~2-2.5ms p50 Relier) and tighter p99 "
            "tails due to faster TCP loopback and finer scheduler quanta. "
            "Absolute overhead in milliseconds is roughly platform-stable; "
            "the percentage shrinks because the baseline is smaller.[/dim]\n"
        )
