"""
Relier CLI — Chaos Engineering.

Tools for triggering deliberate failures to validate cluster reliability.
"""

import asyncio
import logging

import typer
from rich.console import Console

# Importing the chaos package registers every scenario with the engine.
import relier.chaos  # noqa: F401
from relier.chaos.engine import chaos_engine
from relier.cli.utils import PRIMARY_COLOR, coro
from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

app = typer.Typer(help="Trigger chaos scenarios to test reliability.")
console = Console()
logger = logging.getLogger(__name__)


async def _stream_resurrection_events(duration: int) -> None:
    """
    Poll Redis for resurrection activity and stream events to the console.

    Watches two signals:
    - ``rl:m:global:resurrected`` — global counter, incremented after each
      confirmed broker replay (see ``PhoenixRegistry._bg_send``).
    - ``rl:monitoring``           — hash of resurrected task IDs and their
      pickup state (0 = pending pickup, 1 = revived).
    """
    redis = await get_relier_redis()

    baseline = int(await redis.get(RedisKeys.metric_global("resurrected")) or 0)
    seen_state: dict[str, str] = {}

    console.print(
        f"[bold {PRIMARY_COLOR}]WATCH[/bold {PRIMARY_COLOR}] "
        f"Streaming resurrection events for {duration}s..."
    )

    loop = asyncio.get_running_loop()
    start = loop.time()
    while (loop.time() - start) < duration:
        try:
            cur = int(await redis.get(RedisKeys.metric_global("resurrected")) or 0)
            monitor = await redis.hgetall(RedisKeys.monitor())  # type: ignore[misc]
        except Exception as exc:
            logger.debug("Watch poll failed.", extra={"error": str(exc)})
            await asyncio.sleep(1.0)
            continue

        if cur > baseline:
            console.print(
                f"  [green]++[/green] {cur - baseline} new resurrection(s) "
                f"confirmed on broker (total={cur})"
            )
            baseline = cur

        for tid, state in monitor.items():
            prev = seen_state.get(tid)
            if prev == state:
                continue
            seen_state[tid] = state
            if state == "0":
                label = "[yellow]RESURRECTED[/yellow] (awaiting pickup)"
            elif state == "1":
                label = "[green]ALIVE[/green] (revived by replacement worker)"
            else:
                label = f"state={state}"
            console.print(f"  [cyan]->[/cyan] {tid}: {label}")

        await asyncio.sleep(0.5)

    console.print(
        f"[bold {PRIMARY_COLOR}]WATCH[/bold {PRIMARY_COLOR}] Done. "
        f"{len(seen_state)} task(s) observed in monitor."
    )


@app.command(name="worker-kill")
@coro
async def worker_kill(
    worker_id: str | None = typer.Option(
        None, help="Specific worker container to kill."
    ),
    watch: bool = typer.Option(
        False, "--watch", help="Stream Phoenix resurrection events after the kill."
    ),
    watch_duration: int = typer.Option(
        30, "--watch-duration", help="Seconds to stream resurrection events for."
    ),
    seed: bool = typer.Option(
        False,
        "--seed",
        help="Dispatch a long-running task immediately before the kill so "
        "Phoenix has something orphaned to resurrect.",
    ),
    seed_duration: int = typer.Option(
        30,
        "--seed-duration",
        help="How long the seeded task should run if --seed is passed.",
    ),
) -> None:
    """Kill a random or specific worker process (SIGKILL)."""
    if seed:
        # Lazy import: keeps `rl chaos --help` from loading Celery.
        from relier.chaos.tasks import chaos_long_running

        marker = f"chaos-kill-seed-{__import__('uuid').uuid4().hex[:8]}"
        await chaos_long_running.apush(duration=seed_duration, marker_key=marker)
        console.print(
            f"[bold {PRIMARY_COLOR}]SEED[/bold {PRIMARY_COLOR}] "
            f"Dispatched {seed_duration}s long-running task. marker={marker}"
        )
        # Give the broker a beat to hand the task to a worker before we kill it.
        await asyncio.sleep(1.0)

    killed = await chaos_engine.run("worker-kill", worker_id=worker_id)
    if killed:
        console.print("[bold red]CHAOS[/bold red] Worker terminated.")
        if watch:
            await _stream_resurrection_events(watch_duration)
    else:
        console.print(
            "[bold yellow]CHAOS[/bold yellow] No worker container was killed. "
            "Run the stack with [bold]make dev[/bold] for the kill step to work. "
            "The seeded task (if any) was discarded by the worker — "
            "add [bold]--include=relier.chaos.tasks[/bold] to your worker command."
        )


@app.command(name="network-partition")
@coro
async def network_partition(
    secs: int = typer.Option(
        15, "--secs", "--duration", help="Duration of the outage in seconds."
    ),
) -> None:
    """Simulate a network partition between workers and Redis."""
    await chaos_engine.run("network-partition", duration=secs)
    console.print(f"[bold red]CHAOS[/bold red] Redis isolated for {secs}s.")


@app.command(name="load-spike")
@coro
async def load_spike(
    rps: int = typer.Option(100, help="Requests per second target."),
    duration: int = typer.Option(10, help="Duration of the spike in seconds."),
) -> None:
    """Flood the dispatch path to exercise admission control."""
    result = await chaos_engine.run("load-spike", rps=rps, duration=duration)
    console.print(
        f"[bold red]CHAOS[/bold red] Load spike of {rps} RPS finished "
        f"accepted={result['accepted']} rejected={result['rejected']} "
        f"errored={result['errored']}"
    )


@app.command(name="task-corrupt")
@coro
async def task_corrupt() -> None:
    """Inject a malformed 'poison pill' envelope into the queue."""
    await chaos_engine.run("task-corrupt")
    console.print(
        "[bold red]CHAOS[/bold red] Poison pill injected, expect a DLQ quarantine."
    )


@app.command(name="slow-task")
@coro
async def slow_task(
    duration: int = typer.Option(
        35, help="Seconds the dispatched task will sleep (must exceed hard_timeout)."
    ),
) -> None:
    """Dispatch a slow task to provoke timeout enforcement and DLQ quarantine."""
    marker = await chaos_engine.run("slow-task", duration=duration)
    console.print(
        f"[bold red]CHAOS[/bold red] Slow task ({duration}s) dispatched. marker={marker}"
    )
