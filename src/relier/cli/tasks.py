"""
Relier CLI — Task Management.

Commands for monitoring in-flight tasks and managing the task lifecycle.
"""

import asyncio
import contextlib
import json
import math
from datetime import datetime
from typing import Any

import typer
from rich.live import Live
from rich.syntax import Syntax

from relier.cli.base import console
from relier.cli.ui.tables import render_inflight_table
from relier.cli.utils import PRIMARY_COLOR, coro
from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

tasks_app = typer.Typer(help="Manage and monitor in-flight tasks.")


async def _get_all_inflight_data() -> dict[str, Any]:
    """FIXED: Uses O(1) global counters instead of SCAN."""
    redis = await get_relier_redis()
    all_registered_workers = await redis.zrange(RedisKeys.workers(), 0, -1)
    active_workers: list[str] = []
    all_tasks: list[dict[str, Any]] = []

    # Per-worker metrics for ACTIVE workers only (for display)
    active_worker_metrics: dict[str, dict[str, int]] = {}

    # --- Fetch Active Tasks + Active Worker Metrics ---
    for w_id_bytes in all_registered_workers:
        w_id = (
            w_id_bytes.decode("utf-8")
            if isinstance(w_id_bytes, bytes)
            else str(w_id_bytes)
        )

        # Check if worker is alive
        is_alive = await redis.exists(RedisKeys.presence(w_id))

        if is_alive:
            active_workers.append(w_id)

            # Fetch metrics for display (only for active workers)
            completed_raw = await redis.get(RedisKeys.metric_worker(w_id, "success"))
            failed_raw = await redis.get(RedisKeys.metric_worker(w_id, "failed"))

            active_worker_metrics[w_id] = {
                "completed": int(completed_raw) if completed_raw else 0,
                "failed": int(failed_raw) if failed_raw else 0,
            }

            # Get tasks currently being processed by this worker
            inflight_tasks = await redis.zrange(
                RedisKeys.inflight(w_id), 0, -1, withscores=True
            )
            for task_id_bytes, started_at in inflight_tasks:
                task_id = (
                    task_id_bytes.decode()
                    if isinstance(task_id_bytes, bytes)
                    else str(task_id_bytes)
                )
                raw_payload = await redis.hget(RedisKeys.phoenix(task_id), "payload")  # type: ignore[misc]
                if raw_payload:
                    payload = json.loads(raw_payload)
                    all_tasks.append(
                        {
                            "task_id": task_id,
                            "worker_id": w_id,
                            "task_name": payload.get("task_name", "unknown"),
                            "queue": payload.get("queue", "default"),
                            "started_at": started_at,
                            "status": "RUNNING",
                        }
                    )
        else:
            # Worker is dead, clean up stale registration
            await redis.zrem(RedisKeys.workers(), w_id_bytes)

    # Sort tasks so the newest appear at the top
    all_tasks.sort(key=lambda x: x["started_at"], reverse=True)

    # ------------------------------------------------------------------
    p95_s: float | None = None
    try:
        raw = await redis.lrange(RedisKeys.task_durations(), 0, 999)  # type: ignore[misc]
        samples: list[float] = []
        for v in raw:
            try:
                s = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                samples.append(float(s))
            except Exception:
                continue

        if samples:
            samples.sort()
            idx = max(0, int(math.ceil(0.95 * len(samples))) - 1)
            p95_s = samples[idx]
    except Exception:
        p95_s = None

    # ------------------------------------------------------------------
    # Best-effort queue depth detection (Redis broker heuristics)
    # ------------------------------------------------------------------
    queue_depth_total = 0
    try:
        from relier.tasks.app import celery_app

        queue_objs = getattr(celery_app.conf, "task_queues", None)
        queue_names: list[str] = []
        if queue_objs:
            for q in queue_objs:
                try:
                    name = getattr(q, "name", str(q))
                except Exception:
                    name = str(q)
                queue_names.append(name)
        else:
            default_q = getattr(celery_app.conf, "task_default_queue", "default")
            queue_names = [default_q]

        async def _probe_queue(key: str) -> int:
            try:
                exists = await redis.exists(key)
                if not exists:
                    return 0
                t = await redis.type(key)
                if isinstance(t, bytes):
                    t = t.decode("utf-8")
                if t == "list":
                    return int(await redis.llen(key))  # type: ignore[misc]
                if t == "stream":
                    try:
                        return int(await redis.xlen(key))
                    except Exception:
                        return 0
                if t == "zset":
                    return int(await redis.zcard(key))
                if t == "set":
                    return int(await redis.scard(key))  # type: ignore[misc]
                if t == "hash":
                    return int(await redis.hlen(key))  # type: ignore[misc]
                try:
                    return int(await redis.llen(key))  # type: ignore[misc]
                except Exception:
                    return 0
            except Exception:
                return 0

        for qn in queue_names:
            candidates = []
            if qn == "default":
                candidates.append("celery")
            candidates += [
                f"celery:{qn}",
                qn,
                f"queue:{qn}",
                f"queues:{qn}",
                f"kombu:queue:{qn}",
                f"kombu:{qn}",
            ]
            depth = 0
            for key in candidates:
                depth = await _probe_queue(key)
                if depth:
                    break
            queue_depth_total += int(depth or 0)
    except Exception:
        queue_depth_total = 0

    # --- Use global counters (O(1)) instead of SCAN ---
    quarantine_count = await redis.hlen(RedisKeys.dlq_hash())  # type: ignore[misc]

    # Fetch TRUE global counters from Redis (all-time)
    cluster_completed_raw = await redis.get(RedisKeys.metric_global("success"))
    cluster_failed_raw = await redis.get(RedisKeys.metric_global("failed"))

    cluster_completed = int(cluster_completed_raw) if cluster_completed_raw else 0
    cluster_failed = int(cluster_failed_raw) if cluster_failed_raw else 0

    return {
        "tasks": all_tasks,
        "active_workers": active_workers,
        "worker_metrics": active_worker_metrics,  # Only active workers
        "p95": p95_s,
        "queue_depth": queue_depth_total,
        "stats": {
            "completed": cluster_completed,  # Global counter (all time)
            "failed": cluster_failed,  # Global counter (all time)
            "quarantined": int(quarantine_count),
        },
    }


@tasks_app.command(name="list")
@coro
async def list_tasks() -> None:
    """List all tasks currently executing across the cluster."""
    result = await _get_all_inflight_data()
    data = result.get("tasks", [])
    active_workers = result.get("active_workers", [])
    if not data and not active_workers:
        console.print(
            "\n[dim]No tasks currently in flight and no workers registered.[/dim]\n"
        )
        return
    console.print(
        render_inflight_table(
            result.get("tasks", []),
            result.get("active_workers", []),
            stats=result.get("stats", {"completed": 0, "failed": 0, "quarantined": 0}),
            queue_depth=result.get("queue_depth"),
            p95=result.get("p95"),
            worker_metrics=result.get("worker_metrics"),
        )
    )


@tasks_app.command(name="inflight")
@coro
async def inflight(
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Enable live-refreshing view (2s interval)."
    ),
    worker: str | None = typer.Option(
        None, "--worker", help="Filter tasks to a specific worker ID."
    ),
) -> None:
    """
    List all tasks currently executing across the cluster.
    """

    def _apply_worker_filter(r: dict[str, Any]) -> dict[str, Any]:
        if not worker:
            return r
        filtered = dict(r)
        filtered["tasks"] = [
            t for t in r.get("tasks", []) if t.get("worker_id") == worker
        ]
        filtered["active_workers"] = [
            w for w in r.get("active_workers", []) if w == worker
        ]
        filtered["worker_metrics"] = {
            k: v for k, v in (r.get("worker_metrics") or {}).items() if k == worker
        }
        return filtered

    result = _apply_worker_filter(await _get_all_inflight_data())

    if not follow:
        data = result.get("tasks", [])
        active_workers = result.get("active_workers", [])
        if not data and not active_workers:
            console.print(
                "\n[dim]No tasks currently in flight and no workers registered.[/dim]\n"
            )
            return
        console.print(
            render_inflight_table(
                result.get("tasks", []),
                result.get("active_workers", []),
                stats=result.get(
                    "stats", {"completed": 0, "failed": 0, "quarantined": 0}
                ),
                queue_depth=result.get("queue_depth"),
                p95=result.get("p95"),
                worker_metrics=result.get("worker_metrics"),
            )
        )
    else:
        # Live refresh mode for real-time monitoring.
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                result = _apply_worker_filter(await _get_all_inflight_data())

                live.update(
                    render_inflight_table(
                        tasks=result.get("tasks", []),
                        active_workers=result.get("active_workers", []),
                        stats=result.get(
                            "stats", {"completed": 0, "failed": 0, "quarantined": 0}
                        ),
                        queue_depth=result.get("queue_depth"),
                        p95=result.get("p95"),
                        worker_metrics=result.get("worker_metrics"),
                    )
                )
                await asyncio.sleep(1)


@tasks_app.command(name="top")
@coro
async def top() -> None:
    """Show a top-like view of task throughput and active workers."""
    result = await _get_all_inflight_data()
    data = result.get("tasks", [])
    active_workers = result.get("active_workers", [])
    stats = result.get("stats", {})
    worker_metrics = result.get("worker_metrics", {})

    console.print(f"\n[bold {PRIMARY_COLOR}]Task Top[/bold {PRIMARY_COLOR}]\n")
    console.print(f"Active Workers: {len(active_workers)}")
    console.print(f"In-Flight Tasks: {len(data)}")

    # Show cluster-wide success/failed
    completed = stats.get("completed", 0)
    failed = stats.get("failed", 0)
    total_processed = completed + failed

    if total_processed > 0:
        success_rate = (completed / total_processed) * 100
        console.print(
            f"Completed Tasks: {completed} ({success_rate:.1f}% success rate)"
        )
        console.print(f"Failed Tasks: {failed}")
    else:
        console.print(f"Completed Tasks: {completed}")
        console.print(f"Failed Tasks: {failed}")

    console.print(f"Queue Depth: {result.get('queue_depth', 'N/A')}")
    console.print(f"P95 Latency: {result.get('p95', 'N/A')}")

    # Show top 5 workers by throughput
    if worker_metrics:
        console.print(
            f"\n[bold {PRIMARY_COLOR}]Top Workers by Throughput[/bold {PRIMARY_COLOR}]"
        )
        sorted_workers = sorted(
            worker_metrics.items(),
            key=lambda x: x[1]["completed"] + x[1]["failed"],
            reverse=True,
        )[:5]

        for w_id, metrics in sorted_workers:
            w_completed = metrics["completed"]
            w_failed = metrics["failed"]
            w_total = w_completed + w_failed

            if w_total > 0:
                w_rate = (w_completed / w_total) * 100
                rate_color = (
                    "green" if w_rate >= 95 else "yellow" if w_rate >= 85 else "red"
                )
                console.print(
                    f"  {w_id}: [{rate_color}]{w_completed}[/{rate_color}] "
                    f"[dim]{w_failed}[/dim] [{rate_color}]({w_rate:.1f}%)[/{rate_color}]"
                )
            else:
                console.print(f"  {w_id}: [dim]no tasks yet[/dim]")

    console.print("")


@tasks_app.command(name="inspect")
@coro
async def inspect(
    task_id: str = typer.Argument(..., help="Task ID to inspect."),
) -> None:
    """Show the full payload, state, and metadata for a task."""
    redis = await get_relier_redis()

    phoenix_data = await redis.hgetall(RedisKeys.phoenix(task_id))  # type: ignore[misc]
    dlq_raw = await redis.hget(RedisKeys.dlq_hash(), task_id)  # type: ignore[misc]
    hb_alive = bool(await redis.exists(RedisKeys.heartbeat(task_id)))
    res_raw = await redis.get(RedisKeys.resurrection(task_id))
    res_count = int(res_raw) if res_raw else 0

    if not phoenix_data and not dlq_raw and not hb_alive:
        console.print(f"[bold red]No data found for task {task_id!r}.[/bold red]")
        raise typer.Exit(code=1)

    if hb_alive:
        status = "RUNNING"
    elif dlq_raw:
        status = "QUARANTINED"
    elif phoenix_data:
        status = "COMPLETED_OR_ORPHANED"
    else:
        status = "UNKNOWN"

    info: dict[str, Any] = {
        "task_id": task_id,
        "status": status,
        "resurrection_count": res_count,
    }

    if phoenix_data:
        payload_raw = phoenix_data.get("payload")
        if payload_raw:
            try:
                info["payload"] = json.loads(payload_raw)
            except Exception:
                info["payload"] = payload_raw
        extras = {k: v for k, v in phoenix_data.items() if k != "payload"}
        if extras:
            info["phoenix"] = extras

    if dlq_raw:
        info["dlq"] = json.loads(dlq_raw)

    console.print(
        f"\n[bold {PRIMARY_COLOR}]Task Details:[/bold {PRIMARY_COLOR}] {task_id}\n"
    )
    syntax = Syntax(
        json.dumps(info, indent=2), "json", theme="ansi_dark", line_numbers=True
    )
    console.print(syntax)
    console.print("")


@tasks_app.command(name="retry")
@coro
async def retry(
    task_id: str = typer.Argument(..., help="Task ID to retry."),
) -> None:
    """Re-queue a failed or quarantined task by ID.

    Checks the DLQ first; falls back to the Phoenix payload for orphaned tasks.
    """
    from relier.core.dlq import DeadLetterQueue
    from relier.tasks.app import celery_app

    redis = await get_relier_redis()

    # Prefer DLQ release path so resurrection history is preserved correctly.
    dlq_raw = await redis.hget(RedisKeys.dlq_hash(), task_id)  # type: ignore[misc]
    if dlq_raw:
        ok = await DeadLetterQueue.release(task_id)
        if ok:
            console.print(
                f"[bold green]Task {task_id} released from DLQ and re-queued.[/bold green]"
            )
        else:
            console.print(
                f"[bold red]Failed to release task {task_id} from DLQ.[/bold red]"
            )
            raise typer.Exit(code=1)
        return

    # Fall back to Phoenix payload for orphaned tasks.
    payload_raw = await redis.hget(RedisKeys.phoenix(task_id), "payload")  # type: ignore[misc]
    if not payload_raw:
        console.print(
            f"[bold red]No payload found for task {task_id!r}. "
            "Is it in the DLQ? Try `rl dlq list`.[/bold red]"
        )
        raise typer.Exit(code=1)

    payload = json.loads(payload_raw)
    task_name = payload.get("task_name")
    if not task_name:
        console.print(
            f"[bold red]Cannot retry {task_id!r}: payload has no task_name.[/bold red]"
        )
        raise typer.Exit(code=1)

    queue = payload.get("queue", "default")
    celery_app.send_task(
        task_name,
        args=payload.get("args", []),
        kwargs=payload.get("kwargs", {}),
        queue=queue,
        task_id=task_id,
    )
    console.print(f"[bold green]Task {task_id} re-queued to {queue!r}.[/bold green]")


@tasks_app.command(name="cancel")
@coro
async def cancel(
    task_id: str = typer.Argument(..., help="Task ID to cancel."),
    terminate: bool = typer.Option(
        True,
        "--terminate/--no-terminate",
        help="Send SIGTERM to the running task process.",
    ),
) -> None:
    """Revoke and cancel a running or queued task."""
    from relier.tasks.app import celery_app

    celery_app.control.revoke(task_id, terminate=terminate, signal="SIGTERM")
    console.print(f"[bold green]Cancel signal sent to task {task_id}.[/bold green]")
    if terminate:
        console.print("[dim]The running task will receive SIGTERM.[/dim]")
    else:
        console.print(
            "[dim]Task is marked revoked and will not start if still queued.[/dim]"
        )


@tasks_app.command(name="logs")
@coro
async def logs(
    task_id: str = typer.Argument(..., help="Task ID to follow."),
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Poll for state changes every 2 seconds."
    ),
) -> None:
    """Stream state changes and metadata for a task.

    Full log aggregation (stdout/stderr) requires a log backend such as
    Loki or Elasticsearch. This command shows task state transitions from
    Redis.
    """

    async def _print_state() -> bool:
        """Print current task state. Returns True if the task is still running."""
        redis = await get_relier_redis()
        hb_alive = bool(await redis.exists(RedisKeys.heartbeat(task_id)))
        phoenix_data = await redis.hgetall(RedisKeys.phoenix(task_id))  # type: ignore[misc]
        dlq_raw = await redis.hget(RedisKeys.dlq_hash(), task_id)  # type: ignore[misc]
        res_raw = await redis.get(RedisKeys.resurrection(task_id))
        res_count = int(res_raw) if res_raw else 0

        ts = datetime.now().strftime("%H:%M:%S")

        if hb_alive:
            status = "[bold green]RUNNING[/bold green]"
            is_running = True
        elif dlq_raw:
            status = "[bold red]QUARANTINED[/bold red]"
            is_running = False
        elif phoenix_data:
            status = "[dim]COMPLETED[/dim]"
            is_running = False
        else:
            status = "[dim]NOT FOUND[/dim]"
            is_running = False

        task_name = "unknown"
        if phoenix_data and phoenix_data.get("payload"):
            with contextlib.suppress(Exception):
                task_name = json.loads(phoenix_data["payload"]).get(
                    "task_name", "unknown"
                )

        console.print(
            f"[dim]{ts}[/dim]  task_id={task_id}  "
            f"task={task_name}  status={status}  resurrections={res_count}"
        )
        return is_running

    if not follow:
        await _print_state()
        return

    console.print(f"[dim]Following task {task_id} (Ctrl+C to stop)...[/dim]\n")
    try:
        while True:
            still_running = await _print_state()
            if not still_running:
                break
            await asyncio.sleep(2.0)
    except KeyboardInterrupt:
        pass
