"""
Relier CLI — Task Management.

Commands for monitoring in-flight tasks and managing the task lifecycle.
"""

import asyncio
import json
import math
from typing import Any, Dict, List

import typer
from rich.live import Live
from rich.align import Align

from relier.cli.base import console
from relier.cli.ui.tables import render_inflight_table
from relier.cli.utils import coro, PRIMARY_COLOR
from relier.storage.redis import get_relier_redis

tasks_app = typer.Typer(help="Manage and monitor in-flight tasks.")


async def _get_all_inflight_data() -> Dict[str, Any]:
    """FIXED: Uses O(1) global counters instead of SCAN."""
    redis = await get_relier_redis()
    all_registered_workers = await redis.smembers("rl:workers")
    active_workers: List[str] = []
    all_tasks: List[Dict[str, Any]] = []

    # Per-worker metrics for ACTIVE workers only (for display)
    active_worker_metrics: Dict[str, Dict[str, int]] = {}

    # --- 1. Fetch Active Tasks + Active Worker Metrics ---
    for w_id_bytes in all_registered_workers:
        w_id = (
            w_id_bytes.decode("utf-8")
            if isinstance(w_id_bytes, bytes)
            else str(w_id_bytes)
        )

        # Check if worker is alive
        is_alive = await redis.exists(f"rl:presence:{w_id}")

        if is_alive:
            active_workers.append(w_id)

            # Fetch metrics for display (only for active workers)
            completed_raw = await redis.get(f"rl:worker:{w_id}:completed")
            failed_raw = await redis.get(f"rl:worker:{w_id}:failed")

            active_worker_metrics[w_id] = {
                "completed": int(completed_raw) if completed_raw else 0,
                "failed": int(failed_raw) if failed_raw else 0,
            }

            # Get tasks currently being processed by this worker
            inflight_tasks = await redis.zrange(
                f"rl:inflight:{w_id}", 0, -1, withscores=True
            )
            for task_id_bytes, started_at in inflight_tasks:
                task_id = (
                    task_id_bytes.decode()
                    if isinstance(task_id_bytes, bytes)
                    else str(task_id_bytes)
                )
                raw_payload = await redis.hget(f"rl:phoenix:{task_id}", "payload")
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
            await redis.srem("rl:workers", w_id_bytes)

    # Sort tasks so the newest appear at the top
    all_tasks.sort(key=lambda x: x["started_at"], reverse=True)

    # ------------------------------------------------------------------
    # Compute p95 from recent duration samples stored in Redis
    # ------------------------------------------------------------------
    p95_s: float | None = None
    try:
        raw = await redis.lrange("rl:task_durations", 0, 999)
        samples: List[float] = []
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
        queue_names: List[str] = []
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
                    return int(await redis.llen(key))
                if t == "stream":
                    try:
                        return int(await redis.xlen(key))
                    except Exception:
                        return 0
                if t == "zset":
                    return int(await redis.zcard(key))
                if t == "set":
                    return int(await redis.scard(key))
                if t == "hash":
                    return int(await redis.hlen(key))
                try:
                    return int(await redis.llen(key))
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

    # --- 4. FIXED: Use global counters (O(1)) instead of SCAN ---
    quarantine_count = await redis.hlen("rl:dlq")

    # These are global counters incremented on actual completion
    cluster_completed = sum(
        m.get("completed", 0) for m in active_worker_metrics.values()
    )
    cluster_failed = sum(m.get("failed", 0) for m in active_worker_metrics.values())

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
            worker_metrics=result.get("worker_metrics"),  # NEW: Add this parameter
        )
    )


@tasks_app.command(name="inflight")
@coro
async def inflight(
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Enable live-refreshing view (2s interval)."
    ),
) -> None:
    """
    List all tasks currently executing across the cluster.
    """
    result = await _get_all_inflight_data()

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
                worker_metrics=result.get("worker_metrics"),  # NEW: Add this parameter
            )
        )
    else:
        # Live refresh mode for real-time monitoring.
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                result = await _get_all_inflight_data()

                live.update(
                    render_inflight_table(
                        tasks=result.get("tasks", []),
                        active_workers=result.get("active_workers", []),
                        stats=result.get(
                            "stats", {"completed": 0, "failed": 0, "quarantined": 0}
                        ),
                        queue_depth=result.get("queue_depth"),
                        p95=result.get("p95"),
                        worker_metrics=result.get(
                            "worker_metrics"
                        ),  # NEW: Add this parameter
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

    # NEW: Show cluster-wide success/failed
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

    # NEW: Show top 5 workers by throughput
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
                    f"  {w_id}: [{rate_color}]{w_completed}✓[/{rate_color}] "
                    f"[dim]{w_failed}✗[/dim] [{rate_color}]({w_rate:.1f}%)[/{rate_color}]"
                )
            else:
                console.print(f"  {w_id}: [dim]no tasks yet[/dim]")

    console.print("")
