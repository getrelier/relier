"""
Relier benchmark tasks: written exactly as the docs prescribe.

All tasks record execution counts to Redis under the bench: namespace
so the benchmark runner can verify delivery rate and dedup guarantees.

In synthetic mode (BENCH_SYNTHETIC=1) the Ollama calls are replaced
with asyncio.sleep so the full reliability stack is exercised without
a GPU or model download.
"""

import asyncio

import redis as redis_lib

from bench.config import (
    BENCH_NS,
    REDIS_URL,
    SYNTHETIC,
    SYNTHETIC_TASK_SLEEP_S,
)

# ── Relier public API ────────────────────────────────────────────────────────
from relier.tasks.context import TaskContext
from relier.tasks.decorator import rl_task

_r = redis_lib.from_url(REDIS_URL, decode_responses=True)


# ── 1. Embedding task (idempotent) ──────────────────────────────────────────
@rl_task(
    queue="default",
    idempotent=True,
    idempotency_ttl=3600,
    soft_timeout=90,
    hard_timeout=100,
)
async def embed_document(doc_id: str, text: str) -> dict:
    """Embed a document chunk. Idempotent: same doc_id runs exactly once."""
    if SYNTHETIC:
        await asyncio.sleep(SYNTHETIC_TASK_SLEEP_S)
        dim = 768  # nomic-embed-text output dimension
    else:
        from bench.ollama_client import embed

        vector = await embed(text)
        dim = len(vector)
    _r.incr(f"{BENCH_NS}:relier:embed_exec")
    return {"doc_id": doc_id, "dim": dim, "status": "ok"}


# ── 2. Summarisation task (idempotent) ──────────────────────────────────────
@rl_task(
    queue="default",
    idempotent=True,
    idempotency_ttl=3600,
    soft_timeout=85,
    hard_timeout=110,
)
async def summarise_text(item_key: str, text: str) -> dict:
    """Summarise a document. Idempotent per item_key."""
    if SYNTHETIC:
        await asyncio.sleep(SYNTHETIC_TASK_SLEEP_S)
        summary = f"[synthetic summary of {item_key}]"
    else:
        from bench.ollama_client import generate

        summary = await generate(f"Summarise in one sentence: {text}", max_tokens=60)
    _r.incr(f"{BENCH_NS}:relier:summary_exec")
    return {"item_key": item_key, "summary": summary[:200]}


# ── 3. Fast no-op task (overhead measurement) ────────────────────────────────
@rl_task(queue="default")
async def fast_noop(task_key: str) -> dict:
    """Minimal task: measures Relier machinery overhead, not AI latency."""
    _r.incr(f"{BENCH_NS}:relier:noop_exec")
    _r.rpush(f"{BENCH_NS}:relier:noop_done", task_key)
    return {"task_key": task_key}


# ── 4. Long-running task (OOM recovery + graceful shutdown tests) ───────────
async def _save_progress(ctx: TaskContext) -> None:
    """Soft-timeout hook: persist progress so the next incarnation can resume."""
    last = ctx.metadata.get("elapsed_s", 0)
    if last:
        await ctx.set_partial({"elapsed_s": last})


@rl_task(
    queue="default",
    soft_timeout=25,
    hard_timeout=30,
    on_soft_timeout=_save_progress,
)
async def long_running(
    task_key: str, target_s: float = 60.0, ctx: TaskContext = None
) -> dict:
    """
    Runs for *target_s* seconds in 1-second increments with periodic checkpoints.
    """
    elapsed = (ctx.partial_result or {}).get("elapsed_s", 0) if ctx else 0
    count = 0
    while elapsed < target_s:
        await asyncio.sleep(1)
        elapsed += 1
        count += 1
        if ctx:
            ctx.metadata["elapsed_s"] = elapsed
        if count % 10 == 0 and ctx:
            await ctx.set_partial({"elapsed_s": elapsed})
    _r.incr(f"{BENCH_NS}:relier:long_exec")
    _r.rpush(f"{BENCH_NS}:relier:long_done", task_key)
    return {"task_key": task_key, "elapsed_s": elapsed}


# ── 5. Delivery-rate task ────────────────────────────────────────────────────
@rl_task(queue="default")
async def delivery_probe(task_key: str, work_s: float = 3.0) -> dict:
    """
    Sleeps *work_s* seconds then marks completion in Redis.
    Used by delivery-rate and graceful-shutdown tests.
    """
    await asyncio.sleep(work_s)
    _r.rpush(f"{BENCH_NS}:relier:delivery_done", task_key)
    return {"task_key": task_key}


# ── 6. OOM probe (no timeout: used specifically for OOM recovery test) ───────
@rl_task(queue="default")
async def oom_probe(task_key: str, target_s: float = 50.0) -> dict:
    """Runs for *target_s* s with no timeout. Used ONLY by the OOM recovery test."""
    _r.rpush(f"{BENCH_NS}:relier:oom_started", task_key)
    elapsed = 0
    while elapsed < target_s:
        await asyncio.sleep(1)
        elapsed += 1
    _r.incr(f"{BENCH_NS}:relier:oom_exec")
    _r.rpush(f"{BENCH_NS}:relier:oom_done", task_key)
    return {"task_key": task_key, "elapsed_s": elapsed}


# ── 7. Phoenix-load probe (resurrection-under-load + steady-state ops tests) ─
@rl_task(queue="default")
async def phoenix_load_probe(task_key: str, target_s: float = 50.0) -> dict:
    """Long-running probe used by Test 7 (ops/sec) and Test 9 (resurrection under load)."""
    _r.rpush(f"{BENCH_NS}:relier:phoenix_load_started", task_key)
    elapsed = 0
    while elapsed < target_s:
        await asyncio.sleep(1)
        elapsed += 1
    _r.rpush(f"{BENCH_NS}:relier:phoenix_load_done", task_key)
    return {"task_key": task_key, "elapsed_s": elapsed}
