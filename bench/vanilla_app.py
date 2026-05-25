"""
Vanilla Celery app: no Relier reliability stack.

This is a stock Celery 5 app with default settings:
  - task_acks_late=False  → tasks acknowledged immediately on pickup;
                            if the worker dies the task is gone.
  - task_reject_on_worker_lost=False → no automatic re-queue on crash.
  - No heartbeat / no Phoenix / no idempotency / no admission control.

Start with:
    celery -A bench.vanilla_app worker -Q vanilla -l warning
"""

import time

import redis as redis_lib
from celery import Celery

from bench.config import (
    BENCH_NS,
    REDIS_URL,
    SYNTHETIC,
    SYNTHETIC_TASK_SLEEP_S,
)

app = Celery(
    "vanilla_bench",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["bench.vanilla_app"],  # self-contained: tasks defined below
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_default_queue="vanilla",
    # ── Vanilla defaults (no reliability) ───────────────────────────────
    task_acks_late=False,  # ACK on pickup → task lost if worker dies
    task_reject_on_worker_lost=False,
    worker_prefetch_multiplier=1,
    task_track_started=True,
)

_r = redis_lib.from_url(REDIS_URL, decode_responses=True)


# ── Vanilla tasks ────────────────────────────────────────────────────────────


@app.task
def embed_document_vanilla(doc_id: str, text: str) -> dict:
    """Vanilla embed: no dedup, no Phoenix, no graceful shutdown."""
    if SYNTHETIC:
        import time as _t

        _t.sleep(SYNTHETIC_TASK_SLEEP_S)
        dim = 768
    else:
        from bench.ollama_client import embed_sync

        dim = len(embed_sync(text))
    _r.incr(f"{BENCH_NS}:vanilla:embed_exec")
    return {"doc_id": doc_id, "dim": dim, "status": "ok"}


@app.task
def fast_noop_vanilla(task_key: str) -> dict:
    """Minimal vanilla task for overhead comparison."""
    _r.incr(f"{BENCH_NS}:vanilla:noop_exec")
    _r.rpush(f"{BENCH_NS}:vanilla:noop_done", task_key)
    return {"task_key": task_key}


@app.task
def long_running_vanilla(task_key: str, target_s: float = 60.0) -> dict:
    """
    Long-running vanilla task: no checkpoints, no resurrection.

    If the worker is killed mid-run, this task is lost forever.
    """
    elapsed = 0.0
    while elapsed < target_s:
        time.sleep(1)
        elapsed += 1

    _r.incr(f"{BENCH_NS}:vanilla:long_exec")
    _r.rpush(f"{BENCH_NS}:vanilla:long_done", task_key)
    return {"task_key": task_key, "elapsed_s": elapsed}


@app.task
def delivery_probe_vanilla(task_key: str, work_s: float = 3.0) -> dict:
    """Delivery-rate probe -- vanilla version."""
    time.sleep(work_s)
    _r.rpush(f"{BENCH_NS}:vanilla:delivery_done", task_key)
    return {"task_key": task_key}


@app.task
def oom_probe_vanilla(task_key: str, target_s: float = 50.0) -> dict:
    """OOM recovery probe -- vanilla version. Task is lost when worker dies."""
    elapsed = 0
    while elapsed < target_s:
        time.sleep(1)
        elapsed += 1
    _r.incr(f"{BENCH_NS}:vanilla:oom_exec")
    _r.rpush(f"{BENCH_NS}:vanilla:oom_done", task_key)
    return {"task_key": task_key, "elapsed_s": elapsed}
