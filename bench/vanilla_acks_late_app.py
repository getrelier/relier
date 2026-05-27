"""
Vanilla Celery app with ``task_acks_late=True``.

This is the "what if I just flip the flag?" comparison — the most obvious
objection to Relier on HN. The bench wires this third app into Test 5
(delivery rate under crash) to show what that single config change actually
buys you, and what it doesn't.

What flipping ``task_acks_late=True`` gives you:
  - Broker re-delivery when a worker is killed mid-task. Tasks are NOT lost.

What it does NOT give you (and Relier does):
  - Idempotency. The redelivered task runs again — and again, and again —
    every time it gets killed. With no dedup, a task that takes 10 s and
    gets killed 5 times will execute 6 times. If that task charges a
    credit card or writes to an external API, you have a duplicate-action
    problem instead of a lost-task problem.
  - Fence tokens. Two workers racing on the redelivered message can both
    complete it.
  - Graceful shutdown drain. SIGTERM still kills the in-flight task.
  - DLQ with payload, admission control, SLO tracking, structured
    observability.

The delivery_probe_acks_late task below counts both COMPLETIONS and
EXECUTIONS so Test 5 can report duplicates.

Start with:
    celery -A bench.vanilla_acks_late_app worker -Q vanilla_acks_late -l warning
"""

import time

import redis as redis_lib
from celery import Celery

from bench.config import (
    BENCH_NS,
    REDIS_URL,
)

app = Celery(
    "vanilla_acks_late_bench",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["bench.vanilla_acks_late_app"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_default_queue="vanilla_acks_late",
    # ── Vanilla with ack_late=True (broker redelivery on crash, no dedup) ─
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
)

_r = redis_lib.from_url(REDIS_URL, decode_responses=True)


@app.task
def delivery_probe_acks_late(task_key: str, work_s: float = 3.0) -> dict:
    """
    Delivery-rate probe under ``task_acks_late=True``.

    Records two distinct counters:

    - ``exec_count`` — incremented at the START of every execution.
      Counts how many times this specific task_key ran, including
      duplicates caused by broker re-delivery.
    - ``delivery_done`` — RPUSHed once per successful completion.
      A completion happens only if the worker doesn't die before sleeping
      through ``work_s``.

    Test 5 compares total executions (sum of ``exec_count`` across all
    task_keys) against unique task_keys to detect duplicates.
    """
    # Per-task execution counter. A duplicate execution increments this past 1.
    _r.hincrby(f"{BENCH_NS}:vanilla_acks_late:exec_count", task_key, 1)
    _r.incr(f"{BENCH_NS}:vanilla_acks_late:total_exec")

    time.sleep(work_s)

    _r.rpush(f"{BENCH_NS}:vanilla_acks_late:delivery_done", task_key)
    return {"task_key": task_key}
