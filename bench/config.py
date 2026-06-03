"""Shared benchmark configuration."""

import os

REDIS_URL = os.getenv("RELIER_REDIS_URL", "redis://localhost:6379/0")
WORKER_CONCURRENCY = int(os.getenv("BENCH_WORKER_CONCURRENCY", "4"))
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = "nomic-embed-text"
GEN_MODEL = "gemma3:4b"

# Redis key prefix for bench counters: never clashes with rl: namespace
BENCH_NS = "bench"

# ── Synthetic mode ────────────────────────────────────────────────────────────
# Replace Ollama calls with asyncio.sleep for fast, high-volume tests.
# Set BENCH_SYNTHETIC=1 in env or pass --synthetic on the CLI.
SYNTHETIC = bool(os.getenv("BENCH_SYNTHETIC", ""))
SYNTHETIC_TASK_SLEEP_S = float(os.getenv("BENCH_SYNTHETIC_SLEEP", "0.5"))

# ── Worker startup ────────────────────────────────────────────────────────────
WORKER_BOOT_WAIT = 5 if SYNTHETIC else 8

# ── Delivery rate test ────────────────────────────────────────────────────────
# N tasks dispatched, DELIVERY_KILL_CYCLES sequential worker SIGKILLs.
# Relier recovers every in-flight task; vanilla loses one per kill.
BATCH_SIZE = int(os.getenv("BENCH_BATCH_SIZE", "500" if SYNTHETIC else "30"))
DELIVERY_KILL_CYCLES = 5 if SYNTHETIC else 1
WORK_S = SYNTHETIC_TASK_SLEEP_S if SYNTHETIC else 8.0

# ── OOM recovery test ─────────────────────────────────────────────────────────
# Repeated kill/resurrect cycles: reports avg and p99 resurrection time.
OOM_CYCLES = 5 if SYNTHETIC else 1
OOM_PROBE_S = 8.0 if SYNTHETIC else 50.0  # How long the OOM probe task runs
OOM_KILL_WAIT = 4 if SYNTHETIC else 8  # Seconds to wait before SIGKILL

# ── Idempotency test ──────────────────────────────────────────────────────────
IDEMPOTENCY_SUBMISSIONS = 50 if SYNTHETIC else 10
# Relier wait: boot + a few task durations
IDEMPOTENCY_RELIER_WAIT_S = (
    int(WORKER_BOOT_WAIT + SYNTHETIC_TASK_SLEEP_S * 5 + 5) if SYNTHETIC else 45
)
# Vanilla wait: boot + all N tasks execute serially
IDEMPOTENCY_VANILLA_WAIT_S = (
    int(WORKER_BOOT_WAIT + IDEMPOTENCY_SUBMISSIONS * SYNTHETIC_TASK_SLEEP_S + 15)
    if SYNTHETIC
    else 120
)

# ── Admission control test ────────────────────────────────────────────────────
ADMISSION_SAMPLES = 5000 if SYNTHETIC else 1000

# ── Graceful shutdown test ────────────────────────────────────────────────────
SHUTDOWN_TASKS = 20 if SYNTHETIC else 15
SHUTDOWN_CYCLES = 3 if SYNTHETIC else 1
SHUTDOWN_WORK_S = SYNTHETIC_TASK_SLEEP_S if SYNTHETIC else 10.0

# ── Resource overhead test ────────────────────────────────────────────────────
RESOURCE_PROBE_S = max(SYNTHETIC_TASK_SLEEP_S * 6, 3.0) if SYNTHETIC else 10.0
RESOURCE_SAMPLE_WAIT = max(2, int(RESOURCE_PROBE_S / 2)) if SYNTHETIC else 4

# ── Steady-state Redis ops/sec (Test 7 sub-test) ─────────────────────────────
# Measurement window; 60 s is long enough to average out bursts.
REDIS_OPS_MEASURE_S = int(os.getenv("BENCH_OPS_MEASURE_S", "60"))

# ── Resurrection under load + steady-state ops (Tests 7 & 9) ─────────────────
# Solo-pool workers spawned for the load tests; each executes exactly 1 task,
# so this is the true concurrent-inflight count during measurement.
PHOENIX_LOAD_WORKERS = int(
    os.getenv("BENCH_PHOENIX_LOAD_WORKERS", "5" if SYNTHETIC else "20")
)
