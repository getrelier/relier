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

# ── Scale profile ─────────────────────────────────────────────────────────────
# "standard" keeps every test fast. "scale" raises the sample size on *every*
# test — not just delivery — so p99s, dedup rates, and recovery counts all have
# real N behind them. Set BENCH_SCALE=scale or pass --scale (which also implies
# --synthetic, since high volume is only feasible with sleep tasks).
# Individual BENCH_* env vars below still override the profile value.
SCALE = os.getenv("BENCH_SCALE", "standard").strip().lower()
_IS_SCALE = SCALE == "scale"


def _resolve(env_var: str, *, ollama: int, synthetic: int, scale: int) -> int:
    """Resolve a scale knob.

    Precedence: explicit ``BENCH_*`` env override > profile value. The profile
    value is the Ollama default unless synthetic mode is on, in which case it is
    the synthetic or scale value depending on ``BENCH_SCALE``.
    """
    raw = os.getenv(env_var)
    if raw not in (None, ""):
        return int(raw)
    if not SYNTHETIC:
        return ollama
    return scale if _IS_SCALE else synthetic


# Synthetic task duration. Scale mode drops to 0.05s so 10k+ task batches drain
# in a reasonable window; standard synthetic stays at 0.5s for readable timing.
SYNTHETIC_TASK_SLEEP_S = float(
    os.getenv("BENCH_SYNTHETIC_SLEEP") or ("0.05" if _IS_SCALE else "0.5")
)

# ── Worker startup ────────────────────────────────────────────────────────────
WORKER_BOOT_WAIT = 5 if SYNTHETIC else 8

# ── Dispatch overhead test (Test 1) ───────────────────────────────────────────
OVERHEAD_SAMPLES = _resolve(
    "BENCH_OVERHEAD_SAMPLES", ollama=200, synthetic=200, scale=2000
)

# ── Delivery rate test (Test 5) ───────────────────────────────────────────────
# N tasks dispatched, DELIVERY_KILL_CYCLES sequential worker SIGKILLs.
# Relier recovers every in-flight task; vanilla loses one per kill.
BATCH_SIZE = _resolve("BENCH_BATCH_SIZE", ollama=30, synthetic=500, scale=10000)
DELIVERY_KILL_CYCLES = _resolve(
    "BENCH_DELIVERY_KILL_CYCLES", ollama=1, synthetic=5, scale=10
)
WORK_S = SYNTHETIC_TASK_SLEEP_S if SYNTHETIC else 8.0

# ── OOM recovery test (Test 4) ────────────────────────────────────────────────
# Repeated kill/resurrect cycles: reports avg and p99 resurrection time.
OOM_CYCLES = _resolve("BENCH_OOM_CYCLES", ollama=1, synthetic=5, scale=20)
OOM_PROBE_S = 8.0 if SYNTHETIC else 50.0  # How long the OOM probe task runs
OOM_KILL_WAIT = 4 if SYNTHETIC else 8  # Seconds to wait before SIGKILL

# ── Idempotency test (Test 3) ─────────────────────────────────────────────────
IDEMPOTENCY_SUBMISSIONS = _resolve(
    "BENCH_IDEMPOTENCY_SUBMISSIONS", ollama=10, synthetic=50, scale=2000
)
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

# ── Admission control test (Test 2) ───────────────────────────────────────────
ADMISSION_SAMPLES = _resolve(
    "BENCH_ADMISSION_SAMPLES", ollama=1000, synthetic=5000, scale=50000
)

# ── Graceful shutdown test (Test 6) ───────────────────────────────────────────
SHUTDOWN_TASKS = _resolve("BENCH_SHUTDOWN_TASKS", ollama=15, synthetic=20, scale=200)
SHUTDOWN_CYCLES = _resolve("BENCH_SHUTDOWN_CYCLES", ollama=1, synthetic=3, scale=5)
SHUTDOWN_WORK_S = SYNTHETIC_TASK_SLEEP_S if SYNTHETIC else 10.0

# ── Resource overhead test (Test 7) ───────────────────────────────────────────
RESOURCE_PROBE_S = max(SYNTHETIC_TASK_SLEEP_S * 6, 3.0) if SYNTHETIC else 10.0
RESOURCE_SAMPLE_WAIT = max(2, int(RESOURCE_PROBE_S / 2)) if SYNTHETIC else 4

# ── Cold-start latency test (Test 8) ──────────────────────────────────────────
COLD_START_TRIALS = _resolve("BENCH_COLD_START_TRIALS", ollama=3, synthetic=3, scale=10)

# ── Steady-state Redis ops/sec (Test 7 sub-test) ─────────────────────────────
# Measurement window; 60 s is long enough to average out bursts.
REDIS_OPS_MEASURE_S = int(os.getenv("BENCH_OPS_MEASURE_S", "60"))

# ── Resurrection under load + steady-state ops (Tests 7 & 9) ─────────────────
# Solo-pool workers spawned for the load tests; each executes exactly 1 task,
# so this is the true concurrent-inflight count during measurement. This knob
# is process-bound (one OS worker process per unit), so scale tops out at 25
# rather than the thousands used for loop-based knobs — raise via the env var
# only if the host can sustain that many concurrent Celery workers.
PHOENIX_LOAD_WORKERS = _resolve(
    "BENCH_PHOENIX_LOAD_WORKERS", ollama=20, synthetic=5, scale=25
)
