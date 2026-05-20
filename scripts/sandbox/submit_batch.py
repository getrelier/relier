"""
Relier batch task submission script.

Dispatches a mix of succeeding and failing tasks directly via the rl_task
push() API so Grafana dashboards have real data to display.

Usage (inside a worker container):

    docker compose exec worker-default python /app/scripts/sandbox/submit_batch.py
    docker compose exec worker-default python /app/scripts/sandbox/submit_batch.py --count 50 --failures 10

Or run locally if relier is installed and RELIER_REDIS_URL points at your broker:

    RELIER_REDIS_URL=redis://localhost:6379/0 python scripts/sandbox/submit_batch.py
"""

import argparse
import asyncio
import time


async def main(count: int, failures: int) -> None:
    # Import celery_app first so shared_task decorators bind to the Redis-backed
    # Celery app rather than the default AMQP app.
    import relier.tasks.app  # noqa: F401
    from relier.tasks.debug import failing_task, increment_task

    total = count + failures
    print(
        f"Submitting {count} increment tasks + {failures} failing tasks = {total} total"
    )

    start = time.perf_counter()
    submitted = 0
    errors = 0

    # Dispatch succeeding tasks.
    for i in range(count):
        try:
            await increment_task.apush(key=f"batch-{i}")
            submitted += 1
        except Exception as exc:
            print(f"  [warn] increment_task {i} dispatch failed: {exc}")
            errors += 1

    # Dispatch intentionally failing tasks to exercise the error-budget panels.
    for i in range(failures):
        try:
            await failing_task.apush()
            submitted += 1
        except Exception as exc:
            print(f"  [warn] failing_task {i} dispatch failed: {exc}")
            errors += 1

    elapsed = time.perf_counter() - start
    print(f"Done — {submitted}/{total} dispatched in {elapsed:.2f}s ({errors} errors)")
    print()
    print("Grafana will show data within ~30s as Prometheus scrapes the metrics.")
    print("  Overview:  http://localhost:3000/d/relier-overview")
    print("  SLO:       http://localhost:3000/d/relier-slo")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit a batch of Relier tasks.")
    parser.add_argument(
        "--count", type=int, default=30, help="Number of succeeding tasks"
    )
    parser.add_argument(
        "--failures", type=int, default=5, help="Number of failing tasks"
    )
    args = parser.parse_args()
    asyncio.run(main(args.count, args.failures))
