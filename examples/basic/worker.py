import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

import producer  # noqa: F401 — registers the @rl_task-decorated function

from relier.tasks.app import celery_app

if __name__ == "__main__":
    celery_app.worker_main(
        [
            "worker",
            "--loglevel=info",
            "--concurrency=4",
            "--queues=default,high_priority,low_priority",
        ]
    )
