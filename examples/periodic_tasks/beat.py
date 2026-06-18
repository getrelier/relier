import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

import tasks  # noqa: F401 — registers tasks and configures beat_schedule

from relier.tasks.app import celery_app

if __name__ == "__main__":
    # Runs the beat scheduler in-process.
    # Equivalent CLI:  celery -A tasks beat --loglevel=info
    #
    # Beat must run as a single instance — never scale it horizontally.
    # Run one beat + N workers:
    #   celery -A tasks worker --loglevel=info &
    #   celery -A tasks beat   --loglevel=info
    celery_app.Beat(loglevel="info").run()
