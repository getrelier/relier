import os
from datetime import timedelta

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from relier import rl_task
from relier.tasks.app import celery_app


@rl_task(queue="default")
async def cleanup_expired_sessions() -> dict:
    # In production: DELETE FROM sessions WHERE expires_at < NOW()
    deleted = 0  # placeholder
    return {"deleted": deleted}


@rl_task(queue="low_priority")
async def send_digest_emails(cohort: str = "all") -> dict:
    sent = 0  # placeholder: query users, send emails
    return {"cohort": cohort, "sent": sent}


@rl_task(queue="default")
async def refresh_leaderboard() -> dict:
    updated = 0  # placeholder: recompute scores
    return {"updated": updated}


# Celery Beat schedule — tasks fire on the given interval without a message producer.
# Run beat alongside workers:  celery -A tasks beat --loglevel=info
celery_app.conf.beat_schedule = {
    "cleanup-sessions-hourly": {
        "task": cleanup_expired_sessions.name,
        "schedule": timedelta(hours=1),
    },
    "send-daily-digest": {
        "task": send_digest_emails.name,
        "schedule": timedelta(days=1),
        "kwargs": {"cohort": "all"},
    },
    "refresh-leaderboard-every-5m": {
        "task": refresh_leaderboard.name,
        "schedule": timedelta(minutes=5),
    },
}
