import asyncio
import os

# Admission control: at most 100 dispatches per 10-second window cluster-wide.
# AdmissionRejectedError is raised on the producer side, never inside the task.
os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")
os.environ.setdefault("RELIER_ADMISSION_LIMIT", "100")
os.environ.setdefault("RELIER_ADMISSION_WINDOW", "10")

from relier import rl_task


@rl_task(queue="default")
async def send_notification(user_id: str, message: str) -> dict:
    await asyncio.sleep(0.05)
    return {"user_id": user_id, "sent": True}


@rl_task(queue="high_priority")
async def send_alert(user_id: str, alert_type: str) -> dict:
    await asyncio.sleep(0.02)
    return {"user_id": user_id, "alert_type": alert_type, "sent": True}
