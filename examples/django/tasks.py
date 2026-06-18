"""
Relier tasks for a Django application.

Add to your Django settings:
    RELIER_REDIS_URL = "redis://localhost:6379/0"
    RELIER_SECRET_KEY = "change-me"

Or export as environment variables before running manage.py / gunicorn.
"""

import asyncio

from relier import rl_task


@rl_task(queue="default", idempotent=True)
async def resize_avatar(user_id: int, image_path: str) -> dict:
    await asyncio.sleep(0.2)  # replace with Pillow resize + S3 upload
    return {"user_id": user_id, "status": "resized"}


@rl_task(queue="low_priority")
async def export_user_data(user_id: int) -> dict:
    await asyncio.sleep(3.0)  # replace with GDPR export logic
    return {"user_id": user_id, "status": "exported"}


@rl_task(queue="high_priority")
async def send_password_reset(user_id: int, token: str) -> dict:
    await asyncio.sleep(0.05)  # replace with your email service
    return {"user_id": user_id, "sent": True}
