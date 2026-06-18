import asyncio

from relier import rl_task


@rl_task(queue="high_priority", idempotent=True, idempotency_ttl=600)
async def send_welcome_email(user_id: str, email: str) -> dict:
    await asyncio.sleep(0.05)  # replace with your email service call
    return {"user_id": user_id, "email": email, "queued": True}


@rl_task(queue="default")
async def generate_report(report_id: str, filters: dict) -> dict:
    await asyncio.sleep(2.0)  # simulate heavy computation
    return {"report_id": report_id, "rows": 1024, "filters": filters}


@rl_task(queue="low_priority")
async def export_user_data(user_id: str) -> dict:
    await asyncio.sleep(5.0)  # simulate GDPR export
    return {"user_id": user_id, "status": "exported"}
