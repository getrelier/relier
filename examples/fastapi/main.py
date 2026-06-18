"""
FastAPI + Relier example.

Start the worker:
    celery -A relier.tasks.app worker --include=tasks --loglevel=info

Run the API:
    uvicorn main:app --reload
"""

import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from tasks import export_user_data, generate_report, send_welcome_email

from relier import AdmissionRejectedError
from relier.tasks.app import celery_app

app = FastAPI(title="Relier + FastAPI")


class SignupBody(BaseModel):
    user_id: str
    email: str


class ReportBody(BaseModel):
    report_id: str
    filters: dict = {}


@app.post("/users/signup", status_code=202)
async def signup(body: SignupBody):
    try:
        receipt = await send_welcome_email.apush(user_id=body.user_id, email=body.email)
    except AdmissionRejectedError as exc:
        raise HTTPException(
            status_code=429, headers={"Retry-After": str(exc.retry_after)}
        ) from exc
    return {"task_id": receipt.id}


@app.post("/reports", status_code=202)
async def create_report(body: ReportBody):
    try:
        receipt = await generate_report.apush(
            report_id=body.report_id, filters=body.filters
        )
    except AdmissionRejectedError as exc:
        raise HTTPException(
            status_code=429, headers={"Retry-After": str(exc.retry_after)}
        ) from exc
    return {"task_id": receipt.id}


@app.post("/users/{user_id}/export", status_code=202)
async def export_data(user_id: str):
    receipt = await export_user_data.apush(user_id=user_id)
    return {"task_id": receipt.id}


@app.get("/tasks/{task_id}")
async def task_status(task_id: str):
    result = celery_app.AsyncResult(task_id)
    return {
        "task_id": task_id,
        "status": result.state,
        "result": result.result if result.ready() else None,
    }
