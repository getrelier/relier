"""
Relier API — Gateway Entry Point.

FastAPI application that serves as the admission controller and task
dispatcher for the Relier reliability cluster.

Middleware stack (applied in order):
    1. AdmissionControlMiddleware — rate-limit before any route logic.

Routers:
    /health  — liveness and readiness probes
    /tasks   — task management and dispatch
    /admin   — DLQ inspection, worker registry, cluster config
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from relier import __version__
from relier.api.middleware import AdmissionControlMiddleware
from relier.api.routers import admin, health, tasks
from relier.config import get_settings
from relier.storage.redis import redis_manager
from relier.telemetry.logging import setup_logging
from relier.telemetry.setup import setup_telemetry

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage startup and shutdown of the API server."""
    setup_logging(level=settings.log_level)
    setup_telemetry(service_name="relier-api")

    # --- TASK DISCOVERY ---
    # We must import the modules containing tasks so the decorators register
    # them with the celery_app in this specific process.
    if settings.env != "production":
        try:
            import relier.tasks.debug  # noqa: F401
            from relier.tasks.app import celery_app

            logger.info(
                "Debug tasks discovered", extra={"tasks": list(celery_app.tasks.keys())}
            )
        except ImportError as e:
            logger.error(f"Failed to load debug tasks: {e}")
    # ----------------------

    logger.info(
        "Relier API starting.", extra={"version": __version__, "env": settings.env}
    )
    yield
    # Shutdown — close connection pools cleanly.
    await redis_manager.close()
    logger.info("Relier API shut down.")


app = FastAPI(
    title="Relier API",
    description="Production Reliability Layer for any FastAPI + Celery Workload. Zero Job loss. Period.",
    version=__version__,
    lifespan=lifespan,
)

# --- Middleware ---
app.add_middleware(AdmissionControlMiddleware)

# --- Routers ---
app.include_router(health.router)
app.include_router(tasks.router)
app.include_router(admin.router)


@app.get("/", tags=["Root"])
async def root() -> dict:
    """Service root — returns version and environment metadata."""
    return {
        "service": "relier-api",
        "version": __version__,
        "status": "active",
        "environment": settings.env,
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all handler for unhandled exceptions.

    Returns a sanitised 500 response and logs the full traceback.
    """
    logger.error(
        "Unhandled API exception.",
        extra={"path": str(request.url), "method": request.method},
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"},
    )
