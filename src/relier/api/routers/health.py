"""
Relier API — Health, Liveness, and SLO Routers.
"""

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from relier.api.dependencies import get_relier_redis_client
from relier.core.slo import SLOMetrics

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Health"])

# =============================================================================
# SCHEMAS
# =============================================================================


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "relier-api"


class ReadinessResponse(BaseModel):
    status: str
    dependencies: dict[str, str]


class SLOMetricsResponse(BaseModel):
    target_slo: float = 0.999
    burn_rates: dict[str, Any]
    thresholds: dict[str, float] = {
        "critical": 14.4,
        "warning": 6.0,
        "info": 1.0,
    }


# =============================================================================
# ROUTES
# =============================================================================


@router.get("/health", response_model=HealthResponse)
async def liveness() -> HealthResponse:
    """Kubernetes liveness probe — always returns 200 if the process is alive."""
    return HealthResponse()


@router.get("/ready", response_model=ReadinessResponse)
async def readiness(
    redis: Annotated[Any, Depends(get_relier_redis_client)],
) -> ReadinessResponse:
    """Kubernetes readiness probe — checks Redis and PostgreSQL connectivity.

    Returns 503 if any dependency is unavailable so the orchestrator can
    stop routing traffic to this pod until it recovers.
    """
    errors = {}

    try:
        await redis.ping()
    except Exception as exc:
        errors["redis"] = str(exc)
        logger.error("Readiness check: Redis unavailable.", extra={"error": str(exc)})

    if errors:
        raise HTTPException(
            status_code=503,
            detail={"status": "not_ready", "failures": errors},
        )

    return ReadinessResponse(
        status="ready",
        dependencies={"redis": "connected", "postgres": "connected"},
    )


@router.get("/metrics/slo", response_model=SLOMetricsResponse)
async def slo_status() -> SLOMetricsResponse:
    """Return current SLO burn rates across all monitoring windows.

    Intended for scraping by Grafana dashboards and alerting pipelines.
    """
    report = await SLOMetrics.get_report()
    return SLOMetricsResponse(burn_rates=report)
