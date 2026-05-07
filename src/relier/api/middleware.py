"""
Relier API — Middleware Stack.

Intercepts every inbound HTTP request before it reaches any route handler
to enforce system-wide capacity limits via atomic Redis rate limiting.
"""

import logging

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from relier.core.admission import admission_control

logger = logging.getLogger(__name__)


class AdmissionControlMiddleware(BaseHTTPMiddleware):
    """Enforce capacity limits using the atomic Lua sliding-window counter.

    Health and readiness probes (``/health``, ``/ready``) are exempt from
    admission control so that orchestrators can always reach them.
    """

    _EXEMPT_PATHS = {"/health", "/ready", "//docs", "/redoc", "/openapi.json"}

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if request.url.path in self._EXEMPT_PATHS:
            return await call_next(request)

        resource_key = request.headers.get("X-Tenant-ID", "global")

        is_admitted, retry_after = await admission_control.check_capacity(resource_key)

        if not is_admitted:
            logger.warning(
                "Request rejected by admission control.",
                extra={"resource_key": resource_key, "retry_after": retry_after},
            )
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "System capacity reached.",
                    "retry_after_seconds": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

        return await call_next(request)
