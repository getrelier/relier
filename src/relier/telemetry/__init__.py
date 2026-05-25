"""
Relier Telemetry — Monitoring and Observability.

Provides OpenTelemetry instrumentation for metrics and spans, plus
structured logging via structlog.
"""

from relier.telemetry.logging import setup_logging
from relier.telemetry.setup import get_meter, get_tracer, setup_telemetry

__all__ = [
    "get_meter",
    "get_tracer",
    "setup_logging",
    "setup_telemetry",
]
