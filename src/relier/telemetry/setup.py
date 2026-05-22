"""
Relier Telemetry — OpenTelemetry SDK Initialization.

Call ``setup_telemetry()`` once at process start (e.g., from the FastAPI
lifespan or the Celery ``worker_process_init`` signal) to configure global
tracer and meter providers that export to the OTLP gRPC collector.

If ``RELIER_OTEL_ENABLED`` is ``false`` (the default), this is a no-op and
the SDK falls back to the no-op providers shipped with ``opentelemetry-api``.
"""

import logging

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from relier import __version__
from relier.config import get_settings

logger = logging.getLogger(__name__)


def setup_telemetry(service_name: str = "relier") -> None:
    """Initialize OTEL SDK providers for tracing and metrics.

    This is idempotent, safe to call multiple times (subsequent calls are
    ignored if OTEL is disabled or already initialized).

    Args:
        service_name: The ``service.name`` resource attribute reported to the
            collector. Defaults to ``"relier"``.
    """
    settings = get_settings()

    if not settings.otel_enabled:
        logger.debug("OpenTelemetry export disabled (RELIER_OTEL_ENABLED=false).")
        return

    endpoint = settings.otel_exporter_otlp_endpoint

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": __version__,
            "deployment.environment": settings.env,
        }
    )

    # --- Tracing ---
    tracer_provider = TracerProvider(resource=resource)
    span_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    trace.set_tracer_provider(tracer_provider)

    # --- Metrics ---
    metric_exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
    reader = PeriodicExportingMetricReader(
        metric_exporter, export_interval_millis=15_000
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)

    logger.info(
        "OpenTelemetry initialized.",
        extra={"service": service_name, "endpoint": endpoint},
    )


def get_tracer(name: str = "relier") -> trace.Tracer:
    """Return a named tracer from the configured global provider."""
    return trace.get_tracer(name)


def get_meter(name: str = "relier") -> metrics.Meter:
    """Return a named meter from the configured global provider."""
    return metrics.get_meter(name)
