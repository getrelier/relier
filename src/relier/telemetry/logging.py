"""
Relier Telemetry — Structured Logging.

Configures structlog to produce machine-readable JSON in production and
human-readable output in development, with automatic injection of the
current OpenTelemetry trace and span IDs into every log record.

Call ``setup_logging()`` once at process start (from the Celery ``worker_process_init`` signal) before any other Relier code
runs.
"""

import logging
import sys

import structlog
from opentelemetry import trace


def inject_otel_context(
    _logger: object,
    _method: str,
    event_dict: structlog.types.EventDict,
) -> structlog.types.EventDict:
    """Processor that injects the active OTEL trace/span IDs into log records.

    Enables log-trace correlation in Grafana, Jaeger, Honeycomb, etc.
    """
    span = trace.get_current_span()
    if span and span.get_span_context().is_valid:
        ctx = span.get_span_context()
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def setup_logging(level: str = "INFO", cache_loggers: bool = True) -> None:
    """Configure structlog for the current environment.

    In ``DEBUG`` mode, renders colorized, human-friendly output to stdout.
    In all other modes, renders newline-delimited JSON suitable for
    structured log aggregators (Loki, CloudWatch, Datadog Logs, etc.).

    Args:
        level:         Standard Python log-level string (e.g., ``"INFO"``).
        cache_loggers: Whether to cache logger wrappers on first use.  Set to
                       ``False`` in Celery worker child processes (prefork)
                       because the cache is inherited from the parent and
                       becomes stale after ``os.fork()``.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Configure the stdlib root logger so third-party libraries
    # (celery, etc.) also flow through structlog.

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.ExtraAdder(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        inject_otel_context,
    ]

    renderer = (
        structlog.dev.ConsoleRenderer(colors=True)
        if level.upper() == "DEBUG"
        else structlog.processors.JSONRenderer()
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        cache_logger_on_first_use=cache_loggers,
    )

    # Wire structlog into the stdlib formatter so existing
    # ``logging.getLogger(__name__)`` calls are rendered the same way.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)
