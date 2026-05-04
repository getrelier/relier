"""
Relier — Production reliability layer for FastAPI + Celery AI workflows.

Core Philosophy:
- Zero job loss: Every task is tracked and resurrected if a worker dies.
- Atomic execution: Idempotency primitives ensure tasks run exactly once.
- Schema safety: Versioned envelopes protect against rolling deployment data corruption.
- observability: Full OpenTelemetry integration for traces and metrics.

Main components:
- relier.tasks: The @rl_task decorator and worker infrastructure.
- relier.core: Resurrection (Phoenix), DLQ, Idempotency, and Schema logic.
- relier.storage: Async connection management for Redis and PostgreSQL.
- relier.telemetry: Structured logging and OTLP instrumentation.
"""

__version__ = "0.1.0"
