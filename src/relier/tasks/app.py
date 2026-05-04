"""
Relier Core — Celery App Factory.

Constructs and configures the Celery application instance, manages the
persistent asyncio event loop bridge via Celery lifecycle signals, and
warms up the Redis / PostgreSQL connection pools on worker startup.

The async bridge pattern
------------------------
Celery workers are synchronous Python processes.  Relier uses async libraries
(``redis.asyncio``, ``sqlalchemy.ext.asyncio``) throughout.  A single
``asyncio.AbstractEventLoop`` is created in ``worker_process_init`` and
stored as ``worker_loop``.  The ``@rl_task`` decorator calls
``loop.run_until_complete(...)`` to execute async logic from the sync
worker thread.
"""

import asyncio
import logging
import socket
import threading
from typing import Any

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from kombu import Exchange, Queue
from sqlalchemy import text

from relier.config import Settings, get_settings
from relier.core.shutdown import GracefulShutdownHandler
from relier.storage.database import db_manager
from relier.storage.redis import redis_manager
from relier.telemetry.logging import setup_logging
from relier.telemetry.setup import setup_telemetry

logger = logging.getLogger(__name__)


# =============================================================================
# Module-level state shared between signals and the decorator
# =============================================================================

worker_loop: asyncio.AbstractEventLoop | None = None
shutdown_handler: GracefulShutdownHandler | None = None
_presence_future: Any | None = None  # Handle for the presence heartbeat coroutine.


# =============================================================================
# Lazy-loaded settings property to avoid circular imports
# =============================================================================


def _get_settings() -> Settings:
    """Lazy-load settings so we pick up testcontainer environment variables."""
    return get_settings()


# =============================================================================
# Celery app factory
# =============================================================================


def create_celery_app() -> Celery:
    """Construct and return the configured Celery application instance."""
    # Debug tasks are only registered in non-production environments.
    includes = []
    if _get_settings().env != "production":
        includes.append("relier.tasks.debug")

    app = Celery(
        "relier",
        include=includes,
    )

    app.conf.update(
        # Broker / backend
        broker_url=str(_get_settings().redis_url),
        result_backend=str(_get_settings().redis_url),
        # Serialization — JSON only; reject pickle payloads.
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        # Queue routing
        task_default_queue="default",
        task_queues=(
            Queue("default", Exchange("default"), routing_key="default"),
            Queue(
                "high_priority", Exchange("high_priority"), routing_key="high_priority"
            ),
            Queue("low_priority", Exchange("low_priority"), routing_key="low_priority"),
            Queue("requeue", Exchange("requeue"), routing_key="requeue"),
        ),
        # * prefetch_multiplier=1: workers take exactly one task at a time.
        # * acks_late=True + reject_on_worker_lost=True: core reliability flags.
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
    )

    return app


# Global Celery instance.
celery_app = create_celery_app()


# =============================================================================
# Worker lifecycle signals
# =============================================================================


def _run_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Entry point for the background event loop thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


@worker_process_init.connect
def init_worker(**kwargs: Any) -> None:
    """Boot the background async loop and warm up connection pools.

    Fires once per worker *process* immediately after the process forks.
    """
    global worker_loop, shutdown_handler, _presence_future

    setup_logging(level=_get_settings().log_level, cache_loggers=False)
    setup_telemetry(service_name="relier-worker")

    logger.info("Initializing background asyncio event loop for worker.")

    worker_loop = asyncio.new_event_loop()

    # Start the loop in a dedicated daemon thread.
    thread = threading.Thread(target=_run_event_loop, args=(worker_loop,), daemon=True)
    thread.start()

    hostname = str(kwargs.get("hostname") or f"celery@{socket.gethostname()}")
    logger.info("Worker process initialized.", extra={"hostname": hostname})
    shutdown_handler = GracefulShutdownHandler(hostname)
    shutdown_handler.install()

    async def _warm_up() -> None:
        # Redis — initializes the connection pool.
        await redis_manager.get_client()

        # PostgreSQL — validates the pool is reachable.
        async with db_manager.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def _presence_loop(worker_id: str) -> None:
        """Refresh the worker presence key in Redis periodically."""
        redis = await redis_manager.get_client()
        presence_key = f"rl:presence:{worker_id}"
        logger.debug(
            "Starting worker presence heartbeat.", extra={"worker_id": worker_id}
        )
        backoff = 20  # Start at the normal interval
        max_backoff = 300  # Cap at 5 minutes
        while True:
            try:
                # Set presence and ensure worker is in the global set.
                await redis.set(presence_key, "1", ex=60)
                await redis.sadd("rl:workers", worker_id)  # type: ignore[misc]
                backoff = 20  # Reset on success
            except Exception as exc:
                logger.error(
                    "Presence heartbeat failed.",
                    extra={"error": str(exc), "retry_in": backoff},
                )
                backoff = min(backoff * 2, max_backoff)
            await asyncio.sleep(backoff)

    try:
        # Blocks until pools are ready.
        future = asyncio.run_coroutine_threadsafe(_warm_up(), worker_loop)
        future.result(timeout=10)

        # Starts the long-running heartbeat task; store future for cancellation.
        _presence_future = asyncio.run_coroutine_threadsafe(
            _presence_loop(hostname), worker_loop
        )

        logger.info(
            "Worker background thread and pools initialized.",
            extra={"hostname": hostname},
        )
    except Exception as exc:
        logger.error(
            "Failed to initialize worker background loop.",
            extra={"error": str(exc)},
            exc_info=True,
        )


@worker_process_shutdown.connect
def shutdown_worker(**kwargs: object) -> None:
    """Drain in-flight tasks and dispose of connection pools.

    Fires when the worker process receives SIGTERM or SIGINT.
    """
    global worker_loop, shutdown_handler, _presence_future

    if not worker_loop:
        return

    logger.info("Celery shutdown signal received. Starting Relier drain.")

    try:
        # Cancel the presence heartbeat loop.
        if _presence_future is not None:
            _presence_future.cancel()
            _presence_future = None

        # The loop is running in a daemon thread via run_forever().
        # We use run_coroutine_threadsafe — NOT run_until_complete.
        if shutdown_handler:
            fut = asyncio.run_coroutine_threadsafe(
                shutdown_handler.drain(), worker_loop
            )
            fut.result(timeout=_get_settings().graceful_shutdown_timeout)
    except Exception as exc:
        logger.error(
            "Error during graceful shutdown.", extra={"error": str(exc)}, exc_info=True
        )
    finally:
        # Always close connection pools, even if drain() failed.
        try:
            fut = asyncio.run_coroutine_threadsafe(redis_manager.close(), worker_loop)
            fut.result(timeout=5)
            fut = asyncio.run_coroutine_threadsafe(db_manager.close(), worker_loop)
            fut.result(timeout=5)
            logger.info("Connection pools closed.")
        except Exception as exc:
            logger.error(
                "Error closing connection pools.",
                extra={"error": str(exc)},
                exc_info=True,
            )
        finally:
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_loop = None
            logger.info("Worker event loop stopped.")
