"""
Relier Core — Celery Runtime Bootstrap.

Constructs and configures the Relier Celery application runtime.

This module is responsible for:
- Celery application configuration
- worker lifecycle orchestration
- persistent asyncio bridge management
- Redis runtime warmup
- worker identity registration
- background heartbeat coordination
- graceful worker draining
- telemetry initialization

Because Celery workers are synchronous prefork processes while Relier
internals are asyncio-native, this module maintains a dedicated
background event loop thread per worker process.
"""

import asyncio
import concurrent.futures
import logging
import os
import socket
import threading
import time
from typing import Any

from celery import Celery
from celery.signals import (
    worker_init,
    worker_process_init,
    worker_process_shutdown,
    worker_ready,
)
from kombu import Exchange, Queue

import relier.tasks.signals  # noqa: F401 — registers signal handlers
from relier.config import Settings, get_settings
from relier.core.keys import RedisKeys
from relier.core.shutdown import GracefulShutdownHandler
from relier.storage.redis import redis_manager
from relier.telemetry.logging import setup_logging
from relier.telemetry.setup import setup_telemetry

logger = logging.getLogger(__name__)

# =============================================================================
# Process-local runtime state
# =============================================================================

# Dedicated asyncio runtime bridge owned by the current worker process.
worker_loop: asyncio.AbstractEventLoop | None = None

# Coordinates graceful workload draining during worker termination.
shutdown_handler: GracefulShutdownHandler | None = None

# Background worker heartbeat coroutine future.
_presence_future: concurrent.futures.Future | None = None

# Background stale-worker cleanup coroutine future.
_cleanup_future: concurrent.futures.Future | None = None


def _get_settings() -> Settings:
    """
    Resolve runtime configuration lazily to support dynamic environment
    overrides during worker bootstrap.
    """
    return get_settings()


# =============================================================================
# Background coordination coroutines
# =============================================================================


async def _presence_loop(worker_id: str) -> None:
    """
    Continuously advertise worker liveness to the Redis coordination layer.
    """

    # Loop-affined Redis runtime client.
    redis = await redis_manager.get_client()

    # Presence TTL key used for worker liveness detection.
    presence_key = RedisKeys.presence(worker_id)
    settings = _get_settings()

    # Adaptive retry interval for degraded Redis connectivity.
    backoff = settings.heartbeat_ttl // 2

    logger.debug("Worker presence heartbeat loop started.", extra={"worker_id": worker_id})

    while True:
        try:
            # Atomically refresh both ephemeral liveness and durable worker registry
            # membership.
            pipe = redis.pipeline()
            pipe.set(presence_key, "1", ex=settings.heartbeat_ttl)
            pipe.zadd(RedisKeys.workers(), {worker_id: time.time()})
            await pipe.execute()
            backoff = settings.heartbeat_ttl // 2
        except Exception as exc:
            logger.error("Worker presence heartbeat update failed.", extra={"error": str(exc)})

            # Exponential backoff prevents tight retry loops during Redis outages.
            backoff = min(backoff * 2, 300)
        await asyncio.sleep(backoff)


async def _cleanup_dead_workers() -> None:
    """
    Continuously prune stale worker registrations from the coordination
    registry.
    """
    redis = await redis_manager.get_client()
    settings = _get_settings()

    # Cleanup cadence scales relative to heartbeat expiration.
    interval = max(2, settings.heartbeat_ttl // 5)

    while True:
        try:
            # Remove abandoned worker identities that have not refreshed their
            # registry score within the retention window.
            cutoff = time.time() - 86400
            pruned = await redis.zremrangebyscore(RedisKeys.workers(), 0, cutoff)
            if pruned:
                logger.debug(
                    "Pruned stale workers from registry.", extra={"count": pruned}
                )
        except Exception as exc:
            logger.error("Worker registry cleanup pass failed.", extra={"error": str(exc)})
        await asyncio.sleep(interval)


# =============================================================================
# Async runtime bridge management
# =============================================================================


def _run_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Own and drive a dedicated asyncio runtime for the worker process.
    """

    # Bind the loop to the current bridge thread before execution begins.
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _ensure_bridge() -> None:
    """
    Ensure the worker process owns an active asyncio bridge runtime.
    """

    # The bridge may already exist in long-lived worker processes.
    global worker_loop
    if worker_loop is None or not worker_loop.is_running():
        # Create an isolated asyncio runtime for async Relier subsystems.
        worker_loop = asyncio.new_event_loop()

        # Run the asyncio runtime independently from Celery's synchronous worker
        # execution threads.
        t = threading.Thread(target=_run_event_loop, args=(worker_loop,), daemon=True)
        t.start()
        logger.debug("Async runtime bridge started for worker process. PID: %s", os.getpid())


# =============================================================================
# Celery runtime factory
# =============================================================================


def create_celery_app() -> Celery:
    """
    Construct and configure the Relier Celery runtime instance.
    """

    # Development-only task modules excluded from production deployments.
    includes = ["relier.tasks.debug"] if _get_settings().env != "production" else []
    app = Celery("relier", include=includes)

    # Runtime reliability defaults optimized for task durability and
    # crash recovery semantics.
    app.conf.update(
        broker_url=str(_get_settings().redis_url),
        result_backend=str(_get_settings().redis_url),
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_default_queue="default",
        task_queues=(
            Queue("default", Exchange("default"), routing_key="default"),
            Queue(
                "high_priority", Exchange("high_priority"), routing_key="high_priority"
            ),
            Queue("low_priority", Exchange("low_priority"), routing_key="low_priority"),
        ),

        # Prevent workers from hoarding unacknowledged tasks.
        worker_prefetch_multiplier=1,

        # Acknowledge tasks only after execution completes successfully.
        task_acks_late=True,

        # Return tasks to the broker if a worker process crashes mid-execution.
        task_reject_on_worker_lost=True,
        task_track_started=True,
        worker_redirect_stdouts=False,  # Preserve native stdout/stderr ownership for structured logging systems.
        worker_redirect_stdouts_level="INFO",
    )
    return app


celery_app = create_celery_app()

# =============================================================================
# Celery worker lifecycle hooks
# =============================================================================


@worker_init.connect
def init_main_process(**kwargs: Any) -> None:
    """
    Initialize the async runtime bridge in the Celery supervisor process.
    """
    _ensure_bridge()


@worker_process_init.connect
def init_worker_process(**kwargs: Any) -> None:
    """
    Initialize per-process async runtime infrastructure after Celery fork.
    """
    global worker_loop, shutdown_handler

    # Celery prefork workers inherit the parent's event loop object after
    # fork(), but not the thread actively driving it.
    #
    # The inherited loop therefore appears "running" while actually being
    # unusable inside the child process.
    #
    # Always create a brand-new event loop runtime per worker process.
    worker_loop = asyncio.new_event_loop()
    t = threading.Thread(target=_run_event_loop, args=(worker_loop,), daemon=True)
    t.start()

    # Reinitialize structured logging inside the isolated worker process.
    setup_logging(level=_get_settings().log_level, cache_loggers=False)
    # Bind telemetry exporters to the child process runtime.
    setup_telemetry(service_name="relier-worker")

    hostname = str(kwargs.get("hostname") or f"celery@{socket.gethostname()}")

    # Process-local graceful shutdown coordinator.
    shutdown_handler = GracefulShutdownHandler(hostname)
    shutdown_handler.install()

    # Eagerly initialize Redis connectivity so failures surface during worker
    # bootstrap rather than first task execution.
    if worker_loop is not None:
        asyncio.run_coroutine_threadsafe(redis_manager.get_client(), worker_loop)

    logger.info("Worker runtime infrastructure initialized successfully.", extra={"hostname": hostname})


@worker_ready.connect
def start_relier_identity(sender: Any, **kwargs: Any) -> None:
    """
    Start background coordination services once the worker becomes ready.
    """
    global _presence_future, _cleanup_future
    hostname = str(sender.hostname)

    _ensure_bridge()  # Supervisor processes may reach readiness before bridge initialization.

    if worker_loop is not None:
        # Schedule long-lived coordination coroutines onto the dedicated runtime
        # bridge thread.
        _presence_future = asyncio.run_coroutine_threadsafe(
            _presence_loop(hostname), worker_loop
        )
        _cleanup_future = asyncio.run_coroutine_threadsafe(
            _cleanup_dead_workers(), worker_loop
        )

    logger.info(
        "Worker coordination services started successfully.", extra={"hostname": hostname}
    )


@worker_process_shutdown.connect
def shutdown_worker(**kwargs: object) -> None:
    """
    Drain workloads and gracefully tear down worker runtime infrastructure.
    """
    global worker_loop, shutdown_handler, _presence_future, _cleanup_future

    if not worker_loop or not worker_loop.is_running():
        return # The bridge may already be stopped during forced interpreter teardown.

    logger.info("Worker shutdown initiated. Entering coordinated drain sequence.")

    try:
        if _presence_future:
            # Stop long-lived coordination services before shutting down the runtime.
            _presence_future.cancel()
        if _cleanup_future:
            # Block until workload draining completes or the timeout budget expires.
            _cleanup_future.cancel()

        if shutdown_handler:
            fut = asyncio.run_coroutine_threadsafe(
                shutdown_handler.drain(), worker_loop
            )
            fut.result(timeout=_get_settings().graceful_shutdown_timeout)
    except Exception as exc:
        logger.error("Worker drain sequence failed.", extra={"error": str(exc)})
    finally:
        try:
            # Gracefully release loop-affined Redis resources.
            fut = asyncio.run_coroutine_threadsafe(redis_manager.close(), worker_loop)
            fut.result(timeout=5)
        except Exception:
            pass
        # Stop the dedicated asyncio bridge runtime.
        worker_loop.call_soon_threadsafe(worker_loop.stop)
        logger.info("Worker async runtime bridge stopped.")
