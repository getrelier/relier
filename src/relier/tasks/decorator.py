"""
Relier Task Decorator — ``@rl_task``.

The single user-facing primitive that wraps any Celery task with the full
Relier reliability stack:

* **Schema versioning** — payloads dispatched via ``push() or apush()`` are
  wrapped in a signed, versioned envelope and migrated on the worker side.
* **Idempotency** — optional duplicate-execution prevention backed by Redis.
* **Phoenix heartbeat** — heartbeat emitted every N seconds so the resurrector
  can detect worker crashes in < 35 s.
* **Inflight registry** — sorted-set tracking for ``rl tasks inflight``.
* **Timeout enforcement** — soft + hard two-tier timeouts with cleanup hooks.
* **Graceful shutdown integration** — task is tracked so the drain loop knows
  when it is safe to exit.

Usage::

    @rl_task(
        queue="high_priority",
        max_resurrections=5,
        idempotent=True,
        idempotency_ttl=3600,
        soft_timeout=25,
        hard_timeout=30,
    )
    async def process_document(doc_id: str) -> dict:
        result = await embed_and_store(doc_id)
        return result

    Timeout features are only supported for async functions.
    Sync tasks will raise an error if timeout parameters are passed.
"""

import asyncio
import functools
import hashlib
import inspect
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable

# Module-level imports are intentionally eager to fail fast at worker startup.
from typing import Any, cast

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from opentelemetry import trace
from opentelemetry.propagate import extract as otel_extract
from opentelemetry.propagate import inject as otel_inject

from relier.core.admission import admission_control
from relier.core.checkpoint import CheckpointStore
from relier.core.exceptions import (
    AdmissionRejectedError,
    IdempotencyInFlightError,
    PayloadIntegrityError,
    SchemaMigrationError,
)
from relier.core.idempotency import idempotency_manager
from relier.core.keys import RedisKeys
from relier.core.phoenix import PhoenixRegistry
from relier.core.schema import SchemaRegistry
from relier.core.slo import SLOMetrics
from relier.core.timeouts import TimeoutEnforcer
from relier.storage.redis import get_relier_redis
from relier.tasks.context import TaskContext, _task_context_var
from relier.telemetry.metrics import (
    idempotency_hits_total,
    task_duration_ms,
    task_overhead_ms,
    tasks_total,
    timeouts_total,
)
from relier.telemetry.setup import get_tracer
from relier.telemetry.spans import (
    ATTR_ADMISSION_RESULT,
    ATTR_TASK_ID,
    ATTR_TASK_IDEMPOTENCY_HIT,
    ATTR_TASK_IS_IDEMPOTENT,
    ATTR_TASK_IS_RESURRECTION,
    ATTR_TASK_NAME,
    ATTR_TASK_QUEUE,
    ATTR_TASK_SCHEMA_VERSION,
    ATTR_WORKER_ID,
    record_exception,
)

tracer = get_tracer("relier.tasks")

logger = logging.getLogger(__name__)

# =============================================================================
# Queue Topology
# =============================================================================

INTERNAL_QUEUES = {"re-queue"}

PUBLIC_QUEUES = {
    "high_priority",
    "default",
    "low_priority",
}

ALL_QUEUES = PUBLIC_QUEUES | INTERNAL_QUEUES

# =============================================================================
# Decorator
# =============================================================================


def rl_task(
    queue: str = "default",
    idempotent: bool = False,
    idempotency_ttl: int = 3600,
    soft_timeout: int | None = None,
    hard_timeout: int | None = None,
    on_soft_timeout: Callable[[TaskContext], Awaitable[None]] | None = None,
) -> Callable:
    """Decorate a function with the full Relier reliability stack.

    Args:
        queue:            Celery queue name for routing.
        idempotent:       Enable automatic deduplication via Redis.
        idempotency_ttl:  TTL in seconds for the cached result.
        soft_timeout:     Seconds before the cleanup hook fires.
        hard_timeout:     Seconds before the task is unconditionally cancelled.
        on_soft_timeout:  Async callable receiving a ``TaskContext`` at soft timeout.
    Raises:
        ValueError:
            If timeout parameters are used on a synchronous function.

        AdmissionRejectedError:
            If cluster admission control rejects task dispatch.
    """
    from relier.config import get_settings

    settings = get_settings()

    if queue not in PUBLIC_QUEUES:
        raise ValueError(
            f"Unknown public queue '{queue}'. Allowed queues: {sorted(PUBLIC_QUEUES)}"
        )

    if (
        idempotent
        and hard_timeout is not None
        and hard_timeout >= settings.idempotency_inflight_ttl
    ):
        raise ValueError(
            f"CONFIGURATION ERROR: hard_timeout ({hard_timeout}s) must be "
            f"< IDEMPOTENCY_INFLIGHT_TTL ({settings.idempotency_inflight_ttl}s).\n"
            f"\n"
            f"Why: If a task runs longer than IN_FLIGHT_TTL, the idempotency key "
            f"expires while the task is still executing, allowing a duplicate "
            f"worker to start the same task (double execution).\n"
            f"\n"
            f"Fix: Either increase RELIER_IDEMPOTENCY_INFLIGHT_TTL or reduce hard_timeout.\n"
            f"Safe formula: hard_timeout < IN_FLIGHT_TTL - 10s (safety buffer)"
        )

    if (
        soft_timeout is not None
        and hard_timeout is not None
        and soft_timeout >= hard_timeout
    ):
        raise ValueError(
            f"soft_timeout ({soft_timeout}s) must be < hard_timeout ({hard_timeout}s)"
        )

    def decorator(func: Callable) -> Callable:
        """The actual decorator applied to the user function."""
        is_async = inspect.iscoroutinefunction(func)

        if not is_async and (soft_timeout or hard_timeout or on_soft_timeout):
            logger.warning(
                "Sync function decorated with timeout params; timeouts will be ignored.",
                extra={"task_name": func.__name__},
            )
            raise ValueError(
                "Timeout parameters are only supported for async functions."
            )

        task_name = f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            """Execute the task inside Relier's reliability orchestration layer."""
            task_id: str = self.request.id or str(uuid.uuid4())
            worker_id: str = self.request.hostname or "unknown-worker"
            loop = _get_worker_loop()

            # ------------------------------------------------------------------
            # Phoenix payload — stored in Redis for resurrection.
            # The envelope (args[0]) is kept intact so resurrection re-dispatches
            # the original wrapped payload, which goes through schema migration
            # again on the next worker pickup.
            # ------------------------------------------------------------------
            phoenix_payload = {
                "task_name": self.name,
                "args": list(args),
                "kwargs": dict(kwargs),
                "queue": (getattr(self.request, "delivery_info", {}) or {}).get(
                    "routing_key", queue
                ),
                "worker_id": worker_id,
            }

            async def _orchestrate() -> Any:
                """Run the complete task lifecycle within the worker event loop."""
                from relier.core.dlq import DeadLetterQueue

                # ------------------------------------------------------------------
                # Trace context propagation.
                #
                # The producer (apush) injects the current span context into the
                # envelope at dispatch time. Extract it here to continue the
                # distributed trace from the producer into the worker.
                #
                # If args[0] looks like an envelope (has schema_version), treat it
                # as one. Otherwise fall back to using args/kwargs directly — this
                # path is used for tests that call .run() without envelope wrapping.
                # ------------------------------------------------------------------
                envelope: dict = (
                    args[0]
                    if args
                    and isinstance(args[0], dict)
                    and "schema_version" in args[0]
                    else {}
                )
                otel_carrier = envelope.get("_otel_context") or {}
                parent_ctx = otel_extract(otel_carrier)
                schema_version = envelope.get(
                    "schema_version", SchemaRegistry.CURRENT_VERSION
                )

                # Resurrection: the resurrector always injects fence tokens into kwargs.
                is_resurrection = "_fence_token" in kwargs

                with tracer.start_as_current_span(
                    "rl.worker.pickup",
                    context=parent_ctx,
                    attributes={
                        ATTR_TASK_ID: task_id,
                        ATTR_TASK_NAME: task_name,
                        ATTR_TASK_QUEUE: queue,
                        ATTR_WORKER_ID: worker_id,
                        ATTR_TASK_IS_RESURRECTION: is_resurrection,
                    },
                ) as pickup_span:
                    logger.debug(
                        "Starting task orchestration.",
                        extra={"task_id": task_id, "worker_id": worker_id},
                    )

                    # ----------------------------------------------------------
                    # rl.schema.migrate — unwrap and migrate the task envelope.
                    # Skipped when called without an envelope (e.g., direct .run()
                    # in tests); in that case use args/kwargs as-is.
                    # ----------------------------------------------------------
                    _t_schema = time.perf_counter()
                    try:
                        if envelope:
                            with tracer.start_as_current_span(
                                "rl.schema.migrate",
                                attributes={
                                    ATTR_TASK_ID: task_id,
                                    ATTR_TASK_NAME: task_name,
                                    ATTR_TASK_SCHEMA_VERSION: schema_version,
                                },
                            ):
                                actual_args, actual_kwargs = (
                                    SchemaRegistry.unwrap_and_migrate(
                                        task_name, envelope
                                    )
                                )
                        else:
                            actual_args, actual_kwargs = args, dict(kwargs)
                    except (PayloadIntegrityError, SchemaMigrationError) as exc:
                        logger.error(
                            "Schema migration failed; quarantining task.",
                            extra={
                                "task_id": task_id,
                                "task_name": task_name,
                                "error": str(exc),
                            },
                        )
                        record_exception(pickup_span, exc)
                        await DeadLetterQueue.quarantine(
                            task_id, reason=type(exc).__name__
                        )
                        return {"status": "quarantined", "error": str(exc)}
                    finally:
                        task_overhead_ms.record(
                            (time.perf_counter() - _t_schema) * 1000,
                            {"rl.task.name": task_name, "phase": "schema"},
                        )

                    # ----------------------------------------------------------
                    # Redis infrastructure setup.
                    # ----------------------------------------------------------
                    idem_result = None
                    redis = await get_relier_redis()
                    expiry_timestamp = time.time() + settings.heartbeat_ttl

                    await redis.set(RedisKeys.presence(worker_id), "1", ex=60)
                    await redis.zadd(
                        RedisKeys.phoenix_expiry_index(), {task_id: expiry_timestamp}
                    )

                    # ----------------------------------------------------------
                    # rl.idempotency.check
                    # ----------------------------------------------------------
                    if idempotent:
                        arg_sig = json.dumps(
                            {"a": list(actual_args), "k": actual_kwargs},
                            sort_keys=True,
                            default=str,
                        )
                        arg_hash = hashlib.sha256(arg_sig.encode()).hexdigest()[:16]
                        idem_key = f"{func.__name__}:{arg_hash}"

                        _t_idem = time.perf_counter()
                        with tracer.start_as_current_span(
                            "rl.idempotency.check",
                            attributes={
                                ATTR_TASK_ID: task_id,
                                ATTR_TASK_NAME: task_name,
                                ATTR_TASK_IS_IDEMPOTENT: True,
                            },
                        ) as idem_span:
                            idem_result = await idempotency_manager.check_or_claim(
                                idem_key, idempotency_ttl
                            )
                            idem_span.set_attribute(
                                ATTR_TASK_IDEMPOTENCY_HIT,
                                idem_result.already_executed,
                            )
                        task_overhead_ms.record(
                            (time.perf_counter() - _t_idem) * 1000,
                            {"rl.task.name": task_name, "phase": "idempotency"},
                        )

                        if idem_result.already_executed:
                            logger.info(
                                "Idempotency hit; returning cached result.",
                                extra={"task_id": task_id, "key": idem_key},
                            )
                            idempotency_hits_total.add(1, {"rl.task.name": task_name})
                            tasks_total.add(
                                1,
                                {
                                    "status": "idempotency_hit",
                                    "rl.task.name": task_name,
                                },
                            )
                            return idem_result.cached_result

                    await PhoenixRegistry.register(task_id, worker_id, phoenix_payload)

                    # LEASING + FENCING ENFORCEMENT (START)
                    # Fence tokens live in kwargs (injected by the resurrector),
                    # never inside the schema-migrated actual_kwargs.
                    fence_token = kwargs.pop("_fence_token", None)
                    lease_key = kwargs.pop("_lease_key", None)
                    fence_key = kwargs.pop("_fence_key", None)

                    is_valid = await PhoenixRegistry.validate_execution(
                        task_id, redis, fence_token, lease_key, fence_key
                    )
                    if not is_valid:
                        return {"status": "rejected", "reason": "duplicate_or_stale"}

                    inflight_key = RedisKeys.inflight(worker_id)

                    try:
                        from relier.tasks.app import shutdown_handler as _sh
                    except ImportError:
                        _sh = None

                    if _sh is not None:
                        _sh.track_task(task_id)

                    # Update worker heartbeat and inflight tracking atomically.
                    pipe = redis.pipeline()
                    pipe.zadd(RedisKeys.workers(), {worker_id: time.time()})
                    pipe.zadd(inflight_key, {task_id: time.time()})
                    await pipe.execute()

                    start_time = time.perf_counter()

                    # Checkpoint lives in kwargs (injected by resurrector or pipeline).
                    checkpoint = await CheckpointStore.resolve(
                        kwargs.pop("checkpoint", None)
                    )

                    ctx = TaskContext(
                        task_id=task_id,
                        task_name=task_name,
                        args=actual_args,
                        kwargs=actual_kwargs,
                        worker_id=worker_id,
                        partial_result=checkpoint,
                    )

                    token = _task_context_var.set(ctx)

                    try:
                        sig = inspect.signature(func)
                        if "ctx" in sig.parameters:
                            actual_kwargs["ctx"] = ctx

                        # --------------------------------------------------
                        # rl.task.execute — the user function runs here.
                        # --------------------------------------------------
                        with tracer.start_as_current_span(
                            "rl.task.execute",
                            attributes={
                                ATTR_TASK_ID: task_id,
                                ATTR_TASK_NAME: task_name,
                                ATTR_WORKER_ID: worker_id,
                                ATTR_TASK_QUEUE: phoenix_payload["queue"],
                                ATTR_TASK_SCHEMA_VERSION: schema_version,
                                ATTR_TASK_IS_RESURRECTION: is_resurrection,
                                ATTR_TASK_IS_IDEMPOTENT: idempotent,
                                ATTR_TASK_IDEMPOTENCY_HIT: False,
                            },
                        ) as exec_span:
                            try:
                                # Safety purge of internal tokens before calling func.
                                for k in ["_fence_token", "_lease_key", "_fence_key"]:
                                    actual_kwargs.pop(k, None)

                                if is_async:
                                    if soft_timeout or hard_timeout:
                                        result = await TimeoutEnforcer.run(
                                            func,
                                            actual_args,
                                            actual_kwargs,
                                            soft=soft_timeout,
                                            hard=hard_timeout,
                                            on_soft=on_soft_timeout,
                                            task_id=task_id,
                                        )
                                    else:
                                        result = await func(
                                            *actual_args, **actual_kwargs
                                        )
                                else:
                                    result = await asyncio.to_thread(
                                        func, *actual_args, **actual_kwargs
                                    )

                                # FENCING CHECK (END) — before committing results.
                                can_commit = await PhoenixRegistry.validate_commit(
                                    task_id, redis, fence_token, lease_key, fence_key
                                )
                                if not can_commit:
                                    return {
                                        "status": "discarded",
                                        "reason": "zombie_fence",
                                    }

                                if idempotent and idem_result is not None:
                                    await idem_result.record_result(result)

                                await SLOMetrics.record_event("success")

                                pipe = redis.pipeline()
                                pipe.incr(RedisKeys.metric_global("success"))
                                pipe.incr(RedisKeys.metric_worker(worker_id, "success"))
                                pipe.expire(
                                    RedisKeys.metric_worker(worker_id, "success"), 86400
                                )
                                pipe.expire(
                                    RedisKeys.metric_worker(worker_id, "failed"), 86400
                                )
                                await pipe.execute()

                                tasks_total.add(
                                    1,
                                    {"status": "completed", "rl.task.name": task_name},
                                )
                                exec_span.add_event(
                                    "rl.task.complete",
                                    {
                                        "rl.task.name": task_name,
                                        "rl.task.id": task_id,
                                    },
                                )
                                exec_span.set_status(trace.Status(trace.StatusCode.OK))
                                return result

                            except TimeoutError as exc:
                                logger.error(
                                    "Task hard timeout exceeded.",
                                    extra={
                                        "task_id": task_id,
                                        "hard_timeout": hard_timeout,
                                    },
                                )
                                timeouts_total.add(
                                    1, {"type": "hard", "rl.task.name": task_name}
                                )
                                await SLOMetrics.record_event("failure")

                                pipe = redis.pipeline()
                                pipe.incr(RedisKeys.metric_global("failed"))
                                pipe.incr(RedisKeys.metric_worker(worker_id, "failed"))
                                pipe.expire(
                                    RedisKeys.metric_worker(worker_id, "failed"), 86400
                                )
                                pipe.expire(
                                    RedisKeys.metric_worker(worker_id, "success"), 86400
                                )
                                await pipe.execute()

                                tasks_total.add(
                                    1,
                                    {
                                        "status": "failed",
                                        "rl.task.name": task_name,
                                        "reason": "timeout",
                                    },
                                )
                                exec_span.add_event(
                                    "rl.task.failed",
                                    {
                                        "reason": "timeout",
                                        "rl.task.name": task_name,
                                        "rl.task.id": task_id,
                                    },
                                )
                                record_exception(exec_span, exc)
                                await DeadLetterQueue.quarantine(
                                    task_id,
                                    reason=type(exc).__name__,
                                    payload=phoenix_payload,
                                )
                                raise SoftTimeLimitExceeded(str(exc)) from exc

                            except Exception as exc:
                                logger.error(
                                    "Task execution failed.",
                                    extra={
                                        "task_id": task_id,
                                        "task_name": task_name,
                                        "worker_id": worker_id,
                                        "queue": phoenix_payload.get(
                                            "queue", "default"
                                        ),
                                    },
                                    exc_info=True,
                                )
                                await SLOMetrics.record_event("failure")

                                pipe = redis.pipeline()
                                pipe.incr(RedisKeys.metric_global("failed"))
                                pipe.incr(RedisKeys.metric_worker(worker_id, "failed"))
                                pipe.expire(
                                    RedisKeys.metric_worker(worker_id, "failed"), 86400
                                )
                                pipe.expire(
                                    RedisKeys.metric_worker(worker_id, "success"), 86400
                                )
                                await pipe.execute()

                                tasks_total.add(
                                    1,
                                    {"status": "failed", "rl.task.name": task_name},
                                )
                                exec_span.add_event(
                                    "rl.task.failed",
                                    {
                                        "reason": type(exc).__name__,
                                        "rl.task.name": task_name,
                                        "rl.task.id": task_id,
                                    },
                                )
                                record_exception(exec_span, exc)

                                await DeadLetterQueue.quarantine(
                                    task_id,
                                    reason=type(exc).__name__,
                                    payload=phoenix_payload,
                                )
                                raise

                            finally:
                                duration_ms = (time.perf_counter() - start_time) * 1000

                                task_duration_ms.record(
                                    duration_ms, {"rl.task.name": task_name}
                                )
                                try:
                                    await redis.lpush(
                                        RedisKeys.task_durations(),
                                        str(duration_ms / 1000.0),
                                    )  # type: ignore[misc]
                                    await redis.ltrim(
                                        RedisKeys.task_durations(), 0, 999
                                    )  # type: ignore[misc]
                                except Exception:
                                    logger.debug(
                                        "Failed to record duration sample.",
                                        extra={"task_id": task_id},
                                    )

                                if _sh is not None:
                                    _sh.untrack_task(task_id)

                                await redis.zrem(inflight_key, task_id)
                                await PhoenixRegistry.complete(task_id)
                    finally:
                        _task_context_var.reset(token)

            try:
                if loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(_orchestrate(), loop)
                    return future.result(
                        timeout=hard_timeout + 10 if hard_timeout else 300
                    )
                else:
                    return loop.run_until_complete(_orchestrate())
            except IdempotencyInFlightError as exc:
                raise self.retry(exc=exc, countdown=5) from exc
            except Exception as exc:
                logger.error(
                    "Async bridge failed.",
                    extra={
                        "task_id": task_id,
                        "error_type": type(exc).__name__,
                    },
                )
                raise

        # Create the Celery task explicitly so we can attach our helper.
        task = shared_task(
            name=task_name,
            queue=queue,
            bind=True,
            acks_late=True,
            max_retries=15,
            reject_on_worker_lost=True,
        )(wrapper)

        async def apush(*d_args: Any, **d_kwargs: Any) -> Any:
            """Async dispatch — use in FastAPI or async Django."""

            _validate_public_dispatch(queue)

            with tracer.start_as_current_span(
                "rl.request.admitted",
                attributes={
                    ATTR_TASK_NAME: task_name,
                    ATTR_TASK_QUEUE: queue,
                },
            ) as admit_span:
                is_admitted, retry_after = await admission_control.check_capacity(
                    "celery-dispatch"
                )
                admit_span.set_attribute(
                    ATTR_ADMISSION_RESULT,
                    "admitted" if is_admitted else "rejected",
                )
                if not is_admitted:
                    admit_span.set_status(
                        trace.Status(trace.StatusCode.ERROR, "Admission rejected")
                    )
                    raise AdmissionRejectedError(
                        f"Relier cluster at capacity. Retry after {retry_after}s",
                        retry_after,
                    )

            task_id = str(uuid.uuid4())

            with tracer.start_as_current_span(
                "rl.task.enqueue",
                attributes={
                    ATTR_TASK_ID: task_id,
                    ATTR_TASK_NAME: task_name,
                    ATTR_TASK_QUEUE: queue,
                },
            ):
                envelope = SchemaRegistry.wrap(task_id, d_args, d_kwargs)
                # Inject current span context so workers can continue the trace.
                otel_carrier: dict[str, str] = {}
                otel_inject(otel_carrier)
                envelope["_otel_context"] = otel_carrier

                return await _dispatch_internal(
                    task=task,
                    queue=queue,
                    envelope=envelope,
                    task_id=task_id,
                )

        def push(*d_args: Any, **d_kwargs: Any) -> Any:
            """Sync dispatch — use in Django views, Flask routes, or sync scripts.

            Blocks the calling thread for ~1ms (admission check) then dispatches.
            Does NOT block on task completion — fire-and-forget like .delay().
            """
            try:
                # Inside a Celery worker — reuse the persistent loop
                import relier.tasks.app

                loop = relier.tasks.app.worker_loop
                if loop and loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(
                        apush(*d_args, **d_kwargs), loop
                    )
                    return future.result(timeout=5.0)
            except (ImportError, AttributeError):
                pass

            # Outside Celery (Django view, Flask route, script)
            return asyncio.run(apush(*d_args, **d_kwargs))

        task.apush = apush
        task.push = push
        return cast(Callable[..., Any], task)

    return decorator


# ======================================================================================
# Helpers
# ======================================================================================
def _validate_public_dispatch(queue: str) -> None:
    """
    Prevent user-facing APIs from publishing into internal queues.
    """
    if queue in INTERNAL_QUEUES:
        raise ValueError(
            f"Queue '{queue}' is reserved for Relier internal recovery and "
            "cannot be used for task routing. "
            f"Allowed queues: {sorted(PUBLIC_QUEUES)}"
        )

    if queue not in PUBLIC_QUEUES:
        raise RuntimeError(
            f"Unknown queue '{queue}'. Allowed queues: {sorted(PUBLIC_QUEUES)}"
        )


async def _dispatch_internal(
    *,
    task: Any,
    queue: str,
    envelope: dict[str, Any],
    task_id: str,
) -> Any:
    """
    Internal privileged dispatch path.

    Used exclusively by Relier recovery/runtime subsystems.
    Uses celery_app.send_task() rather than task.apply_async() so the
    broker connection is drawn from the app-level pool, which is thread-safe
    and works correctly when called via run_in_executor.
    """

    if queue not in ALL_QUEUES:
        raise RuntimeError(f"Unknown queue '{queue}'.")

    from relier.tasks.app import celery_app

    loop = asyncio.get_running_loop()

    return await loop.run_in_executor(
        None,
        lambda: celery_app.send_task(
            task.name,
            args=(envelope,),
            queue=queue,
            task_id=task_id,
        ),
    )


def _get_worker_loop() -> asyncio.AbstractEventLoop:
    """
    Return the worker-scoped asyncio event loop.

    Falls back to lazy initialization inside Celery workers and creates
    a standalone loop when running outside worker execution contexts.
    """
    try:
        import relier.tasks.app

        if relier.tasks.app.worker_loop is not None:
            return relier.tasks.app.worker_loop

        import os

        if "CELERY_LOADER" in os.environ:
            from relier.tasks.app import init_worker_process

            logger.warning("init_worker didn't fire; performing lazy initialization.")
            init_worker_process()
            if relier.tasks.app.worker_loop is not None:
                return relier.tasks.app.worker_loop
    except (ImportError, AttributeError):
        pass

    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
