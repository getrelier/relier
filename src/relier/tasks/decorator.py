"""
Relier Task Decorator — ``@rl_task``.

The single user-facing primitive that wraps any Celery task with the full
Relier reliability stack:

* **Schema versioning** — payloads dispatched via ``delay_versioned()`` are
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
from typing import Any

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from opentelemetry import trace

from relier.core.exceptions import (
    IdempotencyInFlightError,
    PayloadIntegrityError,
    SchemaMigrationError,
)
from relier.core.idempotency import idempotency_manager
from relier.core.phoenix import PhoenixRegistry
from relier.core.schema import SchemaRegistry
from relier.core.slo import slo_metrics
from relier.core.timeouts import TimeoutEnforcer
from relier.storage.redis import get_relier_redis
from relier.tasks.context import TaskContext, _task_context_var
from relier.telemetry.metrics import (
    idempotency_hits_total,
    task_duration_ms,
    tasks_total,
)
from relier.telemetry.setup import get_tracer

tracer = get_tracer("relier.tasks")

logger = logging.getLogger(__name__)


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
    """

    def decorator(func: Callable) -> Callable:
        """The actual decorator applied to the user function."""
        is_async = inspect.iscoroutinefunction(func)

        if not is_async and (soft_timeout or hard_timeout or on_soft_timeout):
            logger.warning(
                f"Sync function '{func.__name__}' decorated with timeout params. "
                "Timeouts are only supported for async tasks; they will be ignored."
            )
            raise ValueError(
                "Timeout parameters are only supported for async functions."
            )

        task_name = f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            """The Celery task wrapper function that orchestrates the entire lifecycle."""
            task_id: str = self.request.id or str(uuid.uuid4())
            worker_id: str = self.request.hostname or "unknown-worker"
            loop = _get_worker_loop()

            # ------------------------------------------------------------------
            # Schema envelope unwrapping
            # ------------------------------------------------------------------
            _schema_error = None
            if args and isinstance(args[0], dict) and "schema_version" in args[0]:
                try:
                    args, kwargs = SchemaRegistry.unwrap_and_migrate(task_name, args[0])
                except (SchemaMigrationError, PayloadIntegrityError) as exc:
                    logger.critical(
                        "Task failed schema validation; quarantining.",
                        extra={"task_id": task_id, "error": str(exc)},
                    )
                    # Quarantine is handled inside _orchestrate() which has
                    # async context.  Flag the error and fall through.
                    _schema_error = exc

            # ------------------------------------------------------------------
            # Phoenix payload — stored in Redis for resurrection
            # ------------------------------------------------------------------
            phoenix_payload = {
                "task_name": self.name,
                "args": list(args),
                "kwargs": kwargs,
                "queue": (getattr(self.request, "delivery_info", {}) or {}).get(
                    "routing_key", queue
                ),
                "worker_id": worker_id,
            }

            async def _orchestrate() -> Any:
                """Async lifecycle manager — all Relier logic runs here."""
                logger.info(
                    "Starting orchestration.",
                    extra={"task_id": task_id, "worker_id": worker_id},
                )

                # Handle schema failures in async context where await is available.
                if _schema_error is not None:
                    from relier.core.dlq import DeadLetterQueue

                    await DeadLetterQueue.quarantine(task_id, reason=str(_schema_error))
                    return {"status": "quarantined", "error": str(_schema_error)}

                idem_result = None
                redis = await get_relier_redis()

                await redis.set(f"rl:presence:{worker_id}", "1", ex=60)

                if idempotent:
                    arg_sig = json.dumps(
                        {"a": list(args), "k": kwargs},
                        sort_keys=True,
                        default=str,
                    )
                    arg_hash = hashlib.sha256(arg_sig.encode()).hexdigest()[:16]
                    idem_key = f"{func.__name__}:{arg_hash}"

                    idem_result = await idempotency_manager.check_or_claim(
                        idem_key, idempotency_ttl
                    )
                    if idem_result.already_executed:
                        logger.info(
                            "Idempotency hit; returning cached result.",
                            extra={"task_id": task_id, "key": idem_key},
                        )
                        idempotency_hits_total.add(1, {"rl.task.name": task_name})
                        tasks_total.add(
                            1, {"status": "idempotency_hit", "rl.task.name": task_name}
                        )
                        return idem_result.cached_result

                await PhoenixRegistry.register(task_id, worker_id, phoenix_payload)

                inflight_key = f"rl:inflight:{worker_id}"

                try:
                    from relier.tasks.app import shutdown_handler as _sh
                except ImportError:
                    _sh = None

                if _sh is not None:
                    _sh.track_task(task_id)

                pipe = redis.pipeline()
                pipe.sadd("rl:workers", worker_id)
                pipe.zadd(inflight_key, {task_id: time.time()})
                await pipe.execute()

                start_time = time.perf_counter()

                checkpoint = kwargs.pop("checkpoint", None)

                ctx = TaskContext(
                    task_id=task_id,
                    task_name=task_name,
                    args=args,
                    kwargs=kwargs,
                    worker_id=worker_id,
                    partial_result=checkpoint,
                )

                token = _task_context_var.set(ctx)

                try:
                    # Check if the user wants the context
                    sig = inspect.signature(func)
                    if "ctx" in sig.parameters:
                        kwargs["ctx"] = ctx

                    with tracer.start_as_current_span(
                        f"rl.task.{task_name}",
                        attributes={
                            "rl.task.id": task_id,
                            "rl.task.name": task_name,
                            "rl.worker.id": worker_id,
                            "rl.queue": phoenix_payload["queue"],
                        },
                    ) as span:
                        try:
                            if is_async:
                                if soft_timeout or hard_timeout:
                                    result = await TimeoutEnforcer.run(
                                        func,
                                        args,
                                        kwargs,
                                        soft=soft_timeout,
                                        hard=hard_timeout,
                                        on_soft=on_soft_timeout,
                                        task_id=task_id,
                                    )
                                else:
                                    result = await func(*args, **kwargs)
                            else:
                                result = await asyncio.to_thread(func, *args, **kwargs)

                            if idempotent and idem_result is not None:
                                await idem_result.record_result(result)

                            # Record Success
                            await slo_metrics.record_event("success")
                            tasks_total.add(
                                1, {"status": "completed", "rl.task.name": task_name}
                            )
                            span.set_status(trace.Status(trace.StatusCode.OK))
                            return result

                        except TimeoutError as exc:
                            logger.error(
                                "Task hard timeout exceeded.",
                                extra={
                                    "task_id": task_id,
                                    "hard_timeout": hard_timeout,
                                },
                            )
                            await slo_metrics.record_event("failure")
                            tasks_total.add(
                                1,
                                {
                                    "status": "failed",
                                    "rl.task.name": task_name,
                                    "reason": "timeout",
                                },
                            )
                            span.record_exception(exc)
                            span.set_status(
                                trace.Status(
                                    trace.StatusCode.ERROR, "Hard timeout exceeded"
                                )
                            )
                            raise SoftTimeLimitExceeded(str(exc)) from exc
                        except Exception as exc:
                            logger.error(
                                "Task execution failed.",
                                extra={"task_id": task_id},
                                exc_info=True,
                            )
                            await slo_metrics.record_event("failure")
                            tasks_total.add(
                                1, {"status": "failed", "rl.task.name": task_name}
                            )
                            span.record_exception(exc)
                            span.set_status(
                                trace.Status(trace.StatusCode.ERROR, str(exc))
                            )
                            raise
                        finally:
                            duration_ms = (time.perf_counter() - start_time) * 1000
                            task_duration_ms.record(
                                duration_ms, {"rl.task.name": task_name}
                            )

                            if _sh is not None:
                                _sh.untrack_task(task_id)

                            pipe = redis.pipeline()
                            pipe.zrem(inflight_key, task_id)
                            await pipe.execute()
                            await PhoenixRegistry.complete(task_id)
                finally:
                    _task_context_var.reset(token)

            try:
                if loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(_orchestrate(), loop)
                    return future.result()
                else:
                    return loop.run_until_complete(_orchestrate())
            except IdempotencyInFlightError as exc:
                raise self.retry(exc=exc, countdown=5) from exc
            except Exception as exc:
                logger.error(
                    "Async bridge failed.",
                    extra={"task_id": task_id, "error": str(exc)},
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

        def delay_versioned(*d_args: Any, **d_kwargs: Any) -> Any:
            """Enqueue the task wrapped in a signed, versioned schema envelope."""
            task_id = str(uuid.uuid4())
            envelope = SchemaRegistry.wrap(task_id, d_args, d_kwargs)
            return task.apply_async(args=(envelope,), queue=queue, task_id=task_id)

        task.delay_versioned = delay_versioned
        return task  # type: ignore[no-any-return]

    return decorator


def _get_worker_loop() -> asyncio.AbstractEventLoop:
    """Return the persistent asyncio event loop for this worker process."""
    try:
        import relier.tasks.app

        if relier.tasks.app.worker_loop is not None:
            return relier.tasks.app.worker_loop

        import os

        if "CELERY_LOADER" in os.environ:
            from relier.tasks.app import init_worker

            logger.warning("init_worker didn't fire; performing lazy initialization.")
            init_worker()
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
