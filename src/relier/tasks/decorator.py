"""
Relier Task Decorator — ``@rl_task``.

The single user-facing primitive that wraps any Celery task with the full
Relier reliability stack:

* **Schema versioning** — payloads dispatched via ``push()`` or ``apush()``
  are wrapped in a signed, versioned envelope and migrated on the worker side.
* **Idempotency** — optional duplicate-execution prevention backed by Redis.
* **Phoenix heartbeat** — heartbeat emitted every N seconds so the resurrector
  can detect worker crashes in < 35 s.
* **Inflight registry** — sorted-set tracking for ``rl tasks inflight``.
* **Timeout enforcement** — soft + hard two-tier timeouts with cleanup hooks.
* **Graceful shutdown integration** — task is tracked so the drain loop knows
  when it is safe to exit.

The decorator preserves your function's exact type signature so IDE tools
(VS Code, PyCharm) provide full autocomplete, hover documentation, and type
linting for all arguments — exactly as they would for the unwrapped function.

Example:
    >>> from relier.tasks.decorator import rl_task
    >>> from relier.tasks.context import TaskContext
    >>>
    >>> @rl_task(
    ...     queue="high_priority",
    ...     idempotent=True,
    ...     idempotency_ttl=3600,
    ...     soft_timeout=25,
    ...     hard_timeout=30,
    ... )
    ... async def process_document(doc_id: str, ctx: TaskContext) -> dict:
    ...     if ctx.partial_result:
    ...         resume_from = ctx.partial_result["step"]
    ...     result = await embed_and_store(doc_id)
    ...     return result
    >>>
    >>> # Async dispatch (FastAPI / async Django):
    >>> async def api_handler(doc_id: str):
    ...     receipt = await process_document.apush(doc_id=doc_id)
    ...     return {"task_id": receipt.id}
    >>>
    >>> # Sync dispatch (Django views, Flask routes, scripts):
    >>> def sync_handler(doc_id: str):
    ...     receipt = process_document.push(doc_id=doc_id)
    ...     return receipt.id

Timeout features are only supported for ``async def`` functions.  Sync tasks
will raise a ``ValueError`` at decoration time if timeout parameters are given.
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
from typing import Any, Generic, ParamSpec, TypeVar, cast

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
# Queue topology
# =============================================================================

INTERNAL_QUEUES = {"re-queue"}
"""Queues reserved exclusively for Relier's internal recovery machinery."""

PUBLIC_QUEUES = {
    "high_priority",
    "default",
    "low_priority",
}
"""Queues available for user task routing via ``@rl_task(queue=...)``.

Allowed values:

- ``"high_priority"`` — for latency-sensitive, user-facing work.
- ``"default"`` — for standard background processing.
- ``"low_priority"`` — for bulk or best-effort jobs.
"""

ALL_QUEUES = PUBLIC_QUEUES | INTERNAL_QUEUES

# =============================================================================
# Type parameters for the typed decorator return value
# =============================================================================

P = ParamSpec("P")
"""Captures the parameter specification of the decorated task function."""

R = TypeVar("R")
"""Captures the return type of the decorated task function."""


# =============================================================================
# RelierTask — typed task handle returned by @rl_task()
# =============================================================================


class RelierTask(Generic[P, R]):
    """A Relier-managed Celery task with type-safe dispatch helpers.

    You never instantiate this class directly.  It is the return type of
    :func:`rl_task` and exists so that IDE tools see the exact parameter
    signature of your underlying function on every dispatch call.

    Attributes:
        __name__:  The unqualified function name (e.g. ``"process_document"``).
        __doc__:   The docstring from the original decorated function.

    Example:
        >>> @rl_task(queue="default")
        ... async def send_invoice(invoice_id: str, retry: bool = False) -> dict:
        ...     ...
        >>>
        >>> # IDE shows:  send_invoice.push(invoice_id: str, retry: bool = False)
        >>> receipt = send_invoice.push(invoice_id="inv-001")
        >>> # IDE shows:  await send_invoice.apush(invoice_id: str, retry: bool = False)
        >>> receipt = await send_invoice.apush(invoice_id="inv-001", retry=True)
    """

    # Celery Task attributes present at runtime via shared_task/cast machinery
    request: Any
    name: str
    retry: Any
    apply: Any
    apply_async: Any
    __wrapped__: Any

    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task body synchronously (Celery internal path)."""
        raise NotImplementedError

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Invoke the task synchronously via the Celery worker (internal path).

        This path is used by Celery internally when a worker executes the
        task body.  Application code should use :meth:`push` or
        :meth:`apush` to dispatch work through the broker with full
        Relier reliability guarantees.

        Args:
            *args:   Positional arguments forwarded to the task function.
            **kwargs: Keyword arguments forwarded to the task function.

        Returns:
            The return value of the decorated task function.
        """
        raise NotImplementedError  # Replaced by Celery task machinery at runtime.

    def push(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        """Dispatch the task synchronously from sync code.

        Performs an admission check, wraps the payload in a versioned
        envelope, and enqueues it via the Celery broker.  Returns
        immediately, does **not** block on task completion.

        Use this in Django views, Flask routes, CLI scripts, or anywhere
        an ``async`` call would be inconvenient.  Internally bridges to
        :meth:`apush` via the worker's persistent asyncio event loop.

        Args:
            *args:   Positional arguments forwarded to the task function.
            **kwargs: Keyword arguments forwarded to the task function.

        Returns:
            A Celery ``AsyncResult`` representing the enqueued task.  Call
            ``result.id`` for the task ID or ``result.get()`` to block on
            the outcome.

        Raises:
            AdmissionRejectedError: If the cluster is at capacity.

        Example:
            >>> receipt = process_document.push(doc_id="doc-123")
            >>> print(receipt.id)  # Celery task ID
        """
        raise NotImplementedError  # Replaced by the dynamic _RelierTaskBase at runtime.

    async def apush(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        """Dispatch the task asynchronously from async code.

        Performs an admission check, wraps the payload in a versioned
        envelope, and enqueues it via the Celery broker.  Returns
        immediately, does **not** block on task completion.

        Use this in FastAPI endpoints, async Django views, or anywhere
        you already have an ``async`` context.

        Args:
            *args:   Positional arguments forwarded to the task function.
            **kwargs: Keyword arguments forwarded to the task function.

        Returns:
            A Celery ``AsyncResult`` representing the enqueued task.

        Raises:
            AdmissionRejectedError: If the cluster is at capacity.  The
                exception carries a ``retry_after`` attribute with the
                suggested back-off duration in seconds.

        Example:
            >>> receipt = await process_document.apush(doc_id="doc-123")
            >>> return {"task_id": receipt.id}
        """
        raise NotImplementedError  # Replaced by the dynamic _RelierTaskBase at runtime.

    def delay(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        """Celery-native dispatch, bypasses Relier's reliability envelope.

        Equivalent to Celery's built-in ``task.delay()``.  Routes directly
        through the broker **without** admission control, schema wrapping,
        idempotency, or OpenTelemetry injection.

        .. warning::
            Tasks dispatched via ``delay()`` will **not** be tracked by the
            Phoenix resurrector and will be lost if the worker crashes
            mid-execution.  Use :meth:`push` or :meth:`apush` for all
            production dispatches.

        This method is exposed for benchmarking and migration convenience
        only.

        Args:
            *args:   Positional arguments forwarded to the task function.
            **kwargs: Keyword arguments forwarded to the task function.

        Returns:
            A Celery ``AsyncResult``.
        """
        raise NotImplementedError  # Delegated to the Celery task object at runtime.


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
) -> Callable[[Callable[P, R]], RelierTask[P, R]]:
    """Wrap a function with the full Relier reliability stack.

    Returns a :class:`RelierTask` that preserves your function's exact type
    signature so IDE tools provide full autocomplete and type-checking on
    every ``push()`` and ``apush()`` call.

    Args:
        queue:            Celery routing queue.  Must be one of
                          ``"high_priority"``, ``"default"``, or
                          ``"low_priority"``.
        idempotent:       Enable automatic deduplication.  When ``True``,
                          tasks with the same arguments will only execute
                          once within ``idempotency_ttl`` seconds.
        idempotency_ttl:  Seconds to cache idempotency results.  Calls with
                          the same arguments within this window return the
                          cached result without re-executing.  Default: 3600.
        soft_timeout:     Seconds before ``on_soft_timeout`` fires.  The task
                          continues running; use the hook to flush partial
                          state.  Async tasks only.
        hard_timeout:     Seconds before the task is unconditionally cancelled.
                          Must be strictly greater than ``soft_timeout``.
                          Async tasks only.
        on_soft_timeout:  Async callable invoked at the soft-timeout boundary.
                          Receives the current :class:`~relier.tasks.context.TaskContext`
                          as its only argument.  Use it to call
                          ``ctx.set_partial(...)`` before the hard deadline.

    Returns:
        A decorator that transforms your function into a
        :class:`RelierTask` with type-safe ``push`` and ``apush`` methods.

    Raises:
        ValueError: If ``queue`` is not in :data:`PUBLIC_QUEUES`.
        ValueError: If timeout parameters are passed to a sync function.
        ValueError: If ``soft_timeout >= hard_timeout``.
        ValueError: If ``hard_timeout >= RELIER_IDEMPOTENCY_INFLIGHT_TTL``
                    when ``idempotent=True`` (double-execution risk).

    Example:
        >>> @rl_task(
        ...     queue="high_priority",
        ...     idempotent=True,
        ...     idempotency_ttl=3600,
        ...     soft_timeout=25,
        ...     hard_timeout=30,
        ... )
        ... async def process_document(doc_id: str) -> dict:
        ...     result = await embed_and_store(doc_id)
        ...     return result
        >>>
        >>> # Async dispatch (FastAPI):
        >>> receipt = await process_document.apush(doc_id="abc-123")
        >>>
        >>> # Sync dispatch (Django view):
        >>> receipt = process_document.push(doc_id="abc-123")
    """
    from relier.config import get_settings

    settings = get_settings()

    if queue not in PUBLIC_QUEUES:
        raise ValueError(
            f"Unknown queue '{queue}'.  Allowed public queues: "
            f"{sorted(PUBLIC_QUEUES)}.  "
            f"Fix: change @rl_task(queue=...) to one of the allowed values."
        )

    if (
        idempotent
        and hard_timeout is not None
        and hard_timeout >= settings.idempotency_inflight_ttl
    ):
        raise ValueError(
            f"CONFIGURATION ERROR: hard_timeout ({hard_timeout}s) must be "
            f"< RELIER_IDEMPOTENCY_INFLIGHT_TTL "
            f"({settings.idempotency_inflight_ttl}s).\n"
            "\n"
            "Why: If a task runs longer than INFLIGHT_TTL the idempotency key "
            "expires while the task is still executing, allowing a duplicate "
            "worker to start the same task (double execution).\n"
            "\n"
            "Fix: either increase RELIER_IDEMPOTENCY_INFLIGHT_TTL or reduce "
            "hard_timeout.  Safe formula: hard_timeout < INFLIGHT_TTL - 10s."
        )

    if (
        soft_timeout is not None
        and hard_timeout is not None
        and soft_timeout >= hard_timeout
    ):
        raise ValueError(
            f"soft_timeout ({soft_timeout}s) must be strictly less than "
            f"hard_timeout ({hard_timeout}s).  "
            "Fix: lower soft_timeout or raise hard_timeout."
        )

    def decorator(func: Callable[P, R]) -> RelierTask[P, R]:
        """Apply Relier's reliability stack to a single task function."""
        is_async = inspect.iscoroutinefunction(func)

        if not is_async and (soft_timeout or hard_timeout or on_soft_timeout):
            raise ValueError(
                f"@rl_task on '{func.__name__}': timeout parameters "
                "(soft_timeout, hard_timeout, on_soft_timeout) are only "
                "supported for async functions.  Either convert the function "
                "to 'async def' or remove the timeout arguments."
            )

        task_name = f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            """Execute the task inside Relier's reliability orchestration layer."""
            task_id: str = self.request.id or str(uuid.uuid4())
            worker_id: str = self.request.hostname or "unknown-worker"
            loop = _get_worker_loop()

            # ------------------------------------------------------------------
            # Phoenix payload stored in Redis for resurrection.
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
                # as one. Otherwise fall back to using args/kwargs directly, this
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
                    # rl.schema.migrate unwrap and migrate the task envelope.
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
                            actual_args, actual_kwargs = args, kwargs
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
                        # rl.task.execute, the user function runs here.
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
                                        result = await func(  # type: ignore[misc]
                                            *actual_args, **actual_kwargs
                                        )
                                else:
                                    result = await asyncio.to_thread(
                                        func, *actual_args, **actual_kwargs
                                    )

                                # FENCING CHECK (END), before committing results.
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

        # Per-task Celery base class. Holding apush/push as METHODS on the
        # class (rather than as monkey-patched instance attributes) survives
        # Celery re-resolving the underlying Task instance from the app
        # registry which `shared_task`'s PromiseProxy does on the first
        # touch of `celery_app`, silently swapping the underlying object.
        # A class-bound method is part of the type lookup, so the helpers
        # stay attached no matter how many times Celery re-registers.
        from celery import Task

        async def _apush(self: Any, *d_args: Any, **d_kwargs: Any) -> Any:
            """Async dispatch, use in FastAPI or async Django.

            Args:
                *d_args:   Positional arguments forwarded to the task function.
                **d_kwargs: Keyword arguments forwarded to the task function.

            Returns:
                A Celery ``AsyncResult`` for the enqueued task.

            Raises:
                AdmissionRejectedError: If the cluster is at capacity.
            """
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
                        f"Relier cluster at capacity.  Retry after {retry_after}s.  "
                        "Consider increasing RELIER_ADMISSION_LIMIT or "
                        "RELIER_ADMISSION_WINDOW, or add back-pressure handling "
                        "in your caller.",
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
                    task=self,
                    queue=queue,
                    envelope=envelope,
                    task_id=task_id,
                )

        def _push(self: Any, *d_args: Any, **d_kwargs: Any) -> Any:
            """Sync dispatch, use in Django views, Flask routes, or scripts.

            Blocks the calling thread for ~1 ms (admission check) then
            dispatches.  Does **not** block on task completion, fire-and-forget.

            Args:
                *d_args:   Positional arguments forwarded to the task function.
                **d_kwargs: Keyword arguments forwarded to the task function.

            Returns:
                A Celery ``AsyncResult`` for the enqueued task.

            Raises:
                AdmissionRejectedError: If the cluster is at capacity.
            """
            try:
                # Inside a Celery worker, reuse the persistent loop.
                import relier.tasks.app

                loop = relier.tasks.app.worker_loop
                if loop and loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(
                        self.apush(*d_args, **d_kwargs), loop
                    )
                    return future.result(timeout=5.0)
            except (ImportError, AttributeError):
                pass

            # Outside Celery (Django view, Flask route, script).
            return asyncio.run(self.apush(*d_args, **d_kwargs))

        _RelierTaskBase = type(
            f"RelierTask_{task_name.replace('.', '_')}",
            (Task,),
            {"apush": _apush, "push": _push},
        )

        task = shared_task(
            name=task_name,
            queue=queue,
            bind=True,
            base=_RelierTaskBase,
            acks_late=True,
            max_retries=15,
            reject_on_worker_lost=True,
        )(wrapper)

        return cast(RelierTask[P, R], task)

    return cast(Callable[[Callable[P, R]], RelierTask[P, R]], decorator)


# =============================================================================
# Helpers
# =============================================================================


def _validate_public_dispatch(queue: str) -> None:
    """Prevent user-facing APIs from publishing into internal queues.

    Args:
        queue: The queue name to validate.

    Raises:
        ValueError: If ``queue`` is in :data:`INTERNAL_QUEUES`.
        RuntimeError: If ``queue`` is not in :data:`ALL_QUEUES`.
    """
    if queue in INTERNAL_QUEUES:
        raise ValueError(
            f"Queue '{queue}' is reserved for Relier's internal recovery "
            "machinery and cannot be used for task routing.  "
            f"Fix: use one of {sorted(PUBLIC_QUEUES)} in @rl_task(queue=...)."
        )

    if queue not in PUBLIC_QUEUES:
        raise RuntimeError(
            f"Unknown queue '{queue}'.  Allowed queues: {sorted(PUBLIC_QUEUES)}."
        )


async def _dispatch_internal(
    *,
    task: Any,
    queue: str,
    envelope: dict[str, Any],
    task_id: str,
) -> Any:
    """Internal privileged dispatch path for Relier recovery subsystems.

    Routes via ``celery_app.send_task()`` rather than ``task.apply_async()``
    so the broker connection is drawn from the app-level pool, thread-safe
    and correct when called via ``run_in_executor``.

    Args:
        task:     The Celery task object whose ``name`` identifies the target.
        queue:    Target queue (must be in :data:`ALL_QUEUES`).
        envelope: Versioned Relier payload envelope.
        task_id:  Pre-generated UUID string for the task.

    Returns:
        The Celery ``AsyncResult`` from ``send_task``.

    Raises:
        RuntimeError: If ``queue`` is not in :data:`ALL_QUEUES`.
    """
    if queue not in ALL_QUEUES:
        raise RuntimeError(
            f"Unknown queue '{queue}'.  Allowed queues: {sorted(ALL_QUEUES)}."
        )

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
    """Return the worker-scoped asyncio event loop.

    Resolution order:
    1. The persistent ``relier.tasks.app.worker_loop`` set by the Celery
       worker-process-init hook.
    2. Lazy initialisation inside Celery workers (if the signal fired late).
    3. The running loop in the current thread (for test contexts).
    4. A freshly created loop as a last resort (CLI / script usage).

    Returns:
        An active :class:`asyncio.AbstractEventLoop`.
    """
    try:
        import relier.tasks.app

        if relier.tasks.app.worker_loop is not None:
            return relier.tasks.app.worker_loop

        import os

        if "CELERY_LOADER" in os.environ:
            from relier.tasks.app import init_worker_process

            logger.warning("init_worker didn't fire; performing lazy initialisation.")
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
