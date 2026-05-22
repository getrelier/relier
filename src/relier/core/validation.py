"""Relier Runtime Validation Engine.

Enforces critical engine-level invariants across external dependencies to
protect Relier's strict zero-job-loss guarantees.

These validation checks are designed to run eagerly during worker bootstrap
(within the post-fork initialization hook) to trap misconfigurations before
the worker begins pulling workloads from the broker.

Attributes:
    logger (logging.Logger): Module-level logger for structured operational telemetry.
"""

import logging

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from relier.config import Settings

logger = logging.getLogger(__name__)


async def validate_redis_reachable(redis: Redis, settings: Settings) -> None:
    """Fail fast at startup if Redis cannot be reached.

    Relier coordinates entirely through Redis, the same way Celery depends on
    its broker and has no local fallback. Rather than letting a missing or
    misconfigured Redis surface later as a confusing mid-operation connection
    error, this check runs during process bootstrap so a worker or the
    resurrector refuses to start at all when Redis is unreachable.

    Args:
        redis: An active, loop-affined ``redis.asyncio.Redis`` client.
        settings: The consolidated runtime configuration.

    Raises:
        RuntimeError: If Redis does not respond to a ``PING``.
    """
    try:
        await redis.ping()
    except Exception as exc:
        if settings.redis_use_sentinel:
            target = f"Sentinel [{settings.redis_sentinel_nodes}]"
        else:
            url = settings.redis_url
            target = f"{url.host}:{url.port}"
        raise RuntimeError(
            f"Relier cannot reach Redis ({target}). Relier requires a running "
            f"Redis instance to coordinate tasks, there is no local fallback. "
            f"Start Redis, or point Relier at one via RELIER_REDIS_URL. "
            f"Refusing to start."
        ) from exc

    logger.info("Redis connectivity verified.")


async def validate_redis_config(redis: Redis, settings: Settings) -> None:
    """Enforces Redis configuration invariants required for transactional durability.

    This check is executed exactly once per worker process lifecycle during
    the post-fork bootstrap phase. It is intentionally omitted from the hot path
    (per-task execution loops) to eliminate runtime latency overhead.

    Args:
        redis: An active, loop-affined `redis.asyncio.Redis` client instance.
        settings: The consolidated operational runtime configuration object.

    Raises:
        RuntimeError: If the remote engine's memory eviction policy is configured
            dangerously, or if the underlying driver fails to query the engine configuration
            due to network degradation or restrictive access control lists (ACLs).

    Note:
        The choice of `noeviction` is non-negotiable for Relier. Under heavy memory pressure,
        alternative policies like `allkeys-lru` will silently discard key-value pairs matching
        the `rl:phoenix:*` namespace, leading to catastrophic, untraceable data loss.
    """
    # CHECK 1: Ensure zero-eviction memory safety boundaries
    try:
        config = await redis.config_get("maxmemory-policy")
    except ResponseError as exc:
        # The CONFIG command is commonly disabled, renamed, or ACL-restricted
        # on managed Redis offerings (AWS ElastiCache, etc.). That is a
        # verification gap, not a fault degrade to a warning instead of
        # blocking worker startup, which would make Relier unrunnable there.
        logger.warning(
            "Unable to verify Redis 'maxmemory-policy' because the CONFIG command "
            "is disabled or restricted (common on managed Redis). Relier cannot "
            "confirm the eviction policy automatically, ensure it is set to "
            "'noeviction' so 'rl:phoenix:*' job payloads are never evicted.",
            extra={"error": str(exc)},
        )
        return
    except Exception as exc:
        # A genuine driver/connectivity failure: the worker cannot operate
        # without Redis, so this remains a hard failure.
        logger.error(
            "CRITICAL: System validation blocked. Unable to reach Redis to inspect "
            "the maxmemory-policy. This typically indicates the connection dropped "
            "mid-handshake.",
            exc_info=True,
        )
        raise RuntimeError(
            "Validation blocked: Cannot reach Redis to verify engine configuration."
        ) from exc

    policy = config.get("maxmemory-policy", "")
    if policy != "noeviction":
        raise RuntimeError(
            f"FATAL CONFIGURATION MISMATCH: Redis maxmemory-policy is set to '{policy}'.\n\n"
            f"Relier explicitly requires a 'noeviction' policy to guarantee zero job loss.\n\n"
            f"Threat Model:\n"
            f"  Under extreme memory pressure, volatile or LRU eviction algorithms (e.g., 'allkeys-lru') "
            f"  will drop 'rl:phoenix:*' schemas out of memory. This silently destroys job payloads "
            f"  before they can be durably acknowledged or routed by healthy workers.\n\n"
            f"Remediation Runbook:\n"
            f"  1. Live Alteration:   redis-cli CONFIG SET maxmemory-policy noeviction\n"
            f"  2. Persistent Config: Append 'maxmemory-policy noeviction' to your redis.conf\n"
            f"  3. Docker Containers: Append '--maxmemory-policy noeviction' to the server command line.\n\n"
            f"Current Unsafe Engine State: {policy}"
        )

    logger.info(
        "Redis memory eviction strategy verified successfully.",
        extra={"policy": policy},
    )

    # CHECK 2: Ensure append-only durability is active.
    # Unlike the eviction policy this is a warning, not a hard failure: RDB-only
    # operation is degraded but still runnable, and CONFIG may be restricted on
    # managed Redis. A loud warning is enough to surface the misconfiguration.
    try:
        aof_config = await redis.config_get("appendonly")
    except ResponseError:
        logger.warning(
            "Unable to verify Redis AOF ('appendonly') because the CONFIG command "
            "is disabled or restricted. Ensure append-only persistence is enabled "
            "so acknowledged writes survive a Redis crash."
        )
        return
    except Exception:
        # A genuine connectivity failure here is already fatal via CHECK 1's
        # path on the next call; tolerate it rather than masking that error.
        logger.warning("Skipped Redis AOF verification due to a transient error.")
        return

    appendonly = aof_config.get("appendonly", "")
    if appendonly != "yes":
        logger.warning(
            "Redis AOF persistence is DISABLED (appendonly='%s'). Relier is running "
            "without durable append-only journalling acknowledged coordination "
            "state will be lost if Redis crashes between RDB snapshots. Enable it "
            "with 'appendonly yes' (see scripts/redis/redis.conf).",
            appendonly,
        )
    else:
        logger.info("Redis AOF persistence verified successfully.")


async def validate_connection_pool(settings: Settings) -> None:
    """Evaluates cluster connection topologies against upper file-descriptor bounds.

    Calculates worst-case file-descriptor consumption using the heuristic formula:
    Estimated Connections = Total Workers * Concurrency Per Worker * Max Connection Pool Size

    This evaluation issues non-blocking warnings instead of hard failures, as
    distributed topologies (such as Redis Cluster, Sentinel, or high-capacity
    enterprise proxies) are architected to absorb massive connection footprints.

    Args:
        settings: The consolidated operational runtime configuration object containing
            concurrency, worker layout, and network pool restrictions.

    Returns:
        None

    """
    # Calculate global connection scaling ceilings
    estimated_total = (
        settings.celery_worker_count
        * settings.celery_worker_concurrency
        * settings.redis_max_connections
    )

    # 8,000 is chosen as a defensive watermark (~80% of standard Redis 10k maxclients limit)
    if estimated_total > 8000:
        logger.warning(
            "HIGH POTENTIAL REDIS CONNECTION PRESSURE DETECTED",
            extra={
                "estimated_total_connections": estimated_total,
                "workers": settings.celery_worker_count,
                "concurrency": settings.celery_worker_concurrency,
                "pool_size": settings.redis_max_connections,
                "remediation_hint": (
                    f"The computed worst-case connection matrix ({estimated_total}) approaches or "
                    f"exceeds standard engine limits. To lower risk of socket starvation, consider:\n"
                    f"  1. Throttling local pool sizing via 'redis_max_connections' (current: {settings.redis_max_connections}).\n"
                    f"  2. Transitioning to a distributed Redis Cluster layout to scale socket handshakes linearly.\n"
                    f"  3. Multiplexing tracking signals through a high-availability Redis Sentinel or proxy layer.\n"
                    f"  4. Elevating the underlying infrastructure threshold using 'CONFIG SET maxclients <val>'."
                ),
            },
        )
    else:
        logger.info(
            "Connection pool footprint verified within safe infrastructural limits.",
            extra={"estimated_total_connections": estimated_total},
        )
