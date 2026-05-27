"""
Relier Configuration Engine.

Defines all global settings for the Relier library using Pydantic V2
``BaseSettings``.  Values are sourced in priority order from
environment variables (``RELIER_*``), a ``.env`` file, and the declared
defaults below.

All fields are validated at construction time.  Cross-field invariants
(e.g. timeout ordering, idempotency TTL safety windows) are enforced by
model validators so misconfiguration is surfaced at process start-up
rather than silently at runtime.

Example:
    >>> from relier.config import get_settings
    >>> s = get_settings()
    >>> s.heartbeat_ttl  # seconds before a crashed worker is detected
    10
    >>> s.redis_url.host
    'localhost'
"""

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, RedisDsn, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Validated, immutable configuration for the Relier reliability layer.

    Every public attribute maps 1-to-1 to a ``RELIER_`` environment variable.
    Pydantic validates types and cross-field invariants at construction time,
    so any misconfiguration raises a descriptive ``ValidationError`` before a
    single task is dispatched.

    Configuration priority:
        1. Environment variables (``RELIER_*``)
        2. ``.env`` file in the working directory
        3. Declared defaults in this class

    Example:
        >>> import os
        >>> os.environ["RELIER_HEARTBEAT_TTL"] = "15"
        >>> from relier.config import Settings
        >>> s = Settings()
        >>> s.heartbeat_ttl
        15
        >>> del os.environ["RELIER_HEARTBEAT_TTL"]
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="RELIER_",
        extra="ignore",
        frozen=True,
    )

    # =========================================================================
    # Core
    # =========================================================================

    env: Literal["development", "staging", "production"] = Field(
        default="development",
        description=(
            "Deployment environment.  Controls debug task registration and "
            "log formatting.  Allowed values: 'development', 'staging', "
            "'production'."
        ),
    )
    secret_key: SecretStr = Field(
        default=SecretStr("change-in-production"),
        description=(
            "Secret used for internal signing operations.  Must be changed "
            "before deploying to production.  Set via RELIER_SECRET_KEY."
        ),
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Minimum log level emitted by all Relier subsystems.",
    )

    # =========================================================================
    # Infrastructure — Redis (direct connection)
    # =========================================================================

    redis_url: RedisDsn = Field(  # type: ignore[assignment]
        default="redis://localhost:6379/0",
        description=(
            "Redis connection URL.  Must use the 'redis://', 'rediss://', or "
            "'redis+unix://' scheme.  Example: 'redis://:password@host:6379/0'. "
            "Ignored when RELIER_REDIS_USE_SENTINEL=true."
        ),
    )
    redis_max_connections: int = Field(
        default=20,
        gt=0,
        description=(
            "Maximum connections in the async Redis pool per worker process. "
            "Increase if you see 'ConnectionPool is full' errors under high "
            "concurrency.  Use validate_connection_pool() to size this safely."
        ),
    )
    redis_socket_timeout: float = Field(
        default=5.0,
        gt=0.0,
        description=(
            "Seconds to wait for a response from Redis before raising a "
            "TimeoutError.  Should be comfortably below soft_timeout."
        ),
    )
    redis_connect_timeout: float = Field(
        default=2.0,
        gt=0.0,
        description=(
            "Seconds to wait when establishing a new TCP connection to Redis."
        ),
    )
    redis_health_check_interval: int = Field(
        default=30,
        ge=0,
        description=(
            "Seconds between automatic health-check pings sent on idle "
            "connections.  Set to 0 to disable.  Recommended: 30."
        ),
    )
    redis_password: SecretStr | None = Field(
        default=None,
        description=(
            "Password for the Redis data nodes (requirepass / masterauth). "
            "Applies to both direct and Sentinel-managed connections. "
            "Set via RELIER_REDIS_PASSWORD."
        ),
    )

    # =========================================================================
    # Infrastructure — Redis Sentinel (high availability)
    # =========================================================================

    redis_use_sentinel: bool = Field(
        default=False,
        description=(
            "Route Redis connections through Sentinel for automatic master "
            "discovery and failover.  When false, Relier connects directly "
            "to redis_url."
        ),
    )
    redis_sentinel_nodes: str = Field(
        default="",
        description=(
            "Comma-separated 'host:port' list of Sentinel instances, e.g. "
            "'sentinel-1:26379,sentinel-2:26379,sentinel-3:26379'.  "
            "Required when RELIER_REDIS_USE_SENTINEL=true."
        ),
    )
    redis_sentinel_master_name: str = Field(
        default="relier-master",
        description=(
            "Name of the monitored master group as declared in "
            "'sentinel monitor <name> ...'."
        ),
    )
    redis_sentinel_password: SecretStr | None = Field(
        default=None,
        description=(
            "Password for authenticating to the Sentinel instances themselves "
            "(Sentinel 'requirepass').  Distinct from redis_password which "
            "authenticates to the data nodes."
        ),
    )

    @property
    def sentinel_node_list(self) -> list[tuple[str, int]]:
        """Parse ``redis_sentinel_nodes`` into ``(host, port)`` tuples.

        Returns:
            A list of ``(host, port)`` tuples parsed from the comma-separated
            ``RELIER_REDIS_SENTINEL_NODES`` value.

        Raises:
            ValueError: If Sentinel is enabled but no valid nodes are
                configured, or if any node entry is not in ``host:port`` form.

        Example:
            >>> import os
            >>> os.environ["RELIER_REDIS_USE_SENTINEL"] = "true"
            >>> os.environ["RELIER_REDIS_SENTINEL_NODES"] = "s1:26379,s2:26379"
            >>> Settings().sentinel_node_list
            [('s1', 26379), ('s2', 26379)]
        """
        nodes: list[tuple[str, int]] = []
        for entry in self.redis_sentinel_nodes.split(","):
            entry = entry.strip()
            if not entry:
                continue
            host, _, port = entry.rpartition(":")
            if not host or not port.isdigit():
                raise ValueError(
                    f"Invalid Sentinel node '{entry}'.  Expected 'host:port', "
                    f"e.g. 'sentinel-1:26379'.  "
                    f"Fix: update RELIER_REDIS_SENTINEL_NODES."
                )
            nodes.append((host, int(port)))
        if self.redis_use_sentinel and not nodes:
            raise ValueError(
                "RELIER_REDIS_USE_SENTINEL is true but "
                "RELIER_REDIS_SENTINEL_NODES is empty.  "
                "Fix: set RELIER_REDIS_SENTINEL_NODES to a comma-separated "
                "list of 'host:port' Sentinel addresses."
            )
        return nodes

    # =========================================================================
    # Celery integration
    # =========================================================================

    celery_worker_count: int = Field(
        default=8,
        gt=0,
        description=(
            "Number of Celery worker processes.  Used for Redis connection "
            "pool sizing and admission control capacity estimation."
        ),
    )
    celery_worker_concurrency: int = Field(
        default=8,
        gt=0,
        description=(
            "Number of concurrent tasks per Celery worker process.  Used for "
            "Redis connection pool sizing and admission control."
        ),
    )

    # =========================================================================
    # Phoenix — task resurrection
    # =========================================================================

    heartbeat_ttl: int = Field(
        default=10,
        gt=0,
        description=(
            "Heartbeat TTL in seconds.  Worker death is detected after this "
            "key expires without renewal.  Decrease for faster crash detection "
            "at the cost of higher Redis write volume."
        ),
    )
    max_resurrections: int = Field(
        default=5,
        ge=0,
        description=(
            "Maximum resurrection attempts before a task is quarantined to the "
            "DLQ.  Set to 0 to disable resurrection entirely."
        ),
    )
    resurrection_check_interval: int = Field(
        default=2,
        gt=0,
        description="Interval in seconds between Phoenix resurrector scan passes.",
    )
    resurrection_requeue_delay: float = Field(
        default=0.05,
        gt=0.0,
        description="Delay in seconds between re-queuing consecutive resurrected tasks.",
    )
    resurrection_batch_size: int = Field(
        default=1000,
        gt=0,
        description=(
            "Maximum tasks re-queued per resurrector scan pass.  Caps the "
            "replay rate during a mass worker failure."
        ),
    )
    resurrection_max_queue_depth: int = Field(
        default=10000,
        gt=0,
        description=(
            "Backpressure threshold.  The resurrector skips its scan pass when "
            "the recovery queue already holds at least this many messages, "
            "preventing it from outrunning workers draining the queue."
        ),
    )
    resurrection_claim_grace_period: int = Field(
        default=30,
        gt=0,
        description=(
            "Seconds to wait after re-queuing a resurrected task before "
            "declaring it 'never claimed' and releasing it back to the expiry "
            "index.  Must outlast a cold Celery worker startup (mingle phase, "
            "Redis validation), otherwise the warning fires as a false "
            "positive during normal cold starts."
        ),
    )

    # =========================================================================
    # Idempotency
    # =========================================================================

    idempotency_default_ttl: int = Field(
        default=3600,
        gt=0,
        description=(
            "Default TTL in seconds for idempotency result-cache keys.  After "
            "this window, the same arguments will re-execute the task."
        ),
    )
    idempotency_inflight_ttl: int = Field(
        default=120,
        gt=0,
        description=(
            "TTL in seconds for in-flight idempotency locks.  Must be strictly "
            "greater than hard_timeout to prevent the lock from expiring while "
            "a task is still running (which would allow double execution).  "
            "Safe formula: RELIER_IDEMPOTENCY_INFLIGHT_TTL > hard_timeout + 10."
        ),
    )

    # =========================================================================
    # Checkpointing — partial-result storage
    # =========================================================================

    checkpoint_max_inline_bytes: int = Field(
        default=262144,
        gt=0,
        description=(
            "Serialised checkpoints at or below this size (bytes) are stored "
            "inline in the Phoenix Redis hash.  Larger ones spill to the blob "
            "backend, or are rejected when the backend is 'inline'."
        ),
    )
    checkpoint_backend: Literal["inline", "filesystem"] = Field(
        default="inline",
        description=(
            "Where oversized checkpoints are stored.  'inline' keeps everything "
            "in Redis and rejects anything over checkpoint_max_inline_bytes.  "
            "'filesystem' spills large checkpoints to checkpoint_dir."
        ),
    )
    checkpoint_dir: str = Field(
        default="/var/lib/relier/checkpoints",
        description=(
            "Directory used by the 'filesystem' checkpoint backend.  MUST be "
            "shared storage accessible by every worker process and the "
            "resurrector — a resurrected task may replay on a different host "
            "than the one that wrote the checkpoint."
        ),
    )

    # =========================================================================
    # Timeouts and graceful shutdown
    # =========================================================================

    soft_timeout: int = Field(
        default=25,
        gt=0,
        description=(
            "Seconds before the soft-timeout hook fires.  The task continues "
            "running after this point; use the hook to flush partial state. "
            "Must be strictly less than hard_timeout."
        ),
    )
    hard_timeout: int = Field(
        default=30,
        gt=0,
        description=(
            "Seconds before the task is unconditionally cancelled via asyncio "
            "cancellation.  Must be strictly greater than soft_timeout."
        ),
    )
    graceful_shutdown_timeout: int = Field(
        default=30,
        gt=0,
        description=(
            "Seconds the shutdown drain sequence will wait for in-flight tasks "
            "to complete before forcefully terminating the worker."
        ),
    )

    # =========================================================================
    # Admission control
    # =========================================================================

    admission_limit: int = Field(
        default=5000,
        gt=0,
        description=(
            "Maximum task dispatches allowed within the admission window.  "
            "Dispatches above this threshold are rejected with HTTP 429."
        ),
    )
    admission_window: int = Field(
        default=10,
        gt=0,
        description="Fixed-window size in seconds for admission control counting.",
    )

    # =========================================================================
    # OpenTelemetry
    # =========================================================================

    otel_enabled: bool = Field(
        default=False,
        description=(
            "Enable OpenTelemetry tracing and metrics export.  When false, "
            "no-op providers are used and no data is exported."
        ),
    )
    otel_exporter_otlp_endpoint: str = Field(
        default="http://localhost:4317",
        description=(
            "OTLP gRPC endpoint for the OpenTelemetry collector.  "
            "Example: 'http://otel-collector:4317'."
        ),
    )

    # =========================================================================
    # Cross-field validators
    # =========================================================================

    @field_validator("redis_url", mode="before")
    @classmethod
    def _validate_redis_scheme(cls, v: object) -> object:
        """Reject Redis URLs with unsupported schemes at construction time.

        Pydantic's ``RedisDsn`` validator already enforces valid schemes, but
        this validator surfaces a clearer, actionable message that names the
        exact environment variable to fix.
        """
        if isinstance(v, str) and not (
            v.startswith("redis://")
            or v.startswith("rediss://")
            or v.startswith("redis+unix://")
        ):
            raise ValueError(
                f"RELIER_REDIS_URL has an unsupported scheme: '{v}'. "
                "The URL must begin with 'redis://' (plain), "
                "'rediss://' (TLS), or 'redis+unix://' (Unix socket). "
                "Fix: update RELIER_REDIS_URL to use a supported scheme."
            )
        return v

    @model_validator(mode="after")
    def _validate_timeout_ordering(self) -> Self:
        """Enforce ``soft_timeout < hard_timeout``.

        A soft timeout equal to or greater than the hard timeout would fire
        the cleanup hook at the same instant the task is cancelled, making
        recovery impossible.
        """
        if self.soft_timeout >= self.hard_timeout:
            raise ValueError(
                f"RELIER_SOFT_TIMEOUT ({self.soft_timeout}s) must be strictly "
                f"less than RELIER_HARD_TIMEOUT ({self.hard_timeout}s).  "
                "If they are equal, the cleanup hook fires at the same instant "
                "as forced cancellation, leaving no recovery window.  "
                f"Fix: set RELIER_SOFT_TIMEOUT to a value below {self.hard_timeout}."
            )
        return self

    @model_validator(mode="after")
    def _validate_idempotency_window(self) -> Self:
        """Enforce ``idempotency_inflight_ttl > hard_timeout``.

        If the in-flight lock expires while the task is still executing, a
        second worker can claim the same idempotency key and begin a duplicate
        execution before the first one finishes.
        """
        safety_buffer = 10
        min_ttl = self.hard_timeout + safety_buffer
        if self.idempotency_inflight_ttl <= self.hard_timeout:
            raise ValueError(
                f"RELIER_IDEMPOTENCY_INFLIGHT_TTL ({self.idempotency_inflight_ttl}s) "
                f"must be greater than RELIER_HARD_TIMEOUT ({self.hard_timeout}s).  "
                "If the in-flight lock expires before the task completes, a second "
                "worker can claim the same key and begin duplicate execution.  "
                f"Fix: set RELIER_IDEMPOTENCY_INFLIGHT_TTL to at least {min_ttl} "
                f"(hard_timeout + {safety_buffer}s safety buffer)."
            )
        return self


@lru_cache
def get_settings() -> Settings:
    """Return the cached, immutable :class:`Settings` singleton for this process.

    The result is memoised after the first call, making it safe to invoke at
    module level in any file without paying repeated parsing costs.

    Returns:
        The validated, frozen :class:`Settings` instance.

    Example:
        >>> from relier.config import get_settings
        >>> s = get_settings()
        >>> s.env
        'development'
        >>> s is get_settings()  # always the same object
        True
    """
    return Settings()
