"""
Relier Configuration Engine.

Defines all global settings for the Relier library using Pydantic V2 BaseSettings.
Enforces strict type validation, secret masking, and immutability. All values
are sourced from environment variables with the ``RELIER_`` prefix, a ``.env``
file, or their declared defaults.

Configuration priority:
    1. Environment variables (``RELIER_*``)
    2. ``.env`` file
    3. Declared defaults below
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, RedisDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Validated, immutable configuration for the Relier reliability layer."""

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
    env: Literal["development", "staging", "production"] = "development"
    secret_key: SecretStr = Field(default=SecretStr("change-in-production"))
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # =========================================================================
    # Infrastructure — Redis
    # =========================================================================
    redis_url: RedisDsn = Field(default="redis://localhost:6379/0")  # type: ignore[assignment]
    redis_max_connections: int = Field(default=20, gt=0)
    redis_socket_timeout: float = Field(default=5.0, gt=0.0)
    redis_connect_timeout: float = Field(default=2.0, gt=0.0)
    redis_health_check_interval: int = Field(default=30, ge=0)
    redis_password: SecretStr | None = Field(
        default=None,
        description="Password for the Redis data nodes (requirepass/masterauth). "
        "Applies to both direct and Sentinel-managed connections.",
    )

    # =========================================================================
    # Infrastructure — Redis Sentinel (high availability)
    # =========================================================================
    redis_use_sentinel: bool = Field(
        default=False,
        description="Route Redis (and the Celery broker/result backend) through "
        "Sentinel for automatic master discovery and failover. When false, "
        "Relier connects directly to redis_url.",
    )
    redis_sentinel_nodes: str = Field(
        default="",
        description="Comma-separated host:port list of Sentinel instances, "
        "e.g. 'sentinel-1:26379,sentinel-2:26379,sentinel-3:26379'.",
    )
    redis_sentinel_master_name: str = Field(
        default="relier-master",
        description="Name of the monitored master group, as declared in "
        "'sentinel monitor <name> ...'.",
    )
    redis_sentinel_password: SecretStr | None = Field(
        default=None,
        description="Password for authenticating to the Sentinel instances "
        "themselves (Sentinel 'requirepass'). Distinct from redis_password.",
    )

    @property
    def sentinel_node_list(self) -> list[tuple[str, int]]:
        """Parse ``redis_sentinel_nodes`` into ``(host, port)`` tuples.

        Raises:
            ValueError: If Sentinel is enabled but no valid nodes are configured.
        """
        nodes: list[tuple[str, int]] = []
        for entry in self.redis_sentinel_nodes.split(","):
            entry = entry.strip()
            if not entry:
                continue
            host, _, port = entry.rpartition(":")
            if not host or not port.isdigit():
                raise ValueError(
                    f"Invalid Sentinel node '{entry}'. Expected 'host:port'."
                )
            nodes.append((host, int(port)))
        if self.redis_use_sentinel and not nodes:
            raise ValueError(
                "redis_use_sentinel is enabled but redis_sentinel_nodes is empty."
            )
        return nodes

    # =========================================================================
    # Celery Integration
    # =========================================================================
    celery_worker_count: int = Field(
        default=8,
        gt=0,
        description="Number of Celery worker processes. Used for Redis connection pool sizing and admission control.",
    )

    celery_worker_concurrency: int = Field(
        default=8,
        gt=0,
        description="Number of concurrent tasks per Celery worker process. Used for Redis connection pool sizing and admission control.",
    )

    # =========================================================================
    # Phoenix — Task Resurrection
    # =========================================================================
    heartbeat_ttl: int = Field(
        default=10,
        gt=0,
        description="Heartbeat TTL in seconds. Worker death detected after this expires.",
    )
    max_resurrections: int = Field(
        default=5,
        ge=0,
        description="Maximum resurrection attempts before a task is quarantined to the DLQ.",
    )
    resurrection_check_interval: int = Field(
        default=2,
        gt=0,
        description="Interval in seconds between resurrector scan passes.",
    )

    resurrection_requeue_delay: float = Field(
        default=0.05,
        gt=0.0,
        description="Delay in seconds between re-queueing resurrected tasks.",
    )
    resurrection_batch_size: int = Field(
        default=1000,
        gt=0,
        description="Maximum number of tasks a single resurrector scan pass "
        "will re-queue. Caps the rate at which a mass worker failure is "
        "replayed onto the recovery queue.",
    )
    resurrection_max_queue_depth: int = Field(
        default=10000,
        gt=0,
        description="Backpressure threshold. When the recovery queue already "
        "holds at least this many messages, the resurrector skips its scan "
        "pass so it cannot outrun the workers draining the queue. Expired "
        "tasks stay in the expiry index and are retried on a later pass.",
    )

    # =========================================================================
    # Idempotency
    # =========================================================================
    idempotency_default_ttl: int = Field(
        default=3600,
        gt=0,
        description="Default TTL in seconds for idempotency result cache keys.",
    )
    idempotency_inflight_ttl: int = Field(
        default=120,
        gt=0,
        description="TTL in seconds for in-flight idempotency locks to prevent indefinite blocking.",  # must be > max hard_timeout
    )

    # =========================================================================
    # Checkpointing — partial-result storage
    # =========================================================================
    checkpoint_max_inline_bytes: int = Field(
        default=262144,
        gt=0,
        description="Serialized checkpoints at or below this size (bytes) are "
        "stored inline in the Phoenix Redis hash. Larger ones spill to the "
        "blob backend, or are rejected when the backend is 'inline'.",
    )
    checkpoint_backend: Literal["inline", "filesystem"] = Field(
        default="inline",
        description="Where oversized checkpoints are stored. 'inline' keeps "
        "everything in Redis and rejects anything over checkpoint_max_inline_bytes. "
        "'filesystem' spills large checkpoints to checkpoint_dir.",
    )
    checkpoint_dir: str = Field(
        default="/var/lib/relier/checkpoints",
        description="Directory used by the 'filesystem' checkpoint backend. "
        "MUST be storage shared by every worker and the resurrector — a "
        "resurrected task is replayed on a different process than the one "
        "that wrote its checkpoint.",
    )

    # =========================================================================
    # Timeouts and Graceful Shutdown
    # =========================================================================
    soft_timeout: int = Field(default=25, gt=0)
    hard_timeout: int = Field(default=30, gt=0)
    graceful_shutdown_timeout: int = Field(default=30, gt=0)

    # =========================================================================
    # Admission Control
    # =========================================================================
    admission_limit: int = Field(
        default=5000,
        gt=0,
        description="Maximum requests allowed within the admission window.",
    )
    admission_window: int = Field(
        default=10,
        gt=0,
        description="Sliding window size in seconds for admission control.",
    )

    # =========================================================================
    # OpenTelemetry
    # =========================================================================
    otel_enabled: bool = Field(
        default=False,
        description="Enable OpenTelemetry tracing and metrics export.",
    )
    otel_exporter_otlp_endpoint: str = Field(
        default="http://localhost:4317",
        description="OTLP gRPC endpoint for the OpenTelemetry collector.",
    )


@lru_cache
def get_settings() -> Settings:
    """Return a cached, immutable ``Settings`` instance.

    The cache means this is only parsed once per process, making it safe
    to call at module level in any file.
    """
    return Settings()
