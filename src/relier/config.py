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

    # =========================================================================
    # Idempotency
    # =========================================================================
    idempotency_default_ttl: int = Field(
        default=3600,
        gt=0,
        description="Default TTL in seconds for idempotency result cache keys.",
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
