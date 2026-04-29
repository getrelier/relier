"""
Relier Storage Layer — PostgreSQL Integration.

Provides asynchronous database connectivity using SQLAlchemy and AsyncPG.
Manages the engine lifecycle via lazy initialization and strict PID-tracking
to prevent connection corruption during Celery preforks.
"""

import logging
import os
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from relier.config import get_settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages the SQLAlchemy async engine and session maker lifecycle.
    Includes fork-safety mechanisms for Celery workers.
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None
        self._pid: int | None = None

    def _get_safe_log_url(self) -> str:
        """Returns a sanitized Postgres URL for logging (masks the password)."""
        url_obj = self.settings.database_url
        host = getattr(url_obj, "host", "localhost")
        port = getattr(url_obj, "port", 5432)
        db = getattr(url_obj, "path", "/relier")
        return f"postgresql+asyncpg://***@{host}:{port}{db}"

    @property
    def engine(self) -> AsyncEngine:
        """
        Lazily initialize the database engine.
        Tracks the Process ID (PID) to prevent corrupted connection pools
        if evaluated before a Celery prefork.
        """
        current_pid = os.getpid()

        # If the process ID changed (e.g., Celery forked), recreate the engine
        if self._engine is not None and self._pid != current_pid:
            logger.warning(
                f"Process fork detected (PID changed from {self._pid} to {current_pid}). "
                "Recreating Relier PostgreSQL engine to prevent pool corruption."
            )
            self._engine = None
            self._sessionmaker = None

        if self._engine is None:
            self._pid = current_pid
            logger.info(
                f"Initializing Relier AsyncPG engine on PID {self._pid} -> {self._get_safe_log_url()}"
            )

            self._engine = create_async_engine(
                self.settings.database_url_str,
                echo=self.settings.env == "development",
                pool_size=self.settings.db_pool_size,
                max_overflow=self.settings.db_max_overflow,
                pool_pre_ping=True,
            )
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Lazily initialize the session factory."""
        # Access self.engine first to trigger fork-safety checks
        _ = self.engine

        if self._sessionmaker is None:
            self._sessionmaker = async_sessionmaker(
                bind=self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False,
            )
        return self._sessionmaker

    async def close(self) -> None:
        """Gracefully dispose of the engine connection pool."""
        if self._engine:
            logger.info(
                f"Disposing Relier PostgreSQL connection pool on PID {os.getpid()}..."
            )
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None
            self._pid = None


# Global instance for shared access across the library
db_manager = DatabaseManager()


async def get_relier_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency injection helper for getting an async Postgres session (FastAPI Depends).
    Ensures the session is properly rolled back on error and closed after usage.
    """
    factory = db_manager.session_factory
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
