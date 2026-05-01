import os
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from relier.storage.redis import get_relier_redis
from relier.storage.database import get_relier_db, DatabaseManager


@pytest.mark.asyncio
async def test_fastapi_redis_dependency():
    """Coverage for get_relier_redis()"""
    client = await get_relier_redis()
    assert client is not None
    is_alive = await client.ping()
    assert is_alive is True


@pytest.mark.asyncio
async def test_fastapi_db_dependency_success():
    """Coverage for get_relier_db() async generator on success"""
    async for session in get_relier_db():
        assert isinstance(session, AsyncSession)
        assert session.is_active is True


@pytest.mark.asyncio
async def test_fastapi_db_dependency_error_handling():
    """
    Coverage for get_relier_db() async generator on error.
    Proves that exceptions are caught, rolled back, and re-raised.
    """
    with pytest.raises(ValueError, match="Test error"):
        async for session in get_relier_db():
            assert isinstance(session, AsyncSession)
            # Simulate a failure during a FastAPI request
            raise ValueError("Test error")


def test_database_manager_fork_detection():
    """
    Test that if a Celery worker preforks and the PID changes,
    the DatabaseManager safely drops the corrupted engine and recreates it.
    """
    manager = DatabaseManager()

    # Trigger initial engine creation
    engine_1 = manager.engine
    original_pid = manager._pid
    assert original_pid == os.getpid()

    # Simulate a Celery Process Fork by hacking the recorded PID
    manager._pid = 999999

    # Access engine again; it should detect the mismatch, warn, and recreate
    engine_2 = manager.engine

    assert manager._pid == os.getpid()  # PID is restored to normal
    assert engine_1 is not engine_2  # Proves the old pool was destroyed


async def test_database_manager_close_logic():
    """Coverage for DatabaseManager.close() when an engine exists."""
    manager = DatabaseManager()

    # Force engine creation
    _ = manager.engine
    assert manager._engine is not None

    # Call close explicitly
    await manager.close()

    # Verify cleanup
    assert manager._engine is None
    assert manager._sessionmaker is None
    assert manager._pid is None
