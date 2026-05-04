import pytest

from relier.storage.database import DatabaseManager
from relier.storage.redis import RedisManager

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio


async def test_redis_manager_lazy_initialization(redis_url: str):
    """
    Test that the Redis pool is not created immediately upon instantiation,
    protecting against connection exhaustion before the worker loop starts.
    """
    manager = RedisManager()

    # Assert lazy initialization: the clients dictionary should be empty
    assert len(manager._clients) == 0

    # Connect by calling get_client()
    client = await manager.get_client()

    # Assert loop-aware cache populated
    assert len(manager._clients) == 1
    assert client is not None

    # Verify the connection is alive
    is_alive = await manager.ping()
    assert is_alive is True

    await manager.close()
    assert len(manager._clients) == 0


async def test_database_manager_lazy_initialization(postgres_url: str):
    """
    Test that the Postgres engine/pool waits for explicit connection,
    allowing the worker process to safely fork before binding.
    """

    manager = DatabaseManager()

    # Assert lazy initialization
    assert manager._engine is None
    assert manager._sessionmaker is None

    # Trigger initialization by accessing the engine property
    _ = manager.engine
    assert manager._engine is not None

    # Disconnect
    await manager.close()
    assert manager._engine is None
    assert manager._sessionmaker is None
