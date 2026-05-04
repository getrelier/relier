import os

import pytest
import pytest_asyncio

from relier.storage.redis import get_relier_redis

# ==========================================
# Container Lifecycle Management
# ==========================================


@pytest.fixture(scope="session")
def postgres_url():
    """Spin up Postgres once per test session, or use CI provided URL."""
    from testcontainers.postgres import PostgresContainer

    if ci_url := os.environ.get("RELIER_DATABASE_URL"):
        yield ci_url
        return

    with PostgresContainer("postgres:16-alpine") as postgres:
        # Relier uses asyncpg
        url = postgres.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql+asyncpg"
        )
        yield url


@pytest.fixture(scope="session")
def redis_url():
    """Spin up Redis once per test session, or use CI provided URL."""
    from testcontainers.redis import RedisContainer

    if ci_url := os.environ.get("RELIER_REDIS_URL"):
        yield ci_url
        return

    with RedisContainer("redis:7-alpine") as redis:
        host = redis.get_container_host_ip()
        port = redis.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


# ==========================================
# Environment Injection
# ==========================================


@pytest.fixture(scope="session", autouse=True)
def setup_env(postgres_url, redis_url):
    """
    Automatically inject the container URLs into the environment variables
    so application settings (e.g., Pydantic BaseSettings) pick them up.
    """
    os.environ["RELIER_DATABASE_URL"] = postgres_url
    os.environ["RELIER_REDIS_URL"] = redis_url

    # CRITICAL: Clear the settings cache so the new environment variables are picked up.
    from relier.config import get_settings

    get_settings.cache_clear()

    # Also ensure the global managers are reset to pick up new settings
    # We use run_until_complete if we were in an async context, but this is a sync session fixture.
    # The managers will lazy-reinitialize on next access.
    import asyncio

    from relier.storage.database import db_manager
    from relier.storage.redis import redis_manager

    loop = asyncio.new_event_loop()
    loop.run_until_complete(redis_manager._test_reset())
    loop.run_until_complete(db_manager._test_reset())
    loop.close()

    # Reconfigure Celery app if it has been imported already
    try:
        from relier.tasks.app import celery_app

        settings = get_settings()
        celery_app.conf.broker_url = str(settings.redis_url)
        celery_app.conf.result_backend = str(settings.redis_url)
    except (ImportError, AttributeError):
        pass


# ==========================================
# Test Isolation (Clean State)
# ==========================================


@pytest_asyncio.fixture(autouse=True)
async def clean_redis_state(setup_env):
    """Ensure global RedisManager state is wiped between tests."""
    from relier.storage.redis import redis_manager

    yield
    await redis_manager._test_reset()


@pytest_asyncio.fixture(autouse=True)
async def clean_db_state(setup_env):
    """Ensure global DatabaseManager state is wiped between tests."""
    from relier.storage.database import db_manager

    yield
    await db_manager._test_reset()


@pytest_asyncio.fixture
async def redis_client(setup_env):
    """
    Provides a live, connected Redis client for the test.
    The database is flushed after every test to ensure isolation.
    """
    # This uses your actual library code to connect to the testcontainer
    client = await get_relier_redis()
    yield client

    # Crucial: Clean up keys after every test so they don't leak into the next one
    await client.flushdb()
