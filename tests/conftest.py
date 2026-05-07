import asyncio
import os
import sys

import pytest
import pytest_asyncio

from relier.storage.redis import get_relier_redis

# Windows compatibility: ProactorEventLoop has issues with async operations timing out.
# Use the more stable WindowsSelectorEventLoopPolicy instead.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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
    import time

    from testcontainers.redis import RedisContainer

    if ci_url := os.environ.get("RELIER_REDIS_URL"):
        print(f"Using CI-provided Redis URL: {ci_url}")
        yield ci_url
        return

    print("Starting Redis container...")
    try:
        with RedisContainer("redis:7-alpine") as redis:
            host = redis.get_container_host_ip()
            port = redis.get_exposed_port(6379)
            url = f"redis://{host}:{port}/0"
            print(f"Redis container started at {host}:{port}")

            # Wait for Redis to be ready with retries
            for attempt in range(30):
                try:
                    import socket

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    result = sock.connect_ex((host, int(port)))
                    sock.close()
                    if result == 0:
                        print(f"✓ Redis container is ready at {url}")
                        break
                except Exception as e:
                    print(f"Redis not ready (attempt {attempt + 1}/30): {e}")
                time.sleep(0.5)

            yield url
    except Exception as e:
        print(f"ERROR: Failed to start Redis container: {e}")
        raise


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

    # Clear the settings cache so the new environment variables are picked up.
    from relier.config import get_settings

    get_settings.cache_clear()

    # Reset managers to pick up new settings using asyncio.run() for proper loop handling
    async def reset_managers():
        from relier.storage.database import db_manager
        from relier.storage.redis import redis_manager

        await redis_manager._test_reset()
        await db_manager._test_reset()

    asyncio.run(reset_managers())

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
    # This uses the actual library code to connect to the testcontainer
    client = await get_relier_redis()
    yield client

    # Clean up keys after every test so they don't leak into the next one
    # Use a timeout and handle gracefully in case Redis is unavailable
    try:
        await asyncio.wait_for(client.flushdb(), timeout=5.0)
    except (TimeoutError, Exception) as e:
        print(f"Warning: Could not flush Redis during teardown: {e}")
        pass


@pytest.fixture(scope="session", autouse=True)
def nuke_stale_workers():
    import subprocess

    # This kills any leftover celery workers before the session starts
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/IM", "celery.exe", "/T"], capture_output=True
        )
    yield
