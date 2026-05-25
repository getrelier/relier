import asyncio
import os
import sys

import pytest
import pytest_asyncio

from relier.storage.redis import redis_manager

# Windows compatibility: ProactorEventLoop has issues with async operations timing out.
# Use the more stable WindowsSelectorEventLoopPolicy instead.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ==========================================
# Container Lifecycle Management
# ==========================================


@pytest.fixture(scope="session")
def redis_url():
    """Spin up Redis once per test session, or use CI provided URL."""
    import time

    from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]

    if ci_url := os.environ.get("RELIER_REDIS_URL"):
        # Use database 15 so tests are isolated from any other Celery workers
        # that may be running on the same Redis instance (e.g. local dev
        # Docker workers on db 0). CI's dedicated Redis is otherwise empty,
        # so this is safe there too.
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(ci_url)
        test_url = urlunparse(parsed._replace(path="/15"))
        print(f"Using CI-provided Redis URL (isolated to db=15): {test_url}")
        yield test_url
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
def setup_env(redis_url) -> None:
    """
    Automatically inject the container URLs into the environment variables
    so application settings (e.g., Pydantic BaseSettings) pick them up.
    """
    os.environ["RELIER_REDIS_URL"] = redis_url

    # Clear the settings cache so the new environment variables are picked up.
    from relier.config import get_settings

    get_settings.cache_clear()

    # Reset managers to pick up new settings using asyncio.run() for proper loop handling
    async def reset_managers():
        from relier.storage.redis import redis_manager

        await redis_manager._test_reset()

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


async def _flushdb_with_retry(client, attempts: int = 3, timeout: float = 5.0) -> None:
    """FLUSHDB with retries.

    Windows + testcontainers Redis occasionally drops the connection under
    sustained load. A single transient TimeoutError on teardown leaks state
    into the next test and produces non-deterministic failures; retrying with
    a fresh client recovers cleanly.
    """
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            await asyncio.wait_for(client.flushdb(), timeout=timeout)
            return
        except (TimeoutError, Exception) as e:
            last_exc = e
            if attempt < attempts - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
                # Re-acquire the client; the pool may have evicted the
                # connection we were holding.
                client = await redis_manager.get_client()
    print(f"Warning: flushdb failed after {attempts} attempts: {last_exc}")


@pytest_asyncio.fixture
async def redis_client(setup_env):
    """
    Provides a live, connected Redis client for the test.
    The database is flushed after every test to ensure isolation.
    """
    # This uses the actual library code to connect to the testcontainer
    client = await redis_manager.get_client()
    await _flushdb_with_retry(client)

    yield client
    await redis_manager.close()


@pytest.fixture(scope="session", autouse=True)
def nuke_stale_workers():
    import subprocess

    # This kills any leftover celery workers before the session starts
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/IM", "celery.exe", "/T"], capture_output=True
        )
    yield
