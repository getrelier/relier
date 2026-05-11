"""
Relier API — Dependencies.

Common FastAPI dependencies for database and Redis access.
"""

from collections.abc import AsyncGenerator

from redis.asyncio import Redis

from relier.storage.redis import get_relier_redis


async def get_relier_redis_client() -> AsyncGenerator[Redis, None]:
    """Dependency for providing a shared Redis client."""
    client = await get_relier_redis()
    yield client
