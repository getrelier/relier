"""
Relier API — Dependencies.

Common FastAPI dependencies for database and Redis access.
"""

from collections.abc import AsyncGenerator

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from relier.storage.database import get_relier_db as get_pg_session
from relier.storage.redis import get_relier_redis


async def get_relier_redis_client() -> AsyncGenerator[Redis, None]:
    """Dependency for providing a shared Redis client."""
    client = await get_relier_redis()
    yield client


async def get_relier_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for providing a scoped SQLAlchemy session."""
    async for session in get_pg_session():
        yield session
