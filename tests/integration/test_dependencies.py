import pytest

from relier.storage.redis import get_relier_redis


@pytest.mark.asyncio
async def test_fastapi_redis_dependency() -> None:
    """Coverage for get_relier_redis()"""
    client = await get_relier_redis()
    assert client is not None
    is_alive = await client.ping()
    assert is_alive is True
