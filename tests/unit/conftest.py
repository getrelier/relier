import pytest
import fnmatch
import json
import sys
from unittest.mock import AsyncMock, patch, MagicMock

# Global mock for tasks module to ensure unit tests never depend on it
sys.modules["relier.tasks"] = MagicMock()
sys.modules["relier.tasks.app"] = MagicMock()

# ==========================================
# Overrides (Disable Testcontainers for Unit Tests)
# ==========================================


@pytest.fixture(scope="session", autouse=True)
def setup_env():
    """Override conftest.py fixture to prevent starting containers."""
    pass


@pytest.fixture(scope="session")
def postgres_url():
    return "postgresql+asyncpg://user:pass@localhost:5432/db"


@pytest.fixture(scope="session")
def redis_url():
    return "redis://localhost:6379/0"


# ==========================================
# Mocks & Helpers
# ==========================================


class FakeRedis:
    """In-memory Redis mock for unit testing."""

    def __init__(self):
        self.data = {}
        self.hdata = {}
        self.zdata = {}  # {key: {member: score}}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.data:
            return False
        self.data[key] = str(value)
        return True

    async def delete(self, *keys):
        for key in keys:
            self.data.pop(key, None)
            self.hdata.pop(key, None)
            self.zdata.pop(key, None)

    async def exists(self, key):
        return 1 if (key in self.data or key in self.hdata or key in self.zdata) else 0

    async def hset(self, name, key, value):
        if name not in self.hdata:
            self.hdata[name] = {}
        self.hdata[name][key] = str(value)

    async def hget(self, name, key):
        return self.hdata.get(name, {}).get(key)

    async def hgetall(self, name):
        return dict(self.hdata.get(name, {}))

    async def hexists(self, name, key):
        return 1 if key in self.hdata.get(name, {}) else 0

    async def hdel(self, name, *keys):
        if name in self.hdata:
            for key in keys:
                self.hdata[name].pop(key, None)

    async def incr(self, key):
        val = int(self.data.get(key, 0)) + 1
        self.data[key] = str(val)
        return val

    async def expire(self, key, time):
        return key in self.data or key in self.hdata or key in self.zdata

    async def scan_iter(self, match=None, count=None):
        all_keys = (
            list(self.data.keys()) + list(self.hdata.keys()) + list(self.zdata.keys())
        )
        matched_keys = [k for k in all_keys if not match or fnmatch.fnmatch(k, match)]
        for k in matched_keys:
            yield k

    async def zadd(self, key, mapping):
        if key not in self.zdata:
            self.zdata[key] = {}
        for member, score in mapping.items():
            self.zdata[key][member] = float(score)
        return len(mapping)

    async def zcount(self, key, min, max):
        if key not in self.zdata:
            return 0
        count = 0
        for score in self.zdata[key].values():
            if min <= score <= max:
                count += 1
        return count

    async def zremrangebyscore(self, key, min, max):
        if key not in self.zdata:
            return 0
        to_remove = [m for m, s in self.zdata[key].items() if min <= s <= max]
        for m in to_remove:
            del self.zdata[key][m]
        return len(to_remove)

    def pipeline(self):
        return FakePipeline(self)

    async def zrem(self, name, *values):
        if name in self.zdata:
            for v in values:
                self.zdata[name].pop(v, None)

    async def hlen(self, name):
        return len(self.hdata.get(name, {}))

    async def hscan(self, name, cursor=0, match=None, count=None):
        data = self.hdata.get(name, {})
        # Simple hscan simulation: return everything in the hash.
        return 0, data

    async def flushdb(self):
        self.data.clear()
        self.hdata.clear()
        self.zdata.clear()

    async def flushall(self):
        self.data.clear()
        self.hdata.clear()
        self.zdata.clear()

    async def eval(self, script, numkeys, *args):
        """Basic Lua script simulation for idempotency."""
        # Simple string-based detection of ACQUIRE vs RELEASE
        if "existing" in script and "NX" in script:
            # ACQUIRE_LUA
            key = args[0]
            val = args[1]
            if key in self.data:
                return [1, self.data[key]]
            self.data[key] = val
            return [0, False]
        elif "==" in script and "DEL" in script:
            # RELEASE_LUA
            key = args[0]
            val = args[1]
            if self.data.get(key) == val:
                del self.data[key]
                return 1
            return 0
        return None


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def set(self, *args, **kwargs):
        self.commands.append(("set", args, kwargs))
        return self

    def hset(self, *args, **kwargs):
        self.commands.append(("hset", args, kwargs))
        return self

    def zadd(self, *args, **kwargs):
        self.commands.append(("zadd", args, kwargs))
        return self

    def zremrangebyscore(self, *args, **kwargs):
        self.commands.append(("zremrangebyscore", args, kwargs))
        return self

    def delete(self, *args, **kwargs):
        self.commands.append(("delete", args, kwargs))
        return self

    async def execute(self):
        for cmd, args, kwargs in self.commands:
            method = getattr(self.redis, cmd)
            await method(*args, **kwargs)
        self.commands = []


@pytest.fixture
async def mock_redis():
    """Provides a FakeRedis instance and patches get_relier_redis in core modules."""
    mock = FakeRedis()

    # Define the modules to patch
    modules = [
        "relier.core.phoenix",
        "relier.core.idempotency",
        "relier.core.dlq",
        "relier.core.slo",
        "relier.storage.redis",
    ]

    patches = [
        patch(f"{m}.get_relier_redis", AsyncMock(return_value=mock)) for m in modules
    ]

    for p in patches:
        p.start()

    yield mock

    for p in patches:
        p.stop()
