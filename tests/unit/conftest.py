import asyncio
import fnmatch
from unittest.mock import AsyncMock, patch

import pytest

# ==========================================
# Overrides (Disable Testcontainers for Unit Tests)
# ==========================================


@pytest.fixture(scope="session", autouse=True)
def setup_env() -> None:
    """Override conftest.py fixture to prevent starting containers."""
    pass


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

    def _s(self, key):
        return key.decode() if isinstance(key, bytes) else str(key)

    async def get(self, key):
        val = self.data.get(key)
        return val.encode() if isinstance(val, str) else val

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

    async def hset(self, name, key=None, value=None, mapping=None):
        name = self._s(name)
        if name not in self.hdata:
            self.hdata[name] = {}

        if mapping is not None:
            for k, v in mapping.items():
                self.hdata[name][k] = str(v)
            return len(mapping)
        if key is not None:
            self.hdata[name][key] = str(value)
            return 1
        return 0

    async def hget(self, name, key):
        name = self._s(name)
        key = self._s(key)

        val = self.hdata.get(name, {}).get(key)
        return val.encode() if isinstance(val, str) else val

    async def hgetall(self, name):
        name = self._s(name)
        data = self.hdata.get(name, {})
        return {
            (k.encode() if isinstance(k, str) else k): (
                v.encode() if isinstance(v, str) else v
            )
            for k, v in data.items()
        }

    async def hexists(self, name, key):
        name = self._s(name)
        key = self._s(key)
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
            # Yield bytes to mimic real Redis aioredis behavior
            yield k.encode("utf-8") if isinstance(k, str) else k

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

    async def sadd(self, name, *values):
        if name not in self.data:
            self.data[name] = set()
        if not isinstance(self.data[name], set):
            # Convert if it was a string (init_worker might do this)
            self.data[name] = {self.data[name]}
        for v in values:
            self.data[name].add(str(v))
        return len(values)

    async def smembers(self, name):
        val = self.data.get(name, set())
        if isinstance(val, str):
            return {val}
        return set(val)

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
            # RELEASE_LUA / CLEANUP_LUA
            key = args[0]
            val = args[1]
            if self.data.get(key) == val:
                del self.data[key]
                return 1
            return 0
        elif "EXISTS" in script and "fence_ttl" in script:
            # RESURRECT_LUA
            lease_key = args[0]
            fence_key = args[1]
            token = args[2]
            if lease_key in self.data:
                return 0
            self.data[lease_key] = token
            self.data[fence_key] = token
            return 1
        elif "fence" in script and "expected" in script and "lease" in script:
            # VALIDATE_LUA
            lease_key = args[0]
            fence_key = args[1]
            expected = args[2]
            if self.data.get(lease_key) != expected:
                return 0
            if self.data.get(fence_key) != expected:
                return 2
            return 1
        elif "fence" in script and "expected" in script:
            # COMMIT_CHECK_LUA
            fence_key = args[0]
            expected = args[1]
            if self.data.get(fence_key) != expected:
                return 0
            return 1
        return None


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def set(self, *args, **kwargs):
        self.commands.append(("set", args, kwargs))
        return self

    def incr(self, *args, **kwargs):
        self.commands.append(("incr", args, kwargs))
        return self

    def hset(self, *args, **kwargs):
        self.commands.append(("hset", args, kwargs))
        return self

    def zadd(self, *args, **kwargs):
        self.commands.append(("zadd", args, kwargs))
        return self

    def zrem(self, *args, **kwargs):
        self.commands.append(("zrem", args, kwargs))
        return self

    def zremrangebyscore(self, *args, **kwargs):
        self.commands.append(("zremrangebyscore", args, kwargs))
        return self

    def delete(self, *args, **kwargs):
        self.commands.append(("delete", args, kwargs))
        return self

    def expire(self, *args, **kwargs):
        self.commands.append(("expire", args, kwargs))
        return self

    def exists(self, *args, **kwargs):
        self.commands.append(("exists", args, kwargs))
        return self

    def hexists(self, *args, **kwargs):
        self.commands.append(("hexists", args, kwargs))
        return self

    def sadd(self, *args, **kwargs):
        self.commands.append(("sadd", args, kwargs))
        return self

    async def execute(self):
        """Execute commands and return the collected results."""
        results = []
        for cmd, args, kwargs in self.commands:
            method = getattr(self.redis, cmd)
            # Await the command and capture its result
            result = await method(*args, **kwargs)
            results.append(result)

        # Clear the pipeline after execution
        self.commands = []
        return results


def mock_run_coroutine(coro, loop=None):
    """Manually mark the coroutine as exhausted properly."""
    while True:
        try:
            coro.send(None)
        except StopIteration as e:
            f = asyncio.Future() # type: ignore[var-annotated]
            f.set_result(e.value)
            return f
        except Exception as e:
            f = asyncio.Future()
            f.set_exception(e)
            return f


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
        "relier.tasks.decorator",
    ]

    patches = [
        patch(f"{m}.get_relier_redis", AsyncMock(return_value=mock)) for m in modules
    ]

    # Silence asynchronous lifecycle warnings globally
    # We remove these as they break tests that actually test drain/close logic
    lifecycle_patches = [] # type: ignore[var-annotated]

    for p in patches + lifecycle_patches:
        p.start()

    yield mock

    for p in patches + lifecycle_patches:
        p.stop()
