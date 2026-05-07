import pytest

pytestmark = pytest.mark.asyncio


async def test_liveness_endpoint():
    from httpx import ASGITransport, AsyncClient

    from relier.api.main import app

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/health")
        assert r.status_code == 200
        assert r.json().get("status") == "ok"


async def test_readiness_success(monkeypatch):
    # Provide healthy fake dependencies
    async def fake_redis_dep():
        class FakeRedis:
            async def ping(self):
                return True

        yield FakeRedis()

    async def fake_db_dep():
        class FakeDB:
            async def execute(self, _):
                return True

        yield FakeDB()

    # Use dependency_overrides so the route picks up the test doubles.
    from relier.api import dependencies as deps_mod
    from relier.api.main import app

    app.dependency_overrides[deps_mod.get_relier_redis_client] = fake_redis_dep
    app.dependency_overrides[deps_mod.get_relier_db] = fake_db_dep

    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/ready")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ready"
        assert body["dependencies"]["redis"] == "connected"


async def test_readiness_fails_when_redis_down(monkeypatch):
    async def bad_redis():
        class BadRedis:
            async def ping(self):
                raise Exception("no redis")

        yield BadRedis()

    async def good_db():
        class GoodDB:
            async def execute(self, _):
                return True

        yield GoodDB()

    from relier.api import dependencies as deps_mod
    from relier.api.main import app

    app.dependency_overrides[deps_mod.get_relier_redis_client] = bad_redis
    app.dependency_overrides[deps_mod.get_relier_db] = good_db

    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/ready")
        assert r.status_code == 503
        body = r.json()
        assert body["detail"]["status"] == "not_ready"
        assert "redis" in body["detail"]["failures"]


async def test_slo_metrics_endpoint(monkeypatch):
    # Patch SLOMetrics.get_report
    async def fake_report():
        return {"window1": {"burn": 0.1}}

    monkeypatch.setattr("relier.core.slo.SLOMetrics.get_report", fake_report)

    from httpx import ASGITransport, AsyncClient

    from relier.api.main import app

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/metrics/slo")
        assert r.status_code == 200
        body = r.json()
        assert "burn_rates" in body
