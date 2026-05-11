import pytest

pytestmark = pytest.mark.asyncio


async def test_admission_allows_when_admitted(monkeypatch) -> None:
    """Middleware should allow requests when admission control admits them."""
    called = {}

    async def fake_check_capacity(resource_key="global"):
        called["resource_key"] = resource_key
        return True, 0

    monkeypatch.setattr(
        "relier.api.middleware.admission_control.check_capacity", fake_check_capacity
    )

    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from relier.api.middleware import AdmissionControlMiddleware

    app = FastAPI()
    app.add_middleware(AdmissionControlMiddleware)

    @app.get("/test")
    async def handler():
        return {"ok": True}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/test", headers={"X-Tenant-ID": "tenant-42"})
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        assert called.get("resource_key") == "tenant-42"


async def test_admission_rejects_when_full(monkeypatch) -> None:
    """Middleware should reject requests when admission control denies them."""

    async def fake_check_capacity(resource_key="global"):
        return False, 5

    monkeypatch.setattr(
        "relier.api.middleware.admission_control.check_capacity", fake_check_capacity
    )

    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from relier.api.middleware import AdmissionControlMiddleware

    app = FastAPI()
    app.add_middleware(AdmissionControlMiddleware)

    @app.get("/test")
    async def handler():
        return {"ok": True}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/test")
        assert r.status_code == 429
        body = r.json()
        assert body["detail"] == "System capacity reached."
        assert body["retry_after_seconds"] == 5
        assert r.headers.get("Retry-After") == "5"


async def test_exempt_paths_bypass_admission(monkeypatch) -> None:
    """Health/readiness endpoints should bypass admission control entirely."""
    called = {"flag": False}

    async def fake_check_capacity(resource_key="global"):
        called["flag"] = True
        return False, 1

    monkeypatch.setattr(
        "relier.api.middleware.admission_control.check_capacity", fake_check_capacity
    )

    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from relier.api.middleware import AdmissionControlMiddleware

    app = FastAPI()
    app.add_middleware(AdmissionControlMiddleware)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/health")
        assert r.status_code == 200
        assert called["flag"] is False
