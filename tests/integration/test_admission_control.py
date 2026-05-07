import asyncio
import os

import pytest

pytestmark = pytest.mark.asyncio


async def test_admission_control_e2e(redis_client):
    """End-to-end admission control test.

    This test configures a tiny admission window and limit, then exercises the
    API to ensure requests are admitted up to the limit and rejected once the
    limit is exceeded. It verifies the 429 payload and `Retry-After` header,
    then waits the provided TTL and confirms requests are accepted again.
    """
    # Reduce the admission window and limit for fast, deterministic testing.
    os.environ["RELIER_ADMISSION_LIMIT"] = "3"
    os.environ["RELIER_ADMISSION_WINDOW"] = "2"

    # Clear cached Settings so the AdmissionController picks up the new values.
    from relier.config import get_settings

    get_settings.cache_clear()

    # Force the Lua script to be reloaded on first use (safe no-op if empty).
    from relier.core.admission import admission_control

    admission_control._script_sha = ""

    # Import the FastAPI app and use an ASGI test client to exercise the middleware.
    from httpx import ASGITransport, AsyncClient

    from relier.api.main import app

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        # Ensure a clean starting point. If Redis is unreachable (e.g., CI
        # provided URL but no server running), skip the integration test rather
        # than failing the whole suite.
        try:
            await redis_client.flushdb()
        except Exception as exc:  # pragma: no cover - environment-dependent
            pytest.skip(f"Redis unavailable for integration test: {exc}")

        limit = int(os.environ["RELIER_ADMISSION_LIMIT"])

        # Make `limit` successful requests.
        for _ in range(limit):
            r = await client.get("/admin/workers")
            assert r.status_code == 200

        # Next request should be rejected with 429 and include Retry-After.
        r = await client.get("/admin/workers")
        assert r.status_code == 429
        body = r.json()
        assert body.get("detail") == "System capacity reached."
        assert "retry_after_seconds" in body
        retry_after = (
            int(body["retry_after_seconds"]) if body["retry_after_seconds"] else 0
        )
        assert retry_after >= 0
        assert r.headers.get("Retry-After") == str(retry_after)

        # Wait until the admission window expires, then verify requests are accepted again.
        await asyncio.sleep(retry_after + 0.25)
        r2 = await client.get("/admin/workers")
        assert r2.status_code == 200
