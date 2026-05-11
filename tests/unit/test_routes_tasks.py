import uuid

import pytest

pytestmark = pytest.mark.asyncio


async def test_trigger_task_enqueues(monkeypatch) -> None:
    # Prepare a fake Celery app with a matching task name
    class DummyCelery:
        def __init__(self):
            self.tasks = {"relier.tasks.debug.checkpoint_task": None}

        def send_task(self, name, args=None, kwargs=None, queue=None, task_id=None):
            # Record last send for test observation
            self._last = {
                "name": name,
                "args": args,
                "kwargs": kwargs,
                "queue": queue,
                "task_id": task_id,
            }

    dummy = DummyCelery()

    # Patch SchemaRegistry.wrap to be deterministic
    def fake_wrap(task_id, args, kwargs):
        return {"task_id": task_id, "args": args, "kwargs": kwargs}

    monkeypatch.setattr("relier.tasks.app.celery_app", dummy, raising=False)
    monkeypatch.setattr(
        "relier.core.schema.SchemaRegistry.wrap", fake_wrap, raising=False
    )

    from httpx import ASGITransport, AsyncClient

    from relier.api.main import app

    payload = {"name": "checkpoint_task", "args": [], "kwargs": {}}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post("/tasks/trigger", json=payload)
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "enqueued"
        assert body["task_name"].endswith(".checkpoint_task")


async def test_get_task_status_includes_backend_and_phoenix(monkeypatch) -> None:
    # Patch PhoenixRegistry.is_active (async)
    async def fake_is_active(task_id):
        return True

    monkeypatch.setattr("relier.core.phoenix.PhoenixRegistry.is_active", fake_is_active)

    class DummyAsyncResult:
        def __init__(self, tid):
            self._tid = tid
            self.state = "SUCCESS"

        @property
        def result(self):
            return {"ok": True}

    class DummyCelery:
        def AsyncResult(self, tid):
            return DummyAsyncResult(tid)

    monkeypatch.setattr("relier.tasks.app.celery_app", DummyCelery(), raising=False)

    from httpx import ASGITransport, AsyncClient

    from relier.api.main import app

    tid = str(uuid.uuid4())
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get(f"/tasks/{tid}")
        assert r.status_code == 200
        body = r.json()
        assert body["is_managed_by_phoenix"] is True
        assert body["backend"]["state"] == "SUCCESS"
