import sys
import types

import pytest

pytestmark = pytest.mark.asyncio


async def test_lifespan_task_discovery(monkeypatch) -> None:
    # Create a fake relier.tasks.app module with a dummy celery_app
    mod_name = "relier.tasks.app"
    fake_mod = types.ModuleType(mod_name)

    class DummyCelery:
        tasks = {"relier.tasks.debug.sample": None}

    fake_mod.celery_app = DummyCelery()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, mod_name, fake_mod)

    from relier.api.main import app, lifespan

    # Running the lifespan should execute the task discovery branch
    async with lifespan(app):
        # Inside the context we can assert the app is the FastAPI instance
        assert hasattr(app, "router")
