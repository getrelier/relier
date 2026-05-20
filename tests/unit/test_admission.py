"""Unit tests for core admission control.

Admission is enforced at task dispatch (the decorator's push/apush), so it is
exercised here directly against AdmissionController rather than through any
HTTP layer.
"""

from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import NoScriptError

from relier.core.admission import AdmissionController

pytestmark = pytest.mark.asyncio


def _redis(*, evalsha_result=None, evalsha_side_effect=None) -> AsyncMock:
    """Build a Redis double for the admission Lua-script path."""
    redis = AsyncMock()
    redis.script_load = AsyncMock(return_value="sha-123")
    if evalsha_side_effect is not None:
        redis.evalsha = AsyncMock(side_effect=evalsha_side_effect)
    else:
        redis.evalsha = AsyncMock(return_value=evalsha_result or [1, 1, 0])
    return redis


class TestCheckCapacity:
    async def test_admitted(self) -> None:
        redis = _redis(evalsha_result=[1, 42, 0])
        controller = AdmissionController()
        with patch("relier.core.admission.get_relier_redis", return_value=redis):
            admitted, retry_after = await controller.check_capacity("global")
        assert admitted is True
        assert retry_after == 0
        redis.script_load.assert_awaited_once()

    async def test_rejected_returns_retry_after(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        redis = _redis(evalsha_result=[0, 5001, 7])
        controller = AdmissionController()
        with (
            patch("relier.core.admission.get_relier_redis", return_value=redis),
            caplog.at_level("WARNING"),
        ):
            admitted, retry_after = await controller.check_capacity("tenant-x")
        assert admitted is False
        assert retry_after == 7
        assert "rejected" in caplog.text

    async def test_fails_open_on_redis_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        controller = AdmissionController()
        with (
            patch(
                "relier.core.admission.get_relier_redis",
                side_effect=ConnectionError("redis down"),
            ),
            caplog.at_level("ERROR"),
        ):
            admitted, retry_after = await controller.check_capacity()
        # Fail open: admission is protective, not a hard dependency — a Redis
        # outage must not block all traffic.
        assert admitted is True
        assert retry_after == 0
        assert "failing open" in caplog.text

    async def test_script_sha_is_cached(self) -> None:
        redis = _redis(evalsha_result=[1, 1, 0])
        controller = AdmissionController()
        with patch("relier.core.admission.get_relier_redis", return_value=redis):
            await controller.check_capacity()
            await controller.check_capacity()
        # The Lua script is loaded once; later calls reuse the cached SHA.
        redis.script_load.assert_awaited_once()
        assert redis.evalsha.await_count == 2


class TestEvalshaFallback:
    async def test_reloads_script_on_noscript_error(self) -> None:
        # First evalsha misses the script cache; the controller reloads and retries.
        redis = _redis(evalsha_side_effect=[NoScriptError("missing"), [1, 1, 0]])
        controller = AdmissionController()
        with patch("relier.core.admission.get_relier_redis", return_value=redis):
            admitted, retry_after = await controller.check_capacity()
        assert admitted is True
        assert retry_after == 0
        assert redis.script_load.await_count == 2
        assert redis.evalsha.await_count == 2
