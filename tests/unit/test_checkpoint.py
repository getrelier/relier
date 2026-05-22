"""Unit tests for size-aware checkpoint storage.

Covers CheckpointStore (inline vs blob routing, the hard size cap, resolve,
GC, sweep), the FilesystemBackend, and the Phoenix/DLQ blob lifecycle hooks.
"""

import json
import os
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from relier.config import Settings
from relier.core.checkpoint import (
    CheckpointStore,
    CheckpointTooLargeError,
    FilesystemBackend,
)
from relier.core.dlq import DeadLetterQueue
from relier.core.keys import RedisKeys
from relier.core.phoenix import PhoenixRegistry

pytestmark = pytest.mark.asyncio

_MARKER = "__relier_checkpoint_ref__"


def _settings(tmp_path, **overrides):
    """Build a Settings object with a small inline limit for fast testing."""
    base = {
        "checkpoint_backend": "inline",
        "checkpoint_dir": str(tmp_path),
        "checkpoint_max_inline_bytes": 100,
    }
    base.update(overrides)
    return Settings(**base)


@pytest.fixture(autouse=True)
def _reset_backend_cache():
    """CheckpointStore caches its backend at class level, reset around tests."""
    CheckpointStore._backend = None
    CheckpointStore._backend_kind = None
    yield
    CheckpointStore._backend = None
    CheckpointStore._backend_kind = None


# =============================================================================
# is_reference
# =============================================================================
class TestIsReference:
    async def test_detects_envelope(self) -> None:
        assert CheckpointStore.is_reference({_MARKER: 1, "ref": "t1"})

    async def test_rejects_plain_values(self) -> None:
        assert not CheckpointStore.is_reference({"step": 1})
        assert not CheckpointStore.is_reference("a string")
        assert not CheckpointStore.is_reference(None)


# =============================================================================
# CheckpointStore.save — inline vs blob routing + hard cap
# =============================================================================
class TestSave:
    async def test_small_checkpoint_stays_inline(self, mock_redis, tmp_path) -> None:
        with patch(
            "relier.core.checkpoint.get_settings", return_value=_settings(tmp_path)
        ):
            await CheckpointStore.save("t1", {"step": 1})
        stored = await mock_redis.hget(RedisKeys.phoenix("t1"), "partial_result")
        assert json.loads(stored) == {"step": 1}

    async def test_oversized_inline_backend_is_rejected(
        self, mock_redis, tmp_path
    ) -> None:
        with (
            patch(
                "relier.core.checkpoint.get_settings", return_value=_settings(tmp_path)
            ),
            pytest.raises(CheckpointTooLargeError) as excinfo,
        ):
            await CheckpointStore.save("t1", {"blob": "x" * 500})
        assert excinfo.value.task_id == "t1"
        assert excinfo.value.size > 100

    async def test_oversized_spills_to_blob(self, mock_redis, tmp_path) -> None:
        big = {"blob": "x" * 500}
        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with patch("relier.core.checkpoint.get_settings", return_value=settings):
            await CheckpointStore.save("t1", big)
            envelope = json.loads(
                await mock_redis.hget(RedisKeys.phoenix("t1"), "partial_result")
            )
            # Redis holds only a tiny reference envelope...
            assert CheckpointStore.is_reference(envelope)
            assert envelope["ref"] == "t1"
            # ...the real payload lives on disk...
            assert (tmp_path / "t1.ckpt").exists()
            # ...and round-trips through resolve().
            assert await CheckpointStore.resolve(envelope) == big

    async def test_blob_then_inline_collects_old_blob(
        self, mock_redis, tmp_path
    ) -> None:
        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with patch("relier.core.checkpoint.get_settings", return_value=settings):
            await CheckpointStore.save("t1", {"blob": "x" * 500})
            assert (tmp_path / "t1.ckpt").exists()
            # A later, small checkpoint must drop the now-orphaned blob.
            await CheckpointStore.save("t1", {"step": 2})
            assert not (tmp_path / "t1.ckpt").exists()
            stored = await mock_redis.hget(RedisKeys.phoenix("t1"), "partial_result")
            assert json.loads(stored) == {"step": 2}


# =============================================================================
# CheckpointStore.resolve
# =============================================================================
class TestResolve:
    async def test_inline_value_passes_through(self, tmp_path) -> None:
        with patch(
            "relier.core.checkpoint.get_settings", return_value=_settings(tmp_path)
        ):
            assert await CheckpointStore.resolve({"step": 3}) == {"step": 3}
            assert await CheckpointStore.resolve(None) is None

    async def test_blob_without_backend_returns_none(self, tmp_path) -> None:
        envelope = {_MARKER: 1, "backend": "filesystem", "ref": "t1", "codec": "gzip"}
        with patch(
            "relier.core.checkpoint.get_settings", return_value=_settings(tmp_path)
        ):
            assert await CheckpointStore.resolve(envelope) is None

    async def test_missing_blob_returns_none(self, tmp_path) -> None:
        envelope = {
            _MARKER: 1,
            "backend": "filesystem",
            "ref": "ghost",
            "codec": "gzip",
        }
        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with patch("relier.core.checkpoint.get_settings", return_value=settings):
            assert await CheckpointStore.resolve(envelope) is None


# =============================================================================
# CheckpointStore.gc
# =============================================================================
class TestGc:
    async def test_inline_and_garbage_values_are_noops(self, tmp_path) -> None:
        with patch(
            "relier.core.checkpoint.get_settings", return_value=_settings(tmp_path)
        ):
            await CheckpointStore.gc(None)
            await CheckpointStore.gc({"step": 1})
            await CheckpointStore.gc("not-json{{{")

    async def test_deletes_referenced_blob(self, mock_redis, tmp_path) -> None:
        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with patch("relier.core.checkpoint.get_settings", return_value=settings):
            await CheckpointStore.save("t1", {"blob": "x" * 500})
            envelope = json.loads(
                await mock_redis.hget(RedisKeys.phoenix("t1"), "partial_result")
            )
            assert (tmp_path / "t1.ckpt").exists()
            await CheckpointStore.gc(envelope)
            assert not (tmp_path / "t1.ckpt").exists()

    async def test_swallows_backend_failure(self, tmp_path) -> None:
        envelope = {_MARKER: 1, "backend": "filesystem", "ref": "t1", "codec": "gzip"}
        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with (
            patch("relier.core.checkpoint.get_settings", return_value=settings),
            patch.object(FilesystemBackend, "delete", side_effect=OSError("disk gone")),
        ):
            # Must log and continue, never raise.
            await CheckpointStore.gc(envelope)


# =============================================================================
# CheckpointStore.sweep
# =============================================================================
class TestSweep:
    async def test_inline_backend_is_noop(self, tmp_path) -> None:
        with patch(
            "relier.core.checkpoint.get_settings", return_value=_settings(tmp_path)
        ):
            assert await CheckpointStore.sweep() == 0

    async def test_removes_only_stale_blobs(self, tmp_path) -> None:
        stale = tmp_path / "stale.ckpt"
        stale.write_bytes(b"old")
        old = time.time() - 200000
        os.utime(stale, (old, old))
        fresh = tmp_path / "fresh.ckpt"
        fresh.write_bytes(b"new")

        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with patch("relier.core.checkpoint.get_settings", return_value=settings):
            removed = await CheckpointStore.sweep()
        assert removed == 1
        assert not stale.exists()
        assert fresh.exists()


# =============================================================================
# FilesystemBackend
# =============================================================================
class TestFilesystemBackend:
    async def test_write_read_delete_roundtrip(self, tmp_path) -> None:
        backend = FilesystemBackend(str(tmp_path))
        await backend.write("task-1", b"payload")
        assert await backend.read("task-1") == b"payload"
        await backend.delete("task-1")
        await backend.delete("task-1")  # deleting a missing blob is not an error

    async def test_rejects_unsafe_reference(self, tmp_path) -> None:
        backend = FilesystemBackend(str(tmp_path))
        with pytest.raises(ValueError, match="Unsafe checkpoint reference"):
            await backend.write("../escape", b"x")

    async def test_sweep_on_missing_directory(self, tmp_path) -> None:
        backend = FilesystemBackend(str(tmp_path / "does-not-exist"))
        assert await backend.sweep(0) == 0


# =============================================================================
# _get_backend
# =============================================================================
class TestGetBackend:
    async def test_unknown_backend_raises(self) -> None:
        bogus = SimpleNamespace(checkpoint_backend="mystery", checkpoint_dir="/tmp")
        with (
            patch("relier.core.checkpoint.get_settings", return_value=bogus),
            pytest.raises(ValueError, match="Unknown checkpoint backend"),
        ):
            CheckpointStore._get_backend()


# =============================================================================
# Lifecycle hooks — Phoenix.complete and DLQ.purge release blobs
# =============================================================================
class TestLifecycleHooks:
    async def test_phoenix_complete_releases_blob(self, mock_redis, tmp_path) -> None:
        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with patch("relier.core.checkpoint.get_settings", return_value=settings):
            await CheckpointStore.save("t1", {"blob": "x" * 500})
            assert (tmp_path / "t1.ckpt").exists()
            await PhoenixRegistry.complete("t1")
            assert not (tmp_path / "t1.ckpt").exists()

    async def test_dlq_purge_releases_blob(self, mock_redis, tmp_path) -> None:
        settings = _settings(tmp_path, checkpoint_backend="filesystem")
        with patch("relier.core.checkpoint.get_settings", return_value=settings):
            await CheckpointStore.save("t1", {"blob": "x" * 500})
            envelope = json.loads(
                await mock_redis.hget(RedisKeys.phoenix("t1"), "partial_result")
            )
            await mock_redis.hset(
                RedisKeys.dlq_hash(),
                "t1",
                json.dumps({"task_id": "t1", "partial_result": envelope}),
            )
            assert (tmp_path / "t1.ckpt").exists()
            await DeadLetterQueue.purge()
            assert not (tmp_path / "t1.ckpt").exists()
