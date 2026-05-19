"""
Relier Core — Checkpoint Storage.

Tasks persist *partial results* (checkpoints) so that a resurrected task can
resume from its last safe state instead of restarting from scratch.

Storing those checkpoints naively is dangerous. Redis is Relier's coordination
backbone and runs with ``maxmemory-policy noeviction``; every write also
replicates to all replicas and lands in the AOF. A task that checkpoints a
large object — embeddings, an image, a document, a model checkpoint — would
bloat Redis memory, flood replication, and can take the whole cluster down.

This module routes every checkpoint through a size-aware store:

* **Small checkpoints** stay inline in the Phoenix Redis hash — fast, no
  external dependency. This is the default.
* **Large checkpoints** spill to a pluggable blob backend; only a tiny
  reference envelope is kept in Redis.
* With **no blob backend configured**, an oversized checkpoint is rejected
  outright (:class:`CheckpointTooLargeError`) rather than silently bloating
  Redis.

Blob storage requirement
------------------------
A blob backend MUST be storage shared by every worker process *and* the
resurrector, because a resurrected task is replayed on a different process
than the one that wrote the checkpoint. Node-local disk does not qualify;
use a shared mount (NFS/EFS, or a shared container volume).

Lifecycle
---------
Blob storage is reclaimed:

* immediately on :meth:`PhoenixRegistry.complete` (task finished),
* immediately on :meth:`DeadLetterQueue.purge` (DLQ cleared),
* eventually via :meth:`CheckpointStore.sweep` for blobs orphaned by a task
  whose Phoenix hash TTL-expired (driven by the resurrection loop).
"""

import asyncio
import gzip
import json
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from relier.config import get_settings
from relier.core.keys import RedisKeys
from relier.storage.redis import get_relier_redis

logger = logging.getLogger(__name__)

# Marker key identifying a checkpoint *reference envelope* — i.e. a checkpoint
# whose real payload lives in a blob backend. Distinctive enough that it
# cannot realistically collide with a user's own checkpoint dict keys.
_REF_MARKER = "__relier_checkpoint_ref__"

# Field within the Phoenix task hash that holds the checkpoint.
_CHECKPOINT_FIELD = "partial_result"

# Blob files are swept once they age past this. It sits deliberately above the
# 24h TTL of the Phoenix task hash (see PhoenixRegistry.register), so a blob is
# only ever swept after the task it belongs to is no longer resurrectable.
_BLOB_SWEEP_AGE_SECONDS = 86400 + 3600


class CheckpointTooLargeError(RuntimeError):
    """Raised when a checkpoint exceeds the inline limit and no blob backend
    is configured to absorb it."""

    def __init__(self, task_id: str, size: int, limit: int) -> None:
        self.task_id = task_id
        self.size = size
        self.limit = limit
        super().__init__(
            f"Checkpoint for task '{task_id}' is {size} bytes, which exceeds "
            f"the {limit}-byte inline limit. Storing it in Redis would risk "
            f"exhausting the coordination store. Either reduce the checkpoint "
            f"size, or set RELIER_CHECKPOINT_BACKEND=filesystem to spill large "
            f"checkpoints to shared storage."
        )


# =============================================================================
# Blob backends
# =============================================================================
class BlobBackend(ABC):
    """Storage for checkpoints too large to keep inline in Redis.

    Implementations MUST be backed by storage shared across every worker and
    the resurrector process — a resurrected task is replayed on a different
    process than the one that wrote the checkpoint.
    """

    @abstractmethod
    async def write(self, ref: str, data: bytes) -> None:
        """Persist ``data`` under ``ref``, overwriting any existing blob."""

    @abstractmethod
    async def read(self, ref: str) -> bytes:
        """Return the blob stored under ``ref``."""

    @abstractmethod
    async def delete(self, ref: str) -> None:
        """Delete the blob stored under ``ref``. A missing blob is not an error."""

    @abstractmethod
    async def sweep(self, max_age_seconds: int) -> int:
        """Delete blobs older than ``max_age_seconds``. Returns the count removed."""


class FilesystemBackend(BlobBackend):
    """Stores checkpoint blobs as files under a shared directory.

    One file per task (named by task id), overwritten in place, so repeated
    checkpoints from the same task never accumulate stale versions. Filesystem
    calls are offloaded to a thread so they never block the event loop.
    """

    _SUFFIX = ".ckpt"
    # Task ids are UUIDs in practice; this guards the path build against any
    # exotic id that could escape the checkpoint directory.
    _SAFE_REF = re.compile(r"^[A-Za-z0-9._-]+$")

    def __init__(self, directory: str) -> None:
        self._dir = Path(directory)

    def _path(self, ref: str) -> Path:
        if not ref or not self._SAFE_REF.match(ref):
            raise ValueError(f"Unsafe checkpoint reference: {ref!r}")
        return self._dir / f"{ref}{self._SUFFIX}"

    async def write(self, ref: str, data: bytes) -> None:
        await asyncio.to_thread(self._write_sync, ref, data)

    def _write_sync(self, ref: str, data: bytes) -> None:
        path = self._path(ref)
        self._dir.mkdir(parents=True, exist_ok=True)
        # Write to a process-unique temp file then atomically rename, so a
        # concurrent reader never observes a partially written checkpoint.
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)

    async def read(self, ref: str) -> bytes:
        return await asyncio.to_thread(self._path(ref).read_bytes)

    async def delete(self, ref: str) -> None:
        await asyncio.to_thread(self._delete_sync, ref)

    def _delete_sync(self, ref: str) -> None:
        self._path(ref).unlink(missing_ok=True)

    async def sweep(self, max_age_seconds: int) -> int:
        return await asyncio.to_thread(self._sweep_sync, max_age_seconds)

    def _sweep_sync(self, max_age_seconds: int) -> int:
        if not self._dir.exists():
            return 0
        cutoff = time.time() - max_age_seconds
        removed = 0
        for path in self._dir.glob(f"*{self._SUFFIX}"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
                    removed += 1
            except OSError:
                continue
        return removed


# =============================================================================
# Checkpoint store
# =============================================================================
class CheckpointStore:
    """Size-aware persistence for task checkpoints (partial results)."""

    _backend: BlobBackend | None = None
    _backend_kind: str | None = None

    @classmethod
    def _get_backend(cls) -> BlobBackend | None:
        """Return the configured blob backend, or ``None`` for inline-only mode."""
        settings = get_settings()
        kind = settings.checkpoint_backend
        if kind == "inline":
            return None
        # Cache the backend, rebuilding it if configuration changed (tests).
        if cls._backend is None or cls._backend_kind != kind:
            if kind == "filesystem":
                cls._backend = FilesystemBackend(settings.checkpoint_dir)
            else:  # defensive — the Literal type already constrains this
                raise ValueError(f"Unknown checkpoint backend: {kind}")
            cls._backend_kind = kind
        return cls._backend

    @staticmethod
    def is_reference(value: Any) -> bool:
        """Return whether a stored checkpoint value is a blob-reference envelope."""
        return isinstance(value, dict) and _REF_MARKER in value

    @classmethod
    async def save(cls, task_id: str, state: Any) -> None:
        """Persist a checkpoint for ``task_id``, choosing inline or blob storage.

        Raises:
            CheckpointTooLargeError: ``state`` exceeds the inline limit and no
                blob backend is configured.
            TypeError: ``state`` is not JSON-serializable.
        """
        settings = get_settings()
        redis = await get_relier_redis()
        task_key = RedisKeys.phoenix(task_id)

        serialized = json.dumps(state)
        size = len(serialized.encode("utf-8"))

        if size <= settings.checkpoint_max_inline_bytes:
            # Small enough to live in Redis. If a previous checkpoint for this
            # task spilled to a blob, drop the now-orphaned blob first.
            if settings.checkpoint_backend != "inline":
                await cls._gc_existing(redis, task_id)
            await redis.hset(task_key, _CHECKPOINT_FIELD, serialized)  # type: ignore[misc]
            return

        backend = cls._get_backend()
        if backend is None:
            raise CheckpointTooLargeError(
                task_id, size, settings.checkpoint_max_inline_bytes
            )

        # Spill to the blob backend; keep only a tiny envelope in Redis.
        compressed = gzip.compress(serialized.encode("utf-8"))
        await backend.write(task_id, compressed)
        envelope = {
            _REF_MARKER: 1,
            "backend": settings.checkpoint_backend,
            "ref": task_id,
            "codec": "gzip",
            "bytes": len(compressed),
        }
        await redis.hset(  # type: ignore[misc]
            task_key, _CHECKPOINT_FIELD, json.dumps(envelope)
        )
        logger.debug(
            "Checkpoint spilled to blob backend.",
            extra={
                "task_id": task_id,
                "raw_bytes": size,
                "stored_bytes": len(compressed),
            },
        )

    @classmethod
    async def _gc_existing(cls, redis: Any, task_id: str) -> None:
        """Release a blob left by a previous checkpoint of ``task_id``, if any."""
        raw = await redis.hget(RedisKeys.phoenix(task_id), _CHECKPOINT_FIELD)
        if raw:
            await cls.gc(raw)

    @classmethod
    async def gc(cls, stored_value: Any) -> None:
        """Release blob storage referenced by a stored checkpoint value.

        Accepts a raw JSON string/bytes, a parsed envelope/state, or ``None``.
        Inline values and legacy raw state are a no-op. Never raises.
        """
        if isinstance(stored_value, bytes):
            stored_value = stored_value.decode("utf-8")
        if isinstance(stored_value, str):
            try:
                stored_value = json.loads(stored_value)
            except (TypeError, json.JSONDecodeError):
                return
        if not cls.is_reference(stored_value):
            return
        backend = cls._get_backend()
        if backend is None:
            return
        try:
            await backend.delete(stored_value["ref"])
        except Exception as exc:
            logger.warning(
                "Failed to delete checkpoint blob.",
                extra={"ref": stored_value.get("ref"), "error": str(exc)},
            )

    @classmethod
    async def resolve(cls, value: Any) -> Any:
        """Return the actual checkpoint data for a stored value.

        A blob-reference envelope is dereferenced; an inline value (or legacy
        raw state) is returned unchanged. Never raises — on a missing or
        corrupt blob it logs and returns ``None`` so task execution can still
        proceed (just without a checkpoint to resume from).
        """
        if not cls.is_reference(value):
            return value
        backend = cls._get_backend()
        if backend is None:
            logger.error(
                "Checkpoint references a blob backend that is not configured.",
                extra={"ref": value.get("ref"), "backend": value.get("backend")},
            )
            return None
        try:
            raw = await backend.read(value["ref"])
            blob = gzip.decompress(raw) if value.get("codec") == "gzip" else raw
            return json.loads(blob.decode("utf-8"))
        except Exception as exc:
            logger.error(
                "Failed to load checkpoint blob; resuming without checkpoint.",
                extra={"ref": value.get("ref"), "error_type": type(exc).__name__},
            )
            return None

    @classmethod
    async def sweep(cls) -> int:
        """Delete checkpoint blobs orphaned past the resurrection window.

        A no-op when no blob backend is configured.
        """
        backend = cls._get_backend()
        if backend is None:
            return 0
        removed = await backend.sweep(_BLOB_SWEEP_AGE_SECONDS)
        if removed:
            logger.info("Swept orphaned checkpoint blobs.", extra={"count": removed})
        return removed
