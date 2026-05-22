"""
Relier Chaos — Task Corruption Scenario.

Injects a 'poison pill' envelope whose payload checksum does not match
its body. The producer side (``apush``) normally guarantees envelope
integrity, so this scenario bypasses it by calling
``celery_app.send_task`` directly with a hand-crafted envelope.

The receiving worker will:
    1. Unwrap the envelope through ``SchemaRegistry.unwrap_and_migrate``
    2. Detect the checksum mismatch and raise ``PayloadIntegrityError``
    3. Quarantine the task to the DLQ via ``DeadLetterQueue.quarantine``

This validates the full integrity-failure path end-to-end.
"""

import asyncio
import logging
import uuid
from datetime import UTC, datetime

from relier.chaos.engine import chaos_engine

logger = logging.getLogger(__name__)

# Any registered task name will do, the worker rejects the envelope before
# the user function ever runs. We target an internal chaos task that the
# Celery app loads in every environment (see ``relier.chaos.tasks``).
POISON_TARGET_TASK = "relier.chaos.tasks.chaos_noop"


@chaos_engine.register("task-corrupt")
class TaskCorruptScenario:
    """Inject a malformed task to trigger PayloadIntegrityError."""

    async def execute(self) -> None:
        """Send a structurally-valid envelope with a deliberately-bad checksum."""
        from relier.tasks.app import celery_app

        task_id = f"chaos-poison-{uuid.uuid4().hex[:8]}"

        # Structurally valid Pydantic envelope (all required fields present),
        # but checksum is wrong on purpose so unwrap_and_migrate rejects it
        # and the decorator routes the task to the DLQ.
        bad_envelope = {
            "schema_version": 1,
            "task_id": task_id,
            "payload": {"args": ["poison"], "kwargs": {}},
            "checksum": "sha256:TAMPERED_INVALID_CHECKSUM_VALUE",
            "enqueued_at": datetime.now(UTC).isoformat(),
        }

        logger.critical(
            "Injecting corrupted poison pill task.",
            extra={"task_id": task_id, "target": POISON_TARGET_TASK},
        )

        # Celery's publishing API is synchronous; dispatch off the event loop
        # so the broker round-trip does not block the asyncio runtime.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: celery_app.send_task(
                POISON_TARGET_TASK,
                args=(bad_envelope,),
                queue="default",
                task_id=task_id,
            ),
        )

        logger.info(
            "Poison pill injected; expect a DLQ quarantine for PayloadIntegrityError.",
            extra={"task_id": task_id},
        )
