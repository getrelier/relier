"""
Relier Core — Redis Key Registry.

Centralizes all Redis key naming conventions used across the Relier runtime.

Why this exists
---------------
Distributed systems depend heavily on predictable key topology for:
- observability
- debugging
- operational tooling
- migrations
- safe cleanup
- cross-service coordination

Centralizing key construction prevents namespace drift, inconsistent
prefixing, and accidental collisions between subsystems.

Design rules
------------
* All keys are rooted under the ``rl:`` namespace.
* Key builders are deterministic and side-effect free.
* Dynamic identifiers are always appended in the final segment.
* Storage topology changes should occur here first.
"""

import uuid


class RedisKeys:
    """
    Canonical Redis namespace registry for all Relier subsystems.
    """

    # Global namespace root for all Relier-managed Redis keys.
    PREFIX = "rl"

    @classmethod
    def admission(cls, resource_key: str) -> str:
        """
        Fixed-window admission control counter namespace.

        Used by the Lua admission controller to enforce cluster-wide or
        tenant-scoped capacity limits.
        """
        return f"{cls.PREFIX}:admission:{resource_key}"

    @classmethod
    def heartbeat(cls, task_id: str) -> str:
        """
        Ephemeral Phoenix heartbeat TTL key.

        Presence indicates the task is actively owned by a live worker.
        """
        return f"{cls.PREFIX}:hb:{task_id}"

    @classmethod
    def payload(cls, task_id: str) -> str:
        """Pattern: rl:payload:{task_id}"""
        return f"{cls.PREFIX}:payload:{task_id}"

    @classmethod
    def inflight(cls, worker_id: str) -> str:
        """
        Worker-local sorted-set tracking currently executing task IDs.
        """
        return f"{cls.PREFIX}:inflight:{worker_id}"

    @classmethod
    def dlq_worker(cls, worker_id: str) -> str:
        """
        Worker-scoped Dead Letter Queue namespace.

        Used for operational grouping or future worker-level quarantine policies.
        """
        return f"{cls.PREFIX}:dlq:{worker_id}"

    @classmethod
    def dlq_task(cls, task_id: str) -> str:
        """
        Task-scoped Dead Letter Queue entry namespace.
        """
        return f"{cls.PREFIX}:dlq:{task_id}"

    @classmethod
    def phoenix(cls, task_id: str) -> str:
        """
        Persistent Phoenix task state record.

        Stores resurrection metadata, payload snapshots, and partial progress.
        """
        return f"{cls.PREFIX}:phoenix:{task_id}"

    @classmethod
    def resurrection(cls, task_id: str) -> str:
        """
        Resurrection attempt counter namespace.

        Tracks how many times a task has been revived after worker failure.
        """
        return f"{cls.PREFIX}:resurrections:{task_id}"

    @classmethod
    def resurrect_lock(cls, task_id: str) -> str:
        """
        Distributed resurrection coordination lock.

        Prevents multiple resurrector processes from re-queuing the same task
        concurrently.
        """
        return f"{cls.PREFIX}:lock:resurrect:{task_id}"

    @classmethod
    def lease(cls, task_id: str) -> str:
        """Short-lived lease: only one worker may own this resurrected task right now."""
        return f"{cls.PREFIX}:lease:{task_id}"

    @classmethod
    def fence(cls, task_id: str) -> str:
        """Fencing token: tags this incarnation. Stale workers get rejected."""
        return f"{cls.PREFIX}:fence:{task_id}"

    # =============================================================================
    # Global / singleton namespaces
    # =============================================================================
    @staticmethod
    def idempotency(key: str) -> str:
        """
        Namespace for idempotency sentinels and cached execution results.
        """
        return f"rl:idem:{key}"

    @staticmethod
    def dlq_hash() -> str:
        """
        Root hash storing all quarantined task envelopes.
        """
        return "rl:dlq"

    @staticmethod
    def monitor() -> str:
        """
        Phoenix resurrection monitoring registry.

        Tracks tasks that were recently resurrected and are awaiting
        confirmation of stable execution.
        """
        return "rl:monitoring"

    @staticmethod
    def in_flight(unique_id: str | None = None) -> str:
        """
        Namespace for transient execution-tracking identifiers.

        Used by Phoenix coordination logic to detect abandoned or orphaned
        workloads after worker failure.
        """
        # Auto-generate a collision-resistant execution identifier when the caller
        # does not provide one explicitly.
        suffix = unique_id or uuid.uuid4().hex
        return f"rl:inflight:{suffix}"

    # =============================================================================
    # Worker & Cluster Health
    # =============================================================================
    @classmethod
    def workers(cls) -> str:
        """
        Sorted-set tracking all active workers and their last-seen timestamps.
        """
        return f"{cls.PREFIX}:workers"

    @classmethod
    def presence(cls, worker_id: str) -> str:
        """
        Ephemeral worker presence key.
        """
        return f"{cls.PREFIX}:presence:{worker_id}"

    # =============================================================================
    # Telemetry & Metrics
    # =============================================================================
    @classmethod
    def metric_global(cls, status: str) -> str:
        """
        Global persistent counter for task outcomes.
        """
        return f"{cls.PREFIX}:m:global:{status}"

    @classmethod
    def metric_worker(cls, worker_id: str, status: str) -> str:
        """
        Worker-scoped session counter for task outcomes.
        """
        return f"{cls.PREFIX}:m:w:{worker_id}:{status}"

    @classmethod
    def slo(cls, window: str, status: str) -> str:
        """
        SLO rolling window sorted-set namespace.
        """
        return f"{cls.PREFIX}:slo:{window}:{status}"

    @staticmethod
    def task_durations() -> str:
        """
        List storing historical task execution durations for telemetry.
        """
        return "rl:task_durations"
