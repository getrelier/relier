"""
Unit tests for Relier Core - Schema Versioning.
Covers task envelopes, payload integrity checks, and schema migrations.
"""

from datetime import UTC, datetime

import pytest

from relier.core.exceptions import PayloadIntegrityError
from relier.core.schema import SchemaRegistry

# ==========================================
# Test Isolation
# ==========================================


@pytest.fixture(autouse=True)
def reset_schema_registry():
    """
    Ensure clean state for the singleton SchemaRegistry between tests.
    Prevents migration registrations from bleeding across test cases.
    """
    original_version = SchemaRegistry.CURRENT_VERSION
    original_migrations = SchemaRegistry._migrations.copy()

    # Reset to baseline
    SchemaRegistry.CURRENT_VERSION = 1
    SchemaRegistry._migrations = {}

    yield

    # Restore after test
    SchemaRegistry.CURRENT_VERSION = original_version
    SchemaRegistry._migrations = original_migrations


# ==========================================
# Test Suites
# ==========================================


class TestEnvelopeCreation:
    def test_envelope_includes_schema_version(self) -> None:
        SchemaRegistry.CURRENT_VERSION = 2
        envelope = SchemaRegistry.wrap("test_task", args=(1, 2), kwargs={"key": "val"})

        assert envelope["schema_version"] == 2
        assert envelope["task_id"] == "test_task"
        assert envelope["payload"]["args"] == (1, 2)
        assert envelope["payload"]["kwargs"] == {"key": "val"}

    def test_envelope_includes_checksum(self) -> None:
        envelope = SchemaRegistry.wrap("test_task", args=(), kwargs={})

        assert "checksum" in envelope
        assert isinstance(envelope["checksum"], str)
        assert envelope["checksum"].startswith("sha256:")

    def test_checksum_detects_payload_tampering(self) -> None:
        envelope = SchemaRegistry.wrap("test_task", args=("original",), kwargs={})

        # Simulate an attacker or network corruption altering the payload
        envelope["payload"]["args"] = ("tampered",)

        with pytest.raises(PayloadIntegrityError, match="Payload checksum mismatch"):
            SchemaRegistry.unwrap_and_migrate("test_task", envelope)

    def test_envelope_includes_enqueued_at_timestamp(self) -> None:
        envelope = SchemaRegistry.wrap("test_task", args=(), kwargs={})

        assert "enqueued_at" in envelope
        # Verify it parses cleanly back to a timezone-aware UTC datetime
        dt = datetime.fromisoformat(envelope["enqueued_at"])
        assert dt.tzinfo == UTC


class TestSchemaMigration:
    def test_v1_migrates_to_v2_correctly(self) -> None:
        SchemaRegistry.CURRENT_VERSION = 2

        @SchemaRegistry.register_migration("my_task", from_version=1)
        def migrate_v1_to_v2(args, kwargs):
            kwargs["new_required_arg"] = "default_value"
            return args, kwargs

        payload = {"args": ("test",), "kwargs": {}}
        envelope = {
            "task_id": "my_task",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": datetime.now(UTC).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }

        args, kwargs = SchemaRegistry.unwrap_and_migrate("my_task", envelope)

        assert args == ("test",)
        assert kwargs["new_required_arg"] == "default_value"

    def test_v2_does_not_migrate_if_already_current(self) -> None:
        SchemaRegistry.CURRENT_VERSION = 2

        migration_called = False

        @SchemaRegistry.register_migration("my_task", from_version=2)
        def migrate_v2_to_v3(args, kwargs):
            nonlocal migration_called
            migration_called = True
            return args, kwargs

        payload = {"args": (), "kwargs": {"status": "current"}}
        envelope = {
            "task_id": "my_task",
            "schema_version": 2,  # Matches CURRENT_VERSION
            "payload": payload,
            "enqueued_at": datetime.now(UTC).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }

        args, kwargs = SchemaRegistry.unwrap_and_migrate("my_task", envelope)

        assert not migration_called
        assert kwargs["status"] == "current"

    def test_migration_chain_v1_to_v3_works(self) -> None:
        SchemaRegistry.CURRENT_VERSION = 3

        @SchemaRegistry.register_migration("chain_task", from_version=1)
        def migrate_1_to_2(args, kwargs):
            kwargs["v2_applied"] = True
            return args, kwargs

        @SchemaRegistry.register_migration("chain_task", from_version=2)
        def migrate_2_to_3(args, kwargs):
            kwargs["v3_applied"] = True
            return args, kwargs

        payload = {"args": (), "kwargs": {"original": True}}
        envelope = {
            "task_id": "chain_task",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": datetime.now(UTC).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }

        args, kwargs = SchemaRegistry.unwrap_and_migrate("chain_task", envelope)

        assert kwargs["original"] is True
        assert kwargs["v2_applied"] is True
        assert kwargs["v3_applied"] is True

    def test_unknown_version_raises_error(self) -> None:
        """
        Tests that schema versions completely out of bounds (e.g. >100 or <1)
        trigger a PayloadIntegrityError via Pydantic validation.
        """
        payload = {"args": (), "kwargs": {}}  # type: ignore[var-annotated]
        envelope = {
            "task_id": "bad_version_task",
            "schema_version": 999,  # Invalid according to Pydantic Field(le=100)
            "payload": payload,
            "enqueued_at": datetime.now(UTC).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }

        with pytest.raises(PayloadIntegrityError, match="Invalid envelope structure"):
            SchemaRegistry.unwrap_and_migrate("bad_version_task", envelope)

    def test_migration_preserves_all_payload_fields(self) -> None:
        SchemaRegistry.CURRENT_VERSION = 2

        @SchemaRegistry.register_migration("preserve_task", from_version=1)
        def migrate_v1_to_v2(args, kwargs):
            kwargs["new_data"] = "added"
            return args, kwargs

        payload = {"args": ("keep_me",), "kwargs": {"existing_kwarg": 42}}
        envelope = {
            "task_id": "preserve_task",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": datetime.now(UTC).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }

        args, kwargs = SchemaRegistry.unwrap_and_migrate("preserve_task", envelope)

        # Verify old data wasn't destroyed
        assert args == ("keep_me",)
        assert kwargs["existing_kwarg"] == 42
        assert kwargs["new_data"] == "added"

    def test_migration_failure_raises_and_routes_to_dlq(self) -> None:
        """Ensure that if a migration function crashes, the error propagates up."""
        SchemaRegistry.CURRENT_VERSION = 2

        @SchemaRegistry.register_migration("crash_task", from_version=1)
        def faulty_migration(args, kwargs):
            raise TypeError("Expected dict but got list in legacy payload")

        payload = {"args": (), "kwargs": {}}  # type: ignore[var-annotated]
        envelope = {
            "task_id": "crash_task",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": datetime.now(UTC).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }

        # The core code re-raises the exception so the Relier DLQ mechanism catches it
        with pytest.raises(TypeError, match="legacy payload"):
            SchemaRegistry.unwrap_and_migrate("crash_task", envelope)


class TestValidate:
    """SchemaRegistry.validate() surfaces misconfigured migrations at worker startup."""

    def test_logs_critical_for_migration_with_unbumped_version(self, caplog) -> None:
        """Migration registered with from_version=CURRENT_VERSION is flagged."""
        import logging

        @SchemaRegistry.register_migration("validate_task", from_version=1)
        def noop(args, kwargs):
            return args, kwargs

        with caplog.at_level(logging.CRITICAL, logger="relier.core.schema"):
            SchemaRegistry.validate()

        assert any("will never run" in r.message for r in caplog.records)
        assert any("validate_task" in r.message for r in caplog.records)

    def test_logs_critical_for_each_bad_migration(self, caplog) -> None:
        """Every misconfigured migration across every task is reported."""
        import logging

        @SchemaRegistry.register_migration("task_a", from_version=1)
        def noop_a(args, kwargs):
            return args, kwargs

        @SchemaRegistry.register_migration("task_b", from_version=1)
        def noop_b(args, kwargs):
            return args, kwargs

        with caplog.at_level(logging.CRITICAL, logger="relier.core.schema"):
            SchemaRegistry.validate()

        messages = [r.message for r in caplog.records if r.levelno == logging.CRITICAL]
        assert any("task_a" in m for m in messages)
        assert any("task_b" in m for m in messages)

    def test_silent_when_all_migrations_correctly_configured(self, caplog) -> None:
        """No CRITICAL log when every migration has from_version < CURRENT_VERSION."""
        import logging

        SchemaRegistry.CURRENT_VERSION = 2

        @SchemaRegistry.register_migration("clean_task", from_version=1)
        def noop(args, kwargs):
            return args, kwargs

        with caplog.at_level(logging.CRITICAL, logger="relier.core.schema"):
            SchemaRegistry.validate()

        assert not any(r.levelno == logging.CRITICAL for r in caplog.records)

    def test_silent_with_no_migrations_registered(self, caplog) -> None:
        """No migrations registered → validate() is a no-op."""
        import logging

        with caplog.at_level(logging.CRITICAL, logger="relier.core.schema"):
            SchemaRegistry.validate()

        assert not any(r.levelno == logging.CRITICAL for r in caplog.records)

    def test_includes_bump_instruction_in_message(self, caplog) -> None:
        """Message tells the developer exactly which version to bump to."""
        import logging

        @SchemaRegistry.register_migration("bump_hint_task", from_version=1)
        def noop(args, kwargs):
            return args, kwargs

        with caplog.at_level(logging.CRITICAL, logger="relier.core.schema"):
            SchemaRegistry.validate()

        assert any("CURRENT_VERSION" in r.message for r in caplog.records)
        # Should tell them to bump to from_version + 1 = 2
        assert any("2" in r.message for r in caplog.records)


class TestRegistrationGuard:
    """register_migration emits a warning when CURRENT_VERSION is not bumped first."""

    def test_warns_when_from_version_equals_current_version(self, caplog) -> None:
        """from_version=1 with CURRENT_VERSION=1 — loop never fires, should warn."""
        import logging

        with caplog.at_level(logging.WARNING, logger="relier.core.schema"):

            @SchemaRegistry.register_migration("guard_task", from_version=1)
            def noop(args, kwargs):
                return args, kwargs

        assert any("will never run" in r.message for r in caplog.records)
        assert any("CURRENT_VERSION" in r.message for r in caplog.records)

    def test_warns_when_from_version_exceeds_current_version(self, caplog) -> None:
        """from_version=3 with CURRENT_VERSION=1 — even further out of range."""
        import logging

        with caplog.at_level(logging.WARNING, logger="relier.core.schema"):

            @SchemaRegistry.register_migration("guard_task_2", from_version=3)
            def noop(args, kwargs):
                return args, kwargs

        assert any("will never run" in r.message for r in caplog.records)

    def test_no_warning_when_current_version_bumped(self, caplog) -> None:
        """from_version=1 with CURRENT_VERSION=2 is valid — no warning."""
        import logging

        SchemaRegistry.CURRENT_VERSION = 2

        with caplog.at_level(logging.WARNING, logger="relier.core.schema"):

            @SchemaRegistry.register_migration("guard_task_3", from_version=1)
            def noop(args, kwargs):
                return args, kwargs

        assert not any("will never run" in r.message for r in caplog.records)

    def test_migration_still_registered_despite_warning(self) -> None:
        """Migration is stored even when the warning fires, so it runs once CURRENT_VERSION is bumped."""

        @SchemaRegistry.register_migration("guard_task_4", from_version=1)
        def adds_field(args, kwargs):
            kwargs["added"] = True
            return args, kwargs

        assert "guard_task_4" in SchemaRegistry._migrations
        assert 1 in SchemaRegistry._migrations["guard_task_4"]

        # Now fix the misconfiguration and verify the migration actually runs.
        SchemaRegistry.CURRENT_VERSION = 2
        payload = {"args": (), "kwargs": {}}
        envelope = {
            "task_id": "guard_task_4",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": datetime.now(UTC).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }
        _, kwargs = SchemaRegistry.unwrap_and_migrate("guard_task_4", envelope)
        assert kwargs["added"] is True
