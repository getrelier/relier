"""
Unit tests for Relier Core - Schema Versioning.
Covers task envelopes, payload integrity checks, and schema migrations.
"""

import json
import pytest
from datetime import datetime, timezone

from pydantic import ValidationError
from relier.core.exceptions import PayloadIntegrityError
from relier.core.schema import SchemaRegistry, TaskEnvelope

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
    
    def test_envelope_includes_schema_version(self):
        SchemaRegistry.CURRENT_VERSION = 2
        envelope = SchemaRegistry.wrap("test_task", args=(1, 2), kwargs={"key": "val"})
        
        assert envelope["schema_version"] == 2
        assert envelope["task_id"] == "test_task"
        assert envelope["payload"]["args"] == (1, 2)
        assert envelope["payload"]["kwargs"] == {"key": "val"}

    def test_envelope_includes_checksum(self):
        envelope = SchemaRegistry.wrap("test_task", args=(), kwargs={})
        
        assert "checksum" in envelope
        assert isinstance(envelope["checksum"], str)
        assert envelope["checksum"].startswith("sha256:")

    def test_checksum_detects_payload_tampering(self):
        envelope = SchemaRegistry.wrap("test_task", args=("original",), kwargs={})
        
        # Simulate an attacker or network corruption altering the payload
        envelope["payload"]["args"] = ("tampered",)
        
        with pytest.raises(PayloadIntegrityError, match="Payload checksum mismatch"):
            SchemaRegistry.unwrap_and_migrate("test_task", envelope)

    def test_envelope_includes_enqueued_at_timestamp(self):
        envelope = SchemaRegistry.wrap("test_task", args=(), kwargs={})
        
        assert "enqueued_at" in envelope
        # Verify it parses cleanly back to a timezone-aware UTC datetime
        dt = datetime.fromisoformat(envelope["enqueued_at"])
        assert dt.tzinfo == timezone.utc


class TestSchemaMigration:
    
    def test_v1_migrates_to_v2_correctly(self):
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
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }
        
        args, kwargs = SchemaRegistry.unwrap_and_migrate("my_task", envelope)
        
        assert args == ("test",)
        assert kwargs["new_required_arg"] == "default_value"

    def test_v2_does_not_migrate_if_already_current(self):
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
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }
        
        args, kwargs = SchemaRegistry.unwrap_and_migrate("my_task", envelope)
        
        assert not migration_called
        assert kwargs["status"] == "current"

    def test_migration_chain_v1_to_v3_works(self):
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
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }
        
        args, kwargs = SchemaRegistry.unwrap_and_migrate("chain_task", envelope)
        
        assert kwargs["original"] is True
        assert kwargs["v2_applied"] is True
        assert kwargs["v3_applied"] is True

    def test_unknown_version_raises_error(self):
        """
        Tests that schema versions completely out of bounds (e.g. >100 or <1)
        trigger a PayloadIntegrityError via Pydantic validation.
        """
        payload = {"args": (), "kwargs": {}}
        envelope = {
            "task_id": "bad_version_task",
            "schema_version": 999,  # Invalid according to Pydantic Field(le=100)
            "payload": payload,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }
        
        with pytest.raises(PayloadIntegrityError, match="Invalid envelope structure"):
            SchemaRegistry.unwrap_and_migrate("bad_version_task", envelope)

    def test_migration_preserves_all_payload_fields(self):
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
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }
        
        args, kwargs = SchemaRegistry.unwrap_and_migrate("preserve_task", envelope)
        
        # Verify old data wasn't destroyed
        assert args == ("keep_me",)
        assert kwargs["existing_kwarg"] == 42
        assert kwargs["new_data"] == "added"
        
    def test_migration_failure_raises_and_routes_to_dlq(self):
        """Ensure that if a migration function crashes, the error propagates up."""
        SchemaRegistry.CURRENT_VERSION = 2
        
        @SchemaRegistry.register_migration("crash_task", from_version=1)
        def faulty_migration(args, kwargs):
            raise TypeError("Expected dict but got list in legacy payload")
            
        payload = {"args": (), "kwargs": {}}
        envelope = {
            "task_id": "crash_task",
            "schema_version": 1,
            "payload": payload,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "checksum": SchemaRegistry._generate_checksum(payload),
        }
        
        # The core code re-raises the exception so the Relier DLQ mechanism catches it
        with pytest.raises(TypeError, match="legacy payload"):
            SchemaRegistry.unwrap_and_migrate("crash_task", envelope)