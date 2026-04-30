import pytest
from relier.core.exceptions import (
    RelierError,
    IdempotencyInFlightError,
    ShadowRegistryError,
    MaxResurrectionsExceededError,
    PayloadIntegrityError,
    SchemaMigrationError,
    AdmissionRejectedError,
)

def test_custom_exceptions():
    """
    Ensure all custom exceptions instantiate correctly, hold their
    custom state, and format their internal messages properly.
    """
    # 1. Base and generic exceptions
    assert isinstance(RelierError("base error"), Exception)
    assert isinstance(ShadowRegistryError("shadow error"), RelierError)
    assert isinstance(PayloadIntegrityError("payload error"), RelierError)
    assert isinstance(SchemaMigrationError("schema error"), RelierError)

    # 2. Idempotency logic
    exc_idem = IdempotencyInFlightError("tx_999")
    assert exc_idem.key == "tx_999"
    assert "tx_999" in str(exc_idem)

    # 3. Phoenix / DLQ logic
    exc_max = MaxResurrectionsExceededError("task_abc123", 5)
    assert exc_max.task_id == "task_abc123"
    assert exc_max.count == 5
    assert "task_abc123" in str(exc_max)
    assert "5" in str(exc_max)

    # 4. Admission Control logic
    exc_adm = AdmissionRejectedError("Capacity reached", retry_after=30)
    assert exc_adm.retry_after == 30
    assert "Capacity reached" in str(exc_adm)