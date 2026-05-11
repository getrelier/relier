from relier.core.exceptions import (
    AdmissionRejectedError,
    IdempotencyInFlightError,
    MaxResurrectionsExceededError,
    PayloadIntegrityError,
    RelierError,
    SchemaMigrationError,
    ShadowRegistryError,
)


def test_custom_exceptions() -> None:
    """
    Ensure all custom exceptions instantiate correctly, hold their
    custom state, and format their internal messages properly.
    """
    # Base and generic exceptions
    assert isinstance(RelierError("base error"), Exception)
    assert isinstance(ShadowRegistryError("shadow error"), RelierError)
    assert isinstance(PayloadIntegrityError("payload error"), RelierError)
    assert isinstance(SchemaMigrationError("schema error"), RelierError)

    # Idempotency logic
    exc_idem = IdempotencyInFlightError("tx_999")
    assert exc_idem.key == "tx_999"
    assert "tx_999" in str(exc_idem)

    # Phoenix / DLQ logic
    exc_max = MaxResurrectionsExceededError("task_abc123", 5)
    assert exc_max.task_id == "task_abc123"
    assert exc_max.count == 5
    assert "task_abc123" in str(exc_max)
    assert "5" in str(exc_max)

    # Admission Control logic
    exc_adm = AdmissionRejectedError("Capacity reached", retry_after=30)
    assert exc_adm.retry_after == 30
    assert "Capacity reached" in str(exc_adm)
