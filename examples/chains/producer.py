import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from celery import chain
from tasks import process_payment, reserve_inventory, send_confirmation, validate_order

if __name__ == "__main__":
    order_id = "ord_789"

    # Steps execute sequentially; each step's return value is passed as the
    # first positional arg to the next step.  If any step raises, the chain stops.
    #
    # Note: chains use Celery's native .s() signatures. Phoenix still protects
    # each individual step on crash; idempotency must be enabled per-task if needed.
    workflow = chain(
        validate_order.s(order_id),
        reserve_inventory.s(),
        process_payment.s(),
        send_confirmation.s(),
    )

    result = workflow.delay()
    print(f"chain id: {result.id}")

    final = result.get(timeout=60)
    print(f"result:   {final}")
