import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from tasks import charge_card, generate_monthly_invoice, send_order_confirmation

if __name__ == "__main__":
    order_id = "ord_001"

    # Dispatch to three queues — worker must consume all three.
    # Run worker with: --queues=default,high_priority,low_priority
    charge_receipt = charge_card.push(order_id=order_id, amount_cents=2999)
    email_receipt = send_order_confirmation.push(
        order_id=order_id, email="user@example.com"
    )
    invoice_receipt = generate_monthly_invoice.push(
        customer_id="cust_42", month="2026-06"
    )

    print(f"high_priority charge:   {charge_receipt.id}")
    print(f"default       email:    {email_receipt.id}")
    print(f"low_priority  invoice:  {invoice_receipt.id}")

    # Only wait on the critical path
    result = charge_receipt.get(timeout=30)
    print(f"\ncharge result: {result}")
