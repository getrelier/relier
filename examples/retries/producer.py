import os
import time

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from tasks import bulk_import, process_payment


def wait_for(receipt, timeout: int = 60):
    deadline = time.monotonic() + timeout
    while not receipt.ready():
        if time.monotonic() > deadline:
            raise TimeoutError(f"task {receipt.id} did not finish in {timeout}s")
        print(f"  [{receipt.state}] waiting...")
        time.sleep(1)
    return receipt.result if receipt.successful() else None


if __name__ == "__main__":
    # --- idempotent payment (safe to dispatch multiple times) ---
    r1 = process_payment.push(payment_id="pay_001", amount_cents=4999)
    r2 = process_payment.push(payment_id="pay_001", amount_cents=4999)  # deduped
    print(f"payment task ids: {r1.id}  /  {r2.id}")
    print(f"result: {wait_for(r1)}\n")

    # --- bulk import with checkpointing ---
    r3 = bulk_import.push(batch_id="batch_42", total_rows=200)
    print(f"import dispatched: {r3.id}")
    print(f"result: {wait_for(r3)}")
