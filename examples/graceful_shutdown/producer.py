import os
import time

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from tasks import bulk_export

if __name__ == "__main__":
    receipt = bulk_export.push(export_id="export_001", total_rows=500)
    print(f"dispatched  {receipt.id}")
    print(
        "Send SIGTERM to the worker now to observe graceful shutdown + checkpoint resumption.\n"
    )

    while not receipt.ready():
        print(f"  [{receipt.state}] waiting...")
        time.sleep(2)

    if receipt.successful():
        print(f"result: {receipt.result}")
    else:
        print(f"failed: {receipt.result}")
