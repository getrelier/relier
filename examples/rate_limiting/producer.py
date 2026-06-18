import os
import time

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")
os.environ.setdefault("RELIER_ADMISSION_LIMIT", "100")
os.environ.setdefault("RELIER_ADMISSION_WINDOW", "10")

from tasks import send_notification

from relier import AdmissionRejectedError


def dispatch_with_backoff(user_id: str, message: str, max_attempts: int = 5):
    for attempt in range(1, max_attempts + 1):
        try:
            return send_notification.push(user_id=user_id, message=message)
        except AdmissionRejectedError as exc:
            if attempt == max_attempts:
                raise
            print(
                f"  rate limited — sleeping {exc.retry_after}s (attempt {attempt}/{max_attempts})"
            )
            time.sleep(exc.retry_after)
    return None


if __name__ == "__main__":
    user_ids = [f"user_{i:04d}" for i in range(150)]
    succeeded = 0
    rejected = 0

    for uid in user_ids:
        try:
            receipt = dispatch_with_backoff(uid, "You have a new message!")
            succeeded += 1
        except AdmissionRejectedError:
            rejected += 1
            print(f"  gave up for {uid}")

    print(f"\ndispatched {succeeded}/{len(user_ids)}  |  rejected {rejected}")
