import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from relier import rl_task


@rl_task(queue="default")
async def greet(name: str) -> str:
    return f"Hello, {name}!"


if __name__ == "__main__":
    # Start the worker first:  python worker.py
    receipt = greet.push(name="World")
    print(f"dispatched  {receipt.id}")

    result = receipt.get(timeout=30)
    print(f"result      {result}")
