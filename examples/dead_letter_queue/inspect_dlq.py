"""
Inspect and recover tasks from the Dead Letter Queue.

Usage:
    python inspect_dlq.py                      # list all DLQ entries
    python inspect_dlq.py --release <task_id>  # re-queue a task for retry
    python inspect_dlq.py --discard <task_id>  # permanently remove a task

Equivalent CLI commands (recommended for interactive use):
    rl dlq list
    rl dlq show   <task_id>
    rl dlq release <task_id>
    rl dlq discard <task_id>
"""

import argparse
import os
import subprocess
import sys

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")


def rl(*args: str) -> int:
    return subprocess.run(["rl", *args]).returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Relier DLQ inspector")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--release", metavar="TASK_ID", help="re-queue task for retry")
    group.add_argument("--discard", metavar="TASK_ID", help="permanently remove task")
    args = parser.parse_args()

    if args.release:
        sys.exit(rl("dlq", "release", args.release))
    elif args.discard:
        sys.exit(rl("dlq", "discard", args.discard))
    else:
        code = rl("dlq", "list")
        if code != 0:
            print("\nTip: dispatch tasks/poison_pill to populate the DLQ, then re-run.")
        sys.exit(code)
