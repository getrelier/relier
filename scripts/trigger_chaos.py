import httpx


def main():
    print("Relier Phoenix Litmus Test")
    print("-" * 33)

    # Trigger via the API to ensure Phoenix Registration happens
    payload = {
        "name": "resurrection_task",  # Using the actual registered name
        "args": [30, "chaos_test_01"],  # duration, marker_key
        "kwargs": {},
    }

    print("Enqueuing task via Relier API...")
    response = httpx.post("http://localhost:8000/tasks/trigger", json=payload)

    if response.status_code == 200:
        task_id = response.json()["task_id"]
        print(f"Task enqueued! ID: {task_id}")

        print("\n[VERIFICATION]")
        print(f"Check status: http://localhost:8000/tasks/{task_id}")
        print("\n[CHAOS STEPS]")
        print("1. Run: docker stop relier-worker")
        print("2. Wait 15 seconds (for heartbeat to expire)")
        print("3. Check http://localhost:8000/admin/dlq (to see it quarantined)")
    else:
        print(f"Failed to trigger: {response.text}")


if __name__ == "__main__":
    main()
