# rl: Relier CLI Manual

## Overview

`rl` is the command-line interface for Relier, the reliability layer for Celery. It exposes subcommands for monitoring tasks, managing the Dead Letter Queue, operating the Phoenix resurrector, running chaos scenarios, and inspecting cluster health.

## Usage

```bash
rl [OPTIONS] COMMAND [ARGS]...
```

Global options:

- `--version`, `-v`: Show the application's version and exit.
- `rl man`: Print the local `rl` manual from `docs/rl.md`.

## Task Management

- `rl tasks inflight [--follow]`: Show currently executing tasks across the cluster.
- `rl tasks list`: Render the current inflight task table once.
- `rl tasks inspect <task_id>`: Inspect a task's Phoenix registry payload, worker, and resurrection state.
- `rl tasks top`: Show a top-like summary of active workers, queue depth, and latency.

## Dead Letter Queue (DLQ)

- `rl dlq list`: List quarantined tasks in the DLQ.
- `rl dlq inspect <task_id>`: View full DLQ payload and error context.
- `rl dlq retry <task_id>`: Re-submit a quarantined task to its original queue.
- `rl dlq purge --confirm`: Permanently delete all DLQ entries.

## Reliability & Resurrector

- `rl resurrect [--interval <seconds>]`: Start the Phoenix resurrector loop.
- `rl run-resurrector [--interval <seconds>]`: Alias for `rl resurrect`.
- `rl doctor`: Check Redis, PostgreSQL, and cluster dependency health.
- `rl bench`: Run a local benchmark to measure Relier overhead.

## Admission Control

- `rl admission status`: Show current admission control state and load-shedding status.

## Worker & Cluster Operations

- `rl worker status`: Show registered worker nodes and in-flight task counts.
- `rl cluster status`: Check Docker Compose infrastructure health.
- `rl cluster scale <workers>`: Scale worker replicas to the requested count.

## Administration

- `rl admin config`: Display active Relier configuration values.
- `rl admin purge-locks`: Delete all idempotency locks from Redis.
- `rl admin reset-admission`: Reset admission control counters.

## Chaos Engineering

- `rl chaos worker-kill [--worker-id <id>]`: Kill a worker process to simulate failure.
- `rl chaos network-partition [--duration <seconds>]`: Simulate Redis outage.
- `rl chaos load-spike [--rps <n>] [--duration <seconds>]`: Create a burst of task load.
- `rl chaos task-corrupt`: Inject a malformed poison-pill task.
- `rl chaos api-timeout [--delay <seconds>]`: Add latency to external API calls.
- `rl chaos slow-task [--duration <seconds>]`: Force the next debug task to hang.

## Examples

Tail inflight tasks live:

```bash
rl tasks inflight --follow
```

Inspect a quarantined task:

```bash
rl tasks inspect 5df7f30a-8404-41e1-bb80-6d480771f13f
```

Retry a DLQ task:

```bash
rl dlq retry 5df7f30a-8404-41e1-bb80-6d480771f13f
```

Start the resurrector with a custom interval:

```bash
rl resurrect --interval 10
```

Run a chaos worker kill:

```bash
rl chaos worker-kill
```

## Installation of man page (Linux)

To install the man page locally:

```bash
sudo cp man/rl.1 /usr/local/share/man/man1/
sudo mandb
man rl
```

---

For more details on any command, run `rl <subcommand> --help` or `rl <group> --help`.
