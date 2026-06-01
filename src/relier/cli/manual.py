"""
Relier CLI - Embedded Manual.

The manual ships inside the package as a Python string so ``rl man`` works the
same way on Windows, macOS, and Linux - no Unix ``man`` command required, no
``docs/`` directory needed at runtime. The content mirrors ``docs/rl.md`` and
``man/rl.1``; keep them in sync when adding new commands.
"""

MANUAL = r"""
# rl - Relier CLI Manual

`rl` is the command-line interface for Relier, the reliability layer for
Celery (built for FastAPI, works with Flask, Django, and any other Python
web framework or script).

Every `rl` command is a short-lived process that talks directly to the same
Redis your workers use - there is no daemon, no API server, no extra
infrastructure to deploy. If Redis is reachable, the CLI works.

## Usage

```
rl [OPTIONS] COMMAND [ARGS]...
```

Global options

| Flag               | Description                                        |
| ------------------ | -------------------------------------------------- |
| `--version`, `-v`  | Print the CLI version and exit.                    |
| `--help`           | Show the top-level help message.                   |

Top-level commands

| Command             | Description                                              |
| ------------------- | -------------------------------------------------------- |
| `rl man`            | Print this manual (you are looking at it).               |
| `rl doctor`         | Check Redis and cluster dependency health.               |
| `rl bench`          | Measure Relier dispatch overhead vs raw Celery.          |
| `rl run-resurrector`| Start the Phoenix resurrector engine (run as a service). |

---

## Command groups at a glance

```
rl cluster     Start, stop, scale, and tail the Docker stack
rl tasks       Monitor in-flight tasks; retry, cancel, or inspect them
rl worker      Drain, restart, and view per-worker metrics
rl dlq         Inspect and release quarantined tasks
rl slo         Error-budget burn rates and reports
rl chaos       Trigger failures to validate reliability guarantees
rl config      Show, validate, and update configuration
rl admission   Admission control status
rl admin       Low-level Redis maintenance
```

Run `rl <group> --help` for the commands in any group, and
`rl <group> <command> --help` for the options on any single command.

---

## Cluster - Docker stack management

```bash
rl cluster up [--detach/--no-detach]    # Start the full stack (default: detached)
rl cluster down                          # Gracefully shut down the stack
rl cluster status                        # Docker + Redis health check
rl cluster scale <N>                     # Scale the worker pool to N replicas
rl cluster logs [--follow] [service]     # Tail stack logs; optionally filter by service
```

These commands shell out to `docker compose` against the `docker-compose.yml`
in the current directory. Bare-metal deployments (no Docker) use the
`make worker` / `make resurrector` targets instead.

---

## Tasks - monitor and control the task lifecycle

```bash
rl tasks inflight [--follow] [--worker ID]    # Live view of running tasks
rl tasks list                                  # One-shot inflight table
rl tasks top                                   # Top-like summary: workers, depth, p95
rl tasks inspect <task_id>                    # Full payload + state + Phoenix metadata
rl tasks retry   <task_id>                    # Re-queue a failed or quarantined task
rl tasks cancel  <task_id> [--no-terminate]   # Revoke a running or queued task
rl tasks logs    <task_id> [--follow]         # Stream state changes for a task
```

Notes

- `retry` checks the DLQ first; falls back to the Phoenix payload for tasks
  whose worker died before quarantine.
- `cancel` sends SIGTERM to the running task by default. `--no-terminate`
  marks the task revoked without killing the running process.
- `logs --follow` polls Redis state every 2 seconds. Full stdout/stderr
  streaming needs a log backend like Loki or Elasticsearch.

---

## Workers - worker pool operations

```bash
rl worker status                  # All active workers with session metrics
rl worker drain   <worker_id>     # Graceful shutdown: finish current tasks then exit
rl worker restart <worker_id>     # Send shutdown signal for rolling restart
rl worker reset   <worker_id>     # Clear session counters for a worker
```

`restart` signals the worker to exit cleanly; your process manager (Docker,
systemd, Kubernetes) is responsible for starting a replacement.

---

## Dead Letter Queue (DLQ)

```bash
rl dlq list                       # Show all quarantined tasks
rl dlq inspect  <task_id>         # Full payload, error context, resurrection history
rl dlq release  <task_id>         # Un-quarantine and re-submit to the original queue
rl dlq retry-all                  # Re-submit every quarantined task
rl dlq purge --confirm            # Permanently delete all DLQ entries (irreversible)
```

Releasing preserves the resurrection count so a task can't bypass
`RELIER_MAX_RESURRECTIONS` by being repeatedly released.

---

## SLO - error budget tracking

```bash
rl slo status [--target 0.999]                       # Burn rate across all windows
rl slo report [--period 3d] [--format json|table]    # Historical burn-rate report
```

Burn-rate scale

| Rate         | Meaning                                |
| ------------ | -------------------------------------- |
| `0x`         | No failures observed                   |
| `< 1x`       | Healthy - on or under budget           |
| `1x – 6x`    | Warning - budget depleting             |
| `6x – 14.4x` | High - exhausts budget in 2–5 days     |
| `> 14.4x`    | Critical - exhausts budget in < 2 days |

---

## Chaos - verify reliability under failure

> Run only against a non-production cluster.

```bash
rl chaos worker-kill [--worker ID] [--seed] [--seed-duration N]
                     [--watch] [--watch-duration N]
                                                  # SIGKILL a worker; optionally seed a
                                                  # long-running task first and stream
                                                  # Phoenix resurrection events live.

rl chaos network-partition [--secs N]             # Disconnect Redis for N seconds.
rl chaos load-spike [--rps N] [--duration N]      # Flood dispatch to stress admission.
rl chaos task-corrupt                             # Inject a malformed poison-pill task.
rl chaos slow-task [--duration N]                 # Dispatch a task that times out.
```

See the Chaos Guide in the docs for what each scenario verifies.

---

## Config - view and update settings

```bash
rl config show                       # Print all active settings (passwords redacted)
rl config validate                   # Validate Redis policy and RELIER_* env vars
rl config set <KEY> <VALUE>          # Write a setting to the local .env file
```

Example

```bash
rl config set RELIER_HEARTBEAT_TTL 15
# Restart workers for the change to take effect.
```

---

## Admission control

```bash
rl admission status     # Current admission state and load-shedding percentage
```

---

## Admin - low-level Redis maintenance

```bash
rl admin config             # Display the active cluster configuration
rl admin purge-locks        # Force-delete every idempotency lock sentinel
rl admin reset-admission    # Reset admission control counters
```

`purge-locks` is a recovery tool - use only when you understand the
implications (stuck in-flight sentinels after a corrupted shutdown, etc.).

---

## Examples

Live task monitoring

```bash
rl tasks inflight --follow
rl tasks inflight --follow --worker rl-worker-2
```

Inspect and release a quarantined task

```bash
rl dlq list
rl dlq inspect 5df7f30a-8404-41e1-bb80-6d480771f13f
rl dlq release 5df7f30a-8404-41e1-bb80-6d480771f13f
```

Validate Phoenix resurrection end-to-end

```bash
rl chaos worker-kill --seed --watch --watch-duration 60
```

Scale workers and tail logs

```bash
rl cluster scale 8
rl cluster logs --follow worker
```

Export SLO data for a dashboard

```bash
rl slo report --format json | jq .
```

---

## See also

- `rl --help`                        Top-level CLI help
- `rl <group> --help`                All commands in a group
- `rl <group> <command> --help`      Options for one command
- Online docs                        https://getrelier.github.io/relier
"""
"""The bundled CLI manual text. Rendered by ``rl man`` via Rich Markdown."""
