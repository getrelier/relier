.PHONY: setup format lint check test test-integration clean \
        worker resurrector dev dev-down dev-logs prod prod-down

# =============================================================================
# Setup & quality
# =============================================================================

# Install the project and dev tooling into a local virtualenv.
setup:
	uv venv
	uv pip install -e ".[dev]"
	pre-commit install

format:
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

check: lint
	uv run mypy src/

test:
	uv run pytest tests/unit/ -v

test-integration:
	uv run pytest tests/integration/ -v --timeout=120

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	@echo "Local cache cleared."

# =============================================================================
# Run bare-metal (no Docker)
# =============================================================================
# Relier is a plain Python library — these targets run the processes directly,
# the same way you would run Celery. They require a reachable Redis: set
# RELIER_REDIS_URL if it is not redis://localhost:6379/0. Relier preflight-checks
# Redis and refuses to start if it is unreachable.
# See docs/running.md.

# Run a Celery worker consuming every Relier queue (fine for local dev).
worker:
	uv run celery -A relier.tasks.app worker -l info \
		-Q high_priority,default,low_priority,re-queue

# Run the Phoenix resurrector (detects dead workers and replays their tasks).
resurrector:
	uv run rl run-resurrector

# =============================================================================
# Run with Docker — dev
# =============================================================================
# Single-node Redis (AOF + RDB) + workers + resurrector. See docker-compose.yml.

dev:
	docker compose up -d --build
	@echo "[SUCCESS] Relier dev cluster running. Logs: make dev-logs"

dev-down:
	docker compose down

dev-logs:
	docker compose logs -f

# =============================================================================
# Run with Docker — prod
# =============================================================================
# HA topology: Redis master + replicas + Sentinel + backup sidecar.
# Requires REDIS_PASSWORD and SENTINEL_PASSWORD (via the environment or a
# .env file). See docker-compose.prod.yml and docs/running.md.

prod:
	docker compose -f docker-compose.prod.yml up -d --build
	@echo "[SUCCESS] Relier prod cluster running."

prod-down:
	docker compose -f docker-compose.prod.yml down
