.PHONY: setup lint format check test test-integration clean

# Setup local environment
setup:
	uv venv
	uv pip install -e ".[dev]"
	pre-commit install

# Formatting and Linting
format:
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

check: lint
	uv run mypy src/

# Testing
test:
	uv run pytest tests/unit/ -v

test-integration:
	uv run pytest tests/integration/ -v --timeout=120

# Clean cache files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	@echo "Local cache cleared."

# ======================================================================
# Docker & Compose Targets
# ======================================================================

COMPOSE := docker compose -p relier-cluster

# Build the images and start the local cluster
dev:
	$(COMPOSE) build
	$(COMPOSE) up -d
	@echo "[SUCCESS] Relier dev cluster running!"
	@echo "  App:  http://localhost:8000"
	@echo "  CLI:  rl status"

# Spin down the cluster
down:
	$(COMPOSE) down
	@echo "[SUCCESS] Relier dev cluster stopped."

# Stop and remove volumes (clean slate)
down-clean:
	$(COMPOSE) down -v
	@echo "[SUCCESS] Relier dev cluster stopped and volumes cleared."

# Shell into the API container
api:
	$(COMPOSE) exec api bash

# Shell into the Worker container
worker:
	$(COMPOSE) exec worker bash

# Shell into Redis
redis:
	$(COMPOSE) exec redis redis-cli

# Shell into Postgres
postgres:
	$(COMPOSE) exec postgres psql -U relier relier
