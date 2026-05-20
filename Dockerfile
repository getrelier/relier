# Use a slim Python image for a smaller footprint
FROM python:3.11-slim-bookworm

# 1. Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:0.1.39 /uv /bin/uv

# 2. Set working directory
WORKDIR /app

# 3. Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

# 4. Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 5. Copy the configuration AND the source code
COPY pyproject.toml README.md ./
COPY src ./src

# 6. Install dependencies
RUN uv venv && uv pip install -e .

# 7. Use the virtualenv by default
ENV PATH="/app/.venv/bin:$PATH"

# Default command is overridden by docker-compose; defaults to a worker.
CMD ["celery", "-A", "relier.tasks.app", "worker", "--loglevel=info"]
