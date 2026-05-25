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

# 5. Create a non-root user that owns /app. Celery refuses to run as root
# unless C_FORCE_ROOT is set; the principle of least privilege also says
# never give container processes uid 0. WORKDIR created /app as root-owned,
# so we hand it to the relier user before any USER-scoped writes happen.
RUN groupadd --system --gid 1000 relier \
    && useradd --system --uid 1000 --gid relier \
        --home-dir /app --shell /usr/sbin/nologin relier \
    && chown relier:relier /app

# 6. Copy the configuration AND the source code (chown to the new user so
# `uv pip install -e .` can write the .venv).
COPY --chown=relier:relier pyproject.toml README.md ./
COPY --chown=relier:relier src ./src

# 7. Install dependencies as the unprivileged user so the resulting venv is
# owned by it. UV_CACHE_DIR is pinned to a path the relier user owns —
# the default ($HOME/.cache/uv) would fall back to /app/.cache which we
# explicitly don't want polluting the working directory.
ENV UV_CACHE_DIR=/app/.uv-cache
USER relier
RUN uv venv && uv pip install -e . && rm -rf "$UV_CACHE_DIR"

# 8. Use the virtualenv by default
ENV PATH="/app/.venv/bin:$PATH"

# Default command is overridden by docker-compose; defaults to a worker.
CMD ["celery", "-A", "relier.tasks.app", "worker", "--loglevel=info"]
