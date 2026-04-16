FROM docker.io/library/python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends locales \
    && locale-gen fr_FR.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first (cache layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY src/ src/
COPY website/ website/
COPY static/ static/
COPY migrations/ migrations/

ARG GIT_COMMIT=unknown
LABEL git-commit=${GIT_COMMIT}
ENV APP_VERSION=Beta-${GIT_COMMIT}

ARG PORT=8000
ENV PORT=${PORT}
EXPOSE ${PORT}

CMD uv run --no-sync gunicorn \
    --bind "0.0.0.0:${PORT}" \
    --workers 2 \
    --timeout 120 \
    "src.app:app"
