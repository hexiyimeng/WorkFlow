# syntax=docker/dockerfile:1.7

FROM node:22-bookworm-slim AS frontend-builder

WORKDIR /app

COPY frontend/package*.json ./frontend/
WORKDIR /app/frontend
RUN --mount=type=cache,target=/root/.npm npm ci

COPY frontend/ ./
RUN npm run build


FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/backend \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    VIRTUAL_ENV=/app/backend/.venv \
    WorkFlow_MODELS_DIR=/models \
    WorkFlow_DASK_LOCAL_DIR=/tmp/workflow_dask_spill

WORKDIR /app/backend

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY backend/pyproject.toml backend/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project

COPY backend/ ./
COPY --from=frontend-builder /app/backend/dist ./dist

ENV PATH="/app/backend/.venv/bin:$PATH"

EXPOSE 8000 8787

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
