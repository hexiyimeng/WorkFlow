# Docker Deployment

This project can run as a single Docker service. The image builds the React
frontend first, writes the static bundle to `backend/dist`, then starts the
FastAPI backend with `uvicorn`.

The backend dependencies are installed with `uv` from `backend/pyproject.toml`
and `backend/uv.lock` inside the Linux image. Do not copy a local Windows
virtual environment into the image.

## Requirements

- Docker Engine with Docker Compose.
- For GPU execution: NVIDIA driver and NVIDIA Container Toolkit installed on
  the host.
- Cellpose model files installed locally under `models/cellpose/`.

## Model Directory

Model files are intentionally not committed to the repository. Put them in:

```text
models/
  cellpose/
    cpsam
```

or:

```text
models/
  cellpose/
    cpsam.pth
```

The compose file mounts `./models` into the container at `/models` and sets:

```text
WorkFlow_MODELS_DIR=/models
```

## CPU Startup

```bash
docker compose up --build
```

Open:

```text
http://localhost:8000
```

The Dask dashboard is exposed on:

```text
http://localhost:8787
```

## GPU Startup

First verify the host GPU is visible to Docker:

```bash
docker run --rm --gpus all nvidia/cuda:12.8.0-base-ubuntu24.04 nvidia-smi
```

Then start with the GPU override:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build
```

The Python dependencies currently use the PyTorch CUDA 12.8 wheel index from
`backend/pyproject.toml`, so the host NVIDIA driver must support CUDA 12.8
runtime containers.

## Useful Runtime Settings

Override these in `docker-compose.yml` or on the command line when needed:

```text
WorkFlow_WORKERS=1
WorkFlow_WORKER_MEMORY_LIMIT_GB=4
WorkFlow_CUDA_MODE=disabled
WorkFlow_DASK_LOCAL_DIR=/tmp/workflow_dask_spill
```

For CPU-only deployments, keep `WorkFlow_CUDA_MODE=disabled`. For GPU
deployments, use `docker-compose.gpu.yml`, which sets `WorkFlow_CUDA_MODE=auto`
and enables `gpus: all`.
