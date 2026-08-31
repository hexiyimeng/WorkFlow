"""Worker-side validation loaded by dask-jobqueue's standard Worker command."""

from __future__ import annotations

from typing import Any

from services.dask_service import WorkerDevicePlugin


def dask_setup(worker: Any) -> None:
    """Validate the Slurm-provided device mask before accepting tasks."""

    WorkerDevicePlugin().setup(worker)


__all__ = ["dask_setup"]
