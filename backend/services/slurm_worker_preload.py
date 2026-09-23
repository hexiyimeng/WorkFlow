"""Worker-side validation loaded by dask-jobqueue's standard Worker command."""

from __future__ import annotations

from typing import Any

from core.platform import should_schedule_malloc_trim
from services.dask_service import WorkerDevicePlugin, WorkerMemoryTrimPlugin


def dask_setup(worker: Any) -> None:
    """Validate the Slurm-provided device mask before accepting tasks."""

    WorkerDevicePlugin().setup(worker)
    if should_schedule_malloc_trim():
        plugin = WorkerMemoryTrimPlugin()
        # Preloads run before the Worker accepts tasks.  Register the plugin in
        # the Worker's own plugin map so its transition and teardown hooks are
        # invoked for the complete task lifecycle.
        worker.plugins[plugin.name] = plugin
        plugin.setup(worker)


__all__ = ["dask_setup"]
