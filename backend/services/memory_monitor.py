"""Driver and Dask Worker memory diagnostics.

CUDA allocations belong to isolated GPU Worker processes, so GPU memory is
queried with ``Client.run`` instead of inspecting CUDA state in the Driver.
Diagnostics are best-effort and never participate in scheduling correctness.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger("WorkFlow.MemoryMonitor")


def collect_worker_memory_snapshot(dask_worker: Any | None = None) -> dict[str, Any]:
    """Collect one memory snapshot inside a Dask Worker process."""
    if dask_worker is None:
        from distributed import get_worker

        dask_worker = get_worker()

    role = str(
        getattr(
            dask_worker,
            "worker_role",
            os.environ.get("WORKFLOW_WORKER_ROLE", "unknown"),
        )
    )
    assigned_device = str(getattr(dask_worker, "assigned_gpu", "unknown"))
    result: dict[str, Any] = {
        "workerName": str(getattr(dask_worker, "name", "")),
        "workerRole": role,
        "assignedDevice": assigned_device,
        "rssMb": None,
        "cudaAllocatedMb": None,
        "cudaReservedMb": None,
    }

    try:
        import psutil

        result["rssMb"] = round(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024), 1)
    except Exception as exc:
        result["rssError"] = f"{type(exc).__name__}: {exc}"

    if role == "gpu":
        try:
            import torch

            result["cudaAllocatedMb"] = round(
                torch.cuda.memory_allocated(0) / (1024 * 1024),
                1,
            )
            result["cudaReservedMb"] = round(
                torch.cuda.memory_reserved(0) / (1024 * 1024),
                1,
            )
        except Exception as exc:
            result["cudaError"] = f"{type(exc).__name__}: {exc}"
    return result


def query_worker_memory(client: Any) -> dict[str, dict[str, Any]]:
    """Return best-effort memory diagnostics from every connected Worker."""
    try:
        return client.run(collect_worker_memory_snapshot)
    except Exception as exc:
        logger.debug("[MemoryMonitor] Failed to collect Worker memory: %s", exc)
        return {}


class MemoryMonitor:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.snapshots: dict[str, dict[str, Any]] = {}
        self._has_psutil = self._check_psutil()

    def _check_psutil(self) -> bool:
        try:
            import psutil  # noqa: F401

            return True
        except ImportError:
            logger.debug("[MemoryMonitor] psutil unavailable; Driver RSS disabled")
            return False

    def _get_process_memory_mb(self) -> float | None:
        if not self._has_psutil:
            return None
        try:
            import psutil

            return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        except Exception as exc:
            logger.debug("[MemoryMonitor] Failed to get Driver RSS: %s", exc)
            return None

    def _get_dask_memory_mb(self, client: Any | None = None) -> dict[str, float] | None:
        """Compatibility view containing only Worker RSS values."""
        if client is None:
            return None
        diagnostics = query_worker_memory(client)
        rss_by_worker = {
            worker: float(info["rssMb"])
            for worker, info in diagnostics.items()
            if info.get("rssMb") is not None
        }
        return rss_by_worker or None

    def take_snapshot(self, name: str, client: Any | None = None) -> dict[str, Any]:
        worker_memory = query_worker_memory(client) if client is not None else None
        snapshot = {
            "timestamp": time.time(),
            "process_mb": self._get_process_memory_mb(),
            # Kept for callers that consume the old snapshot schema. Driver-side
            # CUDA values were misleading once GPU work moved into Nannies.
            "gpu": None,
            "dask_workers": (
                {
                    worker: float(info["rssMb"])
                    for worker, info in (worker_memory or {}).items()
                    if info.get("rssMb") is not None
                }
                or None
            ),
            "worker_memory": worker_memory or None,
        }
        self.snapshots[name] = snapshot
        return snapshot

    def log_snapshot(
        self,
        name: str,
        client: Any | None = None,
        level: str = "info",
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}

        snapshot = self.take_snapshot(name, client)
        parts: list[str] = []
        if snapshot["process_mb"] is not None:
            parts.append(f"Driver={snapshot['process_mb']:.0f}MB")
        for worker, info in (snapshot.get("worker_memory") or {}).items():
            worker_parts = [
                str(info.get("workerRole", "unknown")),
                f"rss:{info['rssMb']:.0f}MB" if info.get("rssMb") is not None else "rss:N/A",
            ]
            if info.get("cudaAllocatedMb") is not None:
                worker_parts.append(f"alloc:{info['cudaAllocatedMb']:.0f}MB")
            if info.get("cudaReservedMb") is not None:
                worker_parts.append(f"res:{info['cudaReservedMb']:.0f}MB")
            parts.append(f"{worker}=[{','.join(worker_parts)}]")

        message = f"[Memory] {name}: {' | '.join(parts) if parts else 'N/A'}"
        if level == "debug":
            logger.debug(message)
        else:
            logger.info(message)
        return snapshot

    def log_delta(self, name1: str, name2: str) -> dict[str, Any]:
        if not self.enabled:
            return {}

        first = self.snapshots.get(name1)
        second = self.snapshots.get(name2)
        if not first or not second:
            logger.warning("[Memory] Cannot compute delta: missing snapshots %s or %s", name1, name2)
            return {}

        delta: dict[str, Any] = {
            "time_elapsed_s": round(second["timestamp"] - first["timestamp"], 1)
        }
        if first.get("process_mb") is not None and second.get("process_mb") is not None:
            process_delta = second["process_mb"] - first["process_mb"]
            delta["process_delta_mb"] = round(process_delta, 1)
            delta["process_delta_percent"] = (
                round(process_delta / first["process_mb"] * 100, 1)
                if first["process_mb"] > 0
                else 0
            )

        worker_deltas: dict[str, dict[str, float]] = {}
        first_workers = first.get("worker_memory") or {}
        second_workers = second.get("worker_memory") or {}
        for worker, current in second_workers.items():
            previous = first_workers.get(worker)
            if not previous:
                continue
            values: dict[str, float] = {}
            for key, output_key in (
                ("rssMb", "rssDeltaMb"),
                ("cudaAllocatedMb", "cudaAllocatedDeltaMb"),
                ("cudaReservedMb", "cudaReservedDeltaMb"),
            ):
                if current.get(key) is not None and previous.get(key) is not None:
                    values[output_key] = round(current[key] - previous[key], 1)
            if values:
                worker_deltas[worker] = values
        if worker_deltas:
            delta["worker_delta"] = worker_deltas

        parts = [f"elapsed={delta['time_elapsed_s']}s"]
        if "process_delta_mb" in delta:
            value = delta["process_delta_mb"]
            parts.append(f"Driver={value:+.0f}MB ({delta['process_delta_percent']:+.1f}%)")
        for worker, values in worker_deltas.items():
            detail = ",".join(f"{key}={value:+.0f}MB" for key, value in values.items())
            parts.append(f"{worker}=[{detail}]")
        logger.info("[Memory] Delta %s -> %s: %s", name1, name2, " | ".join(parts))
        return delta


_memory_monitor: MemoryMonitor | None = None


def get_memory_monitor(enabled: bool = True) -> MemoryMonitor:
    global _memory_monitor
    if _memory_monitor is None:
        _memory_monitor = MemoryMonitor(enabled=enabled)
    return _memory_monitor
