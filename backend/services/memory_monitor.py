"""Driver and Dask Worker memory diagnostics.

Worker RSS is read from the Scheduler's heartbeat metrics.  Diagnostics must
never fan a function out with ``Client.run``: that executes on every Worker's
control thread and, on a large Windows cluster, one slow Worker can leave an
uncancellable RPC outstanding for minutes.  Diagnostics are best-effort and
never participate in scheduling correctness.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger("WorkFlow.MemoryMonitor")


def _scheduler_worker_memory(
    worker_address: object,
    worker_info: dict[str, Any],
) -> dict[str, Any]:
    """Convert one Scheduler Worker identity into the monitor schema."""
    startup = dict(worker_info.get("workflowDevice") or {})
    resources = dict(worker_info.get("resources") or {})
    role = str(startup.get("workerRole") or "")
    if not role:
        if float(resources.get("GPU", 0) or 0) > 0:
            role = "gpu"
        elif float(resources.get("CPU", 0) or 0) > 0:
            role = "cpu"
        else:
            role = "unknown"

    metrics = dict(worker_info.get("metrics") or {})
    raw_rss = metrics.get("memory")
    try:
        rss_mb = round(float(raw_rss) / (1024 * 1024), 1)
    except (TypeError, ValueError, OverflowError):
        rss_mb = None

    assigned_device = startup.get("assignedDevice")
    if not assigned_device:
        assigned_device = (
            "cuda:0"
            if role == "gpu"
            else "cpu"
            if role == "cpu"
            else "unknown"
        )

    return {
        "workerName": str(worker_info.get("name") or worker_address),
        "workerRole": role,
        "assignedDevice": str(assigned_device),
        "rssMb": rss_mb,
        # PyTorch allocator counters cannot be obtained safely from Scheduler
        # heartbeats.  Keep the stable schema and report them as unavailable.
        "cudaAllocatedMb": None,
        "cudaReservedMb": None,
    }


def query_worker_memory(client: Any) -> dict[str, dict[str, Any]]:
    """Return best-effort Worker RSS without executing code on Workers."""
    try:
        scheduler_info = client.scheduler_info(n_workers=-1)
        return {
            str(address): _scheduler_worker_memory(address, dict(info or {}))
            for address, info in dict(
                scheduler_info.get("workers", {})
            ).items()
        }
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

    def collect_snapshot(self, client: Any | None = None) -> dict[str, Any]:
        """Collect a snapshot without mutating monitor state or logging it."""
        worker_memory = query_worker_memory(client) if client is not None else None
        return {
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

    def take_snapshot(self, name: str, client: Any | None = None) -> dict[str, Any]:
        snapshot = self.collect_snapshot(client)
        self.snapshots[name] = snapshot
        return snapshot

    def record_snapshot(
        self,
        name: str,
        snapshot: dict[str, Any],
        level: str = "info",
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}

        self.snapshots[name] = snapshot
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

    def log_snapshot(
        self,
        name: str,
        client: Any | None = None,
        level: str = "info",
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        return self.record_snapshot(
            name,
            self.collect_snapshot(client),
            level=level,
        )

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
