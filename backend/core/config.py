import logging
import math
import multiprocessing
import os
import platform
import socket
from pathlib import Path

from core.platform import dask_spill_dir

logger = logging.getLogger("WorkFlow.Config")


def _parse_nonnegative_int(value: str, *, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.")
    return parsed


def _parse_positive_int(value: str, *, name: str) -> int:
    parsed = _parse_nonnegative_int(value, name=name)
    if parsed == 0:
        raise ValueError(f"{name} must be greater than zero, got {value!r}.")
    return parsed


def _parse_nonnegative_float(value: str, *, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative number, got {value!r}.") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{name} must be a non-negative number, got {value!r}.")
    return parsed


def _parse_gpu_ids(value: str, *, name: str = "WorkFlow_GPU_IDS") -> tuple[str, ...]:
    parts = tuple(part.strip() for part in value.split(","))
    if not parts or any(not part for part in parts):
        raise ValueError(f"{name} must not contain empty GPU identifiers, got {value!r}.")
    if len(set(parts)) != len(parts):
        raise ValueError(f"{name} must not contain duplicate GPU identifiers, got {value!r}.")
    return parts


def _get_system_memory_gb():
    """Return total physical memory in GB, or None if detection fails."""
    try:
        import psutil

        total_gb = psutil.virtual_memory().total / (1024 ** 3)
        return total_gb if total_gb > 0 else None
    except Exception:
        pass

    try:
        if platform.system() == "Windows":
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            mem_status = MEMORYSTATUSEX()
            mem_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            success = ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem_status))
            if success and mem_status.ullTotalPhys:
                return mem_status.ullTotalPhys / (1024 ** 3)
            return None

        try:
            with Path("/proc/meminfo").open("r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        total_gb = float(line.split()[1]) / (1024 * 1024)
                        return total_gb if total_gb > 0 else None
        except Exception:
            pass
    except Exception as e:
        logger.debug(f"Failed to get system memory: {e}")

    return None


def _is_main_process():
    return multiprocessing.current_process().name == "MainProcess"


def _log_override(message):
    if _is_main_process():
        logger.warning(message)
    else:
        logger.debug(message)


class AppConfig:
    """
    Global runtime configuration.

    This module intentionally does not configure provider-specific model cache
    paths such as Cellpose, StarDist, SAM, etc. Model storage is handled by
    core.model_registry using a generic provider layout:

        backend/models/{provider}/

    Provider-specific nodes may translate that generic directory into whatever
    a third-party library requires at import/load time.
    """

    _instance = None

    # N_WORKERS and WORKER_MEMORY_LIMIT_GB remain compatibility aliases for
    # installations that still use the original single worker-pool settings.
    N_WORKERS = 1
    CPU_WORKERS = 1
    GPU_WORKERS = None
    GPU_IDS = None
    DASHBOARD_ADDRESS = ":8787"
    DASHBOARD_HOST = None

    WORKER_MEMORY_LIMIT_GB = 0
    CPU_WORKER_MEMORY_LIMIT_GB = 0
    GPU_WORKER_MEMORY_LIMIT_GB = 0
    MAX_IN_FLIGHT_WINDOWS = None
    DASK_LOCAL_DIR = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
            cls._instance._detect_environment()
        return cls._instance

    def _detect_environment(self):
        hostname = socket.gethostname().lower()
        cpu_count = multiprocessing.cpu_count()
        sys_mem_gb = _get_system_memory_gb()

        if _is_main_process():
            mem_info = f"{sys_mem_gb:.1f}GB" if sys_mem_gb else "unknown"
            logger.info(f"[Config] Host={hostname} | RAM={mem_info} | CPU={cpu_count}")

        logger.debug("[Config] CUDA detection is deferred to DaskService.start_cluster().")

        # Keep a useful CPU pool without spawning one process for every core on
        # large multi-GPU servers. WorkFlow_CPU_WORKERS can raise this limit.
        self.CPU_WORKERS = max(1, min(8, cpu_count - 2 if cpu_count > 2 else 1))
        self.N_WORKERS = self.CPU_WORKERS
        self.GPU_WORKERS = None
        self.GPU_IDS = None
        self.MAX_IN_FLIGHT_WINDOWS = None
        logger.debug(f"[Config] CPU worker default: Workers={self.CPU_WORKERS}")

        if sys_mem_gb and self.N_WORKERS > 0:
            auto_memory_per_worker = (sys_mem_gb / self.N_WORKERS) * 0.7
            logger.debug(
                f"[Config] CPU worker memory estimate: "
                f"{auto_memory_per_worker:.1f}GB/worker "
                f"(system {sys_mem_gb:.1f}GB / {self.N_WORKERS} workers * 0.7)"
            )

        legacy_workers = os.getenv("WorkFlow_WORKERS")
        if legacy_workers is not None:
            self.N_WORKERS = _parse_nonnegative_int(
                legacy_workers,
                name="WorkFlow_WORKERS",
            )
            self.CPU_WORKERS = self.N_WORKERS
            _log_override(f"   -> [Override] WorkFlow_WORKERS={self.N_WORKERS}")

        cpu_workers = os.getenv("WorkFlow_CPU_WORKERS")
        if cpu_workers is not None:
            self.CPU_WORKERS = _parse_nonnegative_int(
                cpu_workers,
                name="WorkFlow_CPU_WORKERS",
            )
            self.N_WORKERS = self.CPU_WORKERS
            _log_override(f"   -> [Override] WorkFlow_CPU_WORKERS={self.CPU_WORKERS}")

        gpu_workers = os.getenv("WorkFlow_GPU_WORKERS")
        if gpu_workers is not None:
            self.GPU_WORKERS = _parse_nonnegative_int(
                gpu_workers,
                name="WorkFlow_GPU_WORKERS",
            )
            _log_override(f"   -> [Override] WorkFlow_GPU_WORKERS={self.GPU_WORKERS}")

        gpu_ids = os.getenv("WorkFlow_GPU_IDS")
        if gpu_ids is not None:
            self.GPU_IDS = _parse_gpu_ids(gpu_ids)
            _log_override(f"   -> [Override] WorkFlow_GPU_IDS={','.join(self.GPU_IDS)}")

        legacy_memory_limit = os.getenv("WorkFlow_WORKER_MEMORY_LIMIT_GB")
        if legacy_memory_limit is not None:
            self.WORKER_MEMORY_LIMIT_GB = _parse_nonnegative_float(
                legacy_memory_limit,
                name="WorkFlow_WORKER_MEMORY_LIMIT_GB",
            )
            self.CPU_WORKER_MEMORY_LIMIT_GB = self.WORKER_MEMORY_LIMIT_GB
            self.GPU_WORKER_MEMORY_LIMIT_GB = self.WORKER_MEMORY_LIMIT_GB
            _log_override(
                f"   -> [Override] WorkFlow_WORKER_MEMORY_LIMIT_GB={self.WORKER_MEMORY_LIMIT_GB}"
            )

        cpu_memory_limit = os.getenv("WorkFlow_CPU_WORKER_MEMORY_LIMIT_GB")
        if cpu_memory_limit is not None:
            self.CPU_WORKER_MEMORY_LIMIT_GB = _parse_nonnegative_float(
                cpu_memory_limit,
                name="WorkFlow_CPU_WORKER_MEMORY_LIMIT_GB",
            )
            _log_override(
                "   -> [Override] WorkFlow_CPU_WORKER_MEMORY_LIMIT_GB="
                f"{self.CPU_WORKER_MEMORY_LIMIT_GB}"
            )

        gpu_memory_limit = os.getenv("WorkFlow_GPU_WORKER_MEMORY_LIMIT_GB")
        if gpu_memory_limit is not None:
            self.GPU_WORKER_MEMORY_LIMIT_GB = _parse_nonnegative_float(
                gpu_memory_limit,
                name="WorkFlow_GPU_WORKER_MEMORY_LIMIT_GB",
            )
            _log_override(
                "   -> [Override] WorkFlow_GPU_WORKER_MEMORY_LIMIT_GB="
                f"{self.GPU_WORKER_MEMORY_LIMIT_GB}"
            )

        max_in_flight = os.getenv("WorkFlow_MAX_IN_FLIGHT_WINDOWS")
        if max_in_flight is not None:
            self.MAX_IN_FLIGHT_WINDOWS = _parse_positive_int(
                max_in_flight,
                name="WorkFlow_MAX_IN_FLIGHT_WINDOWS",
            )
            _log_override(
                f"   -> [Override] WorkFlow_MAX_IN_FLIGHT_WINDOWS={self.MAX_IN_FLIGHT_WINDOWS}"
            )

        if os.getenv("WorkFlow_DASK_LOCAL_DIR"):
            self.DASK_LOCAL_DIR = str(dask_spill_dir(os.getenv("WorkFlow_DASK_LOCAL_DIR")))
            _log_override(f"   -> [Override] WorkFlow_DASK_LOCAL_DIR={self.DASK_LOCAL_DIR}")

        if os.getenv("WorkFlow_DASHBOARD_HOST"):
            self.DASHBOARD_HOST = os.getenv("WorkFlow_DASHBOARD_HOST")
            _log_override(f"   -> [Override] WorkFlow_DASHBOARD_HOST={self.DASHBOARD_HOST}")

        cpu_mem_limit_str = (
            f"{self.CPU_WORKER_MEMORY_LIMIT_GB:.1f}GB"
            if self.CPU_WORKER_MEMORY_LIMIT_GB
            else "auto"
        )
        gpu_mem_limit_str = (
            f"{self.GPU_WORKER_MEMORY_LIMIT_GB:.1f}GB"
            if self.GPU_WORKER_MEMORY_LIMIT_GB
            else "auto"
        )
        spill_str = self.DASK_LOCAL_DIR or "auto"
        logger.debug(
            f"[Config] Final base config: CPUWorkers={self.CPU_WORKERS}, "
            f"GPUWorkers={self.GPU_WORKERS if self.GPU_WORKERS is not None else 'auto'}, "
            f"CPUMemoryLimit={cpu_mem_limit_str}, GPUMemoryLimit={gpu_mem_limit_str}, "
            f"SpillDir={spill_str}; "
            "GPU worker settings are finalized by DaskService."
        )


config = AppConfig()
