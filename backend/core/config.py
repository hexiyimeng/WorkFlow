import logging
import multiprocessing
import os
import platform
import socket

logger = logging.getLogger("WorkFlow.Config")


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
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
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

    N_WORKERS = 1
    CHUNK_MULTIPLE = 1
    DASHBOARD_ADDRESS = ":8787"
    DASHBOARD_HOST = None

    WORKER_MEMORY_LIMIT_GB = 0
    DASK_LOCAL_DIR = None
    CHUNK_RISK_THRESHOLD_MB = 256

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

        self.N_WORKERS = max(1, cpu_count - 2)
        self.CHUNK_MULTIPLE = 1
        logger.debug(
            f"[Config] CPU fallback defaults: Workers={self.N_WORKERS}, "
            f"ChunkMult={self.CHUNK_MULTIPLE}"
        )

        if sys_mem_gb and self.N_WORKERS > 0:
            auto_memory_per_worker = (sys_mem_gb / self.N_WORKERS) * 0.7
            logger.debug(
                f"[Config] CPU fallback memory estimate: "
                f"{auto_memory_per_worker:.1f}GB/worker "
                f"(system {sys_mem_gb:.1f}GB / {self.N_WORKERS} workers * 0.7)"
            )

        if os.getenv("WorkFlow_WORKERS"):
            self.N_WORKERS = int(os.getenv("WorkFlow_WORKERS"))
            _log_override(f"   -> [Override] WorkFlow_WORKERS={self.N_WORKERS}")

        if os.getenv("WorkFlow_CHUNK"):
            self.CHUNK_MULTIPLE = int(os.getenv("WorkFlow_CHUNK"))
            _log_override(f"   -> [Override] WorkFlow_CHUNK={self.CHUNK_MULTIPLE}")

        if os.getenv("WorkFlow_WORKER_MEMORY_LIMIT_GB"):
            self.WORKER_MEMORY_LIMIT_GB = float(os.getenv("WorkFlow_WORKER_MEMORY_LIMIT_GB"))
            _log_override(
                f"   -> [Override] WorkFlow_WORKER_MEMORY_LIMIT_GB={self.WORKER_MEMORY_LIMIT_GB}"
            )

        if os.getenv("WorkFlow_DASK_LOCAL_DIR"):
            self.DASK_LOCAL_DIR = os.getenv("WorkFlow_DASK_LOCAL_DIR")
            _log_override(f"   -> [Override] WorkFlow_DASK_LOCAL_DIR={self.DASK_LOCAL_DIR}")

        if os.getenv("WorkFlow_DASHBOARD_HOST"):
            self.DASHBOARD_HOST = os.getenv("WorkFlow_DASHBOARD_HOST")
            _log_override(f"   -> [Override] WorkFlow_DASHBOARD_HOST={self.DASHBOARD_HOST}")

        mem_limit_str = f"{self.WORKER_MEMORY_LIMIT_GB:.1f}GB" if self.WORKER_MEMORY_LIMIT_GB else "auto"
        spill_str = self.DASK_LOCAL_DIR or "auto"
        logger.debug(
            f"[Config] Final base config: Workers={self.N_WORKERS}, "
            f"ChunkMult={self.CHUNK_MULTIPLE}, WorkerMemLimit={mem_limit_str}, "
            f"SpillDir={spill_str}; GPU worker settings are finalized by DaskService."
        )


config = AppConfig()
