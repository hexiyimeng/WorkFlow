import logging
import os
import platform
import tempfile

import dask.config
from dask.distributed import Client, LocalCluster
from distributed import WorkerPlugin

from core.logger import logger
from core.config import _get_system_memory_gb, _is_main_process, config


def _detect_cuda_for_cluster():
    """Lazily check CUDA when starting the cluster, not while importing this module."""
    try:
        import torch
    except Exception as e:
        logger.debug(f"PyTorch unavailable; GPU cluster mode disabled: {e}")
        return False, 0

    try:
        has_gpu = bool(torch.cuda.is_available())
        gpu_count = int(torch.cuda.device_count()) if has_gpu else 0
        return has_gpu, gpu_count
    except Exception as e:
        logger.debug(f"CUDA detection failed; GPU cluster mode disabled: {e}")
        return False, 0


def _get_dask_memory_thresholds():
    """Return Dask worker memory thresholds, honoring environment overrides."""
    defaults = {
        "distributed.worker.memory.target": 0.60,
        "distributed.worker.memory.spill": 0.70,
        "distributed.worker.memory.pause": 0.82,
        "distributed.worker.memory.terminate": 0.95,
    }

    env_overrides = {
        "WorkFlow_DASK_TARGET": "distributed.worker.memory.target",
        "WorkFlow_DASK_SPILL": "distributed.worker.memory.spill",
        "WorkFlow_DASK_PAUSE": "distributed.worker.memory.pause",
        "WorkFlow_DASK_TERMINATE": "distributed.worker.memory.terminate",
    }

    result = defaults.copy()
    for env_var, config_key in env_overrides.items():
        env_val = os.getenv(env_var)
        if env_val is not None:
            result[config_key] = float(env_val)
            message = f"   -> [Override] {config_key}={env_val} (via {env_var})"
            if _is_main_process():
                logger.warning(message)
            else:
                logger.debug(message)

    return result


def _compute_worker_memory_limit(n_workers=None):
    """Compute the per-worker Dask memory limit."""
    worker_count = max(1, int(n_workers or config.N_WORKERS or 1))

    if config.WORKER_MEMORY_LIMIT_GB > 0:
        limit_str = f"{config.WORKER_MEMORY_LIMIT_GB:.1f}GB"
        logger.debug(f"[Dask] Worker memory_limit explicit config: {limit_str}")
        return limit_str

    sys_mem_gb = _get_system_memory_gb()
    if sys_mem_gb is None:
        logger.warning(
            "[Dask] Could not detect system memory; using Dask memory_limit=auto. "
            "Set WorkFlow_WORKER_MEMORY_LIMIT_GB to override."
        )
        return "auto"

    per_worker_mem_gb = (sys_mem_gb / worker_count) * 0.7
    if per_worker_mem_gb <= 0:
        logger.warning(
            "[Dask] Computed invalid worker memory limit; using Dask memory_limit=auto. "
            "Set WorkFlow_WORKER_MEMORY_LIMIT_GB to override."
        )
        return "auto"

    limit_str = f"{per_worker_mem_gb:.1f}GB"
    logger.debug(
        f"[Dask] Worker memory calculation: {limit_str} "
        f"(system {sys_mem_gb:.1f}GB / {worker_count} workers * 0.7)"
    )
    return limit_str


def _get_dask_local_dir():
    """Return the Dask spill directory, honoring config/env overrides."""
    if config.DASK_LOCAL_DIR:
        return config.DASK_LOCAL_DIR

    dask_dir = os.path.join(tempfile.gettempdir(), "WorkFlow_dask_spill")
    os.makedirs(dask_dir, exist_ok=True)
    return dask_dir


class MultiGPUDevicePlugin(WorkerPlugin):
    def setup(self, worker):
        """Bind a Dask worker process to a GPU when CUDA is available."""
        try:
            import torch

            gpu_count = torch.cuda.device_count()
            if gpu_count > 0:
                worker_idx = int(worker.name)
                assigned_gpu = worker_idx % gpu_count
                worker.assigned_gpu = f"cuda:{assigned_gpu}"
                logger.debug(f"Worker {worker.name} bound to {worker.assigned_gpu}")
            else:
                worker.assigned_gpu = "cpu"
        except Exception as e:
            # Only default to cuda:0 if explicitly allowed; otherwise fall back to CPU
            # to prevent silent multi-worker contention on cuda:0.
            allow_implicit = os.getenv("WorkFlow_ALLOW_IMPLICIT_CUDA0", "").lower() in ("1", "true", "yes")
            worker.assigned_gpu = "cuda:0" if allow_implicit else "cpu"
            logger.debug(f"Failed to bind GPU for worker {worker.name}, assigned={worker.assigned_gpu}: {e}")


# Backward-compatible alias for external code that may import the old name.
WindowsMultiGPUPlugin = MultiGPUDevicePlugin


_memory_thresholds = _get_dask_memory_thresholds()
_worker_ttl = os.getenv("WorkFlow_DASK_WORKER_TTL", "2h")
dask.config.set({
    "optimization.fuse.active": True,
    "optimization.fuse.max_width": 2,
    "array.chunk-size": "256MB",
    "distributed.worker.memory.target": _memory_thresholds["distributed.worker.memory.target"],
    "distributed.worker.memory.spill": _memory_thresholds["distributed.worker.memory.spill"],
    "distributed.worker.memory.pause": _memory_thresholds["distributed.worker.memory.pause"],
    "distributed.worker.memory.terminate": _memory_thresholds["distributed.worker.memory.terminate"],
    "distributed.scheduler.worker-ttl": _worker_ttl,
})

logger.debug(
    f"[Dask] Memory thresholds: target={_memory_thresholds['distributed.worker.memory.target']}, "
    f"spill={_memory_thresholds['distributed.worker.memory.spill']}, "
    f"pause={_memory_thresholds['distributed.worker.memory.pause']}, "
    f"terminate={_memory_thresholds['distributed.worker.memory.terminate']}, "
    f"worker-ttl={_worker_ttl}"
)


class DaskService:
    _instance = None
    client = None
    cluster = None

    recommended_chunk_multiple = 1

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DaskService, cls).__new__(cls)
        return cls._instance

    def get_client(self):
        if self.client:
            return self.client
        try:
            return Client.current()
        except Exception as e:
            logger.debug(f"Failed to get current client: {e}")
            return None

    def start_cluster(self):
        """Start the Dask cluster."""
        if self.client:
            return self.client

        n_workers = config.N_WORKERS
        dask_local_dir = _get_dask_local_dir()

        try:
            has_gpu, gpu_count = _detect_cuda_for_cluster()
            if has_gpu and not os.getenv("WorkFlow_WORKERS"):
                n_workers = gpu_count if gpu_count > 1 else 1

            if has_gpu and gpu_count <= 1:
                cluster_workers = 1
            else:
                cluster_workers = n_workers

            memory_limit = _compute_worker_memory_limit(cluster_workers)

            if has_gpu:
                if gpu_count > 1 and cluster_workers > 1:
                    logger.info(f"[Dask] Starting GPU mode: {cluster_workers} workers on {gpu_count} GPUs")
                    self.cluster = LocalCluster(
                        n_workers=cluster_workers,
                        threads_per_worker=1,
                        processes=True,
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                        memory_limit=memory_limit,
                        local_directory=dask_local_dir,
                    )
                    self.client = Client(self.cluster)
                    self.client.register_plugin(MultiGPUDevicePlugin(), name="gpu_device_pinning")
                else:
                    logger.info("[Dask] Starting GPU mode: 1 worker on cuda:0")
                    self.cluster = LocalCluster(
                        n_workers=1,
                        threads_per_worker=1,
                        processes=True,
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                        memory_limit=memory_limit,
                        local_directory=dask_local_dir,
                    )
                    self.client = Client(self.cluster)
                    self.client.register_plugin(MultiGPUDevicePlugin(), name="gpu_device_pinning")
            else:
                logger.info(f"[Dask] Starting CPU mode: {cluster_workers} workers")
                self.cluster = LocalCluster(
                    n_workers=cluster_workers,
                    threads_per_worker=1,
                    dashboard_address=config.DASHBOARD_ADDRESS,
                    silence_logs=logging.WARNING,
                    memory_limit=memory_limit,
                    local_directory=dask_local_dir,
                )
                self.client = Client(self.cluster)

            if platform.system() == "Linux":
                self.client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.loop.call_later(60, self._trim_memory)
                )

            logger.info(f"[Dask] Dashboard: {self.client.dashboard_link}")
            logger.info(f"[Dask] Worker memory_limit: {memory_limit}")
            if has_gpu:
                logger.info(f"[Dask] Spill directory: {dask_local_dir}")
            logger.debug(
                f"[Dask] Memory thresholds: target={_memory_thresholds['distributed.worker.memory.target']}, "
                f"spill={_memory_thresholds['distributed.worker.memory.spill']}, "
                f"pause={_memory_thresholds['distributed.worker.memory.pause']}, "
                f"terminate={_memory_thresholds['distributed.worker.memory.terminate']}"
            )

            return self.client

        except Exception as e:
            logger.error(f"[Dask] Start failed: {e}")
            return None

    def stop_cluster(self):
        # Clear worker cache on all workers.
        if self.client:
            try:
                from core.worker_cache import force_clear_worker_cache

                stats = self.client.run(force_clear_worker_cache)
                logger.info(f"[Dask] Worker cache cleared on cluster stop: {stats}")
            except Exception as e:
                logger.debug(f"Failed to clear worker cache on stop: {e}")

            try:
                self.client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")
            self.client = None
        if self.cluster:
            try:
                self.cluster.close()
            except Exception as e:
                logger.warning(f"Error closing cluster: {e}")
            self.cluster = None

    def _trim_memory(self):
        import ctypes
        import gc

        gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception as e:
            logger.debug(f"Memory trim failed: {e}")


dask_service = DaskService()
