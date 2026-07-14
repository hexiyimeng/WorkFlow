import logging
import os
import threading

import dask.config
from dask.distributed import Client, LocalCluster
from distributed import WorkerPlugin

from core.logger import logger
from core.config import _get_system_memory_gb, _is_main_process, config
from core.platform import current_platform, dask_spill_dir, should_schedule_malloc_trim


def _detect_cuda_for_cluster():
    """Lazily check CUDA when starting the cluster, not while importing this module."""
    cuda_mode = os.getenv("WorkFlow_CUDA_MODE", "auto").strip().lower()
    if cuda_mode in {"0", "false", "off", "disabled", "cpu"}:
        logger.info("[Dask] CUDA detection disabled by WorkFlow_CUDA_MODE=%s", cuda_mode)
        return False, 0

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
    directory = dask_spill_dir(config.DASK_LOCAL_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    return str(directory)


def _worker_index(worker) -> int:
    try:
        return int(worker.name)
    except Exception:
        return abs(hash(str(getattr(worker, "name", "")))) % 10_000


def _malloc_trim_once():
    import ctypes
    import gc

    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception as e:
        logger.debug(f"Memory trim failed: {e}")


def _schedule_malloc_trim_on_scheduler(dask_scheduler):
    dask_scheduler.loop.call_later(60, _malloc_trim_once)


class MultiGPUDevicePlugin(WorkerPlugin):
    def setup(self, worker):
        """Bind a Dask worker process to a GPU when CUDA is available."""
        try:
            import torch

            gpu_count = torch.cuda.device_count()
            if gpu_count > 0:
                worker_idx = _worker_index(worker)
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


_memory_thresholds = _get_dask_memory_thresholds()
_worker_ttl = os.getenv("WorkFlow_DASK_WORKER_TTL", "2h")
dask.config.set({
    "optimization.fuse.active": True,
    "optimization.fuse.max_width": 2,
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
    _cluster_lock = threading.RLock()

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

    def ensure_client(self):
        """Return an available Dask client, starting a local cluster if needed."""
        with self._cluster_lock:
            if self.client:
                try:
                    self.client.scheduler_info()
                    return self.client
                except Exception as e:
                    logger.warning(f"[Dask] Existing client is unavailable; restarting cluster: {e}")
                    self.client = None
                    self.cluster = None

            try:
                current = Client.current()
                current.scheduler_info()
                self.client = current
                return self.client
            except Exception as e:
                logger.debug(f"[Dask] No current client available before ensure_client startup: {e}")

            client = self.start_cluster()
            if client is None:
                raise RuntimeError(
                    "Dask cluster startup failed: DaskService.start_cluster() returned None. "
                    "Check Dask startup logs and environment configuration."
                )
            try:
                client.scheduler_info()
            except Exception as exc:
                raise RuntimeError(
                    f"Dask cluster startup failed: created client is not usable ({type(exc).__name__}: {exc})."
                ) from exc
            return client

    def start_cluster(self):
        """Start the Dask cluster."""
        with self._cluster_lock:
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

                cluster_workers = max(1, int(cluster_workers or 1))
                memory_limit = _compute_worker_memory_limit(cluster_workers)
                logger.info(
                    "[Dask] Startup plan: platform=%s processes=True workers=%s threads_per_worker=1 "
                    "memory_limit=%s local_directory=%s cuda_mode=%s gpu_count=%s dashboard_address=%s",
                    current_platform(),
                    cluster_workers,
                    memory_limit,
                    dask_local_dir,
                    "gpu" if has_gpu else "cpu",
                    gpu_count,
                    config.DASHBOARD_ADDRESS,
                )

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
                        processes=True,
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                        memory_limit=memory_limit,
                        local_directory=dask_local_dir,
                    )
                    self.client = Client(self.cluster)

                if should_schedule_malloc_trim():
                    # Dask serializes scheduler callables; keep this as a module-level function.
                    self.client.run_on_scheduler(_schedule_malloc_trim_on_scheduler)

                logger.info(f"[Dask] Dashboard: {self.client.dashboard_link}")
                logger.info(f"[Dask] Worker memory_limit: {memory_limit}")
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
                if self.client:
                    try:
                        self.client.close()
                    except Exception:
                        pass
                if self.cluster:
                    try:
                        self.cluster.close()
                    except Exception:
                        pass
                self.client = None
                self.cluster = None
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


dask_service = DaskService()
