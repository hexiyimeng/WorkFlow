from __future__ import annotations

import logging
import operator
import os
import threading
from dataclasses import dataclass
from typing import Any, Mapping

import dask.config
from dask.distributed import Client, Nanny, Scheduler, SpecCluster
from distributed import WorkerPlugin, get_worker

from core.config import _get_system_memory_gb, _is_main_process, config
from core.logger import logger
from core.platform import current_platform, dask_spill_dir, should_schedule_malloc_trim


CPU_RESOURCE_NAME = "CPU"
GPU_RESOURCE_NAME = "GPU"


@dataclass(frozen=True)
class ClusterResourceSummary:
    scheduler_address: str
    cpu_workers: tuple[str, ...]
    gpu_workers: tuple[str, ...]
    total_cpu_slots: float
    total_gpu_slots: float


def _normalize_worker_count(value: object, *, name: str) -> int:
    """Return a non-negative integral Worker count."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.")
    try:
        count = int(operator.index(value))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f"{name} must be a non-negative integer, got {value!r}."
        ) from exc
    if count < 0:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.")
    return count


def _positive_timeout_from_env(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number, got {raw_value!r}.") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number, got {raw_value!r}.")
    return value


def _cluster_start_timeout() -> float:
    return _positive_timeout_from_env(
        "WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS",
        120.0,
    )


def _cluster_close_timeout() -> float:
    return _positive_timeout_from_env(
        "WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS",
        15.0,
    )


def _detect_cuda_for_cluster() -> tuple[bool, int]:
    """Lazily inspect CUDA only while the local cluster is being started."""
    cuda_mode = os.getenv("WorkFlow_CUDA_MODE", "auto").strip().lower()
    if cuda_mode in {"0", "false", "off", "disabled", "cpu"}:
        logger.info("[Dask] CUDA detection disabled by WorkFlow_CUDA_MODE=%s", cuda_mode)
        return False, 0

    # An explicitly empty parent mask means that no child process may expose a
    # GPU, even if CUDA was initialized elsewhere before this check.
    parent_mask = os.environ.get("CUDA_VISIBLE_DEVICES")
    if parent_mask is not None and parent_mask.strip() in {"", "-1"}:
        return False, 0

    try:
        import torch
    except Exception as exc:
        logger.debug("PyTorch unavailable; no GPU Workers will be started: %s", exc)
        return False, 0

    try:
        gpu_count = int(torch.cuda.device_count()) if torch.cuda.is_available() else 0
        return gpu_count > 0, gpu_count
    except Exception as exc:
        logger.debug("CUDA detection failed; no GPU Workers will be started: %s", exc)
        return False, 0


def _parse_device_list(value: str, *, setting_name: str) -> tuple[str, ...]:
    parts = tuple(part.strip() for part in value.split(","))
    if not parts or any(not part for part in parts):
        raise ValueError(
            f"{setting_name} must not contain empty GPU identifiers, got {value!r}."
        )
    if len(set(parts)) != len(parts):
        raise ValueError(
            f"{setting_name} must not contain duplicate GPU identifiers, got {value!r}."
        )
    return parts


def resolve_gpu_worker_ids(
    *,
    detected_gpu_count: int,
    configured_gpu_ids: tuple[str, ...] | None,
    configured_gpu_workers: int | None,
    parent_cuda_visible_devices: str | None,
) -> tuple[str, ...]:
    """Resolve the physical identifiers assigned to individual GPU Workers.

    When the parent has a CUDA visibility mask, identifiers are selected only
    from that mask. This prevents a child Worker from exposing a device that an
    administrator intentionally hid from the backend process.
    """
    if detected_gpu_count < 0:
        raise ValueError("detected_gpu_count must be non-negative.")
    if configured_gpu_workers is not None and configured_gpu_workers < 0:
        raise ValueError("WorkFlow_GPU_WORKERS must be a non-negative integer.")

    if parent_cuda_visible_devices is None:
        available_ids = tuple(str(index) for index in range(detected_gpu_count))
    elif parent_cuda_visible_devices.strip() in {"", "-1"}:
        available_ids = ()
    else:
        parent_ids = _parse_device_list(
            parent_cuda_visible_devices,
            setting_name="CUDA_VISIBLE_DEVICES",
        )
        # torch reports logical devices after applying the parent mask. Use at
        # most that many entries while retaining the original physical mapping.
        available_ids = parent_ids[:detected_gpu_count]

    if configured_gpu_ids is None:
        selected_ids = available_ids
    else:
        if not configured_gpu_ids or any(not str(item).strip() for item in configured_gpu_ids):
            raise ValueError("WorkFlow_GPU_IDS must contain at least one non-empty identifier.")
        selected_ids = tuple(str(item).strip() for item in configured_gpu_ids)
        if len(set(selected_ids)) != len(selected_ids):
            raise ValueError("WorkFlow_GPU_IDS must not contain duplicate GPU identifiers.")

        unavailable = tuple(item for item in selected_ids if item not in available_ids)
        if unavailable:
            raise ValueError(
                "WorkFlow_GPU_IDS selects devices that are not visible to the parent "
                f"process: {unavailable!r}. Available devices are {available_ids!r}."
            )

    worker_count = len(selected_ids) if configured_gpu_workers is None else configured_gpu_workers
    if worker_count > len(selected_ids):
        raise ValueError(
            "WorkFlow_GPU_WORKERS cannot exceed the number of selected visible GPUs "
            f"({worker_count} requested, {len(selected_ids)} available)."
        )
    return selected_ids[:worker_count]


def _get_dask_memory_thresholds() -> dict[str, float]:
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
        env_value = os.getenv(env_var)
        if env_value is not None:
            result[config_key] = float(env_value)
            message = f"   -> [Override] {config_key}={env_value} (via {env_var})"
            if _is_main_process():
                logger.warning(message)
            else:
                logger.debug(message)
    return result


def _compute_worker_memory_limit(
    n_workers: int | None = None,
    *,
    configured_limit_gb: float | None = None,
) -> str:
    """Compute a host-memory limit for one Worker process."""
    worker_count = max(1, int(n_workers or config.N_WORKERS or 1))
    explicit_limit = (
        config.WORKER_MEMORY_LIMIT_GB
        if configured_limit_gb is None
        else configured_limit_gb
    )
    if explicit_limit > 0:
        return f"{explicit_limit:.1f}GB"

    system_memory_gb = _get_system_memory_gb()
    if system_memory_gb is None:
        logger.warning(
            "[Dask] Could not detect system memory; using memory_limit=auto. "
            "Set a CPU/GPU worker memory limit to override."
        )
        return "auto"

    per_worker_gb = (system_memory_gb / worker_count) * 0.7
    if per_worker_gb <= 0:
        return "auto"
    return f"{per_worker_gb:.1f}GB"


def _get_dask_local_dir() -> str:
    directory = dask_spill_dir(config.DASK_LOCAL_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    return str(directory)


def _worker_resources(worker: Any) -> dict[str, float]:
    state = getattr(worker, "state", None)
    resources = getattr(state, "total_resources", None)
    if resources is None:
        resources = getattr(worker, "total_resources", None)
    if resources is None:
        resources = getattr(worker, "resources", None)
    return {str(key): float(value) for key, value in dict(resources or {}).items()}


class WorkerDevicePlugin(WorkerPlugin):
    """Validate the explicit Worker role and establish its logical device."""

    def setup(self, worker: Any) -> None:
        role = os.environ.get("WORKFLOW_WORKER_ROLE", "cpu").strip().lower()
        resources = _worker_resources(worker)

        if role == "cpu":
            if resources.get(CPU_RESOURCE_NAME) != 1 or resources.get(GPU_RESOURCE_NAME, 0) != 0:
                raise RuntimeError("CPU Worker must register exactly resources={'CPU': 1}.")
            visible_devices = os.environ.get("CUDA_VISIBLE_DEVICES")
            if visible_devices is None or visible_devices != "":
                raise RuntimeError(
                    "CPU Worker must explicitly set CUDA_VISIBLE_DEVICES=''."
                )
            worker.worker_role = "cpu"
            worker.assigned_gpu = "cpu"
            return

        if role != "gpu":
            raise RuntimeError(f"Unknown Worker role: {role!r}.")
        if resources.get(GPU_RESOURCE_NAME) != 1 or resources.get(CPU_RESOURCE_NAME, 0) != 0:
            raise RuntimeError("GPU Worker must register exactly resources={'GPU': 1}.")

        visible_devices = os.environ.get("CUDA_VISIBLE_DEVICES", "")
        visible_ids = _parse_device_list(
            visible_devices,
            setting_name="CUDA_VISIBLE_DEVICES",
        )
        if len(visible_ids) != 1:
            raise RuntimeError("Each GPU Worker must see exactly one CUDA device.")

        try:
            import torch
        except Exception as exc:
            raise RuntimeError("GPU Worker started without PyTorch CUDA support.") from exc
        if not torch.cuda.is_available():
            raise RuntimeError("GPU Worker started without an available CUDA device.")
        if int(torch.cuda.device_count()) != 1:
            raise RuntimeError("Each GPU Worker must see exactly one CUDA device.")

        torch.cuda.set_device(0)
        worker.worker_role = "gpu"
        worker.assigned_gpu = "cuda:0"


def worker_device_diagnostics(dask_worker: Any | None = None) -> dict[str, Any]:
    """Return role/device information from inside one Worker process."""
    worker = dask_worker if dask_worker is not None else get_worker()
    role = str(getattr(worker, "worker_role", os.getenv("WORKFLOW_WORKER_ROLE", "")))
    assigned_device = str(getattr(worker, "assigned_gpu", ""))
    result: dict[str, Any] = {
        "workerName": str(getattr(worker, "name", "")),
        "workerRole": role,
        "assignedDevice": assigned_device,
        "cudaVisibleDevices": os.getenv("CUDA_VISIBLE_DEVICES", ""),
        "physicalGpuId": os.getenv("WORKFLOW_PHYSICAL_GPU_ID") if role == "gpu" else None,
        "resources": _worker_resources(worker),
    }
    if role == "gpu":
        try:
            import torch

            result["cudaDeviceCount"] = int(torch.cuda.device_count())
            result["cudaCurrentDevice"] = int(torch.cuda.current_device())
        except Exception as exc:
            result["cudaError"] = f"{type(exc).__name__}: {exc}"
    return result


def _malloc_trim_once() -> None:
    import ctypes
    import gc

    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception as exc:
        logger.debug("Memory trim failed: %s", exc)


def _schedule_malloc_trim_on_scheduler(dask_scheduler: Any) -> None:
    dask_scheduler.loop.call_later(60, _malloc_trim_once)


def build_local_cluster_specs(
    *,
    cpu_workers: int,
    gpu_ids: tuple[str, ...],
    cpu_memory_limit: str,
    gpu_memory_limit: str,
    local_directory: str,
    dashboard_address: str,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Build one Scheduler spec and a strict heterogeneous Worker spec."""
    if cpu_workers < 0:
        raise ValueError("CPU Worker count must be non-negative.")
    if cpu_workers == 0 and not gpu_ids:
        raise ValueError("At least one CPU or GPU Worker must be configured.")

    scheduler_spec: dict[str, Any] = {
        "cls": Scheduler,
        "options": {"dashboard_address": dashboard_address},
    }
    worker_specs: dict[str, dict[str, Any]] = {}

    for index in range(cpu_workers):
        worker_specs[f"cpu-{index}"] = {
            "cls": Nanny,
            "options": {
                "nthreads": 1,
                "resources": {CPU_RESOURCE_NAME: 1},
                "env": {
                    "WORKFLOW_WORKER_ROLE": "cpu",
                    "CUDA_VISIBLE_DEVICES": "",
                },
                "memory_limit": cpu_memory_limit,
                "local_directory": local_directory,
                "silence_logs": logging.WARNING,
            },
        }

    for index, physical_gpu_id in enumerate(gpu_ids):
        worker_specs[f"gpu-{index}"] = {
            "cls": Nanny,
            "options": {
                "nthreads": 1,
                "resources": {GPU_RESOURCE_NAME: 1},
                "env": {
                    "WORKFLOW_WORKER_ROLE": "gpu",
                    "WORKFLOW_PHYSICAL_GPU_ID": physical_gpu_id,
                    "CUDA_VISIBLE_DEVICES": physical_gpu_id,
                },
                "memory_limit": gpu_memory_limit,
                "local_directory": local_directory,
                "silence_logs": logging.WARNING,
            },
        }
    return scheduler_spec, worker_specs


def cluster_resource_summary_from_scheduler_info(
    scheduler_info: Mapping[str, Any],
) -> ClusterResourceSummary:
    cpu_workers: list[str] = []
    gpu_workers: list[str] = []
    total_cpu_slots = 0.0
    total_gpu_slots = 0.0

    for worker_address, worker_info in dict(scheduler_info.get("workers", {})).items():
        resources = dict(worker_info.get("resources", {}) or {})
        cpu_slots = float(resources.get(CPU_RESOURCE_NAME, 0) or 0)
        gpu_slots = float(resources.get(GPU_RESOURCE_NAME, 0) or 0)
        total_cpu_slots += cpu_slots
        total_gpu_slots += gpu_slots
        if cpu_slots > 0:
            cpu_workers.append(str(worker_address))
        if gpu_slots > 0:
            gpu_workers.append(str(worker_address))

    return ClusterResourceSummary(
        scheduler_address=str(scheduler_info.get("address", "")),
        cpu_workers=tuple(sorted(cpu_workers)),
        gpu_workers=tuple(sorted(gpu_workers)),
        total_cpu_slots=total_cpu_slots,
        total_gpu_slots=total_gpu_slots,
    )


_memory_thresholds = _get_dask_memory_thresholds()
_worker_ttl = os.getenv("WorkFlow_DASK_WORKER_TTL", "2h")
dask.config.set(
    {
        "optimization.fuse.active": True,
        "optimization.fuse.max_width": 2,
        "distributed.worker.memory.target": _memory_thresholds[
            "distributed.worker.memory.target"
        ],
        "distributed.worker.memory.spill": _memory_thresholds[
            "distributed.worker.memory.spill"
        ],
        "distributed.worker.memory.pause": _memory_thresholds[
            "distributed.worker.memory.pause"
        ],
        "distributed.worker.memory.terminate": _memory_thresholds[
            "distributed.worker.memory.terminate"
        ],
        "distributed.scheduler.worker-ttl": _worker_ttl,
    }
)


class DaskService:
    _instance: DaskService | None = None
    client: Client | None = None
    cluster: SpecCluster | None = None
    active_cpu_workers: int = 0
    active_gpu_workers: int = 0
    active_gpu_ids: tuple[str, ...] = ()
    _cluster_lock = threading.RLock()

    def __new__(cls) -> DaskService:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_client(self) -> Client | None:
        # Only clients created and validated by this service are eligible for
        # execution.  Adopting an arbitrary Client.current() would allow a
        # Worker that merely advertises CPU/GPU slots to bypass role and device
        # isolation validation.
        return self.client

    def ensure_client(
        self,
        *,
        cpu_workers: int | None = None,
        gpu_workers: int | None = None,
    ) -> Client:
        """Return a service-owned client sized for the requested topology."""
        with self._cluster_lock:
            if self.client is None:
                try:
                    Client.current()
                except Exception as exc:
                    logger.debug(
                        "[Dask] No current client before local startup: %s",
                        exc,
                    )
                else:
                    raise RuntimeError(
                        "An unmanaged Dask Client is already current. WorkFlow will "
                        "not adopt a cluster that has not passed strict Worker role "
                        "and CUDA isolation validation."
                    )

            return self.start_cluster(
                cpu_workers=cpu_workers,
                gpu_workers=gpu_workers,
            )

    def start_cluster(
        self,
        *,
        cpu_workers: int | None = None,
        gpu_workers: int | None = None,
    ) -> Client:
        """Start or reuse one mixed cluster with the requested Worker counts.

        Explicit counts come from the execution resource plan and take
        precedence over the process-wide ``WorkFlow_*_WORKERS`` settings.
        Omitting both arguments preserves the former configuration-driven API.
        """
        with self._cluster_lock:
            requested_cpu_workers = _normalize_worker_count(
                config.CPU_WORKERS if cpu_workers is None else cpu_workers,
                name=(
                    "WorkFlow_CPU_WORKERS"
                    if cpu_workers is None
                    else "cpu_workers"
                ),
            )
            requested_gpu_workers = (
                config.GPU_WORKERS if gpu_workers is None else gpu_workers
            )
            if requested_gpu_workers is not None:
                requested_gpu_workers = _normalize_worker_count(
                    requested_gpu_workers,
                    name=(
                        "WorkFlow_GPU_WORKERS"
                        if gpu_workers is None
                        else "gpu_workers"
                    ),
                )

            if requested_gpu_workers == 0:
                gpu_ids: tuple[str, ...] = ()
            else:
                has_gpu, detected_gpu_count = _detect_cuda_for_cluster()
                if not has_gpu:
                    detected_gpu_count = 0
                try:
                    gpu_ids = resolve_gpu_worker_ids(
                        detected_gpu_count=detected_gpu_count,
                        configured_gpu_ids=config.GPU_IDS,
                        configured_gpu_workers=requested_gpu_workers,
                        parent_cuda_visible_devices=os.environ.get(
                            "CUDA_VISIBLE_DEVICES"
                        ),
                    )
                except ValueError as exc:
                    if requested_gpu_workers:
                        raise RuntimeError(
                            "The workflow requires "
                            f"{requested_gpu_workers} GPU Worker(s), but they "
                            f"cannot be provisioned: {exc} "
                            "CPU fallback is not supported."
                        ) from exc
                    raise

            total_workers = requested_cpu_workers + len(gpu_ids)
            if total_workers <= 0:
                raise ValueError("At least one CPU or GPU Worker must be configured.")

            desired_topology = (
                requested_cpu_workers,
                len(gpu_ids),
                gpu_ids,
            )
            if self.client is not None:
                try:
                    scheduler_summary = cluster_resource_summary_from_scheduler_info(
                        self.client.scheduler_info(n_workers=-1)
                    )
                except Exception as exc:
                    logger.warning(
                        "[Dask] Existing client is unavailable; restarting: %s",
                        exc,
                    )
                    self.stop_cluster()
                else:
                    active_topology = (
                        self.active_cpu_workers,
                        self.active_gpu_workers,
                        self.active_gpu_ids,
                    )
                    scheduler_topology_matches = (
                        len(scheduler_summary.cpu_workers)
                        == requested_cpu_workers
                        and len(scheduler_summary.gpu_workers) == len(gpu_ids)
                        and scheduler_summary.total_cpu_slots
                        == float(requested_cpu_workers)
                        and scheduler_summary.total_gpu_slots
                        == float(len(gpu_ids))
                    )
                    if (
                        active_topology == desired_topology
                        and scheduler_topology_matches
                    ):
                        try:
                            self.validate_cluster_topology(
                                expected_cpu_workers=requested_cpu_workers,
                                expected_gpu_workers=len(gpu_ids),
                                expected_gpu_ids=gpu_ids,
                                verify_device_isolation=True,
                                client=self.client,
                            )
                        except Exception as exc:
                            logger.warning(
                                "[Dask] Existing topology validation failed; "
                                "restarting: %s",
                                exc,
                            )
                            self.stop_cluster()
                        else:
                            return self.client
                    else:
                        logger.info(
                            "[Dask] Rebuilding cluster for topology change: "
                            "cpu=%s gpu=%s ids=%s -> cpu=%s gpu=%s ids=%s",
                            self.active_cpu_workers,
                            self.active_gpu_workers,
                            self.active_gpu_ids,
                            requested_cpu_workers,
                            len(gpu_ids),
                            gpu_ids,
                        )
                        self.stop_cluster()

            local_directory = _get_dask_local_dir()
            cpu_memory_limit = _compute_worker_memory_limit(
                total_workers,
                configured_limit_gb=config.CPU_WORKER_MEMORY_LIMIT_GB,
            )
            gpu_memory_limit = _compute_worker_memory_limit(
                total_workers,
                configured_limit_gb=config.GPU_WORKER_MEMORY_LIMIT_GB,
            )
            scheduler_spec, worker_specs = build_local_cluster_specs(
                cpu_workers=requested_cpu_workers,
                gpu_ids=gpu_ids,
                cpu_memory_limit=cpu_memory_limit,
                gpu_memory_limit=gpu_memory_limit,
                local_directory=local_directory,
                dashboard_address=config.DASHBOARD_ADDRESS,
            )

            logger.info(
                "[Dask] Startup plan: platform=%s scheduler=1 cpu_workers=%s "
                "gpu_workers=%s selected_gpu_ids=%s threads_per_worker=1 "
                "cpu_memory_limit=%s gpu_memory_limit=%s local_directory=%s",
                current_platform(),
                requested_cpu_workers,
                len(gpu_ids),
                gpu_ids,
                cpu_memory_limit,
                gpu_memory_limit,
                local_directory,
            )

            cluster: SpecCluster | None = None
            client: Client | None = None
            try:
                cluster = SpecCluster(
                    workers=worker_specs,
                    scheduler=scheduler_spec,
                    asynchronous=False,
                    silence_logs=logging.WARNING,
                    name="WorkFlow local mixed cluster",
                )
                client = Client(cluster)
                client.wait_for_workers(
                    total_workers,
                    timeout=_cluster_start_timeout(),
                )
                client.register_plugin(WorkerDevicePlugin(), name="worker_device")

                summary = self.validate_cluster_topology(
                    expected_cpu_workers=requested_cpu_workers,
                    expected_gpu_workers=len(gpu_ids),
                    expected_gpu_ids=gpu_ids,
                    verify_device_isolation=True,
                    client=client,
                )
                # Publish the client only after every Worker has passed role,
                # resource, and CUDA-isolation validation.
                self.cluster = cluster
                self.client = client
                self.active_cpu_workers = requested_cpu_workers
                self.active_gpu_workers = len(gpu_ids)
                self.active_gpu_ids = gpu_ids

                if should_schedule_malloc_trim():
                    client.run_on_scheduler(_schedule_malloc_trim_on_scheduler)

                logger.info("[Dask] Scheduler started: %s", summary.scheduler_address)
                logger.info("[Dask] Dashboard: %s", client.dashboard_link)
                logger.info(
                    "[Dask] CPU Workers: %s | GPU Workers: %s | CPU slots: %s | GPU slots: %s",
                    len(summary.cpu_workers),
                    len(summary.gpu_workers),
                    summary.total_cpu_slots,
                    summary.total_gpu_slots,
                )
                if summary.gpu_workers:
                    logger.info("[Dask] All GPU Workers are isolated to one CUDA device")
                logger.info(
                    "[Dask] Worker memory: CPU=%s GPU=%s | spill=%s",
                    cpu_memory_limit,
                    gpu_memory_limit,
                    local_directory,
                )
                return client
            except Exception as exc:
                logger.error("[Dask] Start failed: %s", exc)
                if client is not None:
                    try:
                        client.close(timeout=_cluster_close_timeout())
                    except Exception:
                        pass
                if cluster is not None:
                    try:
                        cluster.close(timeout=_cluster_close_timeout())
                    except Exception:
                        pass
                self.client = None
                self.cluster = None
                self.active_cpu_workers = 0
                self.active_gpu_workers = 0
                self.active_gpu_ids = ()
                raise RuntimeError(f"Dask mixed cluster startup failed: {exc}") from exc

    def get_cluster_resource_summary(
        self,
        client: Client | None = None,
    ) -> ClusterResourceSummary:
        active_client = client or self.get_client()
        if active_client is None:
            raise RuntimeError("No live Dask client is available for resource inspection.")
        return cluster_resource_summary_from_scheduler_info(
            active_client.scheduler_info(n_workers=-1)
        )

    def count_cpu_workers(self, client: Client | None = None) -> int:
        return len(self.get_cluster_resource_summary(client).cpu_workers)

    def count_gpu_workers(self, client: Client | None = None) -> int:
        return len(self.get_cluster_resource_summary(client).gpu_workers)

    def get_worker_diagnostics(
        self,
        client: Client | None = None,
    ) -> dict[str, dict[str, Any]]:
        active_client = client or self.get_client()
        if active_client is None:
            raise RuntimeError("No live Dask client is available for Worker diagnostics.")
        return active_client.run(worker_device_diagnostics)

    def validate_cluster_topology(
        self,
        *,
        expected_cpu_workers: int | None = None,
        expected_gpu_workers: int | None = None,
        expected_gpu_ids: tuple[str, ...] | None = None,
        verify_device_isolation: bool = True,
        client: Client | None = None,
    ) -> ClusterResourceSummary:
        active_client = client or self.get_client()
        if active_client is None:
            raise RuntimeError("No live Dask client is available for topology validation.")
        # Client.scheduler_info() defaults to only five Workers. Topology and
        # capacity checks must always inspect the complete heterogeneous pool.
        scheduler_info = active_client.scheduler_info(n_workers=-1)
        summary = cluster_resource_summary_from_scheduler_info(scheduler_info)
        errors: list[str] = []

        workers = dict(scheduler_info.get("workers", {}))
        for worker_address, worker_info in workers.items():
            resources = dict(worker_info.get("resources", {}) or {})
            cpu_slots = float(resources.get(CPU_RESOURCE_NAME, 0) or 0)
            gpu_slots = float(resources.get(GPU_RESOURCE_NAME, 0) or 0)
            if (cpu_slots, gpu_slots) not in {(1.0, 0.0), (0.0, 1.0)}:
                errors.append(
                    f"Worker {worker_address!r} must register exactly CPU=1 or GPU=1, "
                    f"got {resources!r}."
                )

        if expected_cpu_workers is not None and len(summary.cpu_workers) != expected_cpu_workers:
            errors.append(
                f"Expected {expected_cpu_workers} CPU Workers, found {len(summary.cpu_workers)}."
            )
        if expected_gpu_workers is not None and len(summary.gpu_workers) != expected_gpu_workers:
            errors.append(
                f"Expected {expected_gpu_workers} GPU Workers, found {len(summary.gpu_workers)}."
            )

        observed_gpu_ids: list[str] = []
        if verify_device_isolation and workers:
            diagnostics = active_client.run(worker_device_diagnostics)
            for worker_address, diagnostic in diagnostics.items():
                role = diagnostic.get("workerRole")
                assigned = diagnostic.get("assignedDevice")
                visible = diagnostic.get("cudaVisibleDevices")
                resources = diagnostic.get("resources", {})
                if role == "cpu":
                    if assigned != "cpu" or visible != "" or resources != {CPU_RESOURCE_NAME: 1.0}:
                        errors.append(
                            f"CPU Worker {worker_address!r} failed device isolation: {diagnostic!r}."
                        )
                elif role == "gpu":
                    visible_ids = tuple(part for part in str(visible).split(",") if part)
                    physical_gpu_id = str(diagnostic.get("physicalGpuId") or "")
                    if (
                        assigned != "cuda:0"
                        or len(visible_ids) != 1
                        or not physical_gpu_id
                        or visible_ids[0] != physical_gpu_id
                        or resources != {GPU_RESOURCE_NAME: 1.0}
                        or diagnostic.get("cudaDeviceCount") != 1
                        or diagnostic.get("cudaCurrentDevice") != 0
                    ):
                        errors.append(
                            f"GPU Worker {worker_address!r} failed device isolation: {diagnostic!r}."
                        )
                    else:
                        observed_gpu_ids.append(physical_gpu_id)
                else:
                    errors.append(f"Worker {worker_address!r} has unknown role {role!r}.")

            if len(observed_gpu_ids) != len(set(observed_gpu_ids)):
                errors.append(
                    "GPU Workers must use distinct physical GPUs, got "
                    f"{tuple(observed_gpu_ids)!r}."
                )
            if (
                expected_gpu_ids is not None
                and set(observed_gpu_ids) != set(expected_gpu_ids)
            ):
                errors.append(
                    "GPU Worker physical IDs do not match the requested topology: "
                    f"expected {expected_gpu_ids!r}, got {tuple(observed_gpu_ids)!r}."
                )

        if errors:
            raise RuntimeError("Invalid Dask cluster topology: " + " ".join(errors))
        return summary

    def validate_resource_availability(
        self,
        *,
        requires_cpu: bool,
        requires_gpu: bool,
        client: Client | None = None,
    ) -> ClusterResourceSummary:
        """Fail synchronously before submission when a resource class is absent."""
        summary = self.get_cluster_resource_summary(client)
        if requires_cpu and summary.total_cpu_slots < 1:
            raise RuntimeError(
                "The workflow requires CPU Workers, but no Worker with resource CPU=1 is available."
            )
        if requires_gpu and summary.total_gpu_slots < 1:
            raise RuntimeError(
                "The workflow requires GPU execution, but no Worker with resource GPU=1 is "
                "available. CPU fallback is not supported."
            )
        return summary

    def stop_cluster(self) -> None:
        with self._cluster_lock:
            client = self.client
            cluster = self.cluster
            # Stop publishing the topology before any potentially slow close
            # RPC. Worker process exit releases worker-local model caches.
            self.client = None
            self.cluster = None
            self.active_cpu_workers = 0
            self.active_gpu_workers = 0
            self.active_gpu_ids = ()
            if client:
                try:
                    client.close(timeout=_cluster_close_timeout())
                except Exception as exc:
                    logger.warning("Error closing client: %s", exc)
            if cluster:
                try:
                    cluster.close(timeout=_cluster_close_timeout())
                except Exception as exc:
                    logger.warning("Error closing cluster: %s", exc)


dask_service = DaskService()
