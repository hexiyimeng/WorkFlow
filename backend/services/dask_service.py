from __future__ import annotations

import logging
import math
import operator
import os
import asyncio
import threading
import time
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

# ``build_local_cluster_specs`` is deliberately a same-host cluster: the
# Driver, Scheduler, Nannies, and Workers all live in one desktop process tree
# (or in one single-node Slurm allocation).  Advertising a physical NIC here
# makes a Windows Driver hairpin through firewall/VPN/RDP network policy merely
# to reach its own Scheduler.  That became unreliable as the number of Nanny
# connections grew.  Loopback is the only address these local processes need.
LOCAL_CLUSTER_HOST = "127.0.0.1"

# GPU inference Workers retain model state and large native/PyTorch workspaces
# that Dask cannot spill.  Give them a larger share of the same bounded host
# memory budget instead of dividing that budget equally with lightweight CPU
# orchestration/writer Workers.  The weighted limits still add up to at most
# 70% of physical host memory when no explicit per-role override is configured.
CPU_WORKER_HOST_MEMORY_WEIGHT = 1.0
GPU_WORKER_HOST_MEMORY_WEIGHT = 3.0
AUTO_WORKER_MEMORY_BUDGET_FRACTION = 0.70


@dataclass(frozen=True)
class ClusterResourceSummary:
    scheduler_address: str
    cpu_workers: tuple[str, ...]
    gpu_workers: tuple[str, ...]
    total_cpu_slots: float
    total_gpu_slots: float


class TransientClusterTopologyError(RuntimeError):
    """A topology mismatch that may resolve after a Nanny restart."""


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
        60.0,
    )


def _remaining_startup_time(deadline: float, *, stage: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(f"Dask cluster startup timed out during {stage}.")
    return remaining


def _remaining_shutdown_time(deadline: float) -> float:
    # Distributed rejects a non-positive callback timeout.  Once the shared
    # graceful budget is exhausted, make only a minimal final close attempt and
    # proceed to the process-handle fallback.
    return max(0.1, deadline - time.monotonic())


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


    trust_slurm_mask = os.getenv("WorkFlow_TRUST_SLURM_CUDA_MASK", "").strip().lower()
    if trust_slurm_mask in {"1", "true", "yes", "on"}:
        if parent_mask is None:
            raise RuntimeError(
                "WorkFlow_TRUST_SLURM_CUDA_MASK requires CUDA_VISIBLE_DEVICES "
                "to be set by the allocation."
            )
        visible_ids = _parse_device_list(
            parent_mask,
            setting_name="CUDA_VISIBLE_DEVICES",
        )
        logger.info(
            "[Dask] Using the Slurm CUDA allocation mask without initializing "
            "CUDA in the compute Driver: %s",
            visible_ids,
        )
        return True, len(visible_ids)

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
    allocation_weight: float = 1.0,
    total_allocation_weight: float | None = None,
) -> str:
    """Compute one Worker's bounded share of host memory.

    ``_get_system_memory_gb`` returns binary GiB.  Explicit ``*_GB`` settings
    retain their historical decimal-GB contract, while automatically computed
    limits use the matching ``GiB`` unit.
    """
    worker_count = max(1, int(n_workers or config.N_WORKERS or 1))
    explicit_limit = (
        config.WORKER_MEMORY_LIMIT_GB
        if configured_limit_gb is None
        else configured_limit_gb
    )
    if explicit_limit > 0:
        return f"{explicit_limit:.1f}GB"

    try:
        weight = float(allocation_weight)
        total_weight = float(
            worker_count
            if total_allocation_weight is None
            else total_allocation_weight
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Worker memory allocation weights must be positive numbers.") from exc
    if (
        not math.isfinite(weight)
        or not math.isfinite(total_weight)
        or weight <= 0
        or total_weight <= 0
    ):
        raise ValueError("Worker memory allocation weights must be positive numbers.")

    system_memory_gb = _get_system_memory_gb()
    if system_memory_gb is None:
        logger.warning(
            "[Dask] Could not detect system memory; using memory_limit=auto. "
            "Set a CPU/GPU worker memory limit to override."
        )
        return "auto"

    per_worker_gib = (
        system_memory_gb
        * AUTO_WORKER_MEMORY_BUDGET_FRACTION
        * weight
        / total_weight
    )
    if per_worker_gib <= 0:
        return "auto"

    # Round down so the sum of all automatically assigned limits never grows
    # beyond the configured fraction merely because of display precision.
    rounded_down_gib = math.floor(per_worker_gib * 10.0) / 10.0
    if rounded_down_gib <= 0:
        rounded_down_mib = math.floor(per_worker_gib * 1024.0)
        if rounded_down_mib > 0:
            return f"{rounded_down_mib}MiB"
        rounded_down_bytes = math.floor(per_worker_gib * (1024.0 ** 3))
        if rounded_down_bytes > 0:
            return f"{rounded_down_bytes}B"
        return "auto"
    return f"{rounded_down_gib:.1f}GiB"


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


def _set_windows_console_ctrl_c_ignore_flag() -> None:
    """Ignore console Ctrl+C at the native Windows process boundary."""
    import ctypes

    set_console_ctrl_handler = ctypes.windll.kernel32.SetConsoleCtrlHandler
    if not set_console_ctrl_handler(None, True):
        raise ctypes.WinError()


def _configure_worker_signal_handling() -> None:
    """Keep Windows Ctrl+C ownership in the Driver process.

    Windows console control events are delivered to every process attached to
    the console.  Without this guard, pressing Ctrl+C in the backend window
    raises KeyboardInterrupt in every Worker; their Nannies then restart them
    while FastAPI is trying to cancel the execution.  Nanny termination uses
    process handles and is unaffected by ignoring SIGINT inside the Worker.
    """
    if os.name != "nt" or os.getenv("WORKFLOW_DASK_WORKER_PROCESS") != "1":
        return
    try:
        # Python's signal handler covers the interpreter, but native runtimes
        # loaded by inference code (notably Intel Fortran/OpenMP) install their
        # own Windows console handlers.  The process-level ignore flag prevents
        # CTRL_C_EVENT from reaching those handlers as well.  Nanny still owns
        # and can terminate the Worker through its process handle.
        _set_windows_console_ctrl_c_ignore_flag()
    except (AttributeError, OSError, RuntimeError, ValueError) as exc:
        logger.warning(
            "[Dask] Worker could not ignore Driver Ctrl+C at the native "
            "console boundary: %s",
            exc,
        )

    # Keep this independent from the native handler.  signal.signal() may be
    # rejected outside the interpreter's main thread, but that must never skip
    # the process-wide native protection used by Intel/CUDA runtimes.
    try:
        import signal

        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (AttributeError, OSError, RuntimeError, ValueError) as exc:
        logger.warning(
            "[Dask] Worker could not install the Python Ctrl+C ignore handler: %s",
            exc,
        )


class WorkerDevicePlugin(WorkerPlugin):
    """Validate the Worker role without initializing a CUDA runtime.

    ``WorkerPlugin.setup`` runs on the Worker's control/event-loop thread.  A
    PyTorch import or first CUDA call here can prevent heartbeats for minutes
    on Windows while several Worker processes contend on DLL loading and CUDA
    initialization.  Runtime CUDA validation therefore belongs to the normal
    GPU task path; this plugin only establishes the already isolated logical
    device name and validates the resource/environment contract.
    """

    def setup(self, worker: Any) -> None:
        _configure_worker_signal_handling()
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

        worker.worker_role = "gpu"
        worker.assigned_gpu = "cuda:0"


def worker_device_diagnostics(dask_worker: Any | None = None) -> dict[str, Any]:
    """Return lightweight role/device information from one Worker process.

    This function can run while the Worker publishes startup information or
    through ``Client.run``; both execute on the Worker's control thread rather
    than its task executor.  It must never import PyTorch or make a CUDA API
    call.
    """
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
    return result


def worker_device_startup_information(worker: Any) -> dict[str, Any]:
    """Publish device metadata atomically with Worker registration."""
    return worker_device_diagnostics(worker)


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
    worker_start_timeout: float | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Build one Scheduler spec and a strict heterogeneous Worker spec."""
    if cpu_workers < 0:
        raise ValueError("CPU Worker count must be non-negative.")
    if cpu_workers == 0 and not gpu_ids:
        raise ValueError("At least one CPU or GPU Worker must be configured.")

    scheduler_spec: dict[str, Any] = {
        "cls": Scheduler,
        "options": {
            "host": LOCAL_CLUSTER_HOST,
            "dashboard_address": dashboard_address,
        },
    }
    worker_specs: dict[str, dict[str, Any]] = {}

    for index in range(cpu_workers):
        worker_specs[f"cpu-{index}"] = {
            "cls": Nanny,
            "options": {
                "host": LOCAL_CLUSTER_HOST,
                "nthreads": 1,
                "resources": {CPU_RESOURCE_NAME: 1},
                "env": {
                    "WORKFLOW_WORKER_ROLE": "cpu",
                    "WORKFLOW_DASK_WORKER_PROCESS": "1",
                    "CUDA_VISIBLE_DEVICES": "",
                },
                "memory_limit": cpu_memory_limit,
                "local_directory": local_directory,
                "silence_logs": logging.WARNING,
                "plugins": (WorkerDevicePlugin(),),
                "startup_information": {
                    "workflowDevice": worker_device_startup_information,
                },
            },
        }

    for index, physical_gpu_id in enumerate(gpu_ids):
        worker_specs[f"gpu-{index}"] = {
            "cls": Nanny,
            "options": {
                "host": LOCAL_CLUSTER_HOST,
                "nthreads": 1,
                "resources": {GPU_RESOURCE_NAME: 1},
                "env": {
                    "WORKFLOW_WORKER_ROLE": "gpu",
                    "WORKFLOW_DASK_WORKER_PROCESS": "1",
                    "WORKFLOW_PHYSICAL_GPU_ID": physical_gpu_id,
                    "CUDA_VISIBLE_DEVICES": physical_gpu_id,
                },
                "memory_limit": gpu_memory_limit,
                "local_directory": local_directory,
                "silence_logs": logging.WARNING,
                "plugins": (WorkerDevicePlugin(),),
                "startup_information": {
                    "workflowDevice": worker_device_startup_information,
                },
            },
        }

    if worker_start_timeout is not None:
        if worker_start_timeout <= 0:
            raise ValueError("worker_start_timeout must be positive when provided.")
        # Unlike Client.wait_for_workers, this timeout covers the synchronous
        # SpecCluster constructor, including Windows process spawn and Worker
        # registration.  Without it, one stuck Nanny can block startup forever
        # before Client.wait_for_workers is ever reached.
        for worker_spec in worker_specs.values():
            worker_spec["options"]["death_timeout"] = worker_start_timeout
    return scheduler_spec, worker_specs


def _cluster_nannies(cluster: Any) -> tuple[Any, ...]:
    candidates = list(dict(getattr(cluster, "workers", {}) or {}).values())
    candidates.extend(tuple(getattr(cluster, "_created", ()) or ()))
    unique: dict[int, Any] = {}
    for candidate in candidates:
        unique[id(candidate)] = candidate
    return tuple(unique.values())


def _cluster_worker_processes_alive(cluster: Any) -> bool:
    for nanny in _cluster_nannies(cluster):
        worker_process = getattr(getattr(nanny, "process", None), "process", None)
        if worker_process is not None:
            try:
                if worker_process.is_alive():
                    return True
            except (AssertionError, OSError, RuntimeError, ValueError):
                return True
    return False


async def _force_kill_cluster_workers(cluster: Any, *, timeout: float) -> None:
    """Close Nannies, then kill any child that resisted graceful shutdown."""
    if cluster is None:
        return

    async def close_one(nanny: Any) -> BaseException | None:
        try:
            # Nanny.close sets status=closing before stopping its child.  That
            # status is essential: killing the underlying AsyncProcess while
            # the Nanny is still running makes _on_worker_exit immediately
            # restart it.
            await asyncio.wait_for(
                nanny.close(
                    timeout=max(0.1, timeout * 0.8),
                    reason="workflow-emergency-cluster-close",
                ),
                timeout=max(0.2, timeout),
            )
            return None
        except BaseException as close_exc:
            worker_process = getattr(nanny, "process", None)
            async_process = getattr(worker_process, "process", None)
            if async_process is None:
                return None
            try:
                # nanny.close has already transitioned the Nanny to closing,
                # so this final OS-process kill cannot trigger a restart.
                await async_process.kill()
                await async_process.join(timeout=max(0.1, timeout))
                if async_process.is_alive():
                    raise RuntimeError("Dask Worker child is still alive after kill().")
                return None
            except BaseException as kill_exc:
                return RuntimeError(
                    f"Nanny close failed ({close_exc}); child kill failed ({kill_exc})."
                )

    results = await asyncio.gather(
        *(close_one(nanny) for nanny in _cluster_nannies(cluster))
    )
    failures = [result for result in results if result is not None]
    if failures:
        raise RuntimeError(
            f"Failed to force-stop {len(failures)} Dask Worker process(es)."
        ) from failures[0]


def _force_kill_cluster_workers_sync(cluster: Any, *, timeout: float) -> None:
    """Run the emergency Worker-process finalizer on SpecCluster's IOLoop."""
    cluster.sync(
        _force_kill_cluster_workers,
        cluster,
        timeout=timeout,
        callback_timeout=max(1.0, timeout * 2.0),
    )
    if _cluster_worker_processes_alive(cluster):
        raise RuntimeError("One or more Dask Worker child processes survived cleanup.")


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
    _cluster_poisoned: bool = False
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
                        "and CUDA visibility-isolation validation."
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
            if self._cluster_poisoned:
                logger.warning(
                    "[Dask] Retrying cleanup of a previously poisoned local cluster."
                )
                self.stop_cluster()
            startup_started = time.monotonic()
            startup_timeout = _cluster_start_timeout()
            startup_deadline = startup_started + startup_timeout
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
                            self._wait_for_stable_topology(
                                expected_cpu_workers=requested_cpu_workers,
                                expected_gpu_workers=len(gpu_ids),
                                expected_gpu_ids=gpu_ids,
                                expected_total_workers=total_workers,
                                deadline=startup_deadline,
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
            total_memory_weight = (
                requested_cpu_workers * CPU_WORKER_HOST_MEMORY_WEIGHT
                + len(gpu_ids) * GPU_WORKER_HOST_MEMORY_WEIGHT
            )
            if total_memory_weight <= 0:
                # The topology validation above already rejects zero Workers;
                # retain a defensive denominator for direct/unit-test callers.
                total_memory_weight = 1.0
            cpu_memory_limit = _compute_worker_memory_limit(
                total_workers,
                configured_limit_gb=config.CPU_WORKER_MEMORY_LIMIT_GB,
                allocation_weight=CPU_WORKER_HOST_MEMORY_WEIGHT,
                total_allocation_weight=total_memory_weight,
            )
            gpu_memory_limit = _compute_worker_memory_limit(
                total_workers,
                configured_limit_gb=config.GPU_WORKER_MEMORY_LIMIT_GB,
                allocation_weight=GPU_WORKER_HOST_MEMORY_WEIGHT,
                total_allocation_weight=total_memory_weight,
            )
            scheduler_spec, worker_specs = build_local_cluster_specs(
                cpu_workers=requested_cpu_workers,
                gpu_ids=gpu_ids,
                cpu_memory_limit=cpu_memory_limit,
                gpu_memory_limit=gpu_memory_limit,
                local_directory=local_directory,
                dashboard_address=config.DASHBOARD_ADDRESS,
                worker_start_timeout=_remaining_startup_time(
                    startup_deadline,
                    stage="preparing Worker processes",
                ),
            )

            logger.info(
                "[Dask] Startup plan: platform=%s host=%s scheduler=1 cpu_workers=%s "
                "gpu_workers=%s selected_gpu_ids=%s threads_per_worker=1 "
                "cpu_memory_limit=%s gpu_memory_limit=%s local_directory=%s",
                current_platform(),
                LOCAL_CLUSTER_HOST,
                requested_cpu_workers,
                len(gpu_ids),
                gpu_ids,
                cpu_memory_limit,
                gpu_memory_limit,
                local_directory,
            )

            cluster: SpecCluster | None = None
            client: Client | None = None
            startup_stage = "creating Scheduler and Worker processes"
            try:
                cluster = SpecCluster(
                    workers=worker_specs,
                    scheduler=scheduler_spec,
                    asynchronous=False,
                    silence_logs=logging.WARNING,
                    name="WorkFlow local mixed cluster",
                )
                # SpecCluster synchronously starts every Nanny.  On Windows,
                # process creation is substantially serialized and the total
                # constructor time may exceed one Nanny's death_timeout even
                # though every Worker starts successfully.  The remote 18-
                # Worker case took 206 seconds and returned all 18 Workers in
                # Status.running.  Give Client connection and atomic topology
                # validation their own phase budget instead of rejecting that
                # already-started cluster with an expired pre-spawn deadline.
                cluster_constructed_at = time.monotonic()
                post_spawn_deadline = cluster_constructed_at + startup_timeout
                logger.info(
                    "[Dask] Scheduler and Nannies constructed: requested=%s "
                    "elapsed=%.1fs",
                    total_workers,
                    cluster_constructed_at - startup_started,
                )
                startup_stage = "connecting the Dask Client"
                client = Client(
                    cluster,
                    timeout=_remaining_startup_time(
                        post_spawn_deadline,
                        stage="connecting the Dask Client",
                    ),
                )
                startup_stage = "waiting for initial Worker registration"
                client.wait_for_workers(
                    total_workers,
                    timeout=_remaining_startup_time(
                        post_spawn_deadline,
                        stage="waiting for Worker registration",
                    ),
                )
                workers_ready_at = time.monotonic()
                logger.info(
                    "[Dask] Worker processes and role metadata registered: "
                    "total=%s elapsed=%.1fs",
                    total_workers,
                    workers_ready_at - startup_started,
                )

                startup_stage = "waiting for a stable validated topology"
                summary = self._wait_for_stable_topology(
                    expected_cpu_workers=requested_cpu_workers,
                    expected_gpu_workers=len(gpu_ids),
                    expected_gpu_ids=gpu_ids,
                    expected_total_workers=total_workers,
                    deadline=post_spawn_deadline,
                    client=client,
                )
                # Publish the client only after every Worker has passed role,
                # resource, and CUDA-visibility validation.
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
                    logger.info(
                        "[Dask] All GPU Workers have distinct one-device CUDA visibility masks"
                    )
                logger.info(
                    "[Dask] Worker memory: CPU=%s GPU=%s | spill=%s",
                    cpu_memory_limit,
                    gpu_memory_limit,
                    local_directory,
                )
                logger.info(
                    "[Dask] Startup validation complete: elapsed=%.1fs",
                    time.monotonic() - startup_started,
                )
                return client
            except Exception as exc:
                partial_workers = {}
                if cluster is not None:
                    partial_workers = {
                        str(name): {
                            "nannyStatus": str(getattr(worker, "status", "unknown")),
                            "workerAddress": str(
                                getattr(worker, "worker_address", "") or ""
                            ),
                        }
                        for name, worker in dict(
                            getattr(cluster, "workers", {}) or {}
                        ).items()
                    }
                logger.error(
                    "[Dask] Start failed after %.1fs during %s: %s | "
                    "partial_workers=%s",
                    time.monotonic() - startup_started,
                    startup_stage,
                    exc,
                    partial_workers,
                )
                cleanup_errors: list[BaseException] = []
                cleanup_deadline = time.monotonic() + _cluster_close_timeout()
                if client is not None:
                    try:
                        client.close(timeout=_remaining_shutdown_time(cleanup_deadline))
                    except Exception as close_exc:
                        cleanup_errors.append(close_exc)
                        logger.warning(
                            "[Dask] Client cleanup after startup failure failed: %s",
                            close_exc,
                        )
                if cluster is not None:
                    try:
                        cluster.close(timeout=_remaining_shutdown_time(cleanup_deadline))
                    except Exception as close_exc:
                        cleanup_errors.append(close_exc)
                        logger.warning(
                            "[Dask] Cluster cleanup after startup failure failed: %s",
                            close_exc,
                        )
                cleanup_unconfirmed = False
                if cluster is not None and _cluster_worker_processes_alive(cluster):
                    try:
                        _force_kill_cluster_workers_sync(cluster, timeout=5.0)
                    except Exception as kill_exc:
                        logger.error(
                            "[Dask] Emergency Worker cleanup after startup failure "
                            "failed: %s",
                            kill_exc,
                            exc_info=True,
                        )
                        cleanup_unconfirmed = _cluster_worker_processes_alive(cluster)
                if cleanup_unconfirmed:
                    # Fail closed: retain the only handles capable of retrying
                    # cleanup and forbid a second cluster beside possible
                    # orphan GPU processes.
                    self.client = client
                    self.cluster = cluster
                    self._cluster_poisoned = True
                else:
                    self.client = None
                    self.cluster = None
                    self._cluster_poisoned = False
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
        scheduler_info = active_client.scheduler_info(n_workers=-1)
        return {
            str(address): dict(worker_info.get("workflowDevice") or {})
            for address, worker_info in dict(
                scheduler_info.get("workers", {})
            ).items()
        }

    def _wait_for_stable_topology(
        self,
        *,
        expected_cpu_workers: int,
        expected_gpu_workers: int,
        expected_gpu_ids: tuple[str, ...],
        expected_total_workers: int,
        deadline: float,
        client: Client,
    ) -> ClusterResourceSummary:
        """Wait through transient Nanny restarts for two matching topologies.

        ``wait_for_workers`` guarantees only that the requested total was seen
        at one instant.  A Worker can restart immediately afterwards.  The old
        startup path then combined a stale scheduler snapshot with a later
        diagnostics snapshot and rejected a topology that had already
        recovered.  Device metadata is now part of each atomic scheduler
        snapshot, and two consecutive snapshots must describe the same Worker
        addresses before the cluster is published.
        """
        last_error: Exception | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                client.wait_for_workers(
                    expected_total_workers,
                    timeout=remaining,
                )
                first = self.validate_cluster_topology(
                    expected_cpu_workers=expected_cpu_workers,
                    expected_gpu_workers=expected_gpu_workers,
                    expected_gpu_ids=expected_gpu_ids,
                    verify_device_isolation=True,
                    client=client,
                )

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    last_error = TransientClusterTopologyError(
                        "Topology validated once but did not remain observable "
                        "for the stability interval."
                    )
                    break
                time.sleep(min(0.2, remaining))
                second = self.validate_cluster_topology(
                    expected_cpu_workers=expected_cpu_workers,
                    expected_gpu_workers=expected_gpu_workers,
                    expected_gpu_ids=expected_gpu_ids,
                    verify_device_isolation=True,
                    client=client,
                )
                if time.monotonic() > deadline:
                    last_error = TransientClusterTopologyError(
                        "Topology validation completed after the startup deadline."
                    )
                    break
                if (
                    first.cpu_workers == second.cpu_workers
                    and first.gpu_workers == second.gpu_workers
                    and first.total_cpu_slots == second.total_cpu_slots
                    and first.total_gpu_slots == second.total_gpu_slots
                ):
                    return second
                last_error = TransientClusterTopologyError(
                    "Worker addresses changed between consecutive topology snapshots."
                )
            except (TransientClusterTopologyError, OSError, TimeoutError) as exc:
                last_error = exc

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.2, remaining))

        detail = f" Last validation error: {last_error}" if last_error else ""
        raise TimeoutError(
            "Dask Worker topology did not stabilize before the startup "
            f"deadline.{detail}"
        ) from last_error

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
        membership_errors: list[str] = []
        contract_errors: list[str] = []

        workers = dict(scheduler_info.get("workers", {}))
        for worker_address, worker_info in workers.items():
            resources = dict(worker_info.get("resources", {}) or {})
            cpu_slots = float(resources.get(CPU_RESOURCE_NAME, 0) or 0)
            gpu_slots = float(resources.get(GPU_RESOURCE_NAME, 0) or 0)
            if (cpu_slots, gpu_slots) not in {(1.0, 0.0), (0.0, 1.0)}:
                contract_errors.append(
                    f"Worker {worker_address!r} must register exactly CPU=1 or GPU=1, "
                    f"got {resources!r}."
                )

        if expected_cpu_workers is not None and len(summary.cpu_workers) != expected_cpu_workers:
            membership_errors.append(
                f"Expected {expected_cpu_workers} CPU Workers, found {len(summary.cpu_workers)}."
            )
        if expected_gpu_workers is not None and len(summary.gpu_workers) != expected_gpu_workers:
            membership_errors.append(
                f"Expected {expected_gpu_workers} GPU Workers, found {len(summary.gpu_workers)}."
            )

        observed_gpu_ids: list[str] = []
        if verify_device_isolation and workers:
            for worker_address, worker_info in workers.items():
                diagnostic = worker_info.get("workflowDevice")
                if not isinstance(diagnostic, Mapping):
                    contract_errors.append(
                        f"Worker {worker_address!r} did not publish workflowDevice "
                        "startup metadata."
                    )
                    continue
                role = diagnostic.get("workerRole")
                assigned = diagnostic.get("assignedDevice")
                visible = diagnostic.get("cudaVisibleDevices")
                resources = diagnostic.get("resources", {})
                if role == "cpu":
                    if assigned != "cpu" or visible != "" or resources != {CPU_RESOURCE_NAME: 1.0}:
                        contract_errors.append(
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
                    ):
                        contract_errors.append(
                            f"GPU Worker {worker_address!r} failed device isolation: {diagnostic!r}."
                        )
                    else:
                        observed_gpu_ids.append(physical_gpu_id)
                else:
                    contract_errors.append(
                        f"Worker {worker_address!r} has unknown role {role!r}."
                    )

            if len(observed_gpu_ids) != len(set(observed_gpu_ids)):
                contract_errors.append(
                    "GPU Workers must use distinct physical GPUs, got "
                    f"{tuple(observed_gpu_ids)!r}."
                )
            if (
                expected_gpu_ids is not None
                and set(observed_gpu_ids) != set(expected_gpu_ids)
            ):
                target = membership_errors if membership_errors else contract_errors
                target.append(
                    "GPU Worker physical IDs do not match the requested topology: "
                    f"expected {expected_gpu_ids!r}, got {tuple(observed_gpu_ids)!r}."
                )

        if contract_errors:
            raise RuntimeError(
                "Invalid Dask cluster topology: "
                + " ".join(contract_errors + membership_errors)
            )
        if membership_errors:
            raise TransientClusterTopologyError(
                "Invalid Dask cluster topology: " + " ".join(membership_errors)
            )
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

    def stop_cluster(self) -> bool:
        """Stop the runtime and report whether shutdown was fully graceful.

        Confirmed emergency process cleanup returns ``False``. If child exit
        cannot be confirmed, the handles remain poisoned and this method raises
        so a replacement cluster cannot start beside orphan GPU processes.
        """
        with self._cluster_lock:
            client = self.client
            cluster = self.cluster
            close_errors: list[BaseException] = []
            close_deadline = time.monotonic() + _cluster_close_timeout()
            if client:
                try:
                    client.close(timeout=_remaining_shutdown_time(close_deadline))
                except Exception as exc:
                    close_errors.append(exc)
                    logger.warning("Error closing client: %s", exc)
            if cluster:
                try:
                    cluster.close(timeout=_remaining_shutdown_time(close_deadline))
                except Exception as exc:
                    close_errors.append(exc)
                    logger.warning("Error closing cluster: %s", exc)

            cleanup_unconfirmed = False
            if cluster is not None and _cluster_worker_processes_alive(cluster):
                if not close_errors:
                    close_errors.append(
                        RuntimeError(
                            "Dask cluster close returned while Worker children were alive."
                        )
                    )
                try:
                    _force_kill_cluster_workers_sync(cluster, timeout=5.0)
                except Exception as exc:
                    close_errors.append(exc)
                    logger.error(
                        "[Dask] Emergency Worker-process cleanup failed: %s",
                        exc,
                        exc_info=True,
                    )
                    cleanup_unconfirmed = _cluster_worker_processes_alive(cluster)

            if cleanup_unconfirmed:
                # Preserve poisoned handles and fail closed.  start_cluster()
                # retries this cleanup before it may provision any new Worker.
                self.client = client
                self.cluster = cluster
                self._cluster_poisoned = True
            else:
                # Only discard handles after every child is confirmed stopped.
                self.client = None
                self.cluster = None
                self._cluster_poisoned = False
            self.active_cpu_workers = 0
            self.active_gpu_workers = 0
            self.active_gpu_ids = ()
            if cleanup_unconfirmed:
                raise RuntimeError(
                    "Dask Worker exit could not be confirmed; the local cluster "
                    "is poisoned and replacement Workers are blocked."
                ) from close_errors[-1]
            if close_errors:
                logger.warning(
                    "[Dask] Graceful cluster close failed, but every Worker "
                    "child process was confirmed stopped."
                )
                return False
            return True


dask_service = DaskService()
