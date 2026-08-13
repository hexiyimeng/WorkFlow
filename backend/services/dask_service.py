from __future__ import annotations

import logging
import hashlib
import math
import operator
import os
import asyncio
import threading
import time
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import dask.config
from dask.distributed import Client, Nanny, Scheduler, Security, SpecCluster
from distributed import WorkerPlugin, get_worker
from distributed.core import Status

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


class WorkflowClient(Client):
    """Client whose independently owned IOLoop cannot leak on connect failure."""

    _failed_start_clients: list[WorkflowClient] = []

    def start(self, **kwargs: Any) -> Any:
        try:
            return super().start(**kwargs)
        except BaseException:
            loop_runner = getattr(self, "_loop_runner", None)
            loop_thread = getattr(loop_runner, "_loop_thread", None)
            if loop_runner is not None:
                try:
                    loop_runner.stop(timeout=5.0)
                except Exception:
                    logger.error(
                        "[Dask] Failed to stop Driver Client IOLoop after "
                        "connection failure.",
                        exc_info=True,
                    )
            if loop_thread is not None and _loop_thread_is_alive(loop_thread):
                setattr(self, "_workflow_lingering_loop_thread", loop_thread)
                self.__class__._failed_start_clients.append(self)
                logger.critical(
                    "[Dask] Driver Client IOLoop survived connection failure; "
                    "replacement cluster startup is blocked until it exits."
                )
            self.status = "closed"
            raise


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


def _external_cluster_security_from_environment() -> tuple[Security | None, dict[str, str] | None]:
    """Load explicit mTLS material for a cross-node Dask cluster.

    Plain TCP is permitted only with an explicit opt-in intended for a trusted
    and ACL-isolated test network.  A Dask Scheduler is a remote-code-execution
    boundary, so silently exposing it from loopback would be unsafe.
    """
    ca = os.getenv("WorkFlow_DASK_TLS_CA", "").strip()
    cert = os.getenv("WorkFlow_DASK_TLS_CERT", "").strip()
    key = os.getenv("WorkFlow_DASK_TLS_KEY", "").strip()
    provided = tuple(bool(item) for item in (ca, cert, key))
    if any(provided) and not all(provided):
        raise ValueError(
            "WorkFlow_DASK_TLS_CA, WorkFlow_DASK_TLS_CERT and "
            "WorkFlow_DASK_TLS_KEY must be configured together."
        )
    if all(provided):
        resolved: dict[str, str] = {}
        for name, value in (("ca", ca), ("cert", cert), ("key", key)):
            path = Path(value).expanduser()
            if not path.is_absolute() or path.is_symlink() or not path.is_file():
                raise ValueError(
                    f"Cross-node Dask TLS {name} must be an absolute regular "
                    f"non-symlink file: {path}"
                )
            resolved[name] = str(path.resolve(strict=True))
        security = Security(
            require_encryption=True,
            tls_ca_file=resolved["ca"],
            tls_client_cert=resolved["cert"],
            tls_client_key=resolved["key"],
            tls_scheduler_cert=resolved["cert"],
            tls_scheduler_key=resolved["key"],
            tls_worker_cert=resolved["cert"],
            tls_worker_key=resolved["key"],
        )
        return security, resolved

    insecure = os.getenv("WorkFlow_DASK_ALLOW_INSECURE_CLUSTER", "").strip().lower()
    if insecure not in {"1", "true", "yes", "on"}:
        raise ValueError(
            "Cross-node Dask requires mTLS. Configure WorkFlow_DASK_TLS_CA/"
            "CERT/KEY, or explicitly set WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=1 "
            "only on a trusted ACL-isolated test network."
        )
    return None, None


def _cluster_start_timeout() -> float:
    return _positive_timeout_from_env(
        "WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS",
        600.0,
    )


def _worker_batch_start_timeout() -> float:
    return _positive_timeout_from_env(
        "WorkFlow_DASK_WORKER_BATCH_START_TIMEOUT_SECONDS",
        180.0,
    )


def _worker_registration_timeout() -> float:
    return _positive_timeout_from_env(
        "WorkFlow_DASK_WORKER_REGISTRATION_TIMEOUT_SECONDS",
        60.0,
    )


def _bounded_phase_deadline(overall_deadline: float, phase_timeout: float) -> float:
    return min(overall_deadline, time.monotonic() + phase_timeout)


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


def _client_close_timeout(deadline: float) -> float:
    """Bound Client shutdown so Cluster/Nanny cleanup keeps most of the budget."""
    remaining = max(0.1, deadline - time.monotonic())
    return max(0.1, min(10.0, remaining * 0.25))


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
        # On a multi-node Slurm allocation the node-local CUDA identifier (for
        # example ``0``) is not globally unique.  The Worker still validates
        # its actual visibility mask against the node-local identifier, while
        # publishing a scheduler-wide identity such as ``c002:0`` through
        # WORKFLOW_PHYSICAL_GPU_ID.
        local_gpu_id = os.environ.get(
            "WORKFLOW_LOCAL_GPU_ID",
            os.environ.get("WORKFLOW_PHYSICAL_GPU_ID", visible_ids[0]),
        )
        if visible_ids[0] != local_gpu_id:
            raise RuntimeError(
                "GPU Worker CUDA visibility does not match its node-local "
                f"assignment: visible={visible_ids[0]!r}, assigned={local_gpu_id!r}."
            )

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
        "localGpuId": os.getenv("WORKFLOW_LOCAL_GPU_ID") if role == "gpu" else None,
        "executionId": os.getenv("WORKFLOW_EXECUTION_ID"),
        "submissionTokenHash": os.getenv("WORKFLOW_SUBMISSION_TOKEN_HASH"),
        "nodeRank": os.getenv("WORKFLOW_NODE_RANK"),
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
        # Bound each Nanny in a Worker-start batch independently from the
        # longer, whole-cluster startup deadline.  Without this, one stuck
        # Nanny can block its batch forever.
        for worker_spec in worker_specs.values():
            worker_spec["options"]["death_timeout"] = worker_start_timeout
    return scheduler_spec, worker_specs


def _cluster_nannies(cluster: Any) -> tuple[Any, ...]:
    candidates = list(dict(getattr(cluster, "workers", {}) or {}).values())
    candidates.extend(tuple(getattr(cluster, "_created", ()) or ()))
    pending = dict(
        getattr(cluster, "_workflow_pending_worker_starts", {}) or {}
    )
    candidates.extend(nanny for nanny, _start_task in pending.values())
    unique: dict[int, Any] = {}
    for candidate in candidates:
        unique[id(candidate)] = candidate
    return tuple(unique.values())


def _safe_runtime_attribute(value: Any, name: str) -> str:
    try:
        return str(getattr(value, name, "") or "")
    except Exception as exc:
        return f"<unavailable:{type(exc).__name__}>"


def _nanny_startup_diagnostic(nanny: Any) -> dict[str, object]:
    try:
        worker_process = getattr(nanny, "process", None)
    except (AttributeError, OSError, RuntimeError, ValueError):
        worker_process = None
    try:
        async_process = getattr(worker_process, "process", None)
    except (AttributeError, OSError, RuntimeError, ValueError):
        async_process = None
    try:
        process_state = getattr(async_process, "_state", None)
    except (AttributeError, OSError, RuntimeError, ValueError):
        process_state = None

    def process_value(name: str) -> object | None:
        for candidate in (async_process, process_state):
            if candidate is None:
                continue
            try:
                value = getattr(candidate, name, None)
            except (AttributeError, OSError, RuntimeError, ValueError):
                continue
            if value is not None:
                return value
        return None

    return {
        "name": _safe_runtime_attribute(nanny, "name"),
        "status": _safe_runtime_attribute(nanny, "status") or "unknown",
        "nannyAddress": _safe_runtime_attribute(nanny, "address"),
        "workerAddress": _safe_runtime_attribute(nanny, "worker_address"),
        "workerPid": process_value("pid"),
        "workerExitCode": process_value("exitcode"),
    }


def _exception_cause_chain(error: BaseException) -> tuple[dict[str, str], ...]:
    """Return a cycle-safe cause chain suitable for logs and UI errors."""
    result: list[dict[str, str]] = []
    visited: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        result.append({
            "exceptionType": type(current).__name__,
            "message": str(current),
        })
        current = current.__cause__ or current.__context__
    return tuple(result)


def _cluster_startup_diagnostics(cluster: Any) -> dict[str, object]:
    workers = dict(getattr(cluster, "workers", {}) or {})
    pending = dict(
        getattr(cluster, "_workflow_pending_worker_starts", {}) or {}
    )
    known_ids = {id(nanny) for nanny in workers.values()}
    known_ids.update(id(nanny) for nanny, _task in pending.values())
    created_only = [
        nanny
        for nanny in tuple(getattr(cluster, "_created", ()) or ())
        if id(nanny) not in known_ids
    ]
    return {
        "workers": {
            str(name): _nanny_startup_diagnostic(nanny)
            for name, nanny in workers.items()
        },
        "pending": {
            str(name): {
                **_nanny_startup_diagnostic(nanny),
                "taskDone": bool(start_task.done()),
                "taskCancelled": bool(start_task.cancelled()),
            }
            for name, (nanny, start_task) in pending.items()
        },
        "createdOnly": [
            _nanny_startup_diagnostic(nanny) for nanny in created_only
        ],
    }


_TERMINAL_NANNY_STATUSES = frozenset({Status.closed, Status.failed})


def _nanny_shutdown_confirmed(nanny: Any) -> bool:
    """Return True only when a Nanny cannot create a late Worker child."""
    startup_lock = getattr(nanny, "_startup_lock", None)
    if startup_lock is not None:
        try:
            if startup_lock.locked():
                return False
        except (AttributeError, RuntimeError):
            return False

    if getattr(nanny, "status", None) not in _TERMINAL_NANNY_STATUSES:
        return False

    worker_process = getattr(getattr(nanny, "process", None), "process", None)
    if worker_process is None:
        return True
    try:
        return not worker_process.is_alive()
    except (AssertionError, OSError, RuntimeError, ValueError):
        return False


def _cluster_nanny_shutdown_confirmed(cluster: Any) -> bool:
    return all(_nanny_shutdown_confirmed(nanny) for nanny in _cluster_nannies(cluster))


def _control_loop_shutdown_confirmed(owner: Any) -> bool:
    if owner is None:
        return True
    lingering_thread = getattr(owner, "_workflow_lingering_loop_thread", None)
    if lingering_thread is not None:
        # Even after this thread dies, the owner still needs one finalization
        # pass to clear the marker and set a terminal status.  Otherwise a
        # timed-out Client remains discoverable through Client.current().
        return False
    loop_runner = getattr(owner, "_loop_runner", None)
    if loop_runner is None:
        # Lightweight test doubles and already-detached runtimes have no loop.
        return True
    try:
        return not bool(loop_runner.is_started())
    except (AttributeError, RuntimeError):
        return False


def _loop_thread_is_alive(loop_thread: Any) -> bool:
    native_thread = getattr(loop_thread, "_thread", loop_thread)
    try:
        return bool(native_thread.is_alive())
    except (AttributeError, RuntimeError):
        return True


def _capture_control_loop_thread(owner: Any) -> Any | None:
    if owner is None:
        return None
    loop_runner = getattr(owner, "_loop_runner", None)
    if loop_runner is None:
        return None
    loop = getattr(loop_runner, "_loop", None)
    real_runner = loop_runner
    all_loops = getattr(type(loop_runner), "_all_loops", {})
    try:
        _count, registered_runner = all_loops.get(loop, (0, loop_runner))
    except (AttributeError, RuntimeError):
        registered_runner = loop_runner
    if registered_runner is not None:
        real_runner = registered_runner
    return getattr(real_runner, "_loop_thread", None)


def _record_lingering_control_thread(owner: Any, loop_thread: Any | None) -> None:
    if owner is not None and loop_thread is not None and _loop_thread_is_alive(loop_thread):
        setattr(owner, "_workflow_lingering_loop_thread", loop_thread)


def _mark_runtime_owner_closed(owner: Any) -> None:
    if isinstance(owner, Client):
        owner.status = "closed"
    elif isinstance(owner, SpecCluster):
        owner.status = Status.closed


def _force_stop_control_loop(owner: Any, *, timeout: float, label: str) -> None:
    if _control_loop_shutdown_confirmed(owner):
        return
    loop_runner = getattr(owner, "_loop_runner", None)
    lingering_thread = getattr(owner, "_workflow_lingering_loop_thread", None)
    if lingering_thread is not None:
        try:
            lingering_thread.join(timeout=max(0.1, timeout))
        except (AttributeError, RuntimeError) as exc:
            raise RuntimeError(f"Failed to join the {label} control thread.") from exc
        if _loop_thread_is_alive(lingering_thread):
            raise RuntimeError(f"The {label} control thread remained active.")
        delattr(owner, "_workflow_lingering_loop_thread")
        if _control_loop_shutdown_confirmed(owner):
            _mark_runtime_owner_closed(owner)
            return

    loop_thread = _capture_control_loop_thread(owner)
    stop_error: BaseException | None = None
    try:
        loop_runner.stop(timeout=max(0.1, timeout))
    except BaseException as exc:
        # Distributed clears LoopRunner._started and its thread reference in a
        # finally block even when the bounded join times out.  Inspect the
        # thread captured above before deciding the loop has stopped.
        stop_error = exc
    if loop_thread is not None and _loop_thread_is_alive(loop_thread):
        _record_lingering_control_thread(owner, loop_thread)
        raise RuntimeError(
            f"The {label} control thread remained active after stop()."
        ) from stop_error
    _mark_runtime_owner_closed(owner)
    if stop_error is not None:
        raise RuntimeError(f"Failed to stop the {label} control loop.") from stop_error
    if not _control_loop_shutdown_confirmed(owner):
        raise RuntimeError(f"The {label} control loop remained active after stop().")
    # A timed-out Distributed close may leave its status at ``closing`` even
    # after the independently owned loop is gone.  Mark the now-inert handle
    # terminal so Client.current() cannot rediscover it as an unmanaged client.
    _mark_runtime_owner_closed(owner)


def _cleanup_failed_workflow_clients() -> None:
    retained: list[WorkflowClient] = []
    for failed_client in tuple(WorkflowClient._failed_start_clients):
        try:
            _force_stop_control_loop(
                failed_client,
                timeout=5.0,
                label="failed Dask Client",
            )
        except Exception:
            retained.append(failed_client)
    WorkflowClient._failed_start_clients[:] = retained
    if retained:
        raise RuntimeError(
            "A failed Dask Client control thread is still alive; replacement "
            "cluster startup is blocked. Restart the backend if it does not exit."
        )


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

    pending = dict(
        getattr(cluster, "_workflow_pending_worker_starts", {}) or {}
    )
    pending_tasks = tuple(start_task for _nanny, start_task in pending.values())
    for start_task in pending_tasks:
        if not start_task.done():
            start_task.cancel()
    if pending_tasks:
        await asyncio.gather(*pending_tasks, return_exceptions=True)

    async def close_one(nanny: Any) -> BaseException | None:
        if _nanny_shutdown_confirmed(nanny):
            return None
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
    if not _cluster_nanny_shutdown_confirmed(cluster):
        raise RuntimeError(
            "One or more Dask Nannies remained in a non-terminal startup state."
        )

    workflow_pending = getattr(cluster, "_workflow_pending_worker_starts", None)
    if isinstance(workflow_pending, dict):
        workflow_pending.clear()


def _force_kill_cluster_workers_sync(cluster: Any, *, timeout: float) -> None:
    """Run the emergency Worker-process finalizer on SpecCluster's IOLoop."""
    cluster.sync(
        _force_kill_cluster_workers,
        cluster,
        timeout=timeout,
        callback_timeout=max(1.0, timeout * 3.0),
    )
    if _cluster_worker_processes_alive(cluster):
        raise RuntimeError("One or more Dask Worker child processes survived cleanup.")
    if not _cluster_nanny_shutdown_confirmed(cluster):
        raise RuntimeError("One or more Dask Nannies survived cleanup.")


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


def get_fresh_scheduler_info(
    client: Client,
    *,
    timeout: float | None = None,
) -> Mapping[str, Any]:
    """Fetch an atomic, untruncated Scheduler identity without Client cache races.

    ``Client.scheduler_info(n_workers=-1)`` looks correct, but Distributed stores
    the response in one shared ``Client._scheduler_identity`` cache and then
    returns that cache.  Its periodic scheduler-info callback concurrently
    refreshes the same cache with the method default of five Workers.  On the
    14-Worker Windows host this raced into repeatable 3-CPU/2-GPU snapshots even
    though all 14 Nannies were running.  Calling the Scheduler identity RPC
    directly returns this request's value and also propagates communication
    errors instead of silently returning a stale cache.
    """
    scheduler_rpc = getattr(client, "scheduler", None)
    identity = getattr(scheduler_rpc, "identity", None)
    if not callable(identity):
        raise RuntimeError("Dask Client has no live Scheduler identity RPC.")
    kwargs: dict[str, Any] = {"n_workers": -1}
    sync_kwargs: dict[str, Any] = {}
    if timeout is not None:
        if timeout <= 0:
            raise TimeoutError("No time remains for Dask Scheduler identity RPC.")
        sync_kwargs["callback_timeout"] = timeout
    scheduler_info = client.sync(identity, **kwargs, **sync_kwargs)
    if not isinstance(scheduler_info, Mapping):
        raise RuntimeError(
            "Dask Scheduler returned an invalid identity payload: "
            f"{type(scheduler_info).__name__}."
        )
    return scheduler_info


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


def _worker_start_batch_size(total_workers: int) -> int:
    """Return the concurrent Worker-start width for one local topology.

    The Driver Client is connected before this function is used, so there is
    no longer a reason to serialize same-host Worker startup merely to protect
    the first Client handshake.  On the target Windows host, serial batches of
    two grew from 8 seconds to 49, 101, then 188 seconds while the historical
    concurrent eight-Worker startup was fast.  Start the requested topology
    together by default and retain an explicit environment cap for sites that
    need to limit process-spawn concurrency.
    """
    if type(total_workers) is not int or total_workers <= 0:
        raise ValueError(
            "total_workers must be a positive integer, "
            f"got {total_workers!r}."
        )
    raw_value = os.getenv("WorkFlow_DASK_WORKER_START_BATCH_SIZE")
    if raw_value is None:
        return total_workers
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "WorkFlow_DASK_WORKER_START_BATCH_SIZE must be a positive integer, "
            f"got {raw_value!r}."
        ) from exc
    if value <= 0:
        raise ValueError(
            "WorkFlow_DASK_WORKER_START_BATCH_SIZE must be a positive integer, "
            f"got {raw_value!r}."
        )
    return min(value, total_workers)


async def _add_worker_spec_batch(
    cluster: SpecCluster,
    worker_specs: Mapping[str, Mapping[str, Any]],
    *,
    start_timeout: float,
    cleanup_timeout: float,
) -> None:
    """Start one transactional Worker batch on the SpecCluster control loop.

    Distributed's normal ``SpecCluster`` reconciliation creates Nanny-start
    Tasks and awaits them before publishing ``cluster.workers``. Cancelling its
    outer coroutine can leave those Tasks running only through
    ``cluster._created``. This project-owned adapter owns every Task explicitly,
    rolls the spec back, and closes every Nanny before propagating an error.
    """
    duplicate_names = set(worker_specs).intersection(cluster.worker_spec)
    if duplicate_names:
        raise RuntimeError(
            "Dask Worker batch contains duplicate names: "
            f"{tuple(sorted(duplicate_names))!r}."
        )

    pending: dict[str, tuple[Any, asyncio.Task[Any]]] = getattr(
        cluster,
        "_workflow_pending_worker_starts",
        None,
    )
    if pending is None:
        pending = {}
        setattr(cluster, "_workflow_pending_worker_starts", pending)

    batch_specs = {name: dict(spec) for name, spec in worker_specs.items()}
    batch_nannies: dict[str, Any] = {}
    batch_tasks: dict[str, asyncio.Task[Any]] = {}
    cluster.worker_spec.update(batch_specs)
    try:
        scheduler_address = (
            getattr(cluster.scheduler, "contact_address", None)
            or cluster.scheduler.address
        )
        for name, spec in batch_specs.items():
            worker_class = spec.get("cls")
            if not isinstance(worker_class, type):
                raise RuntimeError(
                    "WorkFlow batched startup requires a concrete Worker class, "
                    f"got {worker_class!r} for {name!r}."
                )
            options = dict(spec.get("options", {}) or {})
            options.setdefault("name", name)
            nanny = worker_class(scheduler_address, **options)
            cluster._created.add(nanny)
            batch_nannies[name] = nanny

        for name, nanny in batch_nannies.items():
            start_task = asyncio.create_task(nanny.start())
            batch_tasks[name] = start_task
            pending[name] = (nanny, start_task)

        results = await asyncio.wait_for(
            asyncio.gather(
                *batch_tasks.values(),
                return_exceptions=True,
            ),
            timeout=start_timeout,
        )
        failures = {
            name: result
            for name, result in zip(batch_tasks, results)
            if isinstance(result, BaseException)
        }
        if failures:
            failure_details: dict[str, dict[str, object]] = {}
            for name, failure in failures.items():
                diagnostic = _nanny_startup_diagnostic(batch_nannies[name])
                failure_details[name] = {
                    "exceptionChain": _exception_cause_chain(failure),
                    **diagnostic,
                }
                logger.error(
                    "[Dask] Nanny %s failed during Worker startup: %s | "
                    "diagnostic=%s",
                    name,
                    failure,
                    diagnostic,
                    exc_info=(type(failure), failure, failure.__traceback__),
                )
            first_failure = next(iter(failures.values()))
            raise RuntimeError(
                f"{len(failures)} Dask Nanny process(es) failed to start in "
                f"batch: {failure_details!r}."
            ) from first_failure

        cluster.workers.update(batch_nannies)
        for nanny in batch_nannies.values():
            nanny._cluster = weakref.ref(cluster)
        for name in batch_nannies:
            pending.pop(name, None)
    except BaseException:
        for name in batch_specs:
            cluster.worker_spec.pop(name, None)
            cluster.workers.pop(name, None)

        for start_task in batch_tasks.values():
            if not start_task.done():
                start_task.cancel()
        if batch_tasks:
            await asyncio.gather(*batch_tasks.values(), return_exceptions=True)

        async def close_one(nanny: Any) -> BaseException | None:
            if _nanny_shutdown_confirmed(nanny):
                return None
            try:
                await asyncio.wait_for(
                    nanny.close(
                        timeout=max(0.1, cleanup_timeout * 0.8),
                        reason="workflow-worker-batch-start-failed",
                    ),
                    timeout=max(0.2, cleanup_timeout),
                )
                return None
            except BaseException as close_exc:
                return close_exc

        close_results = await asyncio.gather(
            *(close_one(nanny) for nanny in batch_nannies.values())
        )
        for name, nanny in batch_nannies.items():
            if _nanny_shutdown_confirmed(nanny):
                pending.pop(name, None)
        close_failures = [result for result in close_results if result is not None]
        if close_failures:
            logger.error(
                "[Dask] %s Nanny process(es) did not close cleanly after a "
                "Worker batch startup failure.",
                len(close_failures),
            )
        raise


def _provision_worker_specs_in_batches(
    cluster: SpecCluster,
    client: Client,
    worker_specs: Mapping[str, Mapping[str, Any]],
    *,
    deadline: float,
    batch_size: int,
    batch_timeout: float,
    registration_timeout: float,
) -> None:
    """Provision Workers incrementally after the Driver Client is connected."""
    items = tuple(worker_specs.items())
    registered = 0
    provisioning_started = time.monotonic()
    batch_count = math.ceil(len(items) / batch_size)
    for offset in range(0, len(items), batch_size):
        batch_number = offset // batch_size + 1
        batch = dict(items[offset : offset + batch_size])
        batch_started = time.monotonic()
        logger.info(
            "[Dask] Starting Worker batch %s/%s: names=%s progress=%s/%s",
            batch_number,
            batch_count,
            tuple(batch),
            registered,
            len(items),
        )
        batch_deadline = _bounded_phase_deadline(deadline, batch_timeout)
        remaining = _remaining_startup_time(
            batch_deadline,
            stage=f"starting Worker batch {batch_number}",
        )
        cluster.sync(
            _add_worker_spec_batch,
            cluster,
            batch,
            start_timeout=remaining,
            cleanup_timeout=min(15.0, batch_timeout),
            # The outer callback must leave room for the coroutine's bounded
            # transactional rollback.  Otherwise Distributed.sync() returns to
            # the caller while Nanny cleanup is still running on the cluster
            # IOLoop, racing the whole-cluster shutdown path.
            callback_timeout=remaining + min(15.0, batch_timeout) + 5.0,
        )
        registered += len(batch)
        # Nanny.start() normally returns only after its Worker has registered,
        # but observe the independent Client as a separate bounded phase.  Do
        # not reuse the process-start deadline here: a healthy Worker that
        # finishes close to its start limit must not be rejected merely because
        # the registration RPC begins a few milliseconds later.
        registration_deadline = _bounded_phase_deadline(
            deadline,
            registration_timeout,
        )
        client.wait_for_workers(
            registered,
            timeout=_remaining_startup_time(
                registration_deadline,
                stage="observing Worker registration",
            ),
        )
        identity_deadline = _bounded_phase_deadline(
            deadline,
            registration_timeout,
        )
        scheduler_info = get_fresh_scheduler_info(
            client,
            timeout=_remaining_startup_time(
                identity_deadline,
                stage="validating Worker batch registration",
            ),
        )
        observed = len(dict(scheduler_info.get("workers", {})))
        if observed < registered:
            raise TransientClusterTopologyError(
                "Scheduler lost a Worker during batched startup: "
                f"expected at least {registered}, found {observed}."
            )
        logger.info(
            "[Dask] Worker batch %s/%s registered: names=%s progress=%s/%s "
            "observed=%s batch_elapsed=%.1fs total_elapsed=%.1fs",
            batch_number,
            batch_count,
            tuple(batch),
            registered,
            len(items),
            observed,
            time.monotonic() - batch_started,
            time.monotonic() - provisioning_started,
        )


class DaskService:
    _instance: DaskService | None = None
    client: Client | None = None
    cluster: SpecCluster | None = None
    active_cpu_workers: int = 0
    active_gpu_workers: int = 0
    active_gpu_ids: tuple[str, ...] = ()
    _cluster_poisoned: bool = False
    _external_workers: bool = False
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
            if self._external_workers:
                if self.client is None:
                    raise RuntimeError(
                        "The external Dask runtime has no live Driver Client."
                    )
                expected_cpu = _normalize_worker_count(
                    0 if cpu_workers is None else cpu_workers,
                    name="cpu_workers",
                )
                expected_gpu = _normalize_worker_count(
                    0 if gpu_workers is None else gpu_workers,
                    name="gpu_workers",
                )
                if (
                    self.active_cpu_workers != expected_cpu
                    or self.active_gpu_workers != expected_gpu
                ):
                    raise RuntimeError(
                        "The externally provisioned Slurm Worker topology does "
                        "not match the Graph resource plan: "
                        f"active CPU/GPU={self.active_cpu_workers}/"
                        f"{self.active_gpu_workers}, requested={expected_cpu}/"
                        f"{expected_gpu}."
                    )
                self._wait_for_stable_topology(
                    expected_cpu_workers=expected_cpu,
                    expected_gpu_workers=expected_gpu,
                    expected_gpu_ids=None,
                    expected_total_workers=expected_cpu + expected_gpu,
                    deadline=time.monotonic() + _cluster_start_timeout(),
                    client=self.client,
                )
                return self.client
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

    def start_external_scheduler(
        self,
        *,
        host: str,
        port: int = 0,
        dashboard_address: str | None = None,
    ) -> Client:
        """Start a service-node Scheduler/Client with no local Workers.

        Slurm Worker launchers connect to the returned Scheduler address from
        one or more compute nodes.  This path deliberately performs no CUDA
        detection on the service node.
        """
        if not isinstance(host, str) or not host.strip():
            raise ValueError("External Dask Scheduler host must be non-empty.")
        if type(port) is not int or port < 0 or port > 65535:
            raise ValueError("External Dask Scheduler port must be 0..65535.")
        with self._cluster_lock:
            if self.client is not None or self.cluster is not None:
                raise RuntimeError(
                    "A Dask runtime is already active; it must be stopped before "
                    "starting an external Slurm Worker allocation."
                )
            if dashboard_address not in (None, ""):
                raise ValueError(
                    "The cross-node Scheduler dashboard must remain disabled; "
                    "use the WorkFlow execution UI instead."
                )
            security, _security_paths = _external_cluster_security_from_environment()
            scheduler_spec = {
                "cls": Scheduler,
                "options": {
                    "host": host.strip(),
                    "port": port,
                    # The Scheduler protocol listens on the configured
                    # compute-facing host.  Do not expose the unauthenticated
                    # Bokeh dashboard on that interface; the application UI
                    # already relays execution state over its loopback HTTP
                    # service.
                    "dashboard": False,
                    "dashboard_address": None,
                    "security": security,
                },
            }
            cluster: SpecCluster | None = None
            client: WorkflowClient | None = None
            try:
                cluster = SpecCluster(
                    scheduler=scheduler_spec,
                    workers={},
                    security=security,
                    asynchronous=False,
                    name="WorkFlow-Slurm-Driver",
                )
                client = WorkflowClient(
                    cluster.scheduler_address,
                    security=cluster.security,
                    timeout=min(60.0, _cluster_start_timeout()),
                    set_as_default=True,
                )
            except BaseException:
                if client is not None:
                    try:
                        client.close(timeout=5.0)
                    except Exception:
                        logger.exception(
                            "[Dask] Failed to close external Driver Client after startup failure."
                        )
                if cluster is not None:
                    try:
                        cluster.close(timeout=10.0)
                    except Exception:
                        logger.exception(
                            "[Dask] Failed to close external Scheduler after startup failure."
                        )
                raise

            self.cluster = cluster
            self.client = client
            self.active_cpu_workers = 0
            self.active_gpu_workers = 0
            self.active_gpu_ids = ()
            self._external_workers = True
            self._cluster_poisoned = False
            logger.info(
                "[Dask] Service-node Driver Scheduler ready for Slurm Workers: %s",
                cluster.scheduler_address,
            )
            return client

    def activate_external_workers(
        self,
        *,
        cpu_workers: int,
        gpu_workers: int,
        timeout: float,
        execution_id: str | None = None,
        submission_token: str | None = None,
    ) -> ClusterResourceSummary:
        """Validate and publish an exact externally launched Worker topology."""
        expected_cpu = _normalize_worker_count(cpu_workers, name="cpu_workers")
        expected_gpu = _normalize_worker_count(gpu_workers, name="gpu_workers")
        if expected_cpu + expected_gpu <= 0:
            raise ValueError("At least one external Worker must be requested.")
        if timeout <= 0:
            raise ValueError("External Worker startup timeout must be positive.")
        # Do not hold the service-wide lifecycle lock while waiting for a
        # queued multi-node allocation to register.  Cancellation cannot stop
        # a synchronous distributed RPC running in ``to_thread``; holding the
        # lock here would therefore prevent ``stop_cluster`` from closing the
        # Client/Scheduler and issuing the authoritative Slurm cleanup.
        with self._cluster_lock:
            if not self._external_workers or self.client is None:
                raise RuntimeError("No external Slurm Dask runtime is active.")
            active_client = self.client

        summary = self._wait_for_stable_topology(
            expected_cpu_workers=expected_cpu,
            expected_gpu_workers=expected_gpu,
            expected_gpu_ids=None,
            expected_total_workers=expected_cpu + expected_gpu,
            deadline=time.monotonic() + timeout,
            client=active_client,
        )
        diagnostics = self.get_worker_diagnostics(active_client)
        if execution_id is not None:
            token_hash = (
                hashlib.sha256(submission_token.encode("utf-8")).hexdigest()
                if submission_token is not None
                else None
            )
            ownership_errors = []
            for address, item in diagnostics.items():
                if item.get("executionId") != execution_id:
                    ownership_errors.append(
                        f"{address} executionId={item.get('executionId')!r}"
                    )
                if token_hash is not None and item.get("submissionTokenHash") != token_hash:
                    ownership_errors.append(
                        f"{address} submission token does not match"
                    )
            if ownership_errors:
                raise RuntimeError(
                    "External Dask Workers failed execution ownership "
                    "validation: " + "; ".join(ownership_errors)
                )
        gpu_ids = tuple(sorted(
            str(item.get("physicalGpuId"))
            for item in diagnostics.values()
            if item.get("workerRole") == "gpu"
        ))

        with self._cluster_lock:
            if not self._external_workers or self.client is not active_client:
                raise RuntimeError(
                    "The external Dask runtime was stopped while Workers were "
                    "registering."
                )
            self.active_cpu_workers = expected_cpu
            self.active_gpu_workers = expected_gpu
            self.active_gpu_ids = gpu_ids
            logger.info(
                "[Dask] External Slurm Worker topology validated: CPU=%s GPU=%s "
                "devices=%s Scheduler=%s",
                expected_cpu,
                expected_gpu,
                gpu_ids,
                summary.scheduler_address,
            )
            return summary

    def uses_external_workers(self, client: Client | None = None) -> bool:
        """Return whether the active Client is backed by Slurm Workers."""
        with self._cluster_lock:
            return bool(
                self._external_workers
                and self.client is not None
                and (client is None or client is self.client)
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
            _cleanup_failed_workflow_clients()
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
            # Parse startup policy before creating any Scheduler or Client so a
            # bad deployment value has no process or socket side effects.
            batch_size = _worker_start_batch_size(total_workers)
            worker_batch_timeout = _worker_batch_start_timeout()
            worker_registration_timeout = _worker_registration_timeout()

            desired_topology = (
                requested_cpu_workers,
                len(gpu_ids),
                gpu_ids,
            )
            if self.client is not None:
                try:
                    scheduler_summary = cluster_resource_summary_from_scheduler_info(
                        get_fresh_scheduler_info(
                            self.client,
                            timeout=_remaining_startup_time(
                                startup_deadline,
                                stage="querying the existing Scheduler",
                            ),
                        )
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
                worker_start_timeout=worker_batch_timeout,
            )

            logger.info(
                "[Dask] Startup plan: platform=%s host=%s scheduler=1 cpu_workers=%s "
                "gpu_workers=%s selected_gpu_ids=%s threads_per_worker=1 "
                "cpu_memory_limit=%s gpu_memory_limit=%s local_directory=%s "
                "worker_batch_size=%s worker_batch_timeout=%.1fs "
                "worker_registration_timeout=%.1fs "
                "total_startup_timeout=%.1fs",
                current_platform(),
                LOCAL_CLUSTER_HOST,
                requested_cpu_workers,
                len(gpu_ids),
                gpu_ids,
                cpu_memory_limit,
                gpu_memory_limit,
                local_directory,
                batch_size,
                worker_batch_timeout,
                worker_registration_timeout,
                startup_timeout,
            )

            cluster: SpecCluster | None = None
            client: Client | None = None
            startup_stage = "creating the Dask Scheduler"
            try:
                # Start the Scheduler with no Workers, then connect an
                # independently looped Driver Client before any Nanny spawn.
                # Client(cluster) reuses SpecCluster's control IOLoop; on the
                # remote Windows host, creating 14 Nannies first left that loop
                # unable to complete the subsequent Client handshake even
                # though all Workers had already registered.  A separate Client
                # loop prevents Worker startup from blocking the only Driver
                # connection.  Once the Client is healthy, Workers start
                # concurrently by default; an explicit batch-size override can
                # still cap process-spawn concurrency for a specific site.
                cluster = SpecCluster(
                    workers={},
                    scheduler=scheduler_spec,
                    asynchronous=False,
                    silence_logs=logging.WARNING,
                    name="WorkFlow local mixed cluster",
                )
                logger.info(
                    "[Dask] Scheduler constructed before Workers: address=%s "
                    "elapsed=%.1fs",
                    cluster.scheduler.address,
                    time.monotonic() - startup_started,
                )
                startup_stage = "connecting the Dask Client"
                client_deadline = _bounded_phase_deadline(startup_deadline, 60.0)
                client = WorkflowClient(
                    cluster.scheduler.address,
                    security=cluster.security,
                    timeout=_remaining_startup_time(
                        client_deadline,
                        stage="connecting the Dask Client",
                    ),
                )
                initial_scheduler_info = get_fresh_scheduler_info(
                    client,
                    timeout=_remaining_startup_time(
                        client_deadline,
                        stage="checking the Scheduler before Worker startup",
                    ),
                )
                if initial_scheduler_info.get("workers"):
                    raise RuntimeError(
                        "A newly created Dask Scheduler unexpectedly reported "
                        "Workers before local provisioning began."
                    )
                logger.info(
                    "[Dask] Driver Client connected before Worker startup: "
                    "scheduler=%s elapsed=%.1fs",
                    initial_scheduler_info.get("address", cluster.scheduler.address),
                    time.monotonic() - startup_started,
                )

                startup_stage = "starting Worker processes in bounded batches"
                _provision_worker_specs_in_batches(
                    cluster,
                    client,
                    worker_specs,
                    deadline=startup_deadline,
                    batch_size=batch_size,
                    batch_timeout=worker_batch_timeout,
                    registration_timeout=worker_registration_timeout,
                )
                workers_ready_at = time.monotonic()
                logger.info(
                    "[Dask] Worker processes and role metadata registered: "
                    "total=%s batch_size=%s elapsed=%.1fs",
                    total_workers,
                    batch_size,
                    workers_ready_at - startup_started,
                )

                startup_stage = "waiting for a stable validated topology"
                validation_deadline = _bounded_phase_deadline(
                    startup_deadline,
                    30.0,
                )
                summary = self._wait_for_stable_topology(
                    expected_cpu_workers=requested_cpu_workers,
                    expected_gpu_workers=len(gpu_ids),
                    expected_gpu_ids=gpu_ids,
                    expected_total_workers=total_workers,
                    deadline=validation_deadline,
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
                startup_diagnostics: dict[str, object] = {}
                if cluster is not None:
                    startup_diagnostics = _cluster_startup_diagnostics(cluster)
                logger.error(
                    "[Dask] Start failed after %.1fs during %s: %s | "
                    "startup_diagnostics=%s",
                    time.monotonic() - startup_started,
                    startup_stage,
                    exc,
                    startup_diagnostics,
                )
                cleanup_errors: list[BaseException] = []
                cleanup_deadline = time.monotonic() + _cluster_close_timeout()
                if client is not None:
                    client_loop_thread = _capture_control_loop_thread(client)
                    try:
                        client.close(timeout=_client_close_timeout(cleanup_deadline))
                    except Exception as close_exc:
                        cleanup_errors.append(close_exc)
                        logger.warning(
                            "[Dask] Client cleanup after startup failure failed: %s",
                            close_exc,
                        )
                    finally:
                        _record_lingering_control_thread(
                            client,
                            client_loop_thread,
                        )
                if cluster is not None:
                    cluster_loop_thread = _capture_control_loop_thread(cluster)
                    try:
                        cluster.close(timeout=_remaining_shutdown_time(cleanup_deadline))
                    except Exception as close_exc:
                        cleanup_errors.append(close_exc)
                        logger.warning(
                            "[Dask] Cluster cleanup after startup failure failed: %s",
                            close_exc,
                        )
                    finally:
                        _record_lingering_control_thread(
                            cluster,
                            cluster_loop_thread,
                        )
                cleanup_unconfirmed = False
                if (
                    cluster is not None
                    and not _cluster_nanny_shutdown_confirmed(cluster)
                ):
                    try:
                        _force_kill_cluster_workers_sync(cluster, timeout=5.0)
                    except Exception as kill_exc:
                        logger.error(
                            "[Dask] Emergency Worker cleanup after startup failure "
                            "failed: %s",
                            kill_exc,
                            exc_info=True,
                        )
                if cluster is not None:
                    cleanup_unconfirmed = not _cluster_nanny_shutdown_confirmed(
                        cluster
                    )
                if not cleanup_unconfirmed:
                    for owner, label in (
                        (client, "Dask Client"),
                        (cluster, "SpecCluster"),
                    ):
                        if not _control_loop_shutdown_confirmed(owner):
                            try:
                                _force_stop_control_loop(
                                    owner,
                                    timeout=5.0,
                                    label=label,
                                )
                            except Exception as loop_exc:
                                cleanup_errors.append(loop_exc)
                                logger.error(
                                    "[Dask] %s loop cleanup after startup failure "
                                    "failed: %s",
                                    label,
                                    loop_exc,
                                    exc_info=True,
                                )
                    cleanup_unconfirmed = not all(
                        _control_loop_shutdown_confirmed(owner)
                        for owner in (client, cluster)
                    )
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
            get_fresh_scheduler_info(active_client)
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
        scheduler_info = get_fresh_scheduler_info(active_client)
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
        expected_gpu_ids: tuple[str, ...] | None,
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
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "Worker registration completed after the startup deadline."
                    )
                first = self.validate_cluster_topology(
                    expected_cpu_workers=expected_cpu_workers,
                    expected_gpu_workers=expected_gpu_workers,
                    expected_gpu_ids=expected_gpu_ids,
                    verify_device_isolation=True,
                    client=client,
                    rpc_timeout=remaining,
                )

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    last_error = TransientClusterTopologyError(
                        "Topology validated once but did not remain observable "
                        "for the stability interval."
                    )
                    break
                time.sleep(min(0.2, remaining))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    last_error = TransientClusterTopologyError(
                        "Topology validated once but did not remain observable "
                        "for the stability interval."
                    )
                    break
                second = self.validate_cluster_topology(
                    expected_cpu_workers=expected_cpu_workers,
                    expected_gpu_workers=expected_gpu_workers,
                    expected_gpu_ids=expected_gpu_ids,
                    verify_device_isolation=True,
                    client=client,
                    rpc_timeout=remaining,
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
        rpc_timeout: float | None = None,
    ) -> ClusterResourceSummary:
        active_client = client or self.get_client()
        if active_client is None:
            raise RuntimeError("No live Dask client is available for topology validation.")
        scheduler_info = get_fresh_scheduler_info(
            active_client,
            timeout=rpc_timeout,
        )
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
                    local_gpu_id = str(
                        diagnostic.get("localGpuId") or physical_gpu_id
                    )
                    if (
                        assigned != "cuda:0"
                        or len(visible_ids) != 1
                        or not physical_gpu_id
                        or visible_ids[0] != local_gpu_id
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
                client_loop_thread = _capture_control_loop_thread(client)
                try:
                    client.close(timeout=_client_close_timeout(close_deadline))
                except Exception as exc:
                    close_errors.append(exc)
                    logger.warning("Error closing client: %s", exc)
                finally:
                    _record_lingering_control_thread(client, client_loop_thread)
            if cluster:
                cluster_loop_thread = _capture_control_loop_thread(cluster)
                try:
                    cluster.close(timeout=_remaining_shutdown_time(close_deadline))
                except Exception as exc:
                    close_errors.append(exc)
                    logger.warning("Error closing cluster: %s", exc)
                finally:
                    _record_lingering_control_thread(cluster, cluster_loop_thread)

            cleanup_unconfirmed = False
            if (
                cluster is not None
                and not _cluster_nanny_shutdown_confirmed(cluster)
            ):
                if not close_errors:
                    close_errors.append(
                        RuntimeError(
                            "Dask cluster close returned before every Nanny reached "
                            "a terminal state."
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
            if cluster is not None:
                cleanup_unconfirmed = not _cluster_nanny_shutdown_confirmed(cluster)
            if not cleanup_unconfirmed:
                for owner, label in (
                    (client, "Dask Client"),
                    (cluster, "SpecCluster"),
                ):
                    if not _control_loop_shutdown_confirmed(owner):
                        try:
                            _force_stop_control_loop(
                                owner,
                                timeout=5.0,
                                label=label,
                            )
                        except Exception as exc:
                            close_errors.append(exc)
                            logger.error(
                                "[Dask] %s loop cleanup failed: %s",
                                label,
                                exc,
                                exc_info=True,
                            )
                cleanup_unconfirmed = not all(
                    _control_loop_shutdown_confirmed(owner)
                    for owner in (client, cluster)
                )

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
            if not cleanup_unconfirmed:
                self._external_workers = False
            if cleanup_unconfirmed:
                raise RuntimeError(
                    "Dask runtime shutdown could not be confirmed; the local "
                    "cluster is poisoned and replacement Workers are blocked."
                ) from close_errors[-1]
            if close_errors:
                logger.warning(
                    "[Dask] Graceful cluster close failed, but every Worker "
                    "child process was confirmed stopped."
                )
                return False
            return True


dask_service = DaskService()
