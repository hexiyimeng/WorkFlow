from __future__ import annotations

import logging
import math
import os
import asyncio
import threading
import time
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import dask.config
from dask.distributed import Client, Nanny, Scheduler, Security, SpecCluster
from distributed import WorkerPlugin, get_worker
from distributed.core import Status
from distributed.diagnostics.plugin import SchedulerPlugin

from core.config import _is_main_process, config
from core.logger import logger
from core.platform import (
    dask_spill_dir,
    reclaim_process_memory,
    should_schedule_malloc_trim,
)
from core.worker_pool import WorkerPool
from core.worker_profiles import WorkerProfile, worker_logical_resources
from core.worker_ownership import (
    execution_ownership_resource,
    is_ownership_resource,
    submission_ownership_resource,
)


CPU_RESOURCE_NAME = "CPU"
GPU_RESOURCE_NAME = "GPU"

# The local Worker Profile cluster is deliberately same-host: the
# Driver, Scheduler, Nannies, and Workers all live in one desktop process tree
# (or in one single-node Slurm allocation).  Advertising a physical NIC here
# makes a Windows Driver hairpin through firewall/VPN/RDP network policy merely
# to reach its own Scheduler.  That became unreliable as the number of Nanny
# connections grew.  Loopback is the only address these local processes need.
LOCAL_CLUSTER_HOST = "127.0.0.1"

@dataclass(frozen=True)
class ClusterResourceSummary:
    scheduler_address: str
    cpu_workers: tuple[str, ...]
    gpu_workers: tuple[str, ...]
    total_cpu_slots: float
    total_gpu_slots: float
    worker_profile_slots: Mapping[str, float]


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


def _nonnegative_worker_count(value: object, *, name: str) -> int:
    """Validate exact planned Worker counts without accepting bool/float coercion."""

    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.")
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
    requested_gpu_workers: int | None,
    parent_cuda_visible_devices: str | None,
) -> tuple[str, ...]:
    """Resolve the physical identifiers assigned to individual GPU Workers.

    When the parent has a CUDA visibility mask, identifiers are selected only
    from that mask. This prevents a child Worker from exposing a device that an
    administrator intentionally hid from the backend process.
    """
    if detected_gpu_count < 0:
        raise ValueError("detected_gpu_count must be non-negative.")
    if requested_gpu_workers is not None and requested_gpu_workers < 0:
        raise ValueError("The Worker Pool GPU count must be non-negative.")

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

    worker_count = len(selected_ids) if requested_gpu_workers is None else requested_gpu_workers
    if worker_count > len(selected_ids):
        raise ValueError(
            "The Worker Pool GPU count cannot exceed the number of selected visible GPUs "
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


def _get_dask_local_dir() -> str:
    directory = dask_spill_dir(config.DASK_LOCAL_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    return str(directory)


def _worker_resources(worker: Any) -> dict[str, float]:
    return worker_logical_resources(worker)


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
        profile = os.environ.get("WORKFLOW_WORKER_PROFILE", "").strip()
        resources = _worker_resources(worker)
        if profile:
            if resources.get(profile) != 1:
                raise RuntimeError(
                    "Worker must advertise WORKFLOW_WORKER_PROFILE with logical value 1."
                )
            if float(resources.get(CPU_RESOURCE_NAME, 0) or 0) <= 0:
                raise RuntimeError("Every Worker Profile must advertise positive CPU capacity.")

        if role == "cpu":
            if not profile and resources.get(CPU_RESOURCE_NAME) != 1:
                raise RuntimeError("Legacy local CPU Worker must advertise CPU=1.")
            if resources.get(GPU_RESOURCE_NAME, 0) != 0:
                raise RuntimeError("CPU Worker must not advertise GPU capability.")
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
        if not profile and resources.get(CPU_RESOURCE_NAME, 0) != 0:
            raise RuntimeError("Legacy local GPU Worker must not advertise CPU capacity.")
        if resources.get(GPU_RESOURCE_NAME) != 1:
            raise RuntimeError("GPU Worker must advertise logical GPU=1.")

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
        "workerProfile": os.getenv("WORKFLOW_WORKER_PROFILE"),
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
    reclaim_process_memory()


def _schedule_malloc_trim_on_scheduler(dask_scheduler: Any) -> None:
    """Periodically return released graph pages from the Scheduler process."""

    interval = float(
        os.getenv(
            "WorkFlow_DASK_SCHEDULER_MEMORY_TRIM_INTERVAL_SECONDS",
            "60",
        )
    )
    if interval <= 0:
        return

    def trim_and_reschedule() -> None:
        status = getattr(dask_scheduler, "status", None)
        if status in {Status.closing, Status.closed, "closing", "closed"}:
            return
        _malloc_trim_once()
        dask_scheduler.loop.call_later(interval, trim_and_reschedule)

    dask_scheduler.loop.call_later(interval, trim_and_reschedule)


class _SchedulerMemoryTrimPlugin(SchedulerPlugin):
    """Install Linux allocator maintenance on an in-process Scheduler."""

    name = "workflow-scheduler-memory-trim"

    async def start(self, scheduler: Any) -> None:
        _schedule_malloc_trim_on_scheduler(scheduler)


def _scheduler_plugins() -> tuple[Any, ...]:
    if not should_schedule_malloc_trim():
        return ()
    return (_SchedulerMemoryTrimPlugin(),)


def build_local_profile_cluster_specs(
    *,
    profiles: Mapping[str, WorkerProfile],
    pools: Mapping[str, WorkerPool],
    gpu_ids: tuple[str, ...],
    local_directory: str,
    dashboard_address: str,
    worker_start_timeout: float,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Build local Dask Worker specs from the same Profile/Pool contract as Slurm."""

    scheduler_spec: dict[str, Any] = {
        "cls": Scheduler,
        "options": {
            "host": LOCAL_CLUSTER_HOST,
            "dashboard_address": dashboard_address,
            "plugins": _scheduler_plugins(),
        },
    }
    worker_specs: dict[str, dict[str, Any]] = {}
    gpu_cursor = 0
    for profile_name in sorted(profiles):
        profile = profiles[profile_name]
        pool = pools[profile_name]
        pool.validate_profile(profile)
        worker_count = pool.worker_count
        for index in range(worker_count):
            physical_gpu_id = ""
            if profile.physical_resources.gpu:
                if gpu_cursor >= len(gpu_ids):
                    raise ValueError(
                        f"Worker Pool {profile_name!r} needs more GPUs than are available."
                    )
                physical_gpu_id = gpu_ids[gpu_cursor]
                gpu_cursor += 1
            role = "gpu" if physical_gpu_id else "cpu"
            worker_name = f"{profile_name}-{index}"
            worker_specs[worker_name] = {
                "cls": Nanny,
                "options": {
                    "host": LOCAL_CLUSTER_HOST,
                    "name": worker_name,
                    "nthreads": profile.threads,
                    "resources": dict(profile.logical_resources),
                    "env": {
                        "WORKFLOW_DASK_WORKER_PROCESS": "1",
                        "WORKFLOW_WORKER_PROFILE": profile.name,
                        "WORKFLOW_WORKER_ROLE": role,
                        "WORKFLOW_PHYSICAL_GPU_ID": physical_gpu_id,
                        "WORKFLOW_LOCAL_GPU_ID": physical_gpu_id,
                        "CUDA_VISIBLE_DEVICES": physical_gpu_id,
                        "OMP_NUM_THREADS": str(profile.threads),
                        "MKL_NUM_THREADS": str(profile.threads),
                        "OPENBLAS_NUM_THREADS": str(profile.threads),
                        "NUMEXPR_NUM_THREADS": str(profile.threads),
                    },
                    "memory_limit": f"{profile.physical_resources.memory_gib}GB",
                    "local_directory": local_directory,
                    "silence_logs": logging.WARNING,
                    "plugins": (WorkerDevicePlugin(),),
                    "startup_information": {
                        "workflowDevice": worker_device_startup_information,
                    },
                    "death_timeout": worker_start_timeout,
                },
            }
    if gpu_cursor != len(gpu_ids):
        raise ValueError("Local GPU selection does not match the Worker Pool plan.")
    if not worker_specs:
        raise ValueError("At least one Worker Profile Pool must be configured.")
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
    worker_profile_slots: dict[str, float] = {}

    for worker_address, worker_info in dict(scheduler_info.get("workers", {})).items():
        resources = dict(worker_info.get("resources", {}) or {})
        cpu_slots = float(resources.get(CPU_RESOURCE_NAME, 0) or 0)
        gpu_slots = float(resources.get(GPU_RESOURCE_NAME, 0) or 0)
        total_cpu_slots += cpu_slots
        total_gpu_slots += gpu_slots
        # Profile Workers with a GPU also advertise their CPU allocation, but
        # they are GPU Workers rather than CPU-only Workers. Keep the role
        # counts disjoint while total_cpu_slots still reports all CPU capacity.
        if cpu_slots > 0 and gpu_slots <= 0:
            cpu_workers.append(str(worker_address))
        if gpu_slots > 0:
            gpu_workers.append(str(worker_address))
        for name, amount in resources.items():
            if (
                name in {CPU_RESOURCE_NAME, GPU_RESOURCE_NAME}
                or is_ownership_resource(name)
            ):
                continue
            worker_profile_slots[name] = worker_profile_slots.get(name, 0.0) + float(amount)

    return ClusterResourceSummary(
        scheduler_address=str(scheduler_info.get("address", "")),
        cpu_workers=tuple(sorted(cpu_workers)),
        gpu_workers=tuple(sorted(gpu_workers)),
        total_cpu_slots=total_cpu_slots,
        total_gpu_slots=total_gpu_slots,
        worker_profile_slots=dict(sorted(worker_profile_slots.items())),
    )


def validate_external_worker_ownership(
    scheduler_info: Mapping[str, Any],
    *,
    execution_id: str,
    submission_tokens: Sequence[str],
) -> None:
    """Validate Worker ownership from Scheduler-registered logical resources.

    Nanny-launched Worker subprocesses do not reliably expose the launcher's
    ambient environment through ``Client.run`` on every Distributed version.
    Logical resources are part of Worker registration itself, so they bind the
    exact Worker identity observed by the Scheduler without exposing raw
    submission tokens.
    """

    execution_resource = execution_ownership_resource(execution_id)
    token_resources = {
        submission_ownership_resource(token) for token in submission_tokens
    }
    if not token_resources:
        raise ValueError("At least one Slurm submission token is required.")

    errors: list[str] = []
    workers = dict(scheduler_info.get("workers", {}) or {})
    for address, worker_info in workers.items():
        resources = dict(worker_info.get("resources", {}) or {})
        if float(resources.get(execution_resource, 0) or 0) != 1:
            errors.append(f"{address} execution ownership mismatch")
        matched_tokens = [
            name
            for name in token_resources
            if float(resources.get(name, 0) or 0) == 1
        ]
        if len(matched_tokens) != 1:
            errors.append(f"{address} submission token mismatch")
    if errors:
        raise RuntimeError(
            "External Worker ownership validation failed: " + "; ".join(errors)
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
_dashboard_token_expiration_seconds = int(
    _positive_timeout_from_env(
        "WorkFlow_DASK_DASHBOARD_TOKEN_EXPIRATION_SECONDS",
        86_400.0,
    )
)
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
        # The on-demand Scheduler is replaced between workflow executions.
        # A browser tab can otherwise retain Bokeh's five-minute token and
        # repeatedly fail its WebSocket connection during a long run.
        "distributed.scheduler.dashboard.bokeh-application.session_token_expiration": (
            _dashboard_token_expiration_seconds
        ),
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
    active_worker_profile_topology: tuple[tuple[object, ...], ...] = ()
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

    def require_active_client(self) -> Client:
        """Return the service-owned Client for an already provisioned runtime."""
        with self._cluster_lock:
            if self.client is None:
                raise RuntimeError("The Dask runtime has no live Driver Client.")
            return self.client

    def start_slurm_jobqueue_scheduler(
        self,
        *,
        host: str,
        port: int,
        dashboard_address: str,
        template_job: Any,
        time_limit: str,
        shared_temp_directory: str,
        python_executable: str,
    ) -> Client:
        """Start one service-node Scheduler owned by ``SLURMCluster``.

        Worker specs are added separately after Resource Planner validation.
        Each spec remains a standard dask-jobqueue ``SLURMJob``.
        """

        from services.slurm_jobqueue_cluster import PlannedSLURMCluster

        if not isinstance(host, str) or not host.strip():
            raise ValueError("SLURMCluster Scheduler host must be non-empty.")
        if type(port) is not int or port < 0 or port > 65535:
            raise ValueError("SLURMCluster Scheduler port must be 0..65535.")
        with self._cluster_lock:
            if self.client is not None or self.cluster is not None:
                raise RuntimeError(
                    "A Dask runtime is already active; it must be stopped before "
                    "starting SLURMCluster."
                )
            security, _security_paths = _external_cluster_security_from_environment()
            cluster: PlannedSLURMCluster | None = None
            client: WorkflowClient | None = None
            try:
                cluster = PlannedSLURMCluster(
                    n_workers=0,
                    queue=template_job.partition,
                    cores=template_job.cpu,
                    memory=f"{template_job.memory_gib}GiB",
                    processes=template_job.processes,
                    nanny=True,
                    walltime=time_limit,
                    job_cpu=template_job.cpu,
                    job_mem=f"{template_job.memory_gib}G",
                    worker_extra_args=[],
                    python=python_executable,
                    shared_temp_directory=shared_temp_directory,
                    scheduler_options={
                        "host": host.strip(),
                        "port": port,
                        # Keep diagnostics on service-node loopback. Operators
                        # reach it through an explicit SSH local forward; CNs
                        # only need the separate Scheduler protocol address.
                        "dashboard": True,
                        "dashboard_address": dashboard_address,
                        "plugins": _scheduler_plugins(),
                    },
                    security=security,
                    asynchronous=False,
                    name="WorkFlow-Planned-SLURMCluster",
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
                            "[Dask] Failed to close SLURMCluster Driver Client."
                        )
                if cluster is not None:
                    try:
                        cluster.close(timeout=10.0)
                    except Exception:
                        logger.exception("[Dask] Failed to close SLURMCluster.")
                raise

            self.cluster = cluster
            self.client = client
            self._external_workers = True
            logger.info(
                "[Dask] Planner-aware SLURMCluster Scheduler ready: %s",
                cluster.scheduler_address,
            )
            return client

    def submit_slurm_jobqueue_workers(
        self,
        specs: Sequence[Any],
    ) -> tuple[Any, ...]:
        """Submit heterogeneous Worker specs through the active SLURMCluster."""

        with self._cluster_lock:
            cluster = self.cluster
            if not self._external_workers or cluster is None:
                raise RuntimeError("No active planner-aware SLURMCluster exists.")
        submit = getattr(cluster, "submit_planned_jobs", None)
        if not callable(submit):
            raise RuntimeError("The active Dask cluster is not a PlannedSLURMCluster.")
        return tuple(submit(specs))

    def stop_slurm_jobqueue_workers(self) -> None:
        """Ask SLURMCluster to cancel Worker jobs but keep Scheduler alive."""

        with self._cluster_lock:
            cluster = self.cluster
            if not self._external_workers or cluster is None:
                return
        stop = getattr(cluster, "stop_planned_jobs", None)
        if not callable(stop):
            raise RuntimeError("The active Dask cluster is not a PlannedSLURMCluster.")
        stop()

    def submitted_slurm_jobqueue_jobs(self) -> tuple[Any, ...]:
        """Return durable identities already accepted by Slurm."""

        with self._cluster_lock:
            cluster = self.cluster
            if not self._external_workers or cluster is None:
                return ()
        records = getattr(cluster, "submitted_job_records", None)
        if not callable(records):
            return ()
        return tuple(records())

    def activate_external_worker_profiles(
        self,
        *,
        expected_profiles: Mapping[str, int],
        timeout: float,
        execution_id: str | None = None,
        submission_token: str | None = None,
        submission_tokens: Sequence[str] | None = None,
    ) -> ClusterResourceSummary:
        """Wait for an exact set of logical Worker Profile capabilities."""

        normalized: dict[str, int] = {}
        for name, count in expected_profiles.items():
            if not isinstance(name, str) or not name:
                raise ValueError("Worker Profile names must be non-empty strings.")
            normalized[name] = _nonnegative_worker_count(
                count, name=f"expected_profiles[{name!r}]"
            )
        expected_total = sum(normalized.values())
        if expected_total <= 0:
            raise ValueError("At least one external Worker Profile must be requested.")
        if timeout <= 0:
            raise ValueError("External Worker startup timeout must be positive.")
        with self._cluster_lock:
            if not self._external_workers or self.client is None:
                raise RuntimeError("No external Slurm Dask runtime is active.")
            active_client = self.client

        deadline = time.monotonic() + timeout
        previous_addresses: tuple[str, ...] | None = None
        stable = 0
        summary: ClusterResourceSummary | None = None
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            active_client.wait_for_workers(expected_total, timeout=remaining)
            scheduler_info = get_fresh_scheduler_info(active_client, timeout=remaining)
            workers = dict(scheduler_info.get("workers", {}))
            observed = {name: 0 for name in normalized}
            unexpected: list[str] = []
            for address, worker_info in workers.items():
                resources = dict(worker_info.get("resources", {}) or {})
                matched = [name for name in normalized if float(resources.get(name, 0) or 0) >= 1]
                if len(matched) != 1:
                    unexpected.append(f"{address}: profiles={matched!r}")
                    continue
                observed[matched[0]] += 1
            addresses = tuple(sorted(str(address) for address in workers))
            if not unexpected and observed == normalized and len(workers) == expected_total:
                stable = stable + 1 if addresses == previous_addresses else 1
                previous_addresses = addresses
                summary = cluster_resource_summary_from_scheduler_info(scheduler_info)
                if stable >= 2:
                    break
            else:
                stable = 0
                previous_addresses = addresses
            time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
        if summary is None or stable < 2:
            raise TimeoutError(
                "Dask Workers did not register the exact planned Worker Profiles "
                f"before timeout: expected={normalized}."
            )

        if execution_id is not None:
            if submission_token is not None and submission_tokens is not None:
                raise ValueError(
                    "Provide submission_token or submission_tokens, not both."
                )
            token_values = (
                tuple(submission_tokens)
                if submission_tokens is not None
                else (() if submission_token is None else (submission_token,))
            )
            validate_external_worker_ownership(
                scheduler_info,
                execution_id=execution_id,
                submission_tokens=token_values,
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

    def ensure_profile_client(
        self,
        *,
        profiles: Sequence[WorkerProfile],
        pools: Sequence[WorkerPool],
        required_profiles: Mapping[str, int],
    ) -> Client:
        """Start/reuse a local cluster whose Workers advertise exact Profiles."""

        required = set(required_profiles)
        profile_by_name = {profile.name: profile for profile in profiles}
        pool_by_name = {pool.profile: pool for pool in pools}
        if len(profile_by_name) != len(profiles) or len(pool_by_name) != len(pools):
            raise ValueError("Worker Profiles and Pools must have unique names.")
        missing_profiles = sorted(required - set(profile_by_name))
        missing_pools = sorted(required - set(pool_by_name))
        if missing_profiles:
            raise ValueError("Configure Worker Profile(s): " + ", ".join(missing_profiles) + ".")
        if missing_pools:
            raise ValueError("Configure Worker Pool(s): " + ", ".join(missing_pools) + ".")
        selected_profiles = {name: profile_by_name[name] for name in sorted(required)}
        selected_pools = {name: pool_by_name[name] for name in sorted(required)}
        topology = tuple(
            (
                name,
                profile.physical_resources.cpu,
                profile.physical_resources.memory_gib,
                profile.physical_resources.gpu,
                profile.threads,
                tuple(sorted(profile.logical_resources.items())),
                selected_pools[name].processes,
                selected_pools[name].scale,
            )
            for name, profile in selected_profiles.items()
        )
        expected_counts = {
            name: selected_pools[name].worker_count for name in selected_profiles
        }

        _cleanup_failed_workflow_clients()
        with self._cluster_lock:
            if self._external_workers:
                if self.client is None:
                    raise RuntimeError("The external Dask runtime has no Driver Client.")
                return self.client
            if self.client is not None and self.active_worker_profile_topology == topology:
                summary = self.get_cluster_resource_summary(self.client)
                if all(
                    int(summary.worker_profile_slots.get(name, 0)) == count
                    for name, count in expected_counts.items()
                ):
                    return self.client
                self.stop_cluster()
            elif self.client is not None or self.cluster is not None:
                self.stop_cluster()

            gpu_count = sum(
                profile.physical_resources.gpu * selected_pools[name].worker_count
                for name, profile in selected_profiles.items()
            )
            if gpu_count:
                has_gpu, detected_gpu_count = _detect_cuda_for_cluster()
                if not has_gpu:
                    detected_gpu_count = 0
                gpu_ids = resolve_gpu_worker_ids(
                    detected_gpu_count=detected_gpu_count,
                    configured_gpu_ids=config.GPU_IDS,
                    requested_gpu_workers=gpu_count,
                    parent_cuda_visible_devices=os.environ.get("CUDA_VISIBLE_DEVICES"),
                )
            else:
                gpu_ids = ()

            total_workers = sum(expected_counts.values())
            startup_timeout = _cluster_start_timeout()
            deadline = time.monotonic() + startup_timeout
            batch_size = _worker_start_batch_size(total_workers)
            batch_timeout = _worker_batch_start_timeout()
            registration_timeout = _worker_registration_timeout()
            scheduler_spec, worker_specs = build_local_profile_cluster_specs(
                profiles=selected_profiles,
                pools=selected_pools,
                gpu_ids=gpu_ids,
                local_directory=_get_dask_local_dir(),
                dashboard_address=config.DASHBOARD_ADDRESS,
                worker_start_timeout=batch_timeout,
            )
            cluster: SpecCluster | None = None
            client: WorkflowClient | None = None
            try:
                cluster = SpecCluster(
                    workers={},
                    scheduler=scheduler_spec,
                    asynchronous=False,
                    silence_logs=logging.WARNING,
                    name="WorkFlow local Worker Profile cluster",
                )
                client = WorkflowClient(
                    cluster.scheduler.address,
                    security=cluster.security,
                    timeout=_remaining_startup_time(deadline, stage="connecting local Driver"),
                )
                _provision_worker_specs_in_batches(
                    cluster,
                    client,
                    worker_specs,
                    deadline=deadline,
                    batch_size=batch_size,
                    batch_timeout=batch_timeout,
                    registration_timeout=registration_timeout,
                )
                scheduler_info = get_fresh_scheduler_info(
                    client,
                    timeout=_remaining_startup_time(deadline, stage="validating Worker Profiles"),
                )
                workers = dict(scheduler_info.get("workers", {}))
                observed = {name: 0 for name in expected_counts}
                for worker_info in workers.values():
                    resources = dict(worker_info.get("resources", {}) or {})
                    matches = [name for name in observed if float(resources.get(name, 0) or 0) >= 1]
                    if len(matches) != 1:
                        raise RuntimeError(
                            f"Local Worker advertises invalid Profile capabilities: {matches!r}."
                        )
                    observed[matches[0]] += 1
                if observed != expected_counts or len(workers) != total_workers:
                    raise RuntimeError(
                        f"Local Worker Profile topology mismatch: expected={expected_counts}, "
                        f"observed={observed}."
                    )
            except BaseException:
                if client is not None:
                    client.close(timeout=5)
                if cluster is not None:
                    cluster.close(timeout=10)
                raise

            self.cluster = cluster
            self.client = client
            self.active_worker_profile_topology = topology
            return client

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
                # Preserve handles and fail closed. The next Profile-cluster
                # request retries cleanup before provisioning any new Worker.
                self.client = client
                self.cluster = cluster
            else:
                # Only discard handles after every child is confirmed stopped.
                self.client = None
                self.cluster = None
            self.active_worker_profile_topology = ()
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
