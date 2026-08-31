"""Service-node Driver controller for Graph-driven Slurm executions.

The long-lived FastAPI process starts one on-demand Dask Scheduler and runs the
workflow Driver locally.  Slurm allocations contain Dask Workers only and may
span multiple compute nodes.  The controller keeps the active execution lease
until the complete Worker allocation is proven terminal and the Scheduler has
closed.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time
from typing import Any, Mapping, Sequence
import uuid

from core.slurm_execution import (
    SlurmPolicy,
    SlurmResourceRequest,
    parse_scontrol_job_record,
    resolve_execution_directory,
    validate_execution_id,
)
from core.cluster_inventory import ClusterInventoryService
from core.resource_planner import (
    SlurmAllocationPlan,
    SlurmJobRequirement,
    plan_workflow_resources,
)
from core.worker_pool import parse_worker_pools
from core.worker_profiles import parse_worker_profiles
from core.state_manager import ExecutionStatus, state_manager
from core.window_execution import (
    ExecutionConfig,
    ExecutionLayout,
    parse_execution_config,
    require_window_recovery_location,
)
from core.workflow_resources import (
    WorkflowResourcePlan,
    build_workflow_resource_plan,
    ensure_executable_resource_plan,
)
from services.executor import (
    execute_graph as execute_graph_on_service_node,
    find_execution_roots,
    validate_graph_acyclic,
    validate_graph_structure,
    validate_graph_types,
)
from services.dask_service import dask_service
from services.slurm_jobqueue_cluster import build_planned_slurm_worker_spec
from services.recovery_service import (
    discover_terminal_outputs,
    inspect_recovery_directory,
)


logger = logging.getLogger("WorkFlow.SlurmControl")

REQUEST_SCHEMA_VERSION = 3
JOB_SCHEMA_VERSION = 3
RESULT_FILENAME = "result.json"
EVENTS_FILENAME = "events.jsonl"
CANCEL_MARKER_FILENAME = "cancel.requested"

_BACKENDS = frozenset({"local", "slurm"})
_TERMINAL_STATUSES = frozenset({
    ExecutionStatus.SUCCEEDED,
    ExecutionStatus.FAILED,
    ExecutionStatus.CANCELLED,
    ExecutionStatus.INTERRUPTED,
})
_SLURM_TERMINAL_STATES = frozenset({
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
})
_SLURM_SAFE_COMMAND_RE = re.compile(r"[^\x00\r\n]+\Z")
_SLURM_OPTION_ENV_PREFIXES = (
    "SBATCH_",
    "SCANCEL_",
    "SQUEUE_",
    "SACCT_",
    "SCONTROL_",
)
_SLURM_CLI_ENV_ALLOWLIST = frozenset({
    "HOME",
    "LANG",
    "LANGUAGE",
    "LOGNAME",
    "MUNGE_SOCKET",
    "PATH",
    "SLURM_CONF",
    "USER",
    "XDG_RUNTIME_DIR",
})


class SlurmSubmissionError(RuntimeError):
    """A formal execution could not be submitted to Slurm."""


class _SlurmEventStreamError(RuntimeError):
    """A complete event record failed structural or ownership validation."""


async def _harvest_background_task(
    task: asyncio.Task[Any],
) -> tuple[Any, bool]:
    """Finish a non-cancellable lifecycle transition before propagating Stop.

    ``asyncio.to_thread`` cannot interrupt the synchronous ``SLURMCluster``
    state correction running on its own IOLoop. Waiting for it prevents scale
    down from racing a still-submitting Worker job.
    """

    cancellation_requested = False
    while True:
        try:
            return await asyncio.shield(task), cancellation_requested
        except asyncio.CancelledError:
            if task.cancelled():
                raise RuntimeError("The SLURMCluster lifecycle task was cancelled.")
            cancellation_requested = True
            current = asyncio.current_task()
            if current is not None and hasattr(current, "uncancel"):
                current.uncancel()


def _slurm_cli_environment(
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build a minimal CLI environment without Slurm option overrides."""
    source = os.environ if environment is None else environment
    result: dict[str, str] = {}
    for name, value in source.items():
        if name == "SLURM_CLUSTERS" or name.startswith(_SLURM_OPTION_ENV_PREFIXES):
            continue
        if name in _SLURM_CLI_ENV_ALLOWLIST or name.startswith("LC_"):
            result[name] = str(value)
    return result


def execution_backend_name(environment: Mapping[str, str] | None = None) -> str:
    env = os.environ if environment is None else environment
    value = str(env.get("WorkFlow_EXECUTION_BACKEND", "local")).strip().lower()
    if value not in _BACKENDS:
        raise ValueError(
            "WorkFlow_EXECUTION_BACKEND must be 'local' or 'slurm', "
            f"got {value!r}."
        )
    return value


def uses_slurm_execution_backend(
    environment: Mapping[str, str] | None = None,
) -> bool:
    return execution_backend_name(environment) == "slurm"


def _env_positive_int(
    env: Mapping[str, str],
    name: str,
    default: int,
    *,
    allow_zero: bool = False,
) -> int:
    raw = env.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}.") from exc
    minimum = 0 if allow_zero else 1
    if value < minimum:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be {qualifier}, got {raw!r}.")
    return value


def _env_positive_float(
    env: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    raw = env.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number, got {raw!r}.") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number, got {raw!r}.")
    return value


def _absolute_directory(value: object, *, name: str, create: bool = False) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty absolute path.")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path: {path}")
    if path.is_symlink():
        raise ValueError(f"{name} must not be a symbolic link: {path}")
    if path == Path(path.anchor):
        raise ValueError(f"{name} must not be a filesystem root: {path}")
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.exists() or not path.is_dir():
        raise ValueError(f"{name} is not an existing directory: {path}")
    resolved = path.resolve(strict=True)
    if resolved == Path(resolved.anchor):
        raise ValueError(f"{name} must not resolve to a filesystem root: {path}")
    return resolved


def _safe_command(value: object, *, name: str, default: str) -> str:
    selected = default if value is None or not str(value).strip() else str(value).strip()
    if not _SLURM_SAFE_COMMAND_RE.fullmatch(selected):
        raise ValueError(f"{name} contains a forbidden control character.")
    return selected


def _scheduler_host(value: object) -> str:
    host = str(value or "").strip()
    if not host:
        raise ValueError(
            "WorkFlow_DASK_SCHEDULER_HOST is required for Slurm execution."
        )
    if host.lower() in {"localhost", "0.0.0.0", "::", "::1"} or host.startswith(
        "127."
    ):
        raise ValueError(
            "WorkFlow_DASK_SCHEDULER_HOST must be reachable from compute nodes."
        )
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", host) is None:
        raise ValueError("WorkFlow_DASK_SCHEDULER_HOST is not a safe host name.")
    return host


def _port_range(value: object, *, name: str) -> str:
    text = str(value or "").strip()
    match = re.fullmatch(r"([0-9]{1,5}):([0-9]{1,5})", text)
    if match is None:
        raise ValueError(f"{name} must have the form START:END.")
    start, stop = (int(item) for item in match.groups())
    if start < 1024 or stop > 65535 or start > stop:
        raise ValueError(f"{name} must be an unprivileged valid port range.")
    return f"{start}:{stop}"


def _loopback_dashboard_address(value: object) -> str:
    text = str(value or "127.0.0.1:8787").strip()
    match = re.fullmatch(r"127\.0\.0\.1:([0-9]{1,5})", text)
    if match is None:
        raise ValueError(
            "WorkFlow_DASK_DASHBOARD_ADDRESS must bind to "
            "127.0.0.1:PORT so it is only reachable through an SSH tunnel."
        )
    port = int(match.group(1))
    if port < 1024 or port > 65535:
        raise ValueError(
            "WorkFlow_DASK_DASHBOARD_ADDRESS must use an unprivileged valid port."
        )
    return f"127.0.0.1:{port}"


def _resolved_executable(
    value: object,
    *,
    name: str,
    default: str,
    search_path: str | None,
) -> str:
    """Resolve one Slurm command to an absolute executable path.

    The compute job starts with ``sbatch --export=NONE`` and cannot inherit the
    submit host's PATH.  Recovery ownership checks therefore need an explicit
    executable path that can be forwarded as a quoted script argument.
    """

    selected = _safe_command(value, name=name, default=default)
    candidate = Path(selected).expanduser()
    if not candidate.is_absolute():
        discovered = shutil.which(selected, path=search_path)
        if discovered is None:
            raise ValueError(f"{name} does not identify an executable: {selected!r}.")
        candidate = Path(discovered)
    try:
        resolved = candidate.resolve(strict=True)
        metadata = resolved.stat()
    except OSError as exc:
        raise ValueError(f"{name} does not identify an executable: {selected!r}.") from exc
    if not stat.S_ISREG(metadata.st_mode) or not os.access(resolved, os.X_OK):
        raise ValueError(f"{name} does not identify an executable: {selected!r}.")
    return str(resolved)


def _optional_resolved_executable(
    value: object,
    *,
    name: str,
    default: str,
    search_path: str | None,
) -> str | None:
    """Resolve an optional Slurm CLI without hiding invalid explicit paths."""

    explicit = value is not None and bool(str(value).strip())
    selected = str(value).strip() if explicit else default
    try:
        return _resolved_executable(
            selected,
            name=name,
            default=default,
            search_path=search_path,
        )
    except ValueError:
        if explicit:
            raise
        return None


def slurm_policy_from_environment(
    environment: Mapping[str, str] | None = None,
) -> SlurmPolicy:
    """Parse Graph-to-allocation limits without touching the filesystem."""

    env = os.environ if environment is None else environment
    # Empty means discover every eligible partition from sinfo.  The singular
    # setting remains a backwards-compatible operator override.
    partition = str(env.get("WorkFlow_SLURM_PARTITION", "")).strip()
    allowed_raw = str(env.get("WorkFlow_SLURM_ALLOWED_PARTITIONS", ""))
    allowed = tuple(item.strip() for item in allowed_raw.split(",") if item.strip())
    excluded_raw = str(env.get("WorkFlow_SLURM_EXCLUDED_PARTITIONS", "mn"))
    excluded = tuple(
        item.strip() for item in excluded_raw.split(",") if item.strip()
    )
    excluded_nodes_raw = str(env.get("WorkFlow_SLURM_EXCLUDED_NODES", ""))
    excluded_nodes = tuple(
        item.strip() for item in excluded_nodes_raw.split(",") if item.strip()
    )
    return SlurmPolicy(
        partition=partition,
        time_limit=str(env.get("WorkFlow_SLURM_TIME_LIMIT", "1-00:00:00")).strip(),
        max_cpus=_env_positive_int(env, "WorkFlow_SLURM_MAX_CPUS", 128),
        max_gpus=_env_positive_int(
            env, "WorkFlow_SLURM_MAX_GPUS", 8, allow_zero=True
        ),
        max_memory_gib=_env_positive_int(
            env, "WorkFlow_SLURM_MAX_MEMORY_GIB", 768
        ),
        max_nodes=_env_positive_int(env, "WorkFlow_SLURM_MAX_NODES", 8),
        cpus_per_node=_env_positive_int(env, "WorkFlow_SLURM_CPUS_PER_NODE", 64),
        gpus_per_node=_env_positive_int(
            env, "WorkFlow_SLURM_GPUS_PER_NODE", 8, allow_zero=True
        ),
        memory_gib_per_node=_env_positive_int(
            env, "WorkFlow_SLURM_MEMORY_GIB_PER_NODE", 512
        ),
        allowed_partitions=allowed,
        excluded_partitions=excluded,
        excluded_nodes=excluded_nodes,
    )


@dataclass(frozen=True)
class SlurmRuntimeConfig:
    runtime_directory: Path
    execution_root: Path
    project_root: Path
    policy: SlurmPolicy
    sbatch_executable: str
    squeue_executable: str
    sacct_executable: str | None
    sinfo_executable: str
    scontrol_executable: str
    scancel_executable: str
    poll_interval_seconds: float
    result_grace_seconds: float
    cancel_grace_seconds: float
    scheduler_host: str = "127.0.0.1"
    scheduler_port: int = 8786
    dashboard_address: str = "127.0.0.1:8787"
    worker_port_range: str = "20000:20999"
    nanny_port_range: str = "21000:21999"
    worker_start_timeout_seconds: float = 600.0
    queue_start_timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        worker = tuple(int(item) for item in self.worker_port_range.split(":"))
        nanny = tuple(int(item) for item in self.nanny_port_range.split(":"))
        if not (worker[1] < nanny[0] or nanny[1] < worker[0]):
            raise ValueError("Dask Worker and Nanny port ranges must not overlap.")
        dashboard_port = int(self.dashboard_address.rsplit(":", 1)[1])
        if dashboard_port == self.scheduler_port:
            raise ValueError("Dask Scheduler and Dashboard ports must be different.")
        for start, stop in (worker, nanny):
            if start <= dashboard_port <= stop:
                raise ValueError(
                    "Dask Dashboard port must not overlap Worker or Nanny ports."
                )

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "SlurmRuntimeConfig":
        env = os.environ if environment is None else environment
        runtime = _absolute_directory(
            env.get("WorkFlow_SLURM_RUNTIME_DIR"),
            name="WorkFlow_SLURM_RUNTIME_DIR",
            create=True,
        )
        execution_root = runtime / "jobs"
        if execution_root.is_symlink():
            raise ValueError(
                f"Slurm execution root must not be a symbolic link: {execution_root}"
            )
        execution_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        execution_root = execution_root.resolve(strict=True)

        project_root = Path(__file__).resolve().parents[2]
        if not (project_root / "backend" / "main.py").is_file():
            raise ValueError(
                "Slurm execution service is not inside a WorkFlow checkout."
            )

        policy = slurm_policy_from_environment(env)
        return cls(
            runtime_directory=runtime,
            execution_root=execution_root,
            project_root=project_root,
            policy=policy,
            sbatch_executable=_safe_command(
                env.get("WorkFlow_SLURM_SBATCH"),
                name="WorkFlow_SLURM_SBATCH",
                default="sbatch",
            ),
            squeue_executable=_resolved_executable(
                env.get("WorkFlow_SLURM_SQUEUE"),
                name="WorkFlow_SLURM_SQUEUE",
                default="squeue",
                search_path=env.get("PATH"),
            ),
            sacct_executable=(
                None
                if "WorkFlow_SLURM_SACCT" in env
                and not str(env.get("WorkFlow_SLURM_SACCT", "")).strip()
                else _optional_resolved_executable(
                    env.get("WorkFlow_SLURM_SACCT"),
                    name="WorkFlow_SLURM_SACCT",
                    default="sacct",
                    search_path=env.get("PATH"),
                )
            ),
            sinfo_executable=_resolved_executable(
                env.get("WorkFlow_SLURM_SINFO"),
                name="WorkFlow_SLURM_SINFO",
                default="sinfo",
                search_path=env.get("PATH"),
            ),
            scontrol_executable=_resolved_executable(
                env.get("WorkFlow_SLURM_SCONTROL"),
                name="WorkFlow_SLURM_SCONTROL",
                default="scontrol",
                search_path=env.get("PATH"),
            ),
            scancel_executable=_safe_command(
                env.get("WorkFlow_SLURM_SCANCEL"),
                name="WorkFlow_SLURM_SCANCEL",
                default="scancel",
            ),
            poll_interval_seconds=_env_positive_float(
                env, "WorkFlow_SLURM_POLL_SECONDS", 1.0
            ),
            result_grace_seconds=_env_positive_float(
                env, "WorkFlow_SLURM_RESULT_GRACE_SECONDS", 20.0
            ),
            cancel_grace_seconds=_env_positive_float(
                env, "WorkFlow_SLURM_CANCEL_GRACE_SECONDS", 120.0
            ),
            scheduler_host=_scheduler_host(
                env.get("WorkFlow_DASK_SCHEDULER_HOST")
            ),
            scheduler_port=_env_positive_int(
                env,
                "WorkFlow_DASK_SCHEDULER_PORT",
                8786,
            ),
            dashboard_address=_loopback_dashboard_address(
                env.get("WorkFlow_DASK_DASHBOARD_ADDRESS")
            ),
            worker_port_range=_port_range(
                env.get("WorkFlow_DASK_WORKER_PORT_RANGE", "20000:20999"),
                name="WorkFlow_DASK_WORKER_PORT_RANGE",
            ),
            nanny_port_range=_port_range(
                env.get("WorkFlow_DASK_NANNY_PORT_RANGE", "21000:21999"),
                name="WorkFlow_DASK_NANNY_PORT_RANGE",
            ),
            worker_start_timeout_seconds=_env_positive_float(
                env, "WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS", 600.0
            ),
            queue_start_timeout_seconds=_env_positive_float(
                env, "WorkFlow_SLURM_QUEUE_START_TIMEOUT_SECONDS", 300.0
            ),
        )


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise ValueError(f"Refusing to replace symbolic link: {path}")
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(payload),
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except BaseException:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _read_json_file(path: Path) -> dict[str, Any]:
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"Expected a regular non-symlink JSON file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}.")
    return value


def _resource_request_from_mapping(value: object) -> SlurmResourceRequest:
    if not isinstance(value, Mapping):
        raise ValueError("Slurm job resources must be a JSON object.")
    expected = {
        "nodes",
        "cpus",
        "gpus",
        "memoryGiB",
        "timeLimit",
        "partition",
        "nodeNames",
        "totalCpus",
        "totalGpus",
        "totalMemoryGiB",
    }
    fields = frozenset(value)
    if fields != frozenset(expected):
        raise ValueError("Slurm job resources have an invalid schema.")
    request = SlurmResourceRequest(
        nodes=value["nodes"],
        cpus=value["cpus"],
        gpus=value["gpus"],
        memory_gib=value["memoryGiB"],
        time_limit=value["timeLimit"],
        partition=value["partition"],
        node_names=tuple(value.get("nodeNames", ())),
    )
    if (
        value["totalCpus"] != request.total_cpus
        or value["totalGpus"] != request.total_gpus
        or value["totalMemoryGiB"] != request.total_memory_gib
    ):
        raise ValueError("Slurm job resource totals are inconsistent.")
    return request


def _git_revision(project_root: Path) -> str:
    try:
        result = subprocess.run(
            ("git", "-C", str(project_root), "rev-parse", "HEAD"),
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            shell=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SlurmSubmissionError(
            "Cannot identify the deployed Git revision for this execution."
        ) from exc
    value = result.stdout.strip()
    if result.returncode != 0 or re.fullmatch(r"[0-9a-fA-F]{40}", value) is None:
        raise SlurmSubmissionError(
            "Cannot identify the deployed Git revision for this execution."
        )
    try:
        status = subprocess.run(
            (
                "git",
                "-C",
                str(project_root),
                "status",
                "--porcelain",
                "--untracked-files=normal",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            shell=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SlurmSubmissionError(
            "Cannot verify that the deployed WorkFlow checkout is immutable."
        ) from exc
    if status.returncode != 0 or status.stdout.strip():
        raise SlurmSubmissionError(
            "Slurm execution requires a clean, committed WorkFlow checkout. "
            "Commit or remove local source changes before submitting a job."
        )
    return value.lower()


def _authoritative_graph_and_plan(
    graph: dict[str, Any],
    execution_config: ExecutionConfig,
) -> tuple[dict[str, Any], WorkflowResourcePlan]:
    selected_graph = graph
    if execution_config.mode == "window" and execution_config.resume_action in {
        "resume",
        "restart",
    }:
        location = execution_config.recovery_location
        if location is None:
            raise ValueError("Window recovery execution requires recoveryLocation.")
        if location.mode == "custom":
            recovery_directory = location.directory
        else:
            validate_graph_structure(graph)
            validate_graph_acyclic(graph)
            validate_graph_types(graph)
            roots = find_execution_roots(graph)
            outputs = discover_terminal_outputs(graph, roots)
            recovery_directory = str(
                ExecutionLayout.resolve(execution_config, outputs).control_directory
            )
        inspection = inspect_recovery_directory(str(recovery_directory))
        selected_graph = inspection.graph

    validate_graph_structure(selected_graph)
    validate_graph_acyclic(selected_graph)
    validate_graph_types(selected_graph)
    roots = find_execution_roots(selected_graph)
    if not roots:
        raise ValueError("No terminal execution root was found in the workflow graph.")
    plan = ensure_executable_resource_plan(
        build_workflow_resource_plan(selected_graph, roots)
    )
    return selected_graph, plan


def _plan_slurm_allocation(
    workflow_plan: WorkflowResourcePlan,
    *,
    worker_profiles: object,
    worker_pools: object,
    config: SlurmRuntimeConfig,
) -> SlurmAllocationPlan:
    if worker_profiles is None or worker_pools is None:
        raise ValueError(
            "Worker Profiles and Worker Pools are required for Slurm execution."
        )
    inventory = ClusterInventoryService(
        sinfo_executable=config.sinfo_executable,
        scontrol_executable=config.scontrol_executable
    ).load()
    partitions = config.policy.resolve_partitions(inventory.partition_names)
    return plan_workflow_resources(
        workflow_plan,
        parse_worker_profiles(worker_profiles),
        parse_worker_pools(worker_pools),
        inventory,
        time_limit=config.policy.time_limit,
        partitions=partitions,
        excluded_nodes=config.policy.excluded_nodes,
    )


def _allocation_holder_request(plan: SlurmAllocationPlan) -> SlurmResourceRequest:
    """Build an aggregate request used only for whole-plan policy validation."""

    if not plan.nodes:
        raise ValueError("Resource Planner returned no node allocations.")
    return SlurmResourceRequest(
        nodes=len(plan.nodes),
        cpus=max(node.cpu for node in plan.nodes),
        gpus=max(node.gpu for node in plan.nodes),
        memory_gib=max(node.memory_gib for node in plan.nodes),
        time_limit=plan.time_limit,
        partition=plan.jobs[0].partition,
        node_names=tuple(node.node for node in plan.nodes),
    )


def validate_allocation_plan_policy(
    plan: SlurmAllocationPlan,
    policy: SlurmPolicy,
) -> None:
    for partition in plan.partitions:
        if partition in policy.excluded_partitions or (
            policy.allowed_partitions
            and partition not in policy.allowed_partitions
        ):
            raise ValueError(f"Planned partition {partition!r} is not allowed.")
    checks = (
        (len(plan.nodes), policy.max_nodes, "nodes"),
        (plan.total_cpu, policy.max_cpus, "total cpus"),
        (plan.total_gpu, policy.max_gpus, "total gpus"),
        (plan.total_memory_gib, policy.max_memory_gib, "total memory GiB"),
    )
    for actual, maximum, label in checks:
        if actual > maximum:
            raise ValueError(f"Planned {label}={actual} exceeds site limit {maximum}.")
    for node in plan.nodes:
        node_checks = (
            (node.cpu, policy.cpus_per_node, "cpus"),
            (node.gpu, policy.gpus_per_node, "gpus"),
            (node.memory_gib, policy.memory_gib_per_node, "memory GiB"),
        )
        for actual, maximum, label in node_checks:
            if actual > maximum:
                raise ValueError(
                    f"Planned {label}={actual} on {node.node} exceeds "
                    f"site per-node limit {maximum}."
                )


def _worker_job_request(
    plan: SlurmAllocationPlan,
    job: SlurmJobRequirement,
) -> SlurmResourceRequest:
    return SlurmResourceRequest(
        nodes=1,
        cpus=job.cpu,
        gpus=job.gpu,
        memory_gib=job.memory_gib,
        time_limit=plan.time_limit,
        partition=job.partition,
        node_names=(job.node,),
    )


def _apply_external_event(execution_id: str, event: Mapping[str, Any]) -> None:
    event_type = event.get("type")
    if event_type == "progress":
        task_id = event.get("taskId")
        if isinstance(task_id, str):
            state_manager.update_node_status(
                task_id,
                str(event.get("message", "")),
                execution_id=execution_id,
                run_state=event.get("runState"),
                device=event.get("device"),
                progress=event.get("progress"),
                progress_type=event.get("progressType"),
                progress_role=event.get("progressRole"),
            )
    elif event_type == "window_progress":
        try:
            state_manager.update_window_progress(
                execution_id,
                current_window=int(event["currentWindow"]),
                completed_windows=int(event["completedWindows"]),
                total_windows=int(event["totalWindows"]),
                progress=float(event["progress"]),
                window_status=str(event.get("windowStatus", "")),
                message=str(event.get("message", "")),
            )
        except (KeyError, TypeError, ValueError):
            logger.warning("Ignoring malformed Window event: %r", event)
    elif event_type == "log":
        state_manager.add_log(
            str(event.get("message", "")),
            "info",
            execution_id=execution_id,
        )
    elif event_type == "execution_finished":
        status = str(event.get("status", ""))
        if status in _TERMINAL_STATUSES:
            state_manager.set_execution_status(
                execution_id,
                status,
                release_active=False,
            )


@dataclass
class _EventCursor:
    offset: int = 0
    last_sequence: int = 0
    reported_stream_error: str | None = None


class SlurmExecutionService:
    """Submit, monitor, and cancel one graph-derived Slurm execution."""

    def __init__(self) -> None:
        self._jobs: dict[str, str] = {}
        self._detach_requests: set[str] = set()
        self._lock = asyncio.Lock()

    def request_monitor_detach(self, execution_id: str) -> None:
        """Mark process shutdown so cancellation detaches instead of scancel."""
        self._detach_requests.add(execution_id)

    def _monitor_detach_requested(self, execution_id: str) -> bool:
        return execution_id in self._detach_requests

    @staticmethod
    def _run_command(
        argv: Sequence[str],
        *,
        timeout: float = 30.0,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            tuple(argv),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,
            env=_slurm_cli_environment(),
        )

    async def _relay_events(
        self,
        *,
        execution_id: str,
        job_id: str,
        event_path: Path,
        cursor: _EventCursor,
    ) -> int:
        try:
            metadata = event_path.lstat()
        except FileNotFoundError:
            return 0
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise _SlurmEventStreamError(
                f"Unsafe Slurm event stream: {event_path}"
            )

        delivered = 0
        with event_path.open("rb") as handle:
            handle.seek(cursor.offset)
            while True:
                start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                if not line.endswith(b"\n"):
                    handle.seek(start)
                    break
                next_offset = handle.tell()
                try:
                    event = json.loads(line.decode("utf-8"))
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise _SlurmEventStreamError(
                        f"Corrupt Slurm event stream at byte {start}: {exc}"
                    ) from exc
                if not isinstance(event, dict):
                    raise _SlurmEventStreamError(
                        "Slurm event stream contains a non-object record."
                    )
                sequence = event.get("sequence")
                if type(sequence) is not int or sequence <= cursor.last_sequence:
                    raise _SlurmEventStreamError(
                        "Slurm event sequence is not strictly increasing."
                    )
                if event.get("executionId") != execution_id:
                    raise _SlurmEventStreamError(
                        "Slurm event belongs to another execution."
                    )
                if event.get("jobId") != job_id:
                    raise _SlurmEventStreamError(
                        "Slurm event belongs to another Slurm job."
                    )
                # Apply and publish a complete record before advancing the
                # durable read cursor.  If a later line is corrupt, already
                # delivered records remain committed while the corrupt line is
                # retried instead of being silently skipped.
                _apply_external_event(execution_id, event)
                await state_manager.broadcast(execution_id, dict(event))
                cursor.offset = next_offset
                cursor.last_sequence = sequence
                delivered += 1

        return delivered

    async def _report_event_stream_error(
        self,
        *,
        execution_id: str,
        job_id: str,
        cursor: _EventCursor,
        error: _SlurmEventStreamError,
    ) -> None:
        """Report one corrupt record without advancing past or applying it."""
        identity = f"{type(error).__name__}: {error}"
        if cursor.reported_stream_error == identity:
            return
        message = (
            f"Slurm job {job_id} has an invalid event-stream record. The record "
            f"was quarantined and was not applied. {identity}"
        )
        logger.warning(message)
        state_manager.add_log(
            message,
            "warning",
            execution_id=execution_id,
        )
        await state_manager.broadcast(execution_id, {
            "type": "warning",
            "executionId": execution_id,
            "jobId": job_id,
            "message": message,
        })
        cursor.reported_stream_error = identity

    async def _query_queue_state(
        self,
        config: SlurmRuntimeConfig,
        job_id: str,
        cluster: str | None = None,
        submission_token: str | None = None,
    ) -> tuple[bool, tuple[str, str, str] | None]:
        """Return (query_succeeded, queue_row).

        An unavailable scheduler command is deliberately different from a
        successful empty result.  Treating both as "job disappeared" could
        release the UI execution lease while the compute job is still alive.
        """
        try:
            argv = [
                config.squeue_executable,
                "--local",
                "--noheader",
                f"--jobs={job_id}",
                "--format=%i|%k|%T|%N|%R",
            ]
            if cluster:
                # The submitted job belongs to the local controller. Do not
                # use -M here: old federation support can require slurmdbd.
                logger.debug("Ignoring sbatch cluster suffix for local squeue: %s", cluster)
            result = await asyncio.to_thread(
                self._run_command,
                tuple(argv),
                timeout=10.0,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            logger.warning("squeue unavailable for job %s: %s", job_id, exc)
            return False, None
        if result.returncode != 0:
            logger.warning("squeue failed for job %s: %s", job_id, result.stderr.strip())
            return False, None
        lines = [item.strip() for item in result.stdout.splitlines() if item.strip()]
        if not lines:
            return True, None
        if len(lines) != 1:
            logger.warning("squeue returned ambiguous rows for root job %s", job_id)
            return False, None
        fields = lines[0].split("|", 4)
        if len(fields) != 5 or fields[0].strip() != job_id:
            logger.warning("squeue returned a mismatched row for root job %s", job_id)
            return False, None
        if submission_token is not None and fields[1].strip() != submission_token:
            logger.error(
                "Slurm job ID %s belongs to another submission token; refusing "
                "to monitor or control it.",
                job_id,
            )
            return False, None
        return True, (
            fields[2].strip().upper(),
            fields[3].strip(),
            fields[4].strip(),
        )

    async def _query_accounting_state(
        self,
        config: SlurmRuntimeConfig,
        job_id: str,
        cluster: str | None = None,
        submission_token: str | None = None,
    ) -> tuple[bool, tuple[str, str, str] | None]:
        if config.sacct_executable is None:
            return False, None
        try:
            argv = [
                config.sacct_executable,
                "-n",
                "-P",
                "-j",
                job_id,
                "--format=JobIDRaw,State,ExitCode,NodeList,Comment",
            ]
            result = await asyncio.to_thread(
                self._run_command,
                tuple(argv),
                timeout=10.0,
            )
        except (OSError, subprocess.SubprocessError):
            return False, None
        if result.returncode != 0:
            return False, None
        root_states: list[tuple[str, str, str]] = []
        for raw_line in result.stdout.splitlines():
            fields = [field.strip() for field in raw_line.split("|")]
            if len(fields) < 2 or fields[0] != job_id or not fields[1]:
                continue
            if submission_token is not None and (
                len(fields) < 5 or fields[4] != submission_token
            ):
                continue
            state = fields[1].split(None, 1)[0].split("+", 1)[0].upper()
            fields.extend([""] * (4 - len(fields)))
            root_states.append((state, fields[2], fields[3]))
        if len(root_states) == 1 and root_states[0][0] in _SLURM_TERMINAL_STATES:
            return True, root_states[0]
        if root_states:
            return True, None
        return False, None

    async def _query_scontrol_state(
        self,
        config: SlurmRuntimeConfig,
        job_id: str,
        cluster: str | None = None,
        submission_token: str | None = None,
    ) -> tuple[bool, tuple[str, str, str] | None]:
        """Return ``(record_found, terminal_state)`` from the controller.

        ``scontrol`` is available on old installations that have no working
        slurmdbd.  A failed query, missing/purged record, malformed output, or
        non-terminal state remains unknown so the execution lease is retained.
        """

        argv = [
            config.scontrol_executable,
            "--local",
            "--oneliner",
            "--quiet",
            "show",
            "job",
            job_id,
        ]
        try:
            result = await asyncio.to_thread(
                self._run_command,
                tuple(argv),
                timeout=10.0,
            )
        except (OSError, subprocess.SubprocessError):
            return False, None
        if result.returncode != 0:
            return False, None
        try:
            record = parse_scontrol_job_record(
                result.stdout,
                expected_job_id=job_id,
            )
        except (TypeError, ValueError):
            return False, None
        if record is None:
            return False, None
        if submission_token is not None and record.comment != submission_token:
            return False, None
        if record.state not in _SLURM_TERMINAL_STATES:
            return True, None
        return True, (record.state, record.exit_code, record.node_list)

    async def _query_terminal_state(
        self,
        config: SlurmRuntimeConfig,
        job_id: str,
        cluster: str | None = None,
        submission_token: str | None = None,
    ) -> tuple[bool, tuple[str, str, str] | None]:
        """Return whether an authoritative record exists and its terminal state.

        Query the controller first: it works without slurmdbd and avoids a
        noisy refused accounting connection on legacy installations.
        """

        control_found, control_state = await self._query_scontrol_state(
            config, job_id, cluster, submission_token
        )
        if control_found:
            return control_found, control_state
        return await self._query_accounting_state(
            config, job_id, cluster, submission_token
        )

    async def _query_job_by_submission_token(
        self,
        config: SlurmRuntimeConfig,
        submission_token: str,
    ) -> tuple[bool, tuple[str, str] | None]:
        """Find the locally submitted job identified by an exact Slurm comment."""
        try:
            result = await asyncio.to_thread(
                self._run_command,
                (
                    config.squeue_executable,
                    "--local",
                    "--noheader",
                    "--format=%i|%k|%T",
                ),
                timeout=10.0,
            )
        except (OSError, subprocess.SubprocessError):
            return False, None
        if result.returncode != 0:
            return False, None
        matches: list[tuple[str, str]] = []
        for raw_line in result.stdout.splitlines():
            fields = [field.strip() for field in raw_line.split("|", 2)]
            if len(fields) != 3 or fields[1] != submission_token:
                continue
            if re.fullmatch(r"[1-9][0-9]*", fields[0]) is None:
                raise SlurmSubmissionError(
                    "Slurm returned an invalid job ID for a submission token."
                )
            matches.append((fields[0], fields[2].upper()))
        if len(matches) > 1:
            raise SlurmSubmissionError(
                "Multiple Slurm jobs have the same WorkFlow submission token."
            )
        return True, matches[0] if matches else None

    async def _recover_ambiguous_submission(
        self,
        *,
        config: SlurmRuntimeConfig,
        execution_id: str,
        submission_token: str,
    ) -> str:
        """Fail closed until sbatch's durable job identity can be recovered."""
        announced = False
        while True:
            _query_succeeded, match = await self._query_job_by_submission_token(
                config,
                submission_token,
            )
            if match is not None:
                return match[0]
            if not announced:
                announced = True
                message = (
                    "The sbatch result is ambiguous. This execution remains active "
                    "and blocks new runs until its Slurm job ID can be recovered "
                    f"from submission token {submission_token}."
                )
                state_manager.add_log(message, "error", execution_id=execution_id)
                await state_manager.broadcast(execution_id, {
                    "type": "error",
                    "executionId": execution_id,
                    "message": message,
                })
            try:
                await asyncio.sleep(config.poll_interval_seconds)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and hasattr(current, "uncancel"):
                    current.uncancel()
                # A Stop request cannot safely release an execution whose job
                # identity is unknown. Continue recovery, then cancel by ID.
                continue

    async def _broadcast_job_state(
        self,
        execution_id: str,
        job_id: str,
        resource_request: SlurmResourceRequest,
        state: str,
        node: str = "",
        reason: str = "",
    ) -> None:
        location = f" on {node}" if node else ""
        detail = f" ({reason})" if reason and reason != node else ""
        message = f"Slurm job {job_id}: {state}{location}{detail}."
        payload = {
            "type": "slurm_job_state",
            "executionId": execution_id,
            "jobId": job_id,
            "state": state,
            "node": node or None,
            "reason": reason or None,
            "resources": resource_request.to_dict(),
            "message": message,
        }
        state_manager.add_log(message, "info", execution_id=execution_id)
        await state_manager.broadcast(execution_id, payload)

    async def _write_job_record_until_success(
        self,
        *,
        path: Path,
        payload: Mapping[str, Any],
        execution_id: str,
        poll_interval_seconds: float,
    ) -> None:
        last_error: str | None = None
        while True:
            try:
                _atomic_write_json(path, payload)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                if error != last_error:
                    last_error = error
                    state_manager.add_log(
                        "Slurm accepted the job, but its job ID could not yet be "
                        f"persisted. The execution lease remains active. {error}",
                        "error",
                        execution_id=execution_id,
                    )
                await asyncio.sleep(poll_interval_seconds)

    async def _monitor_job(
        self,
        *,
        config: SlurmRuntimeConfig,
        execution_id: str,
        job_id: str,
        run_directory: Path,
        resource_request: SlurmResourceRequest,
        cluster: str | None = None,
        submission_token: str | None = None,
        cursor: _EventCursor | None = None,
    ) -> dict[str, Any]:
        event_path = run_directory / EVENTS_FILENAME
        result_path = run_directory / RESULT_FILENAME
        if cursor is None:
            cursor = _EventCursor()
        last_queue_state: tuple[str, str, str] | None = None
        absent_since: float | None = None
        absent_confirmations = 0

        def load_terminal_result() -> dict[str, Any] | None:
            if not result_path.exists():
                return None
            result = _read_json_file(result_path)
            if result.get("schemaVersion") not in {1, REQUEST_SCHEMA_VERSION}:
                raise RuntimeError("Slurm result has an unsupported schema version.")
            if result.get("executionId") != execution_id:
                raise RuntimeError("Slurm result belongs to another execution.")
            if result.get("jobId") != job_id:
                raise RuntimeError("Slurm result belongs to another Slurm job.")
            if result.get("status") not in _TERMINAL_STATUSES:
                raise RuntimeError("Slurm result has an invalid terminal status.")
            return result

        while True:
            terminal_result = load_terminal_result()

            try:
                await self._relay_events(
                    execution_id=execution_id,
                    job_id=job_id,
                    event_path=event_path,
                    cursor=cursor,
                )
            except _SlurmEventStreamError as exc:
                if terminal_result is None:
                    raise
                # The runner closes and fsyncs the event stream before it
                # atomically publishes result.json.  A strictly validated
                # terminal result is therefore authoritative even if a
                # complete event record is corrupt.  The bad record remains
                # unapplied and the cursor is deliberately not advanced.
                await self._report_event_stream_error(
                    execution_id=execution_id,
                    job_id=job_id,
                    cursor=cursor,
                    error=exc,
                )
                return terminal_result

            if terminal_result is not None:
                return terminal_result

            queue_query_succeeded, queue_state = await self._query_queue_state(
                config,
                job_id,
                cluster,
                submission_token,
            )
            if not queue_query_succeeded:
                # Preserve the active lease and keep waiting for the durable
                # runner result.  A transient Slurm control-plane outage must
                # never be mistaken for a terminal compute job.
                absent_since = None
                absent_confirmations = 0
            elif queue_state is not None:
                absent_since = None
                absent_confirmations = 0
                if queue_state != last_queue_state:
                    last_queue_state = queue_state
                    await self._broadcast_job_state(
                        execution_id,
                        job_id,
                        resource_request,
                        *queue_state,
                    )
            else:
                absent_confirmations += 1
                if absent_since is None:
                    absent_since = time.monotonic()
                if (
                    absent_confirmations >= 2
                    and time.monotonic() - absent_since
                    >= config.result_grace_seconds
                ):
                    history_found, terminal_state = await self._query_terminal_state(
                        config,
                        job_id,
                        cluster,
                        submission_token,
                    )
                    if terminal_state is None:
                        if history_found:
                            # An exact non-terminal controller record
                            # contradicts squeue absence. Keep the lease and
                            # require later scheduler agreement or a result.
                            absent_since = None
                            absent_confirmations = 0
                            await asyncio.sleep(config.poll_interval_seconds)
                            continue
                        # The successful exact squeue query has continuously
                        # reported no root allocation for the full grace
                        # period. Old controllers can purge scontrol history
                        # and may have no slurmdbd at all. Give a concurrently
                        # published runner result one final chance, then fail
                        # closed as a lost runner result rather than keeping the
                        # UI lease forever. Scheduler *query failures* never
                        # enter this branch because they reset absent_since.
                        final_runner_result = load_terminal_result()
                        if final_runner_result is not None:
                            return final_runner_result
                        return {
                            "schemaVersion": 1,
                            "executionId": execution_id,
                            "jobId": job_id,
                            "status": ExecutionStatus.FAILED,
                            "message": (
                                "Slurm job disappeared from squeue without a "
                                "durable runner result, and no terminal root-job "
                                "record remains in sacct or scontrol."
                            ),
                            "finishedAt": _utc_timestamp(),
                            "runnerHost": None,
                            "exitCode": None,
                        }
                    state, exit_code, node = terminal_state
                    final_runner_result = load_terminal_result()
                    if final_runner_result is not None:
                        return final_runner_result
                    status = (
                        ExecutionStatus.CANCELLED
                        if state == "CANCELLED"
                        else ExecutionStatus.FAILED
                    )
                    return {
                        "schemaVersion": 1,
                        "executionId": execution_id,
                        "jobId": job_id,
                        "status": status,
                        "message": (
                            "Slurm job ended without a runner result: "
                            f"state={state}, exitCode={exit_code or 'unknown'}, "
                            f"node={node or 'unknown'}"
                        ),
                        "finishedAt": _utc_timestamp(),
                        "runnerHost": node or None,
                        "exitCode": exit_code or None,
                    }

            await asyncio.sleep(config.poll_interval_seconds)

    async def _monitor_until_terminal(
        self,
        *,
        config: SlurmRuntimeConfig,
        execution_id: str,
        job_id: str,
        run_directory: Path,
        resource_request: SlurmResourceRequest,
        cluster: str | None = None,
        submission_token: str | None = None,
    ) -> dict[str, Any]:
        """Retry control-plane monitoring without releasing a live job lease."""
        last_error: str | None = None
        cursor = _EventCursor()
        while True:
            try:
                return await self._monitor_job(
                    config=config,
                    execution_id=execution_id,
                    job_id=job_id,
                    run_directory=run_directory,
                    resource_request=resource_request,
                    cluster=cluster,
                    submission_token=submission_token,
                    cursor=cursor,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                if isinstance(exc, _SlurmEventStreamError):
                    await self._report_event_stream_error(
                        execution_id=execution_id,
                        job_id=job_id,
                        cursor=cursor,
                        error=exc,
                    )
                elif error != last_error:
                    last_error = error
                    message = (
                        f"Slurm job {job_id} monitoring is temporarily unavailable; "
                        f"the compute lease remains active. {error}"
                    )
                    logger.exception(message)
                    state_manager.add_log(
                        message,
                        "warning",
                        execution_id=execution_id,
                    )
                    await state_manager.broadcast(execution_id, {
                        "type": "warning",
                        "executionId": execution_id,
                        "jobId": job_id,
                        "message": message,
                    })
                await asyncio.sleep(config.poll_interval_seconds)

    async def _send_cancel(
        self,
        config: SlurmRuntimeConfig,
        job_id: str,
        *,
        whole_job: bool,
        cluster: str | None = None,
        submission_token: str | None = None,
    ) -> None:
        query_succeeded, queue_state = await self._query_queue_state(
            config,
            job_id,
            cluster,
            submission_token,
        )
        if not query_succeeded:
            raise SlurmSubmissionError(
                f"Cannot prove ownership of Slurm job {job_id}; refusing scancel."
            )
        if queue_state is None:
            return
        argv = [config.scancel_executable]
        if not whole_job:
            argv.extend(("--batch", "--signal=TERM"))
        argv.append(job_id)
        result = await asyncio.to_thread(
            self._run_command,
            tuple(argv),
            timeout=15.0,
        )
        if result.returncode != 0:
            raise SlurmSubmissionError(
                "scancel failed for job "
                f"{job_id}: {result.stderr.strip() or 'unknown error'}"
            )

    async def _cancel_and_wait_for_terminal(
        self,
        *,
        config: SlurmRuntimeConfig,
        execution_id: str,
        job_id: str,
        cluster: str | None,
        submission_token: str,
        run_directory: Path,
        resource_request: SlurmResourceRequest,
    ) -> dict[str, Any]:
        """Request cancellation but retain the lease until terminal proof."""
        try:
            _atomic_write_json(run_directory / CANCEL_MARKER_FILENAME, {
                "schemaVersion": 1,
                "executionId": execution_id,
                "jobId": job_id,
                "requestedAt": _utc_timestamp(),
            })
        except Exception as exc:
            # TERM without a valid marker becomes an interruption rather than
            # a user-cancelled result, but it is still safer than leaving a
            # remote writer unmonitored.
            state_manager.add_log(
                f"Could not persist the Slurm cancellation marker: {exc}",
                "warning",
                execution_id=execution_id,
            )

        query_succeeded, queue_state = await self._query_queue_state(
            config,
            job_id,
            cluster,
            submission_token,
        )
        pending = bool(
            query_succeeded
            and queue_state is not None
            and queue_state[0] in {"PENDING", "CONFIGURING"}
        )
        try:
            await self._send_cancel(
                config,
                job_id,
                whole_job=pending,
                cluster=cluster,
                submission_token=submission_token,
            )
        except Exception as exc:
            state_manager.add_log(
                f"Initial scancel request for job {job_id} failed; "
                f"the active lease is retained and cancellation will retry. {exc}",
                "warning",
                execution_id=execution_id,
            )

        monitor_task = asyncio.create_task(self._monitor_until_terminal(
            config=config,
            execution_id=execution_id,
            job_id=job_id,
            cluster=cluster,
            submission_token=submission_token,
            run_directory=run_directory,
            resource_request=resource_request,
        ))
        try:
            try:
                return await asyncio.wait_for(
                    asyncio.shield(monitor_task),
                    timeout=config.cancel_grace_seconds,
                )
            except asyncio.TimeoutError:
                pass

            retry_interval = max(1.0, config.poll_interval_seconds * 5.0)
            while True:
                try:
                    await self._send_cancel(
                        config,
                        job_id,
                        whole_job=True,
                        cluster=cluster,
                        submission_token=submission_token,
                    )
                except Exception as exc:
                    state_manager.add_log(
                        f"Forced scancel for job {job_id} failed; the execution "
                        f"remains CANCELLING and blocks new runs. {exc}",
                        "error",
                        execution_id=execution_id,
                    )
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(monitor_task),
                        timeout=retry_interval,
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            if not monitor_task.done():
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)

    async def _await_cancellation_confirmation(
        self,
        task: asyncio.Task[dict[str, Any]],
        *,
        execution_id: str,
    ) -> dict[str, Any]:
        """Ignore repeated Stop clicks, but permit explicit process detach."""
        while True:
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                if self._monitor_detach_requested(execution_id):
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise
                if task.cancelled():
                    raise
                current = asyncio.current_task()
                if current is not None and hasattr(current, "uncancel"):
                    current.uncancel()

    async def _finalize_result(
        self,
        execution_id: str,
        result: Mapping[str, Any],
        *,
        job_id: str | None = None,
    ) -> None:
        if result.get("executionId") != execution_id:
            raise RuntimeError("Slurm result belongs to another execution.")
        if job_id is not None and result.get("jobId") != job_id:
            raise RuntimeError("Slurm result belongs to another Slurm job.")
        status = str(result.get("status", ""))
        if status not in _TERMINAL_STATUSES:
            raise RuntimeError(f"Slurm runner returned invalid status {status!r}.")
        session = state_manager.get_execution(execution_id)
        if session is not None and not ExecutionStatus.is_finished(session.status):
            state_manager.set_execution_status(
                execution_id,
                status,
                release_active=False,
            )
            payload = {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": status,
                "message": str(result.get("message", "Slurm execution finished")),
            }
            await state_manager.broadcast(execution_id, payload)
        state_manager.add_log(
            str(result.get("message", "Slurm execution finished")),
            (
                "success"
                if status == ExecutionStatus.SUCCEEDED
                else "warning"
                if status in {ExecutionStatus.CANCELLED, ExecutionStatus.INTERRUPTED}
                else "error"
            ),
            execution_id=execution_id,
        )

    @staticmethod
    def _write_terminal_job_record(
        *,
        job_path: Path,
        execution_id: str,
        job_id: str,
        cluster: str | None,
        resource_request: SlurmResourceRequest,
        submitted_at: object,
        result: Mapping[str, Any],
    ) -> None:
        _atomic_write_json(job_path, {
            "schemaVersion": JOB_SCHEMA_VERSION,
            "executionId": execution_id,
            "jobId": job_id,
            "cluster": cluster,
            "state": str(result["status"]),
            "resources": resource_request.to_dict(),
            "submittedAt": submitted_at,
            "finishedAt": result.get("finishedAt"),
        })

    @staticmethod
    def _write_rejected_submission_record(
        *,
        job_path: Path,
        execution_id: str,
        submission_token: str,
        resource_request: SlurmResourceRequest,
        submitted_at: object,
        message: str,
    ) -> None:
        """Durably close a submission that Slurm provably did not accept."""
        _atomic_write_json(job_path, {
            "schemaVersion": JOB_SCHEMA_VERSION,
            "executionId": execution_id,
            "jobId": None,
            "state": ExecutionStatus.FAILED,
            "submissionToken": submission_token,
            "resources": resource_request.to_dict(),
            "submittedAt": submitted_at,
            "finishedAt": _utc_timestamp(),
            "message": message,
        })

    async def _resume_existing_job(
        self,
        *,
        config: SlurmRuntimeConfig,
        execution_id: str,
        job_id: str,
        cluster: str | None,
        submission_token: str,
        submitted_at: object,
        run_directory: Path,
        job_path: Path,
        resource_request: SlurmResourceRequest,
    ) -> None:
        try:
            result = await self._monitor_until_terminal(
                config=config,
                execution_id=execution_id,
                job_id=job_id,
                cluster=cluster,
                submission_token=submission_token,
                run_directory=run_directory,
                resource_request=resource_request,
            )
            await self._finalize_result(execution_id, result, job_id=job_id)
            self._write_terminal_job_record(
                job_path=job_path,
                execution_id=execution_id,
                job_id=job_id,
                cluster=cluster,
                resource_request=resource_request,
                submitted_at=submitted_at,
                result=result,
            )
        except asyncio.CancelledError:
            if self._monitor_detach_requested(execution_id):
                return
            session = state_manager.get_execution(execution_id)
            explicit_cancel = bool(
                session is not None
                and session.status == ExecutionStatus.CANCELLING
            )
            if explicit_cancel:
                cancellation_task = asyncio.create_task(
                    self._cancel_and_wait_for_terminal(
                        config=config,
                        execution_id=execution_id,
                        job_id=job_id,
                        cluster=cluster,
                        submission_token=submission_token,
                        run_directory=run_directory,
                        resource_request=resource_request,
                    )
                )
                try:
                    result = await self._await_cancellation_confirmation(
                        cancellation_task,
                        execution_id=execution_id,
                    )
                except asyncio.CancelledError:
                    if self._monitor_detach_requested(execution_id):
                        return
                    raise
                await self._finalize_result(execution_id, result, job_id=job_id)
                self._write_terminal_job_record(
                    job_path=job_path,
                    execution_id=execution_id,
                    job_id=job_id,
                    cluster=cluster,
                    resource_request=resource_request,
                    submitted_at=submitted_at,
                    result=result,
                )
            raise
        finally:
            self._detach_requests.discard(execution_id)
            self._jobs.pop(execution_id, None)
            state_manager.clear_active_execution(execution_id)
            state_manager.cleanup_old_executions()

    async def reconcile_active_job(self) -> str | None:
        """Reattach the control plane to one durable non-terminal Slurm job.

        The application intentionally permits only one active execution.  A
        second non-terminal record is therefore treated as an integrity error
        rather than choosing an arbitrary job to control.
        """
        config = SlurmRuntimeConfig.from_environment()
        candidates: list[
            tuple[
                str,
                str,
                str | None,
                str,
                object,
                Path,
                Path,
                SlurmResourceRequest,
            ]
        ] = []
        for run_directory in sorted(config.execution_root.iterdir()):
            if run_directory.is_symlink() or not run_directory.is_dir():
                continue
            job_path = run_directory / "job.json"
            if not job_path.exists():
                continue
            record = _read_json_file(job_path)
            if record.get("schemaVersion") != JOB_SCHEMA_VERSION:
                raise SlurmSubmissionError(f"Invalid Slurm job record: {job_path}")
            execution_id = validate_execution_id(record.get("executionId"))
            if execution_id != run_directory.name:
                raise SlurmSubmissionError(
                    f"Slurm job record directory mismatch: {job_path}"
                )
            state = str(record.get("state", ""))
            if state in _TERMINAL_STATUSES:
                continue
            job_id_value = record.get("jobId")
            if job_id_value is None and state == "submitting":
                result_path = run_directory / RESULT_FILENAME
                recovered_job_id: str | None = None
                if result_path.exists():
                    recovered_result = _read_json_file(result_path)
                    candidate_id = str(recovered_result.get("jobId") or "")
                    if re.fullmatch(r"[1-9][0-9]*", candidate_id):
                        recovered_job_id = candidate_id
                if recovered_job_id is None:
                    submission_token = record.get("submissionToken")
                    if not isinstance(submission_token, str) or not submission_token:
                        raise SlurmSubmissionError(
                            "Ambiguous Slurm submission has no recovery token; "
                            f"refusing to start the control plane: {job_path}"
                        )
                    query_succeeded, token_match = (
                        await self._query_job_by_submission_token(
                            config,
                            submission_token,
                        )
                    )
                    if token_match is not None:
                        recovered_job_id = token_match[0]
                    else:
                        query_detail = (
                            "the scheduler returned no matching active job"
                            if query_succeeded
                            else "the scheduler query was unavailable"
                        )
                        raise SlurmSubmissionError(
                            "Ambiguous Slurm submission cannot be proven absent "
                            f"({query_detail}); refusing to accept new executions. "
                            f"Record: {job_path}"
                        )
                job_id_value = recovered_job_id
                record = dict(record)
                record["jobId"] = recovered_job_id
                record["state"] = "submitted"
                _atomic_write_json(job_path, record)
            job_id = str(job_id_value or "")
            if re.fullmatch(r"[1-9][0-9]*", job_id) is None:
                raise SlurmSubmissionError(f"Invalid Slurm job ID in {job_path}")
            cluster_value = record.get("cluster")
            if cluster_value is not None and not isinstance(cluster_value, str):
                raise SlurmSubmissionError(f"Invalid Slurm cluster in {job_path}")
            submission_token = record.get("submissionToken")
            if not isinstance(submission_token, str) or not submission_token:
                raise SlurmSubmissionError(
                    f"Non-terminal Slurm job has no ownership token: {job_path}"
                )
            resource_request = _resource_request_from_mapping(
                record.get("resources")
            )
            candidates.append((
                execution_id,
                job_id,
                cluster_value,
                submission_token,
                record.get("submittedAt"),
                run_directory,
                job_path,
                resource_request,
            ))

        if not candidates:
            return None
        if len(candidates) > 1:
            ids = ", ".join(item[0] for item in candidates)
            raise SlurmSubmissionError(
                "Multiple non-terminal Slurm job records require operator "
                f"reconciliation before startup: {ids}"
            )

        (
            execution_id,
            job_id,
            cluster,
            submission_token,
            submitted_at,
            run_directory,
            job_path,
            resource_request,
        ) = candidates[0]
        state_manager.start_execution(execution_id)
        task = asyncio.create_task(self._resume_existing_job(
            config=config,
            execution_id=execution_id,
            job_id=job_id,
            cluster=cluster,
            submission_token=submission_token,
            submitted_at=submitted_at,
            run_directory=run_directory,
            job_path=job_path,
            resource_request=resource_request,
        ))
        if not state_manager.attach_execution_task(execution_id, task):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            state_manager.clear_active_execution(execution_id)
            raise SlurmSubmissionError(
                f"Cannot attach the reconciled Slurm monitor for {execution_id}."
            )
        self._jobs[execution_id] = job_id
        message = f"Reattached to Slurm job {job_id} after control-plane restart."
        state_manager.add_log(message, "info", execution_id=execution_id)
        return execution_id

    # ------------------------------------------------------------------
    # Service-node Driver / on-demand Scheduler architecture (schema v3)
    # ------------------------------------------------------------------
    @staticmethod
    def _worker_security_payload(config: SlurmRuntimeConfig) -> dict[str, str] | None:
        values = {
            "tlsCaFile": os.getenv("WorkFlow_DASK_TLS_CA", "").strip(),
            "tlsCertFile": os.getenv("WorkFlow_DASK_TLS_CERT", "").strip(),
            "tlsKeyFile": os.getenv("WorkFlow_DASK_TLS_KEY", "").strip(),
        }
        provided = tuple(bool(value) for value in values.values())
        if any(provided) and not all(provided):
            raise ValueError(
                "WorkFlow_DASK_TLS_CA, CERT and KEY must be configured together."
            )
        if not any(provided):
            if os.getenv("WorkFlow_DASK_ALLOW_INSECURE_CLUSTER", "").strip().lower() \
                    not in {"1", "true", "yes", "on"}:
                raise ValueError(
                    "Cross-node Dask requires mTLS, or an explicit insecure "
                    "opt-in on a trusted isolated test network."
                )
            return None

        runtime = config.runtime_directory.resolve(strict=True)
        resolved: dict[str, str] = {}
        for name, raw_path in values.items():
            path = Path(raw_path)
            if not path.is_absolute() or path.is_symlink() or not path.is_file():
                raise ValueError(f"{name} must be an absolute regular TLS file.")
            canonical = path.resolve(strict=True)
            try:
                canonical.relative_to(runtime)
            except ValueError as exc:
                raise ValueError(
                    f"{name} must be below the shared Slurm runtime directory."
                ) from exc
            resolved[name] = str(canonical)
        return resolved

    async def _wait_for_worker_allocation_terminal(
        self,
        *,
        config: SlurmRuntimeConfig,
        execution_id: str,
        job_id: str,
        cluster: str | None,
        submission_token: str,
        cancellation_already_sent: bool = False,
    ) -> tuple[str, str, str]:
        """Cancel the complete Worker allocation and retain ownership until terminal."""
        cancel_sent = cancellation_already_sent
        last_error: str | None = None
        while True:
            try:
                if not cancel_sent:
                    await self._send_cancel(
                        config,
                        job_id,
                        whole_job=True,
                        cluster=cluster,
                        submission_token=submission_token,
                    )
                    cancel_sent = True

                found, terminal = await self._query_terminal_state(
                    config,
                    job_id,
                    cluster,
                    submission_token,
                )
                if terminal is not None:
                    return terminal

                queue_ok, queue_row = await self._query_queue_state(
                    config,
                    job_id,
                    cluster,
                    submission_token,
                )
                if queue_ok and queue_row is not None:
                    state, node, reason = queue_row
                    if state in _SLURM_TERMINAL_STATES:
                        return state, reason, node
                elif not found:
                    # A purged or unavailable record is not proof that remote
                    # Writers stopped. Keep the active lease and retry.
                    cancel_sent = False
                await asyncio.sleep(config.poll_interval_seconds)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and hasattr(current, "uncancel"):
                    current.uncancel()
                # Backend shutdown and repeated Stop clicks must not interrupt
                # the safety barrier between remote Writers and Scheduler/lock cleanup.
                continue
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                if message != last_error:
                    last_error = message
                    state_manager.add_log(
                        f"Waiting to terminate Slurm Worker allocation {job_id}: {message}",
                        "warning",
                        execution_id=execution_id,
                    )
                cancel_sent = False
                await asyncio.sleep(config.poll_interval_seconds)

    async def _wait_for_worker_allocation_running(
        self,
        *,
        config: SlurmRuntimeConfig,
        execution_id: str,
        job_id: str,
        cluster: str | None,
        submission_token: str,
        resource_request: SlurmResourceRequest,
    ) -> None:
        """Wait for Slurm scheduling without consuming Worker startup time.

        The Dask registration timeout starts only after every allocation is
        running.  Queue wait has its own bound so a partially runnable set is
        cancelled instead of retaining resources indefinitely.
        """

        last_state: tuple[str, str, str] | None = None
        deadline = time.monotonic() + config.queue_start_timeout_seconds
        while True:
            if time.monotonic() >= deadline:
                raise SlurmSubmissionError(
                    "The complete Worker allocation did not become runnable "
                    f"within {config.queue_start_timeout_seconds:g} seconds. "
                    "The execution is being rolled back so partial Worker Jobs "
                    "do not retain cluster resources."
                )
            query_ok, queue_state = await self._query_queue_state(
                config,
                job_id,
                cluster,
                submission_token,
            )
            if not query_ok:
                await asyncio.sleep(config.poll_interval_seconds)
                continue
            if queue_state is None:
                found, terminal = await self._query_terminal_state(
                    config,
                    job_id,
                    cluster,
                    submission_token,
                )
                if terminal is not None:
                    raise SlurmSubmissionError(
                        "Slurm Worker allocation ended before Dask Workers "
                        f"registered: state={terminal[0]}, "
                        f"exitCode={terminal[1] or 'unknown'}."
                    )
                if found:
                    await asyncio.sleep(config.poll_interval_seconds)
                    continue
                # A just-submitted job can be briefly absent from both views.
                await asyncio.sleep(config.poll_interval_seconds)
                continue

            state, node, reason = queue_state
            if queue_state != last_state:
                last_state = queue_state
                await self._broadcast_job_state(
                    execution_id,
                    job_id,
                    resource_request,
                    state,
                    node,
                    reason,
                )
            if state in {"RUNNING", "COMPLETING"}:
                return
            if state in _SLURM_TERMINAL_STATES:
                raise SlurmSubmissionError(
                    "Slurm Worker allocation ended before Dask Workers "
                    f"registered: state={state}, reason={reason or 'unknown'}."
                )
            await asyncio.sleep(config.poll_interval_seconds)

    async def reconcile_active_job(self) -> str | None:
        """Terminate orphan Worker allocations; a dead local Driver cannot resume."""
        config = SlurmRuntimeConfig.from_environment()
        for job_path in sorted(config.execution_root.glob("*/job.json")):
            try:
                record = _read_json_file(job_path)
            except Exception:
                logger.exception("Cannot inspect durable Slurm job record %s", job_path)
                continue
            if str(record.get("state", "")).lower() in _TERMINAL_STATUSES:
                continue
            execution_id = str(record.get("executionId", ""))
            raw_job_ids = record.get("jobIds")
            raw_tokens = record.get("submissionTokens")
            raw_clusters = record.get("clusters")
            if not isinstance(raw_job_ids, list):
                raw_job_ids = [record.get("jobId")]
                raw_tokens = [record.get("submissionToken")]
                raw_clusters = [record.get("cluster")]
            if (
                not isinstance(raw_tokens, list)
                or not isinstance(raw_clusters, list)
                or len(raw_job_ids) != len(raw_tokens)
                or len(raw_job_ids) != len(raw_clusters)
            ):
                raise SlurmSubmissionError(
                    f"Non-terminal Worker record {job_path} has incomplete Pool jobs."
                )
            jobs: list[tuple[str, str | None, str]] = []
            for job_id, cluster, token in zip(raw_job_ids, raw_clusters, raw_tokens):
                if (
                    not isinstance(job_id, str)
                    or re.fullmatch(r"[1-9][0-9]*", job_id) is None
                    or not isinstance(token, str)
                    or not token
                    or (cluster is not None and not isinstance(cluster, str))
                ):
                    raise SlurmSubmissionError(
                        f"Non-terminal Worker record {job_path} has an unsafe Pool job."
                    )
                jobs.append((job_id, cluster, token))
            pending_tokens = record.get("pendingSubmissionTokens")
            if pending_tokens is None:
                legacy_pending = record.get("pendingSubmissionToken")
                pending_tokens = [] if legacy_pending is None else [legacy_pending]
            if not isinstance(pending_tokens, list):
                raise SlurmSubmissionError(
                    f"Non-terminal Worker record {job_path} has invalid pending tokens."
                )
            for pending_token in pending_tokens:
                if not isinstance(pending_token, str) or not pending_token:
                    raise SlurmSubmissionError(
                        f"Non-terminal Worker record {job_path} has an unsafe pending token."
                    )
                query_ok, match = await self._query_job_by_submission_token(
                    config, pending_token
                )
                if not query_ok:
                    raise SlurmSubmissionError(
                        "Slurm is unavailable while reconciling a pending Pool job."
                    )
                if match is not None and all(item[0] != match[0] for item in jobs):
                    jobs.append((match[0], None, pending_token))
            if jobs:
                await asyncio.gather(*(
                    self._wait_for_worker_allocation_terminal(
                        config=config,
                        execution_id=execution_id,
                        job_id=job_id,
                        cluster=cluster,
                        submission_token=token,
                    )
                    for job_id, cluster, token in jobs
                ))
            record.update({
                "schemaVersion": JOB_SCHEMA_VERSION,
                "jobId": jobs[0][0] if jobs else None,
                "jobIds": [item[0] for item in jobs],
                "submissionTokens": [item[2] for item in jobs],
                "pendingSubmissionToken": None,
                "pendingSubmissionTokens": [],
                "state": ExecutionStatus.INTERRUPTED,
                "finishedAt": _utc_timestamp(),
                "message": (
                    "Service-node Driver was not alive after restart; orphan "
                    "Slurm Workers were cancelled and the graph was not auto-resumed."
                ),
            })
            _atomic_write_json(job_path, record)
        return None

    async def _execute_graph_impl(
        self,
        graph: dict[str, Any],
        execution_id: str,
        execution_config: ExecutionConfig | dict[str, Any] | None,
        *,
        worker_profiles: object = None,
        worker_pools: object = None,
    ) -> str:
        """Run Driver locally while Slurm supplies only remote Dask Workers."""
        execution_id = validate_execution_id(execution_id)
        selected_config = require_window_recovery_location(
            parse_execution_config(execution_config)
        )
        authoritative_graph, plan = _authoritative_graph_and_plan(
            graph, selected_config
        )
        config = SlurmRuntimeConfig.from_environment()
        allocation_plan = await asyncio.to_thread(
            _plan_slurm_allocation,
            plan,
            worker_profiles=worker_profiles,
            worker_pools=worker_pools,
            config=config,
        )
        resource_request = _allocation_holder_request(allocation_plan)
        validate_allocation_plan_policy(allocation_plan, config.policy)
        maximum_local_workers = max(sum(node.workers.values()) for node in allocation_plan.nodes)
        for name, configured_range in (
            ("WorkFlow_DASK_WORKER_PORT_RANGE", config.worker_port_range),
            ("WorkFlow_DASK_NANNY_PORT_RANGE", config.nanny_port_range),
        ):
            start, stop = (int(item) for item in configured_range.split(":"))
            if stop - start + 1 < maximum_local_workers:
                raise ValueError(
                    f"{name} needs at least {maximum_local_workers} ports for "
                    "the busiest compute node."
                )
        code_revision = await asyncio.to_thread(_git_revision, config.project_root)
        # Standard SLURMJob Worker commands receive these paths directly from
        # dask-jobqueue; validate that compute nodes can read them before any
        # Scheduler or Job is created.
        self._worker_security_payload(config)
        run_directory = resolve_execution_directory(
            config.execution_root, execution_id
        )
        run_directory.mkdir(mode=0o700, exist_ok=False)
        job_path = run_directory / "job.json"
        submitted_at = _utc_timestamp()
        submission_token_prefix = (
            "wf:" + hashlib.sha256(execution_id.encode("utf-8")).hexdigest()[:16]
            + ":" + uuid.uuid4().hex[:16]
        )

        submitted_jobs: list[tuple[str, str | None, str, SlurmResourceRequest]] = []
        job_requests: dict[str, SlurmResourceRequest] = {}
        scheduler_started = False
        worker_terminals: dict[str, tuple[str, str, str]] = {}
        final_error: BaseException | None = None
        external_cleanup_completed = False

        async def external_cleanup_barrier() -> None:
            nonlocal external_cleanup_completed
            if external_cleanup_completed:
                return
            if scheduler_started:
                # Capture partially submitted jobs even when one SLURMJob
                # failed while the heterogeneous spec set was starting.
                known_ids = {item[0] for item in submitted_jobs}
                for record in await asyncio.to_thread(
                    dask_service.submitted_slurm_jobqueue_jobs
                ):
                    if record.job_id in known_ids:
                        continue
                    request = job_requests.get(record.allocation_id)
                    if request is not None:
                        submitted_jobs.append((
                            record.job_id,
                            None,
                            record.submission_token,
                            request,
                        ))
                        known_ids.add(record.job_id)
            cancelled_by_cluster = False
            if scheduler_started:
                try:
                    await asyncio.to_thread(
                        dask_service.stop_slurm_jobqueue_workers
                    )
                    cancelled_by_cluster = True
                except Exception:
                    logger.exception(
                        "SLURMCluster could not scale Worker jobs to zero; "
                        "falling back to owned direct cancellation."
                    )
            if submitted_jobs:
                terminal_results = await asyncio.gather(*(
                    self._wait_for_worker_allocation_terminal(
                        config=config,
                        execution_id=execution_id,
                        job_id=job_id,
                        cluster=cluster_name,
                        submission_token=token,
                        cancellation_already_sent=cancelled_by_cluster,
                    )
                    for job_id, cluster_name, token, _request in submitted_jobs
                ))
                worker_terminals.update(
                    (submitted_jobs[index][0], terminal)
                    for index, terminal in enumerate(terminal_results)
                )
            if scheduler_started:
                await asyncio.to_thread(dask_service.stop_cluster)
            external_cleanup_completed = True

        try:
            client = await asyncio.to_thread(
                dask_service.start_slurm_jobqueue_scheduler,
                host=config.scheduler_host,
                port=config.scheduler_port,
                template_job=allocation_plan.jobs[0],
                time_limit=allocation_plan.time_limit,
                shared_temp_directory=str(config.runtime_directory),
                python_executable=str(
                    config.project_root / "backend" / ".venv" / "bin" / "python"
                ),
                dashboard_address=config.dashboard_address,
            )
            scheduler_started = True
            scheduler_address = str(client.scheduler.address)
            _atomic_write_json(job_path, {
                "schemaVersion": JOB_SCHEMA_VERSION,
                "executionId": execution_id,
                "jobId": None,
                "jobIds": [],
                "state": "submitting",
                "submissionTokens": [],
                "pendingSubmissionToken": None,
                "pendingSubmissionTokens": [],
                "resources": resource_request.to_dict(),
                "allocationPlan": allocation_plan.to_dict(),
                "schedulerAddress": scheduler_address,
                "submittedAt": submitted_at,
                "clusterManager": "dask_jobqueue.SLURMCluster",
            })
            planned_specs = []
            planned_jobs_by_allocation = {
                job.allocation_id: job for job in allocation_plan.jobs
            }
            interface = os.getenv("WorkFlow_DASK_INTERFACE", "").strip() or None
            protocol = "tls://" if scheduler_address.startswith("tls://") else "tcp://"
            for job_index, planned_job in enumerate(allocation_plan.jobs, start=1):
                job_request = _worker_job_request(allocation_plan, planned_job)
                config.policy.validate_request(job_request)
                job_requests[planned_job.allocation_id] = job_request
                submission_token = f"{submission_token_prefix}:{job_index}"
                planned_specs.append(build_planned_slurm_worker_spec(
                    allocation_plan,
                    planned_job,
                    execution_id=execution_id,
                    submission_token=submission_token,
                    project_root=config.project_root,
                    runtime_directory=config.runtime_directory,
                    run_directory=run_directory,
                    python_executable=(
                        config.project_root / "backend" / ".venv" / "bin" / "python"
                    ),
                    sbatch_executable=config.sbatch_executable,
                    scancel_executable=config.scancel_executable,
                    interface=interface,
                    protocol=protocol,
                    security=client.security,
                    worker_port_range=config.worker_port_range,
                    nanny_port_range=config.nanny_port_range,
                ))

            _atomic_write_json(job_path, {
                "schemaVersion": JOB_SCHEMA_VERSION,
                "executionId": execution_id,
                "jobId": None,
                "jobIds": [],
                "clusters": [],
                "state": "submitting",
                "submissionTokens": [],
                "pendingSubmissionToken": None,
                "pendingSubmissionTokens": [
                    spec.submission_token for spec in planned_specs
                ],
                "resources": resource_request.to_dict(),
                "allocationPlan": allocation_plan.to_dict(),
                "schedulerAddress": scheduler_address,
                "submittedAt": submitted_at,
                "clusterManager": "dask_jobqueue.SLURMCluster",
            })
            submission_task = asyncio.create_task(asyncio.to_thread(
                dask_service.submit_slurm_jobqueue_workers, planned_specs
            ))
            submitted_records, cancelled_during_submission = (
                await _harvest_background_task(submission_task)
            )
            for record in submitted_records:
                planned_job = planned_jobs_by_allocation[record.allocation_id]
                job_request = job_requests[record.allocation_id]
                submitted_jobs.append((
                    record.job_id,
                    None,
                    record.submission_token,
                    job_request,
                ))
                await state_manager.broadcast(execution_id, {
                    "type": "slurm_job_submitted",
                    "executionId": execution_id,
                    "jobId": record.job_id,
                    "allocationId": planned_job.allocation_id,
                    "profile": planned_job.profile,
                    "resources": job_request.to_dict(),
                    "message": (
                        f"SLURMCluster Worker job {record.job_id} submitted: "
                        f"{planned_job.workers} {planned_job.profile} Worker(s) "
                        f"on {planned_job.node} ({planned_job.partition})."
                    ),
                })
            self._jobs[execution_id] = ",".join(item[0] for item in submitted_jobs)
            if cancelled_during_submission:
                raise asyncio.CancelledError

            _atomic_write_json(job_path, {
                "schemaVersion": JOB_SCHEMA_VERSION,
                "executionId": execution_id,
                "jobId": submitted_jobs[0][0],
                "jobIds": [item[0] for item in submitted_jobs],
                "clusters": [item[1] for item in submitted_jobs],
                "state": "workers_starting",
                "submissionTokens": [item[2] for item in submitted_jobs],
                "pendingSubmissionToken": None,
                "pendingSubmissionTokens": [],
                "resources": resource_request.to_dict(),
                "allocationPlan": allocation_plan.to_dict(),
                "schedulerAddress": scheduler_address,
                "submittedAt": submitted_at,
                "clusterManager": "dask_jobqueue.SLURMCluster",
            })
            # Queue time does not consume the bounded Dask registration clock.
            # All waits are one logical allocation: if any member fails or
            # times out, stop its sibling waits and let the outer cleanup
            # barrier scale the complete SLURMCluster back to zero.
            allocation_waits = [
                asyncio.create_task(self._wait_for_worker_allocation_running(
                    config=config,
                    execution_id=execution_id,
                    job_id=job_id,
                    cluster=cluster_name,
                    submission_token=token,
                    resource_request=job_request,
                ))
                for job_id, cluster_name, token, job_request in submitted_jobs
            ]
            try:
                await asyncio.gather(*allocation_waits)
            except BaseException:
                for wait in allocation_waits:
                    wait.cancel()
                await asyncio.gather(*allocation_waits, return_exceptions=True)
                raise
            await asyncio.to_thread(
                dask_service.activate_external_worker_profiles,
                expected_profiles=allocation_plan.worker_counts,
                timeout=config.worker_start_timeout_seconds,
                execution_id=execution_id,
                submission_tokens=tuple(item[2] for item in submitted_jobs),
            )
            _atomic_write_json(job_path, {
                "schemaVersion": JOB_SCHEMA_VERSION,
                "executionId": execution_id,
                "jobId": submitted_jobs[0][0],
                "jobIds": [item[0] for item in submitted_jobs],
                "clusters": [item[1] for item in submitted_jobs],
                "state": "driver_running",
                "submissionTokens": [item[2] for item in submitted_jobs],
                "pendingSubmissionToken": None,
                "pendingSubmissionTokens": [],
                "resources": resource_request.to_dict(),
                "allocationPlan": allocation_plan.to_dict(),
                "schedulerAddress": scheduler_address,
                "submittedAt": submitted_at,
                "clusterManager": "dask_jobqueue.SLURMCluster",
            })
            await execute_graph_on_service_node(
                authoritative_graph,
                execution_id,
                selected_config,
                release_active_execution=False,
                external_cleanup_barrier=external_cleanup_barrier,
            )
        except BaseException as exc:
            final_error = exc
            session = state_manager.get_execution(execution_id)
            if session is not None and not ExecutionStatus.is_finished(session.status):
                status = (
                    ExecutionStatus.CANCELLED
                    if session.status == ExecutionStatus.CANCELLING
                    else ExecutionStatus.INTERRUPTED
                    if isinstance(exc, asyncio.CancelledError)
                    else ExecutionStatus.FAILED
                )
                state_manager.set_execution_status(
                    execution_id, status, release_active=False
                )
                await state_manager.broadcast(execution_id, {
                    "type": "execution_finished",
                    "executionId": execution_id,
                    "status": status,
                    "message": str(exc) or status,
                })
        finally:
            # Remote Writers must be proven gone before closing their Scheduler
            # and before releasing the application's single-execution lease.
            await external_cleanup_barrier()

            session = state_manager.get_execution(execution_id)
            application_status = (
                session.status if session is not None else ExecutionStatus.INTERRUPTED
            )
            _atomic_write_json(job_path, {
                "schemaVersion": JOB_SCHEMA_VERSION,
                "executionId": execution_id,
                "jobId": submitted_jobs[0][0] if submitted_jobs else None,
                "jobIds": [item[0] for item in submitted_jobs],
                "clusters": [item[1] for item in submitted_jobs],
                "state": application_status,
                "submissionTokens": [item[2] for item in submitted_jobs],
                "pendingSubmissionToken": None,
                "pendingSubmissionTokens": [],
                "resources": resource_request.to_dict(),
                "allocationPlan": allocation_plan.to_dict(),
                "submittedAt": submitted_at,
                "finishedAt": _utc_timestamp(),
                "workerAllocationsTerminal": {
                    job_id: list(terminal)
                    for job_id, terminal in worker_terminals.items()
                },
                "driverHost": config.scheduler_host,
                "clusterManager": "dask_jobqueue.SLURMCluster",
            })
            self._jobs.pop(execution_id, None)
            state_manager.clear_active_execution(execution_id)
            state_manager.cleanup_old_executions()

        if final_error is not None:
            raise final_error
        return execution_id

    async def execute_graph(
        self,
        graph: dict[str, Any],
        execution_id: str,
        execution_config: ExecutionConfig | dict[str, Any] | None,
        *,
        worker_profiles: object = None,
        worker_pools: object = None,
    ) -> str:
        try:
            return await self._execute_graph_impl(
                graph,
                execution_id,
                execution_config,
                worker_profiles=worker_profiles,
                worker_pools=worker_pools,
            )
        except BaseException as exc:
            session = state_manager.get_execution(execution_id)
            if session is not None and not ExecutionStatus.is_finished(session.status):
                status = (
                    ExecutionStatus.CANCELLED
                    if session.status == ExecutionStatus.CANCELLING
                    else ExecutionStatus.INTERRUPTED
                    if isinstance(exc, asyncio.CancelledError)
                    else ExecutionStatus.FAILED
                )
                state_manager.set_execution_status(
                    execution_id, status, release_active=False
                )
                await state_manager.broadcast(execution_id, {
                    "type": "execution_finished",
                    "executionId": execution_id,
                    "status": status,
                    "message": str(exc) or status,
                })
            state_manager.clear_active_execution(execution_id)
            state_manager.cleanup_old_executions()
            raise


slurm_execution_service = SlurmExecutionService()


__all__ = [
    "SlurmExecutionService",
    "SlurmRuntimeConfig",
    "SlurmSubmissionError",
    "execution_backend_name",
    "slurm_policy_from_environment",
    "slurm_execution_service",
    "uses_slurm_execution_backend",
    "validate_allocation_plan_policy",
]
