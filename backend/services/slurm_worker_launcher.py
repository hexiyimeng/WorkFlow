"""Launch one node's Dask Workers inside a multi-node Slurm allocation.

This module is deliberately a Worker-only process.  The WorkFlow Driver and
Dask Scheduler are owned by the long-lived service process; an ``srun`` task
starts exactly one instance of this launcher on every allocated compute node.
The shared, immutable request tells each task rank how many CPU and GPU
Workers belong on its node.

The launcher never executes a workflow graph and never mutates Window recovery
state.  Its only responsibilities are validating the allocation, registering
strictly isolated Nannies with the external Scheduler, and shutting them down
when Slurm terminates the allocation.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import ipaddress
import json
import logging
import os
from pathlib import Path
import re
import signal
import socket
import stat
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from distributed import Nanny, Security

from core.slurm_execution import validate_execution_id
from services.dask_service import (
    CPU_RESOURCE_NAME,
    GPU_RESOURCE_NAME,
    WorkerDevicePlugin,
    worker_device_startup_information,
)


logger = logging.getLogger("WorkFlow.SlurmWorkerLauncher")

REQUEST_SCHEMA_VERSION = 2
_REQUEST_FIELDS = frozenset({
    "schemaVersion",
    "executionId",
    "submissionToken",
    "codeRevision",
    "schedulerAddress",
    "resourcePlan",
    "runtimeDirectory",
    "security",
    "allowInsecure",
    "workerMemoryGiB",
    "networkInterface",
    "workerPortRange",
    "nannyPortRange",
})
_RESOURCE_FIELDS = frozenset({
    "cpuWorkers",
    "gpuWorkers",
    "nodes",
    "cpus",
    "gpus",
    "memoryGiB",
    "totalCpus",
    "totalGpus",
    "totalMemoryGiB",
    "cpuWorkersByNode",
    "gpuWorkersByNode",
    "timeLimit",
    "partition",
})
_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z")
_JOB_ID_RE = re.compile(r"[1-9][0-9]*\Z")
_SECURITY_FIELDS = frozenset({"tlsCaFile", "tlsCertFile", "tlsKeyFile"})
_WORKER_MEMORY_FIELDS = frozenset({"cpu", "gpu"})


class WorkerLauncherValidationError(ValueError):
    """The immutable Worker request or Slurm task layout is invalid."""


@dataclass(frozen=True, slots=True)
class WorkerResourcePlan:
    cpu_workers: int
    gpu_workers: int
    cpu_workers_by_node: tuple[int, ...]
    gpu_workers_by_node: tuple[int, ...]

    @property
    def node_count(self) -> int:
        return len(self.cpu_workers_by_node)

    @property
    def gpus_per_allocated_node(self) -> int:
        """Return the homogeneous Slurm 19.05 GRES request per node.

        Slurm 19.05's portable ``--gres=gpu:N`` syntax is per node.  An
        uneven logical layout therefore reserves the maximum count on every
        node while the final launcher may intentionally start fewer Workers.
        """

        return max(self.gpu_workers_by_node, default=0)


@dataclass(frozen=True, slots=True)
class WorkerLauncherRequest:
    execution_id: str
    submission_token: str
    code_revision: str | None
    scheduler_address: str
    resource_plan: WorkerResourcePlan
    runtime_directory: Path
    security: Mapping[str, Path] | None
    allow_insecure: bool
    cpu_worker_memory_gib: int
    gpu_worker_memory_gib: int
    network_interface: str | None
    worker_port_range: tuple[int, int]
    nanny_port_range: tuple[int, int]


@dataclass(frozen=True, slots=True)
class WorkerAllocation:
    job_id: str
    rank: int
    node_count: int
    node_name: str
    cpu_workers: int
    gpu_workers: int
    visible_gpu_ids: tuple[str, ...]
    allocated_cpus: int

    @property
    def selected_gpu_ids(self) -> tuple[str, ...]:
        return self.visible_gpu_ids[: self.gpu_workers]


def _strict_nonnegative_integer(value: object, *, name: str) -> int:
    if type(value) is not int or value < 0:
        raise WorkerLauncherValidationError(f"{name} must be a non-negative integer.")
    return value


def _strict_integer_sequence(value: object, *, name: str) -> tuple[int, ...]:
    if not isinstance(value, list) or not value:
        raise WorkerLauncherValidationError(f"{name} must be a non-empty JSON array.")
    return tuple(
        _strict_nonnegative_integer(item, name=f"{name}[{index}]")
        for index, item in enumerate(value)
    )


def _validate_scheduler_address(value: object, *, secure: bool) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise WorkerLauncherValidationError(
            "schedulerAddress must be a non-empty external Dask address."
        )
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise WorkerLauncherValidationError("schedulerAddress has an invalid port.") from exc
    if (
        parsed.scheme != ("tls" if secure else "tcp")
        or not parsed.hostname
        or port is None
        or not 1 <= port <= 65535
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise WorkerLauncherValidationError(
            f"schedulerAddress must be exactly "
            f"{'tls' if secure else 'tcp'}://HOST:PORT."
        )

    host = parsed.hostname.rstrip(".").lower()
    if host in {"localhost", "0.0.0.0", "::"}:
        raise WorkerLauncherValidationError(
            "schedulerAddress must be reachable from compute nodes, not loopback "
            "or a wildcard address."
        )
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None and (address.is_loopback or address.is_unspecified):
        raise WorkerLauncherValidationError(
            "schedulerAddress must be reachable from compute nodes, not loopback "
            "or a wildcard address."
        )
    return value


def _parse_port_range(value: object, *, name: str) -> tuple[int, int]:
    if not isinstance(value, str):
        raise WorkerLauncherValidationError(f"{name} must have the form START:END.")
    match = re.fullmatch(r"([0-9]{1,5}):([0-9]{1,5})", value)
    if match is None:
        raise WorkerLauncherValidationError(f"{name} must have the form START:END.")
    start, end = (int(item) for item in match.groups())
    if start < 1024 or end > 65535 or start > end:
        raise WorkerLauncherValidationError(
            f"{name} must be an ordered unprivileged TCP port range."
        )
    return start, end


def parse_worker_launcher_request(payload: Mapping[str, Any]) -> WorkerLauncherRequest:
    fields = frozenset(payload)
    missing = sorted(_REQUEST_FIELDS - fields)
    unknown = sorted(fields - _REQUEST_FIELDS)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing fields: {missing}")
        if unknown:
            details.append(f"unknown fields: {unknown}")
        raise WorkerLauncherValidationError(
            "Invalid schema-v2 Worker request (" + "; ".join(details) + ")."
        )
    if type(payload["schemaVersion"]) is not int or payload["schemaVersion"] != 2:
        raise WorkerLauncherValidationError(
            f"Unsupported Worker request schemaVersion "
            f"{payload['schemaVersion']!r}; expected 2."
        )
    try:
        execution_id = validate_execution_id(payload["executionId"])
    except ValueError as exc:
        raise WorkerLauncherValidationError(str(exc)) from exc

    token = payload["submissionToken"]
    if not isinstance(token, str) or _TOKEN_RE.fullmatch(token) is None:
        raise WorkerLauncherValidationError(
            "submissionToken must be a safe 1-128 character Slurm ownership token."
        )

    revision = payload["codeRevision"]
    if revision is not None and (
        not isinstance(revision, str)
        or re.fullmatch(r"[0-9a-fA-F]{40}", revision) is None
    ):
        raise WorkerLauncherValidationError(
            "codeRevision must be a 40-character Git hash or null."
        )
    if isinstance(revision, str):
        revision = revision.lower()

    raw_plan = payload["resourcePlan"]
    if not isinstance(raw_plan, Mapping) or frozenset(raw_plan) != _RESOURCE_FIELDS:
        raise WorkerLauncherValidationError(
            "resourcePlan must be the complete canonical Slurm allocation map."
        )
    cpu_total = _strict_nonnegative_integer(
        raw_plan["cpuWorkers"], name="resourcePlan.cpuWorkers"
    )
    gpu_total = _strict_nonnegative_integer(
        raw_plan["gpuWorkers"], name="resourcePlan.gpuWorkers"
    )
    cpu_layout = _strict_integer_sequence(
        raw_plan["cpuWorkersByNode"], name="resourcePlan.cpuWorkersByNode"
    )
    gpu_layout = _strict_integer_sequence(
        raw_plan["gpuWorkersByNode"], name="resourcePlan.gpuWorkersByNode"
    )
    if len(cpu_layout) != len(gpu_layout):
        raise WorkerLauncherValidationError(
            "CPU and GPU per-node layouts must have the same length."
        )
    nodes = _strict_nonnegative_integer(raw_plan["nodes"], name="resourcePlan.nodes")
    cpus = _strict_nonnegative_integer(raw_plan["cpus"], name="resourcePlan.cpus")
    gpus = _strict_nonnegative_integer(raw_plan["gpus"], name="resourcePlan.gpus")
    memory_gib = _strict_nonnegative_integer(
        raw_plan["memoryGiB"], name="resourcePlan.memoryGiB"
    )
    if nodes <= 0 or cpus <= 0 or memory_gib <= 0 or nodes != len(cpu_layout):
        raise WorkerLauncherValidationError(
            "resourcePlan nodes, cpus and memoryGiB must describe the per-node layout."
        )
    if gpus != max(gpu_layout, default=0):
        raise WorkerLauncherValidationError(
            "resourcePlan.gpus must equal the maximum GPU Workers on one node."
        )
    if cpus < max(
        (cpu + gpu for cpu, gpu in zip(cpu_layout, gpu_layout)), default=0
    ):
        raise WorkerLauncherValidationError(
            "resourcePlan.cpus cannot host the busiest node's Workers."
        )
    for field_name, expected in (
        ("totalCpus", nodes * cpus),
        ("totalGpus", nodes * gpus),
        ("totalMemoryGiB", nodes * memory_gib),
    ):
        actual = _strict_nonnegative_integer(
            raw_plan[field_name], name=f"resourcePlan.{field_name}"
        )
        if actual != expected:
            raise WorkerLauncherValidationError(
                f"resourcePlan.{field_name} must equal {expected}."
            )
    for field_name in ("timeLimit", "partition"):
        value = raw_plan[field_name]
        if not isinstance(value, str) or not value.strip() or value != value.strip():
            raise WorkerLauncherValidationError(
                f"resourcePlan.{field_name} must be a non-empty canonical string."
            )
    if sum(cpu_layout) != cpu_total or sum(gpu_layout) != gpu_total:
        raise WorkerLauncherValidationError(
            "Per-node Worker layouts must sum exactly to their declared totals."
        )
    if cpu_total + gpu_total <= 0:
        raise WorkerLauncherValidationError("The Worker request must contain a Worker.")
    if any(cpu + gpu == 0 for cpu, gpu in zip(cpu_layout, gpu_layout)):
        raise WorkerLauncherValidationError(
            "Every allocated node must host at least one requested Worker."
        )

    runtime_value = payload["runtimeDirectory"]
    if not isinstance(runtime_value, str) or not runtime_value:
        raise WorkerLauncherValidationError("runtimeDirectory must be an absolute path.")
    runtime = Path(runtime_value)
    if not runtime.is_absolute():
        raise WorkerLauncherValidationError("runtimeDirectory must be an absolute path.")

    raw_security = payload["security"]
    allow_insecure = payload["allowInsecure"]
    if type(allow_insecure) is not bool:
        raise WorkerLauncherValidationError("allowInsecure must be a boolean.")
    parsed_security: Mapping[str, Path] | None
    if raw_security is None:
        if not allow_insecure:
            raise WorkerLauncherValidationError(
                "security=null requires allowInsecure=true."
            )
        parsed_security = None
    else:
        if allow_insecure:
            raise WorkerLauncherValidationError(
                "TLS security requires allowInsecure=false."
            )
        if (
            not isinstance(raw_security, Mapping)
            or frozenset(raw_security) != _SECURITY_FIELDS
        ):
            raise WorkerLauncherValidationError(
                "security must be null or contain exactly tlsCaFile, "
                "tlsCertFile and tlsKeyFile."
            )
        security_paths: dict[str, Path] = {}
        for name in sorted(_SECURITY_FIELDS):
            value = raw_security[name]
            path = Path(value) if isinstance(value, str) else Path()
            if not isinstance(value, str) or not value or not path.is_absolute():
                raise WorkerLauncherValidationError(
                    f"security.{name} must be an absolute file path."
                )
            security_paths[name] = path
        parsed_security = security_paths

    worker_ports = _parse_port_range(
        payload["workerPortRange"], name="workerPortRange"
    )
    nanny_ports = _parse_port_range(
        payload["nannyPortRange"], name="nannyPortRange"
    )
    if not (worker_ports[1] < nanny_ports[0] or nanny_ports[1] < worker_ports[0]):
        raise WorkerLauncherValidationError(
            "workerPortRange and nannyPortRange must not overlap."
        )
    maximum_local_workers = max(
        (cpu + gpu for cpu, gpu in zip(cpu_layout, gpu_layout)), default=0
    )
    for name, port_range in (
        ("workerPortRange", worker_ports),
        ("nannyPortRange", nanny_ports),
    ):
        if port_range[1] - port_range[0] + 1 < maximum_local_workers:
            raise WorkerLauncherValidationError(
                f"{name} needs at least {maximum_local_workers} ports for the "
                "busiest compute node."
            )

    raw_worker_memory = payload["workerMemoryGiB"]
    if (
        not isinstance(raw_worker_memory, Mapping)
        or frozenset(raw_worker_memory) != _WORKER_MEMORY_FIELDS
    ):
        raise WorkerLauncherValidationError(
            "workerMemoryGiB must contain exactly positive cpu and gpu values."
        )
    cpu_worker_memory_gib = _strict_nonnegative_integer(
        raw_worker_memory["cpu"], name="workerMemoryGiB.cpu"
    )
    gpu_worker_memory_gib = _strict_nonnegative_integer(
        raw_worker_memory["gpu"], name="workerMemoryGiB.gpu"
    )
    if cpu_worker_memory_gib <= 0 or gpu_worker_memory_gib <= 0:
        raise WorkerLauncherValidationError(
            "workerMemoryGiB.cpu and workerMemoryGiB.gpu must be positive integers."
        )
    if any(
        cpu_count * cpu_worker_memory_gib
        + gpu_count * gpu_worker_memory_gib
        > memory_gib
        for cpu_count, gpu_count in zip(cpu_layout, gpu_layout)
    ):
        raise WorkerLauncherValidationError(
            "workerMemoryGiB exceeds resourcePlan.memoryGiB on a compute node."
        )
    raw_interface = payload["networkInterface"]
    if raw_interface is not None and (
        not isinstance(raw_interface, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}", raw_interface) is None
    ):
        raise WorkerLauncherValidationError(
            "networkInterface must be null or a safe interface name."
        )

    return WorkerLauncherRequest(
        execution_id=execution_id,
        submission_token=token,
        code_revision=revision,
        scheduler_address=_validate_scheduler_address(
            payload["schedulerAddress"], secure=parsed_security is not None
        ),
        resource_plan=WorkerResourcePlan(
            cpu_workers=cpu_total,
            gpu_workers=gpu_total,
            cpu_workers_by_node=cpu_layout,
            gpu_workers_by_node=gpu_layout,
        ),
        runtime_directory=runtime,
        security=parsed_security,
        allow_insecure=allow_insecure,
        cpu_worker_memory_gib=cpu_worker_memory_gib,
        gpu_worker_memory_gib=gpu_worker_memory_gib,
        network_interface=raw_interface,
        worker_port_range=worker_ports,
        nanny_port_range=nanny_ports,
    )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise WorkerLauncherValidationError(f"Duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _read_request(path: Path) -> WorkerLauncherRequest:
    if not path.is_absolute():
        raise WorkerLauncherValidationError("Worker request path must be absolute.")
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise WorkerLauncherValidationError(f"Worker request does not exist: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise WorkerLauncherValidationError(
            f"Worker request must be a regular non-symlink file: {path}"
        )
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            payload = json.load(handle, object_pairs_hook=_reject_duplicate_keys)
    except WorkerLauncherValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkerLauncherValidationError(f"Cannot read Worker request JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise WorkerLauncherValidationError("Worker request must be a JSON object.")
    request = parse_worker_launcher_request(payload)

    runtime = request.runtime_directory
    try:
        runtime_metadata = runtime.lstat()
    except FileNotFoundError as exc:
        raise WorkerLauncherValidationError(
            f"runtimeDirectory does not exist: {runtime}"
        ) from exc
    if stat.S_ISLNK(runtime_metadata.st_mode) or not stat.S_ISDIR(runtime_metadata.st_mode):
        raise WorkerLauncherValidationError(
            f"runtimeDirectory must be a real shared directory: {runtime}"
        )
    try:
        path.resolve(strict=True).relative_to(runtime.resolve(strict=True))
    except ValueError as exc:
        raise WorkerLauncherValidationError(
            "Worker request must be stored below runtimeDirectory."
        ) from exc
    if request.security is not None:
        resolved_runtime = runtime.resolve(strict=True)
        for name, security_path in request.security.items():
            try:
                metadata = security_path.lstat()
                security_path.resolve(strict=True).relative_to(resolved_runtime)
            except (FileNotFoundError, ValueError) as exc:
                raise WorkerLauncherValidationError(
                    f"security.{name} must be a shared file below runtimeDirectory."
                ) from exc
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise WorkerLauncherValidationError(
                    f"security.{name} must be a regular non-symlink file."
                )
    return request


def validate_code_revision(request: WorkerLauncherRequest) -> None:
    if request.code_revision is None:
        return
    project_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ("git", "-C", str(project_root), "rev-parse", "HEAD"),
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
            shell=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorkerLauncherValidationError("Cannot verify Worker code revision.") from exc
    if result.returncode != 0 or result.stdout.strip().lower() != request.code_revision:
        raise WorkerLauncherValidationError(
            "The shared WorkFlow checkout changed after Worker submission."
        )


def _environment_integer(
    environment: Mapping[str, str],
    names: Sequence[str],
    *,
    minimum: int,
) -> int:
    for name in names:
        value = environment.get(name)
        if value is None:
            continue
        match = re.match(r"\s*([0-9]+)", value)
        if match is not None and int(match.group(1)) >= minimum:
            return int(match.group(1))
    raise WorkerLauncherValidationError(
        f"Slurm did not expose a valid {names[0]} value."
    )


def _visible_gpu_ids(environment: Mapping[str, str]) -> tuple[str, ...]:
    raw = environment.get("CUDA_VISIBLE_DEVICES")
    if raw is None:
        return ()
    normalized = raw.strip()
    if normalized.lower() in {"", "-1", "none", "nodevfiles"}:
        return ()
    values = tuple(item.strip() for item in normalized.split(","))
    if any(not value for value in values) or len(values) != len(set(values)):
        raise WorkerLauncherValidationError(
            f"CUDA_VISIBLE_DEVICES is not a unique device list: {raw!r}."
        )
    return values


def validate_slurm_worker_allocation(
    request: WorkerLauncherRequest,
    environment: Mapping[str, str] | None = None,
    *,
    hostname: str | None = None,
) -> WorkerAllocation:
    env = os.environ if environment is None else environment
    job_id = str(env.get("SLURM_JOB_ID", "")).strip()
    if _JOB_ID_RE.fullmatch(job_id) is None:
        raise WorkerLauncherValidationError(
            "SLURM_JOB_ID is required; Worker launchers must run inside an allocation."
        )
    node_count = _environment_integer(env, ("SLURM_NNODES",), minimum=1)
    task_count = _environment_integer(env, ("SLURM_NTASKS",), minimum=1)
    rank = _environment_integer(env, ("SLURM_PROCID",), minimum=0)
    local_rank = _environment_integer(env, ("SLURM_LOCALID",), minimum=0)
    node_rank = _environment_integer(env, ("SLURM_NODEID",), minimum=0)
    expected_nodes = request.resource_plan.node_count
    if node_count != expected_nodes or task_count != expected_nodes:
        raise WorkerLauncherValidationError(
            "Slurm must launch exactly one Worker launcher per planned node "
            f"(planned={expected_nodes}, nodes={node_count}, tasks={task_count})."
        )
    if rank >= expected_nodes or local_rank != 0 or node_rank != rank:
        raise WorkerLauncherValidationError(
            "Invalid srun rank layout; expected one task per node with matching "
            "SLURM_PROCID and SLURM_NODEID."
        )

    cpu_workers = request.resource_plan.cpu_workers_by_node[rank]
    gpu_workers = request.resource_plan.gpu_workers_by_node[rank]
    allocated_cpus = _environment_integer(
        env,
        ("SLURM_CPUS_PER_TASK", "SLURM_CPUS_ON_NODE"),
        minimum=1,
    )
    if allocated_cpus < cpu_workers + gpu_workers:
        raise WorkerLauncherValidationError(
            f"Node rank {rank} exposes {allocated_cpus} CPU(s), but its Worker "
            f"layout needs at least {cpu_workers + gpu_workers}."
        )

    visible = _visible_gpu_ids(env)
    if len(visible) < gpu_workers:
        raise WorkerLauncherValidationError(
            f"Node rank {rank} exposes {len(visible)} GPU(s), but its Worker "
            f"layout needs {gpu_workers}."
        )
    # An uneven plan deliberately over-reserves the last node because portable
    # Slurm 19.05 GRES requests are homogeneous and per-node.  Only the first
    # requested devices are passed to child Workers; extra allocated devices
    # remain inaccessible to all WorkFlow tasks.
    node_name = str(
        hostname
        or env.get("SLURMD_NODENAME")
        or socket.getfqdn()
    ).strip()
    if not node_name or node_name.lower() in {"localhost", "localhost.localdomain"}:
        raise WorkerLauncherValidationError("Cannot determine a routable compute-node name.")

    return WorkerAllocation(
        job_id=job_id,
        rank=rank,
        node_count=node_count,
        node_name=node_name,
        cpu_workers=cpu_workers,
        gpu_workers=gpu_workers,
        visible_gpu_ids=visible,
        allocated_cpus=allocated_cpus,
    )


def _worker_memory_limit(
    role: str,
    request: WorkerLauncherRequest,
) -> str:
    if role == "cpu":
        return f"{request.cpu_worker_memory_gib}GB"
    if role == "gpu":
        return f"{request.gpu_worker_memory_gib}GB"
    raise WorkerLauncherValidationError(f"Unknown Worker memory role: {role!r}.")


def build_nanny_options(
    request: WorkerLauncherRequest,
    allocation: WorkerAllocation,
    *,
    environment: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], ...]:
    env = os.environ if environment is None else environment
    if request.security is None:
        if not request.allow_insecure:
            raise WorkerLauncherValidationError(
                "Multi-node Dask without TLS requires an immutable "
                "allowInsecure=true request."
            )
        security: Security | None = None
    else:
        security = Security(
            tls_ca_file=str(request.security["tlsCaFile"]),
            tls_worker_cert=str(request.security["tlsCertFile"]),
            tls_worker_key=str(request.security["tlsKeyFile"]),
            require_encryption=True,
        )
    interface = request.network_interface or ""
    host = allocation.node_name
    if not interface and not host:
        raise WorkerLauncherValidationError("A compute-node Dask host or interface is required.")
    local_root_value = str(env.get("SLURM_TMPDIR", "")).strip()
    if local_root_value:
        local_root = Path(local_root_value)
        if not local_root.is_absolute():
            raise WorkerLauncherValidationError("SLURM_TMPDIR must be absolute.")
        local_root = local_root / "workflow" / request.execution_id
    else:
        local_root = (
            request.runtime_directory
            / "jobs"
            / request.execution_id
            / "worker-scratch"
            / f"node-{allocation.rank}"
        )
    local_root.mkdir(parents=True, exist_ok=True, mode=0o700)

    common_env = {
        "WORKFLOW_DASK_WORKER_PROCESS": "1",
        "WORKFLOW_EXECUTION_ID": request.execution_id,
        "WORKFLOW_SUBMISSION_TOKEN_HASH": hashlib.sha256(
            request.submission_token.encode("utf-8")
        ).hexdigest(),
        "WORKFLOW_NODE_RANK": str(allocation.rank),
        "WorkFlow_MODELS_DIR": str(request.runtime_directory / "models"),
        "CELLPOSE_LOCAL_MODELS_PATH": str(
            request.runtime_directory / "models" / "cellpose"
        ),
        "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
        "MALLOC_TRIM_THRESHOLD_": "0",
    }
    network: dict[str, Any] = {"interface": interface} if interface else {"host": host}
    death_timeout = float(env.get("WorkFlow_DASK_WORKER_CONNECT_TIMEOUT_SECONDS", "120"))
    if death_timeout <= 0:
        raise WorkerLauncherValidationError(
            "WorkFlow_DASK_WORKER_CONNECT_TIMEOUT_SECONDS must be positive."
        )

    options: list[dict[str, Any]] = []
    worker_port_range = (
        f"{request.worker_port_range[0]}:{request.worker_port_range[1]}"
    )
    nanny_port_range = (
        f"{request.nanny_port_range[0]}:{request.nanny_port_range[1]}"
    )
    cpu_memory = _worker_memory_limit("cpu", request) if allocation.cpu_workers else ""
    gpu_memory = _worker_memory_limit("gpu", request) if allocation.gpu_workers else ""
    for local_index in range(allocation.cpu_workers):
        options.append({
            "scheduler_ip": request.scheduler_address,
            "name": (
                f"wf-{request.execution_id}-n{allocation.rank}-cpu-{local_index}"
            ),
            "nthreads": 1,
            "resources": {CPU_RESOURCE_NAME: 1},
            "env": {
                **common_env,
                "WORKFLOW_WORKER_ROLE": "cpu",
                "CUDA_VISIBLE_DEVICES": "",
            },
            "memory_limit": cpu_memory,
            "local_directory": str(local_root / f"cpu-{local_index}"),
            "dashboard": False,
            "silence_logs": logging.WARNING,
            "plugins": (WorkerDevicePlugin(),),
            "startup_information": {
                "workflowDevice": worker_device_startup_information,
            },
            "death_timeout": death_timeout,
            "reconnect": False,
            "security": security,
            # Give every concurrently starting Nanny the complete allowed
            # range. Distributed retries occupied ports within the range,
            # which also avoids deterministic collisions when Slurm permits
            # two independent allocations to share one compute node.
            "worker_port": worker_port_range,
            "port": nanny_port_range,
            **network,
        })
    for local_index, local_gpu_id in enumerate(allocation.selected_gpu_ids):
        global_gpu_id = f"{allocation.node_name}:{local_gpu_id}"
        options.append({
            "scheduler_ip": request.scheduler_address,
            "name": (
                f"wf-{request.execution_id}-n{allocation.rank}-gpu-{local_index}"
            ),
            "nthreads": 1,
            "resources": {GPU_RESOURCE_NAME: 1},
            "env": {
                **common_env,
                "WORKFLOW_WORKER_ROLE": "gpu",
                "WORKFLOW_PHYSICAL_GPU_ID": global_gpu_id,
                "WORKFLOW_LOCAL_GPU_ID": local_gpu_id,
                "CUDA_VISIBLE_DEVICES": local_gpu_id,
            },
            "memory_limit": gpu_memory,
            "local_directory": str(local_root / f"gpu-{local_index}"),
            "dashboard": False,
            "silence_logs": logging.WARNING,
            "plugins": (WorkerDevicePlugin(),),
            "startup_information": {
                "workflowDevice": worker_device_startup_information,
            },
            "death_timeout": death_timeout,
            "reconnect": False,
            "security": security,
            "worker_port": worker_port_range,
            "port": nanny_port_range,
            **network,
        })
    return tuple(options)


async def _close_nannies(nannies: Sequence[Any], *, timeout: float) -> None:
    async def close_one(nanny: Any) -> None:
        await nanny.close(timeout=timeout, reason="slurm-worker-launcher-stop")

    results = await asyncio.gather(
        *(close_one(nanny) for nanny in reversed(tuple(nannies))),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, BaseException)]
    if failures:
        raise RuntimeError(f"Failed to close {len(failures)} Dask Nanny process(es).") from failures[0]


async def run_worker_launcher(
    request: WorkerLauncherRequest,
    allocation: WorkerAllocation,
    *,
    environment: Mapping[str, str] | None = None,
    stop_event: asyncio.Event | None = None,
    nanny_factory: Callable[..., Any] = Nanny,
) -> None:
    """Start this rank's Nannies concurrently and hold them until termination."""

    env = os.environ if environment is None else environment
    options = build_nanny_options(request, allocation, environment=env)
    if len(options) != allocation.cpu_workers + allocation.gpu_workers:
        raise RuntimeError("Internal Worker option count does not match the node plan.")
    stop = stop_event or asyncio.Event()
    nannies = [nanny_factory(**item) for item in options]
    startup_timeout = float(env.get("WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS", "300"))
    close_timeout = float(env.get("WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS", "120"))
    if startup_timeout <= 0 or close_timeout <= 0:
        raise WorkerLauncherValidationError("Dask startup and close timeouts must be positive.")

    try:
        await asyncio.wait_for(
            asyncio.gather(*(nanny.start() for nanny in nannies)),
            timeout=startup_timeout,
        )
        logger.info(
            "Worker launcher ready: execution=%s job=%s node=%s rank=%s "
            "cpu_workers=%s gpu_workers=%s addresses=%s",
            request.execution_id,
            allocation.job_id,
            allocation.node_name,
            allocation.rank,
            allocation.cpu_workers,
            allocation.gpu_workers,
            tuple(str(getattr(nanny, "worker_address", "")) for nanny in nannies),
        )

        stop_task = asyncio.create_task(stop.wait())
        finished_tasks = [asyncio.create_task(nanny.finished()) for nanny in nannies]
        done, pending = await asyncio.wait(
            (stop_task, *finished_tasks),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task not in done:
            raise RuntimeError(
                "A Dask Nanny exited before Slurm terminated the Worker allocation."
            )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    finally:
        await _close_nannies(nannies, timeout=close_timeout)


def _install_signal_handlers(stop_event: asyncio.Event) -> list[signal.Signals]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        installed.append(signum)
    return installed


async def run_request_path(
    request_path: Path,
    *,
    environment: Mapping[str, str] | None = None,
    hostname: str | None = None,
) -> None:
    request = _read_request(request_path)
    validate_code_revision(request)
    allocation = validate_slurm_worker_allocation(
        request,
        environment,
        hostname=hostname,
    )
    stop_event = asyncio.Event()
    installed = _install_signal_handlers(stop_event)
    try:
        await run_worker_launcher(
            request,
            allocation,
            environment=environment,
            stop_event=stop_event,
        )
    finally:
        loop = asyncio.get_running_loop()
        for signum in installed:
            loop.remove_signal_handler(signum)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request_path", type=Path)
    parser.add_argument(
        "--print-layout",
        action="store_true",
        help="print NODE_COUNT GPUS_PER_NODE for the Slurm holder script",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = _argument_parser().parse_args(argv)
    try:
        request = _read_request(args.request_path)
        if args.print_layout:
            print(
                request.resource_plan.node_count,
                request.resource_plan.gpus_per_allocated_node,
            )
            return 0
        asyncio.run(run_request_path(args.request_path))
        return 0
    except WorkerLauncherValidationError as exc:
        logger.error("Worker request rejected: %s", exc)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception:
        logger.exception("Slurm Worker launcher failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
