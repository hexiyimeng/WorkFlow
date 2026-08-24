"""Start the Resource Planner's Dask Workers on one allocated compute node."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import socket
import stat
from typing import Any, Callable, Mapping, Sequence

from distributed import Nanny, Security

from core.slurm_execution import validate_execution_id
from core.worker_profiles import normalize_worker_profile
from services.dask_service import WorkerDevicePlugin, worker_device_startup_information


logger = logging.getLogger("WorkFlow.SlurmWorkerLauncher")
REQUEST_SCHEMA_VERSION = 3
_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z")
_JOB_ID_RE = re.compile(r"[1-9][0-9]*\Z")


class WorkerLauncherValidationError(ValueError):
    pass


def _integer(value: object, *, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise WorkerLauncherValidationError(
            f"{name} must be an integer >= {minimum}, got {value!r}."
        )
    return value


def _environment_integer(value: object, *, name: str, minimum: int = 0) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise WorkerLauncherValidationError(f"{name} must be an integer.") from exc
    return _integer(parsed, name=name, minimum=minimum)


def _port_range(value: object, *, name: str) -> tuple[int, int]:
    match = re.fullmatch(r"([0-9]{1,5}):([0-9]{1,5})", str(value))
    if match is None:
        raise WorkerLauncherValidationError(f"{name} must use START:END syntax.")
    start, stop = (int(item) for item in match.groups())
    if start < 1024 or stop > 65535 or start > stop:
        raise WorkerLauncherValidationError(f"{name} is outside the allowed port range.")
    return start, stop


@dataclass(frozen=True, slots=True)
class PlannedWorker:
    allocation_id: str
    profile: str
    node: str
    index: int
    threads: int
    memory_gib: int
    gpu: int
    logical_resources: Mapping[str, float]


@dataclass(frozen=True, slots=True)
class WorkerLauncherRequest:
    execution_id: str
    submission_token: str
    scheduler_address: str
    workers: tuple[PlannedWorker, ...]
    planned_nodes: tuple[str, ...]
    node_resources: Mapping[str, tuple[int, int, int]]
    runtime_directory: Path
    security: Mapping[str, Path] | None
    allow_insecure: bool
    network_interface: str | None
    worker_port_range: tuple[int, int]
    nanny_port_range: tuple[int, int]

    @property
    def maximum_gpus_per_node(self) -> int:
        return max((resources[2] for resources in self.node_resources.values()), default=0)


def _parse_security(value: object, *, allow_insecure: bool) -> Mapping[str, Path] | None:
    if value is None:
        if not allow_insecure:
            raise WorkerLauncherValidationError("security=null requires allowInsecure=true.")
        return None
    if allow_insecure or not isinstance(value, Mapping):
        raise WorkerLauncherValidationError("TLS security and allowInsecure are mutually exclusive.")
    expected = {"tlsCaFile", "tlsCertFile", "tlsKeyFile"}
    if set(value) != expected:
        raise WorkerLauncherValidationError("security has an invalid schema.")
    result: dict[str, Path] = {}
    for name in expected:
        path = Path(value[name]) if isinstance(value[name], str) else Path()
        if not path.is_absolute():
            raise WorkerLauncherValidationError(f"security.{name} must be absolute.")
        result[name] = path
    return result


def parse_worker_launcher_request(payload: Mapping[str, Any]) -> WorkerLauncherRequest:
    if payload.get("schemaVersion") != REQUEST_SCHEMA_VERSION:
        raise WorkerLauncherValidationError("Unsupported Worker launcher request schema.")
    execution_id = validate_execution_id(payload.get("executionId"))
    token = payload.get("submissionToken")
    if not isinstance(token, str) or _TOKEN_RE.fullmatch(token) is None:
        raise WorkerLauncherValidationError("submissionToken is invalid.")
    scheduler_address = payload.get("schedulerAddress")
    if not isinstance(scheduler_address, str) or not scheduler_address.startswith(("tcp://", "tls://")):
        raise WorkerLauncherValidationError("schedulerAddress must be a Dask TCP/TLS address.")
    runtime = Path(payload.get("runtimeDirectory", ""))
    if not runtime.is_absolute():
        raise WorkerLauncherValidationError("runtimeDirectory must be absolute.")
    allow_insecure = payload.get("allowInsecure")
    if type(allow_insecure) is not bool:
        raise WorkerLauncherValidationError("allowInsecure must be a boolean.")
    security = _parse_security(payload.get("security"), allow_insecure=allow_insecure)

    raw_plan = payload.get("allocationPlan")
    if not isinstance(raw_plan, Mapping):
        raise WorkerLauncherValidationError("allocationPlan must be an object.")
    raw_nodes = raw_plan.get("nodes")
    raw_jobs = raw_plan.get("jobs")
    if not isinstance(raw_nodes, list) or not raw_nodes or not isinstance(raw_jobs, list):
        raise WorkerLauncherValidationError("allocationPlan must contain nodes and jobs arrays.")
    node_resources: dict[str, tuple[int, int, int]] = {}
    for index, item in enumerate(raw_nodes):
        if not isinstance(item, Mapping) or not isinstance(item.get("node"), str):
            raise WorkerLauncherValidationError(f"allocationPlan.nodes[{index}] is invalid.")
        name = item["node"]
        if name in node_resources:
            raise WorkerLauncherValidationError(f"Duplicate planned node {name!r}.")
        node_resources[name] = (
            _integer(item.get("cpu"), name=f"nodes[{index}].cpu", minimum=1),
            _integer(item.get("memoryGiB"), name=f"nodes[{index}].memoryGiB", minimum=1),
            _integer(item.get("gpu"), name=f"nodes[{index}].gpu"),
        )

    workers: list[PlannedWorker] = []
    allocation_ids: set[str] = set()
    accumulated: dict[str, list[int]] = {
        node: [0, 0, 0] for node in node_resources
    }
    for job_index, item in enumerate(raw_jobs):
        if not isinstance(item, Mapping):
            raise WorkerLauncherValidationError(f"allocationPlan.jobs[{job_index}] is invalid.")
        node = item.get("node")
        profile = normalize_worker_profile(
            item.get("profile"),
            owner=f"allocationPlan.jobs[{job_index}]",
        )
        allocation_id = item.get("allocationId")
        slurm = item.get("slurm")
        logical = item.get("logicalResources")
        count = _integer(item.get("workers"), name=f"jobs[{job_index}].workers", minimum=1)
        threads = _integer(item.get("threads"), name=f"jobs[{job_index}].threads", minimum=1)
        if node not in node_resources or not isinstance(allocation_id, str) or not allocation_id:
            raise WorkerLauncherValidationError(f"allocationPlan.jobs[{job_index}] identity is invalid.")
        if allocation_id in allocation_ids:
            raise WorkerLauncherValidationError(f"Duplicate allocationId {allocation_id!r}.")
        allocation_ids.add(allocation_id)
        if not isinstance(slurm, Mapping) or not isinstance(logical, Mapping):
            raise WorkerLauncherValidationError(f"allocationPlan.jobs[{job_index}] resources are invalid.")
        if slurm.get("nodes") != 1 or slurm.get("nodelist") != [node]:
            raise WorkerLauncherValidationError(
                "Each planned job must request exactly its single target node."
            )
        processes = _integer(item.get("processes"), name="job.processes", minimum=1)
        if processes != count:
            raise WorkerLauncherValidationError("job.processes must equal job.workers.")
        total_cpu = _integer(slurm.get("cpus"), name="job.cpus", minimum=1)
        total_memory = _integer(slurm.get("memoryGiB"), name="job.memoryGiB", minimum=1)
        total_gpu = _integer(slurm.get("gpus"), name="job.gpus")
        if total_cpu % count != 0 or total_memory % count != 0 or total_gpu % count != 0:
            raise WorkerLauncherValidationError("Job resources must divide exactly across its Workers.")
        gpu_per_worker = total_gpu // count
        if gpu_per_worker not in {0, 1}:
            raise WorkerLauncherValidationError("Each Dask Worker may use zero or one GPU.")
        logical_resources: dict[str, float] = {}
        for name, amount in logical.items():
            if not isinstance(name, str) or isinstance(amount, bool) or not isinstance(amount, (int, float)) or amount <= 0:
                raise WorkerLauncherValidationError("logicalResources must contain positive numbers.")
            logical_resources[name] = float(amount)
        if logical_resources.get(profile) != 1:
            raise WorkerLauncherValidationError("A Worker must advertise its Profile with value 1.")
        if logical_resources.get("CPU") != total_cpu / count:
            raise WorkerLauncherValidationError("Logical CPU must equal physical CPU per Worker.")
        expected_gpu = total_gpu / count
        if logical_resources.get("GPU", 0) != expected_gpu:
            raise WorkerLauncherValidationError("Logical GPU must equal physical GPU per Worker.")
        accumulated[node][0] += total_cpu
        accumulated[node][1] += total_memory
        accumulated[node][2] += total_gpu
        for worker_index in range(count):
            workers.append(PlannedWorker(
                allocation_id=allocation_id,
                profile=profile,
                node=node,
                index=worker_index,
                threads=threads,
                memory_gib=total_memory // count,
                gpu=gpu_per_worker,
                logical_resources=logical_resources,
            ))

    for node, planned_resources in accumulated.items():
        if tuple(planned_resources) != node_resources[node]:
            raise WorkerLauncherValidationError(
                f"Planned jobs do not match aggregate resources for node {node!r}."
            )

    maximum_local = max(
        sum(1 for worker in workers if worker.node == node)
        for node in node_resources
    )
    worker_ports = _port_range(payload.get("workerPortRange"), name="workerPortRange")
    nanny_ports = _port_range(payload.get("nannyPortRange"), name="nannyPortRange")
    if not (worker_ports[1] < nanny_ports[0] or nanny_ports[1] < worker_ports[0]):
        raise WorkerLauncherValidationError("Worker and Nanny port ranges overlap.")
    if any(stop - start + 1 < maximum_local for start, stop in (worker_ports, nanny_ports)):
        raise WorkerLauncherValidationError("Port ranges are narrower than the busiest planned node.")
    interface = payload.get("networkInterface")
    if interface is not None and not isinstance(interface, str):
        raise WorkerLauncherValidationError("networkInterface must be a string or null.")
    return WorkerLauncherRequest(
        execution_id=execution_id,
        submission_token=token,
        scheduler_address=scheduler_address,
        workers=tuple(workers),
        planned_nodes=tuple(node_resources),
        node_resources=node_resources,
        runtime_directory=runtime,
        security=security,
        allow_insecure=allow_insecure,
        network_interface=interface,
        worker_port_range=worker_ports,
        nanny_port_range=nanny_ports,
    )


def _visible_gpu_ids(environment: Mapping[str, str]) -> tuple[str, ...]:
    value = str(environment.get("CUDA_VISIBLE_DEVICES", "")).strip()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _current_node(environment: Mapping[str, str]) -> str:
    return str(environment.get("SLURMD_NODENAME") or socket.gethostname()).strip()


def build_nanny_options(
    request: WorkerLauncherRequest,
    *,
    environment: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], ...]:
    env = os.environ if environment is None else environment
    node = _current_node(env)
    if node not in request.node_resources:
        raise WorkerLauncherValidationError(
            f"Allocated node {node!r} is absent from the Resource Planner output."
        )
    planned = [worker for worker in request.workers if worker.node == node]
    allocated_cpus = _environment_integer(
        env.get("SLURM_CPUS_ON_NODE", env.get("SLURM_CPUS_PER_TASK", 0)),
        name="allocated CPUs",
        minimum=1,
    )
    required_cpu, _required_memory, required_gpu = request.node_resources[node]
    if allocated_cpus < required_cpu:
        raise WorkerLauncherValidationError("Slurm allocated fewer CPUs than planned.")
    visible = _visible_gpu_ids(env)
    if len(visible) < required_gpu:
        raise WorkerLauncherValidationError("Slurm exposed fewer GPUs than planned Gres.")
    security = None if request.security is None else Security(
        tls_ca_file=str(request.security["tlsCaFile"]),
        tls_worker_cert=str(request.security["tlsCertFile"]),
        tls_worker_key=str(request.security["tlsKeyFile"]),
        require_encryption=True,
    )
    local_root = Path(env.get("SLURM_TMPDIR") or (
        request.runtime_directory / "jobs" / request.execution_id / "worker-scratch" / node
    ))
    local_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    token_hash = hashlib.sha256(request.submission_token.encode("utf-8")).hexdigest()
    gpu_cursor = 0
    options: list[dict[str, Any]] = []
    for worker in planned:
        local_gpu = ""
        if worker.gpu:
            local_gpu = visible[gpu_cursor]
            gpu_cursor += 1
        role = "gpu" if worker.gpu else "cpu"
        worker_name = f"wf-{request.execution_id}-{worker.allocation_id}-{worker.index}"
        options.append({
            "scheduler_ip": request.scheduler_address,
            "name": worker_name,
            "nthreads": worker.threads,
            "resources": dict(worker.logical_resources),
            "env": {
                "WORKFLOW_DASK_WORKER_PROCESS": "1",
                "WORKFLOW_EXECUTION_ID": request.execution_id,
                "WORKFLOW_SUBMISSION_TOKEN_HASH": token_hash,
                "WORKFLOW_WORKER_PROFILE": worker.profile,
                "WORKFLOW_WORKER_ROLE": role,
                "WORKFLOW_PHYSICAL_GPU_ID": f"{node}:{local_gpu}" if local_gpu else "",
                "WORKFLOW_LOCAL_GPU_ID": local_gpu,
                "CUDA_VISIBLE_DEVICES": local_gpu,
                "OMP_NUM_THREADS": str(worker.threads),
                "MKL_NUM_THREADS": str(worker.threads),
                "OPENBLAS_NUM_THREADS": str(worker.threads),
                "NUMEXPR_NUM_THREADS": str(worker.threads),
                "WorkFlow_MODELS_DIR": str(request.runtime_directory / "models"),
                "CELLPOSE_LOCAL_MODELS_PATH": str(request.runtime_directory / "models" / "cellpose"),
            },
            "memory_limit": f"{worker.memory_gib}GB",
            "local_directory": str(local_root / worker_name),
            "dashboard": False,
            "silence_logs": logging.WARNING,
            "plugins": (WorkerDevicePlugin(),),
            "startup_information": {"workflowDevice": worker_device_startup_information},
            "death_timeout": float(env.get("WorkFlow_DASK_WORKER_CONNECT_TIMEOUT_SECONDS", "120")),
            "reconnect": False,
            "security": security,
            "worker_port": f"{request.worker_port_range[0]}:{request.worker_port_range[1]}",
            "port": f"{request.nanny_port_range[0]}:{request.nanny_port_range[1]}",
            **({"interface": request.network_interface} if request.network_interface else {"host": node}),
        })
    return tuple(options)


async def run_worker_launcher(
    request: WorkerLauncherRequest,
    *,
    environment: Mapping[str, str] | None = None,
    stop_event: asyncio.Event | None = None,
    nanny_factory: Callable[..., Any] = Nanny,
) -> None:
    options = build_nanny_options(request, environment=environment)
    if not options:
        raise WorkerLauncherValidationError("This allocated node has no planned Workers.")
    stop = stop_event or asyncio.Event()
    nannies = [nanny_factory(**option) for option in options]
    try:
        await asyncio.wait_for(
            asyncio.gather(*(nanny.start() for nanny in nannies)),
            timeout=300,
        )
        stop_task = asyncio.create_task(stop.wait())
        finished = [asyncio.create_task(nanny.finished()) for nanny in nannies]
        done, pending = await asyncio.wait((stop_task, *finished), return_when=asyncio.FIRST_COMPLETED)
        if stop_task not in done:
            raise RuntimeError("A planned Dask Nanny exited before Slurm termination.")
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    finally:
        await asyncio.gather(
            *(nanny.close(timeout=120, reason="slurm-worker-launcher-stop") for nanny in reversed(nannies)),
            return_exceptions=True,
        )


def _load_request(path: Path) -> WorkerLauncherRequest:
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise WorkerLauncherValidationError("Worker request must be a regular non-symlink file.")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        raise WorkerLauncherValidationError("Worker request must contain a JSON object.")
    return parse_worker_launcher_request(payload)


async def _async_main(path: Path) -> None:
    request = _load_request(path)
    env = os.environ
    if _JOB_ID_RE.fullmatch(str(env.get("SLURM_JOB_ID", ""))) is None:
        raise WorkerLauncherValidationError("SLURM_JOB_ID is required.")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    await run_worker_launcher(request, environment=env, stop_event=stop)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-layout", action="store_true")
    parser.add_argument("request_path")
    args = parser.parse_args(argv)
    request = _load_request(Path(args.request_path))
    if args.print_layout:
        print(len(request.planned_nodes), request.maximum_gpus_per_node)
        return 0
    asyncio.run(_async_main(Path(args.request_path)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "PlannedWorker",
    "WorkerLauncherRequest",
    "WorkerLauncherValidationError",
    "build_nanny_options",
    "parse_worker_launcher_request",
    "run_worker_launcher",
]
