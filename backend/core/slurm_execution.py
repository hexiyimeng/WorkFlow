"""Pure helpers for submitting one WorkFlow execution to Slurm.

This module deliberately has no environment or subprocess dependencies.  The
service layer owns configuration loading and must invoke the returned argv with
``shell=False``.  Keeping resource arithmetic and input validation here makes
it possible to validate a graph-derived request before anything is submitted.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re
from typing import Iterable


_PARTITION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_SAFE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_SAFE_COMMENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z")
_SBATCH_RESULT_RE = re.compile(
    r"(?P<job_id>[1-9][0-9]*)(?:;(?P<cluster>[A-Za-z0-9][A-Za-z0-9_.-]{0,63}))?\Z"
)
_SLURM_JOB_ID_RE = re.compile(r"[1-9][0-9]*\Z")
_SCONTROL_FIELD_RE = re.compile(r"(?:^|\s)([A-Za-z][A-Za-z0-9_]*)=(\S*)")
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{number}" for number in range(1, 10)),
    *(f"LPT{number}" for number in range(1, 10)),
}


def _require_int(value: object, *, name: str, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        qualifier = "positive" if minimum == 1 else "non-negative"
        raise ValueError(f"{name} must be a {qualifier} integer, got {value!r}.")
    return value


def _validate_partition(value: object, *, name: str = "partition") -> str:
    if not isinstance(value, str) or not _PARTITION_RE.fullmatch(value):
        raise ValueError(
            f"{name} must contain only letters, digits, '.', '_' or '-' and "
            f"must start with a letter or digit, got {value!r}."
        )
    return value


def _slurm_time_seconds(value: object, *, name: str = "time_limit") -> int:
    """Validate a finite Slurm 19.05 time value and return its duration.

    Supported forms are the portable Slurm forms ``minutes``,
    ``minutes:seconds``, ``hours:minutes:seconds`` and
    ``days-hours[:minutes[:seconds]]``.
    """

    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty Slurm time string.")

    match = re.fullmatch(r"(?:(?P<days>[0-9]+)-)?(?P<body>[0-9]+(?::[0-9]{1,2}){0,2})", value)
    if match is None:
        raise ValueError(f"{name} is not a valid finite Slurm time value: {value!r}.")

    days_text = match.group("days")
    fields = [int(field) for field in match.group("body").split(":")]
    days = int(days_text) if days_text is not None else 0

    if days_text is not None:
        hours = fields[0]
        minutes = fields[1] if len(fields) >= 2 else 0
        seconds = fields[2] if len(fields) == 3 else 0
        if hours >= 24 or minutes >= 60 or seconds >= 60:
            raise ValueError(f"{name} has an out-of-range component: {value!r}.")
    elif len(fields) == 1:
        hours = 0
        minutes = fields[0]
        seconds = 0
    elif len(fields) == 2:
        hours = 0
        minutes, seconds = fields
        if seconds >= 60:
            raise ValueError(f"{name} has an out-of-range seconds value: {value!r}.")
    else:
        hours, minutes, seconds = fields
        if minutes >= 60 or seconds >= 60:
            raise ValueError(f"{name} has an out-of-range component: {value!r}.")

    total = (((days * 24) + hours) * 60 + minutes) * 60 + seconds
    if total <= 0:
        raise ValueError(f"{name} must request a positive duration, got {value!r}.")
    return total


@dataclass(frozen=True, slots=True, kw_only=True)
class SlurmResourceRequest:
    """Validated homogeneous Slurm allocation for one execution.

    ``cpu_workers`` and ``gpu_workers`` are workflow-wide totals.  ``cpus``,
    ``gpus`` and ``memory_gib`` are deliberately *per-node* Slurm requests;
    homogeneous allocations apply those values to every allocated node.  The
    explicit Worker placement lets the compute launcher start only the planned
    processes on each node while keeping the single-node representation fully
    backward compatible.
    """

    cpu_workers: int
    gpu_workers: int
    nodes: int
    cpus: int
    gpus: int
    memory_gib: int
    time_limit: str
    partition: str
    cpu_workers_by_node: tuple[int, ...] = ()
    gpu_workers_by_node: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        _require_int(self.cpu_workers, name="cpu_workers", minimum=0)
        _require_int(self.gpu_workers, name="gpu_workers", minimum=0)
        if self.cpu_workers + self.gpu_workers <= 0:
            raise ValueError("At least one CPU or GPU Worker must be requested.")
        nodes = _require_int(self.nodes, name="nodes", minimum=1)

        cpu_placement = self.cpu_workers_by_node
        if not cpu_placement:
            if nodes != 1:
                raise ValueError(
                    "cpu_workers_by_node is required for a multi-node request."
                )
            cpu_placement = (self.cpu_workers,)
        gpu_placement = self.gpu_workers_by_node
        if not gpu_placement:
            if nodes != 1:
                raise ValueError(
                    "gpu_workers_by_node is required for a multi-node request."
                )
            gpu_placement = (self.gpu_workers,)

        for name, placement, expected_total in (
            ("cpu_workers_by_node", cpu_placement, self.cpu_workers),
            ("gpu_workers_by_node", gpu_placement, self.gpu_workers),
        ):
            if not isinstance(placement, tuple) or len(placement) != nodes:
                raise ValueError(f"{name} must be a tuple with one entry per node.")
            for index, value in enumerate(placement):
                _require_int(value, name=f"{name}[{index}]", minimum=0)
            if sum(placement) != expected_total:
                raise ValueError(
                    f"{name} sums to {sum(placement)}, expected {expected_total}."
                )
        if any(
            cpu_count + gpu_count <= 0
            for cpu_count, gpu_count in zip(cpu_placement, gpu_placement)
        ):
            raise ValueError("Every allocated node must run at least one Worker.")

        object.__setattr__(self, "cpu_workers_by_node", cpu_placement)
        object.__setattr__(self, "gpu_workers_by_node", gpu_placement)

        _require_int(self.cpus, name="cpus", minimum=1)
        maximum_local_workers = max(
            cpu_count + gpu_count
            for cpu_count, gpu_count in zip(cpu_placement, gpu_placement)
        )
        if self.cpus < maximum_local_workers:
            raise ValueError(
                "cpus must provide at least one CPU slot for every CPU and GPU "
                "Worker placed on one node."
            )
        _require_int(self.gpus, name="gpus", minimum=0)
        _require_int(self.memory_gib, name="memory_gib", minimum=1)
        maximum_local_gpu_workers = max(gpu_placement)
        if self.gpus != maximum_local_gpu_workers:
            raise ValueError(
                "gpus must exactly equal the maximum GPU Worker count placed on "
                "one node so every GPU Worker receives one allocated GPU."
            )
        _slurm_time_seconds(self.time_limit)
        _validate_partition(self.partition)

    @property
    def time(self) -> str:
        """Alias matching Slurm's ``--time`` terminology."""

        return self.time_limit

    @property
    def total_cpus(self) -> int:
        return self.nodes * self.cpus

    @property
    def total_gpus(self) -> int:
        return self.nodes * self.gpus

    @property
    def total_memory_gib(self) -> int:
        return self.nodes * self.memory_gib

    def to_dict(self) -> dict[str, object]:
        return {
            "cpuWorkers": self.cpu_workers,
            "gpuWorkers": self.gpu_workers,
            "nodes": self.nodes,
            "cpus": self.cpus,
            "gpus": self.gpus,
            "memoryGiB": self.memory_gib,
            "totalCpus": self.total_cpus,
            "totalGpus": self.total_gpus,
            "totalMemoryGiB": self.total_memory_gib,
            "cpuWorkersByNode": list(self.cpu_workers_by_node),
            "gpuWorkersByNode": list(self.gpu_workers_by_node),
            "timeLimit": self.time_limit,
            "partition": self.partition,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class SlurmPolicy:
    """Administrator-provided ceilings and per-Worker allocation policy."""

    partition: str
    time_limit: str
    cpus_per_cpu_worker: int
    cpus_per_gpu_worker: int
    memory_gib_per_cpu_worker: int
    memory_gib_per_gpu_worker: int
    max_cpu_workers: int
    max_gpu_workers: int
    max_cpus: int
    max_gpus: int
    max_memory_gib: int
    max_nodes: int
    cpus_per_node: int
    gpus_per_node: int
    memory_gib_per_node: int
    base_cpus: int = 1
    base_memory_gib: int = 1
    allowed_partitions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        partition = _validate_partition(self.partition)
        _slurm_time_seconds(self.time_limit)

        for field_name in (
            "base_cpus",
            "cpus_per_cpu_worker",
            "cpus_per_gpu_worker",
            "memory_gib_per_cpu_worker",
            "memory_gib_per_gpu_worker",
            "max_cpus",
            "max_memory_gib",
            "max_nodes",
            "cpus_per_node",
            "memory_gib_per_node",
        ):
            _require_int(getattr(self, field_name), name=field_name, minimum=1)
        for field_name in (
            "base_memory_gib",
            "max_cpu_workers",
            "max_gpu_workers",
            "max_gpus",
            "gpus_per_node",
        ):
            _require_int(getattr(self, field_name), name=field_name, minimum=0)
        if self.max_cpu_workers + self.max_gpu_workers <= 0:
            raise ValueError("The Slurm policy must allow at least one Worker.")

        raw_allowed = self.allowed_partitions
        if not isinstance(raw_allowed, tuple):
            raise ValueError("allowed_partitions must be a tuple of partition names.")
        allowed = raw_allowed or (partition,)
        if len(set(allowed)) != len(allowed):
            raise ValueError("allowed_partitions must not contain duplicates.")
        for index, candidate in enumerate(allowed):
            _validate_partition(candidate, name=f"allowed_partitions[{index}]")
        if partition not in allowed:
            raise ValueError(
                f"partition {partition!r} is not present in allowed_partitions."
            )
        object.__setattr__(self, "allowed_partitions", allowed)

        if self.base_cpus > self.max_cpus:
            raise ValueError("base_cpus exceeds max_cpus.")
        if self.base_cpus > self.cpus_per_node:
            raise ValueError("base_cpus exceeds cpus_per_node.")
        if self.base_memory_gib > self.max_memory_gib:
            raise ValueError("base_memory_gib exceeds max_memory_gib.")
        if self.base_memory_gib > self.memory_gib_per_node:
            raise ValueError("base_memory_gib exceeds memory_gib_per_node.")
        if self.max_gpu_workers > self.max_gpus:
            raise ValueError("max_gpu_workers exceeds max_gpus.")

    def resource_request(
        self,
        *,
        cpu_workers: int,
        gpu_workers: int,
        partition: str | None = None,
    ) -> SlurmResourceRequest:
        """Convert graph Worker totals into a bounded homogeneous request."""

        return build_slurm_resource_request(
            self,
            cpu_workers=cpu_workers,
            gpu_workers=gpu_workers,
            partition=partition,
        )


def _balanced_counts(total: int, nodes: int) -> tuple[int, ...]:
    """Spread identical Workers deterministically with a difference of at most one."""

    quotient, remainder = divmod(total, nodes)
    return tuple(
        quotient + (1 if index < remainder else 0)
        for index in range(nodes)
    )


def _cpu_capacity_for_gpu_count(policy: SlurmPolicy, gpu_count: int) -> int:
    """Return how many CPU Workers still fit beside ``gpu_count`` GPU Workers."""

    remaining_cpus = (
        policy.cpus_per_node
        - policy.base_cpus
        - gpu_count * policy.cpus_per_gpu_worker
    )
    remaining_memory = (
        policy.memory_gib_per_node
        - policy.base_memory_gib
        - gpu_count * policy.memory_gib_per_gpu_worker
    )
    if remaining_cpus < 0 or remaining_memory < 0:
        return -1
    return min(
        remaining_cpus // policy.cpus_per_cpu_worker,
        remaining_memory // policy.memory_gib_per_cpu_worker,
    )


def _place_cpu_workers(
    policy: SlurmPolicy,
    *,
    cpu_workers: int,
    gpu_workers_by_node: tuple[int, ...],
) -> tuple[int, ...] | None:
    """Place CPU Workers without exceeding either per-node resource envelope.

    GPU placement is balanced first because homogeneous ``--gres`` applies its
    maximum to every node.  CPU Workers are then assigned to the node whose
    projected CPU/memory utilization is lowest.  The stable node-index tie
    breaker makes identical inputs produce an identical launcher plan.
    """

    capacities = tuple(
        _cpu_capacity_for_gpu_count(policy, gpu_count)
        for gpu_count in gpu_workers_by_node
    )
    if any(capacity < 0 for capacity in capacities):
        return None
    if sum(capacities) < cpu_workers:
        return None

    placement = [0] * len(gpu_workers_by_node)
    remaining = cpu_workers

    # Homogeneous allocations must not contain a node on which the launcher
    # starts no Worker.  Reserve one CPU Worker for each GPU-free node first.
    for index, gpu_count in enumerate(gpu_workers_by_node):
        if gpu_count != 0:
            continue
        if remaining <= 0 or capacities[index] <= 0:
            return None
        placement[index] = 1
        remaining -= 1

    while remaining:
        candidates: list[tuple[tuple[float, float, float, int, int], int]] = []
        for index, (cpu_count, gpu_count, capacity) in enumerate(
            zip(placement, gpu_workers_by_node, capacities)
        ):
            if cpu_count >= capacity:
                continue
            projected_cpu_count = cpu_count + 1
            projected_cpus = (
                policy.base_cpus
                + projected_cpu_count * policy.cpus_per_cpu_worker
                + gpu_count * policy.cpus_per_gpu_worker
            )
            projected_memory = (
                policy.base_memory_gib
                + projected_cpu_count * policy.memory_gib_per_cpu_worker
                + gpu_count * policy.memory_gib_per_gpu_worker
            )
            cpu_fraction = projected_cpus / policy.cpus_per_node
            memory_fraction = projected_memory / policy.memory_gib_per_node
            candidates.append((
                (
                    max(cpu_fraction, memory_fraction),
                    cpu_fraction,
                    memory_fraction,
                    projected_cpu_count,
                    index,
                ),
                index,
            ))
        if not candidates:
            return None
        _, selected_index = min(candidates)
        placement[selected_index] += 1
        remaining -= 1

    return tuple(placement)


def build_slurm_resource_request(
    policy: SlurmPolicy,
    *,
    cpu_workers: int,
    gpu_workers: int,
    partition: str | None = None,
) -> SlurmResourceRequest:
    """Build the smallest feasible homogeneous allocation for graph counts.

    Placement is deterministic and uses no scheduler state.  That keeps
    preflight read-only while allowing the service to persist the exact plan
    before ``sbatch``.  Slurm may select any matching hosts; node ranks map to
    the placement tuple after the allocation is granted.
    """

    cpu_workers = _require_int(cpu_workers, name="cpu_workers", minimum=0)
    gpu_workers = _require_int(gpu_workers, name="gpu_workers", minimum=0)
    if cpu_workers + gpu_workers <= 0:
        raise ValueError("At least one CPU or GPU Worker must be requested.")
    if cpu_workers > policy.max_cpu_workers:
        raise ValueError(
            f"cpu_workers={cpu_workers} exceeds policy limit "
            f"{policy.max_cpu_workers}."
        )
    if gpu_workers > policy.max_gpu_workers:
        raise ValueError(
            f"gpu_workers={gpu_workers} exceeds policy limit "
            f"{policy.max_gpu_workers}."
        )

    selected_partition = policy.partition if partition is None else _validate_partition(partition)
    if selected_partition not in policy.allowed_partitions:
        raise ValueError(
            f"partition {selected_partition!r} is not allowed; expected one of "
            f"{policy.allowed_partitions!r}."
        )

    minimum_cpus = (
        policy.base_cpus
        + cpu_workers * policy.cpus_per_cpu_worker
        + gpu_workers * policy.cpus_per_gpu_worker
    )
    minimum_memory_gib = (
        policy.base_memory_gib
        + cpu_workers * policy.memory_gib_per_cpu_worker
        + gpu_workers * policy.memory_gib_per_gpu_worker
    )

    # These are optimistic whole-execution lower bounds (only one copy of the
    # base overhead).  Rejecting them early preserves clear errors for requests
    # that cannot fit under the global policy regardless of placement.
    if minimum_cpus > policy.max_cpus:
        raise ValueError(
            f"Calculated cpus={minimum_cpus} exceeds policy limit {policy.max_cpus}."
        )
    if gpu_workers > policy.max_gpus:
        raise ValueError(
            f"Calculated gpus={gpu_workers} exceeds policy limit {policy.max_gpus}."
        )
    if minimum_memory_gib > policy.max_memory_gib:
        raise ValueError(
            f"Calculated memory_gib={minimum_memory_gib} exceeds policy limit "
            f"{policy.max_memory_gib}."
        )

    maximum_useful_nodes = min(
        policy.max_nodes,
        cpu_workers + gpu_workers,
    )
    for nodes in range(1, maximum_useful_nodes + 1):
        gpu_placement = _balanced_counts(gpu_workers, nodes)
        if max(gpu_placement) > policy.gpus_per_node:
            continue
        cpu_placement = _place_cpu_workers(
            policy,
            cpu_workers=cpu_workers,
            gpu_workers_by_node=gpu_placement,
        )
        if cpu_placement is None:
            continue

        node_cpus = tuple(
            policy.base_cpus
            + local_cpu_workers * policy.cpus_per_cpu_worker
            + local_gpu_workers * policy.cpus_per_gpu_worker
            for local_cpu_workers, local_gpu_workers in zip(
                cpu_placement,
                gpu_placement,
            )
        )
        node_memory = tuple(
            policy.base_memory_gib
            + local_cpu_workers * policy.memory_gib_per_cpu_worker
            + local_gpu_workers * policy.memory_gib_per_gpu_worker
            for local_cpu_workers, local_gpu_workers in zip(
                cpu_placement,
                gpu_placement,
            )
        )
        cpus = max(node_cpus)
        gpus = max(gpu_placement)
        memory_gib = max(node_memory)
        if cpus > policy.cpus_per_node or memory_gib > policy.memory_gib_per_node:
            continue

        request = SlurmResourceRequest(
            cpu_workers=cpu_workers,
            gpu_workers=gpu_workers,
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
            memory_gib=memory_gib,
            time_limit=policy.time_limit,
            partition=selected_partition,
            cpu_workers_by_node=cpu_placement,
            gpu_workers_by_node=gpu_placement,
        )
        if (
            request.total_cpus <= policy.max_cpus
            and request.total_gpus <= policy.max_gpus
            and request.total_memory_gib <= policy.max_memory_gib
        ):
            return request

    raise ValueError(
        "Graph Worker requirements cannot be placed in a homogeneous Slurm "
        "allocation within the configured node and execution limits: "
        f"cpu_workers={cpu_workers}, gpu_workers={gpu_workers}, "
        f"max_nodes={policy.max_nodes}, cpus_per_node={policy.cpus_per_node}, "
        f"gpus_per_node={policy.gpus_per_node}, "
        f"memory_gib_per_node={policy.memory_gib_per_node}."
    )


def _safe_argv_value(value: os.PathLike[str] | str, *, name: str) -> str:
    result = os.fspath(value)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{name} must be a non-empty path or string.")
    if any(character in result for character in ("\0", "\r", "\n")):
        raise ValueError(f"{name} contains a forbidden control character.")
    return result


def _absolute_path(value: os.PathLike[str] | str, *, name: str) -> str:
    result = _safe_argv_value(value, name=name)
    if not Path(result).is_absolute():
        raise ValueError(f"{name} must be an absolute path, got {result!r}.")
    return result


def build_sbatch_argv(
    request: SlurmResourceRequest,
    *,
    script_path: os.PathLike[str] | str,
    job_name: str,
    output_path: os.PathLike[str] | str,
    error_path: os.PathLike[str] | str | None = None,
    work_directory: os.PathLike[str] | str | None = None,
    script_arguments: Iterable[os.PathLike[str] | str] = (),
    sbatch_executable: os.PathLike[str] | str = "sbatch",
    comment: str | None = None,
) -> tuple[str, ...]:
    """Return an injection-safe argv for ``subprocess(..., shell=False)``.

    The flags used here are supported by Slurm 19.05.  In particular this uses
    ``--gres=gpu:N`` rather than the newer ``--gpus`` family of options.
    """

    if not isinstance(request, SlurmResourceRequest):
        raise TypeError("request must be a SlurmResourceRequest.")
    executable = _safe_argv_value(sbatch_executable, name="sbatch_executable")
    if not _SAFE_NAME_RE.fullmatch(job_name):
        raise ValueError(
            "job_name must contain only letters, digits, '_' or '-' and must "
            "start with a letter or digit."
        )
    script = _absolute_path(script_path, name="script_path")
    output = _absolute_path(output_path, name="output_path")

    argv = [
        executable,
        "--parsable",
        "--export=NONE",
        f"--partition={request.partition}",
        f"--nodes={request.nodes}",
        f"--ntasks={request.nodes}",
        "--ntasks-per-node=1",
        f"--cpus-per-task={request.cpus}",
        f"--mem={request.memory_gib}G",
        f"--time={request.time_limit}",
        f"--job-name={job_name}",
        f"--output={output}",
    ]
    if comment is not None:
        if not isinstance(comment, str) or _SAFE_COMMENT_RE.fullmatch(comment) is None:
            raise ValueError(
                "comment must be 1-128 ASCII letters, digits, ':', '.', '_' or "
                "'-', starting with a letter or digit."
            )
        argv.append(f"--comment={comment}")
    if error_path is not None:
        argv.append(f"--error={_absolute_path(error_path, name='error_path')}")
    if work_directory is not None:
        argv.append(
            f"--chdir={_absolute_path(work_directory, name='work_directory')}"
        )
    if request.gpus:
        argv.append(f"--gres=gpu:{request.gpus}")

    argv.append(script)
    argv.extend(
        _safe_argv_value(argument, name="script_argument")
        for argument in script_arguments
    )
    return tuple(argv)


@dataclass(frozen=True, slots=True)
class SbatchSubmission:
    job_id: str
    cluster: str | None = None


@dataclass(frozen=True, slots=True)
class ScontrolJobRecord:
    """A strictly identified root allocation returned by ``scontrol -o``."""

    job_id: str
    state: str
    exit_code: str = ""
    node_list: str = ""
    comment: str = ""


def parse_scontrol_job_record(
    output: str,
    *,
    expected_job_id: str,
) -> ScontrolJobRecord | None:
    """Parse one exact root-job record from Slurm 19.05 ``scontrol`` output.

    This helper is intentionally fail-closed.  ``scontrol show job -o`` is
    expected to emit exactly one single-line record for the requested root
    allocation.  Empty output, extra records, a step/array identifier, missing
    state, or duplicate required fields is therefore not evidence that a job
    ended.
    """

    if not isinstance(output, str):
        raise TypeError("scontrol output must be a string.")
    if not isinstance(expected_job_id, str) or not _SLURM_JOB_ID_RE.fullmatch(
        expected_job_id
    ):
        raise ValueError("expected_job_id must be a positive decimal job ID.")

    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) != 1:
        return None

    fields: dict[str, str] = {}
    duplicated: set[str] = set()
    for match in _SCONTROL_FIELD_RE.finditer(lines[0]):
        name, value = match.groups()
        if name in fields:
            duplicated.add(name)
        fields[name] = value

    required = {"JobId", "JobState"}
    if duplicated.intersection(required):
        return None
    if fields.get("JobId") != expected_job_id:
        return None
    raw_state = fields.get("JobState", "")
    if not raw_state:
        return None
    state = raw_state.split("+", 1)[0].upper()
    if not state or re.fullmatch(r"[A-Z_]+", state) is None:
        return None
    return ScontrolJobRecord(
        job_id=expected_job_id,
        state=state,
        exit_code=fields.get("ExitCode", ""),
        node_list=fields.get("NodeList", ""),
        comment=fields.get("Comment", ""),
    )


def parse_sbatch_submission(output: str | bytes) -> SbatchSubmission:
    """Parse ``sbatch --parsable`` output (``jobid`` or ``jobid;cluster``)."""

    if isinstance(output, bytes):
        try:
            output = output.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("sbatch output must be ASCII.") from exc
    if not isinstance(output, str):
        raise TypeError("sbatch output must be str or bytes.")
    normalized = output.strip()
    match = _SBATCH_RESULT_RE.fullmatch(normalized)
    if match is None:
        raise ValueError(f"Invalid sbatch --parsable output: {output!r}.")
    return SbatchSubmission(
        job_id=match.group("job_id"),
        cluster=match.group("cluster"),
    )


def parse_sbatch_job_id(output: str | bytes) -> str:
    """Return only the numeric job ID from ``sbatch --parsable`` output."""

    return parse_sbatch_submission(output).job_id


def validate_execution_id(execution_id: object) -> str:
    """Validate the identifier used as a direct child directory name."""

    if not isinstance(execution_id, str) or not _SAFE_NAME_RE.fullmatch(execution_id):
        raise ValueError(
            "execution_id must be 1-128 ASCII letters, digits, '_' or '-', "
            "starting with a letter or digit."
        )
    if execution_id.upper() in _WINDOWS_RESERVED_NAMES:
        raise ValueError(f"execution_id {execution_id!r} is a reserved path name.")
    return execution_id


def _reject_symlink(path: Path, *, name: str) -> None:
    # ``is_symlink`` uses lstat and therefore also catches dangling links.
    if path.is_symlink():
        raise ValueError(f"{name} must not be a symbolic link: {path}.")


def resolve_execution_directory(
    execution_root: os.PathLike[str] | str,
    execution_id: object,
    *,
    must_exist: bool = False,
) -> Path:
    """Resolve one execution directory below a fixed, non-symlink root.

    This function does not create directories.  The caller should create the
    configured root during service startup, then use the returned direct child
    path.  Existing execution paths must be real directories, never symlinks.
    """

    safe_id = validate_execution_id(execution_id)
    root = Path(execution_root)
    if not root.is_absolute():
        raise ValueError(f"execution_root must be absolute, got {root!s}.")
    _reject_symlink(root, name="execution_root")
    if not root.exists():
        raise ValueError(f"execution_root does not exist: {root}.")
    if not root.is_dir():
        raise ValueError(f"execution_root is not a directory: {root}.")

    resolved_root = root.resolve(strict=True)
    candidate = root / safe_id
    _reject_symlink(candidate, name="execution directory")
    if candidate.exists():
        if not candidate.is_dir():
            raise ValueError(f"Execution path is not a directory: {candidate}.")
        resolved_candidate = candidate.resolve(strict=True)
    else:
        if must_exist:
            raise ValueError(f"Execution directory does not exist: {candidate}.")
        resolved_candidate = resolved_root / safe_id

    try:
        relative = resolved_candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("Execution directory escapes the configured root.") from exc
    if len(relative.parts) != 1 or relative.name != safe_id:
        raise ValueError("Execution directory must be a direct child of its root.")
    return resolved_candidate


__all__ = [
    "SbatchSubmission",
    "ScontrolJobRecord",
    "SlurmPolicy",
    "SlurmResourceRequest",
    "build_sbatch_argv",
    "build_slurm_resource_request",
    "parse_sbatch_job_id",
    "parse_sbatch_submission",
    "parse_scontrol_job_record",
    "resolve_execution_directory",
    "validate_execution_id",
]
