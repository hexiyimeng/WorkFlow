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
    """Concrete Slurm resources supplied by the Resource Planner."""

    nodes: int
    cpus: int
    gpus: int
    memory_gib: int
    time_limit: str
    partition: str
    node_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        nodes = _require_int(self.nodes, name="nodes", minimum=1)
        _require_int(self.cpus, name="cpus", minimum=1)
        _require_int(self.gpus, name="gpus", minimum=0)
        _require_int(self.memory_gib, name="memory_gib", minimum=1)
        _slurm_time_seconds(self.time_limit)
        _validate_partition(self.partition)
        if self.node_names:
            if len(self.node_names) != nodes:
                raise ValueError("node_names must contain one name per requested node.")
            for index, name in enumerate(self.node_names):
                if not isinstance(name, str) or _PARTITION_RE.fullmatch(name) is None:
                    raise ValueError(f"node_names[{index}] is not a safe Slurm node name.")
            if len(set(self.node_names)) != len(self.node_names):
                raise ValueError("node_names must not contain duplicates.")

    @property
    def time(self) -> str:
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
            "nodes": self.nodes,
            "cpus": self.cpus,
            "gpus": self.gpus,
            "memoryGiB": self.memory_gib,
            "totalCpus": self.total_cpus,
            "totalGpus": self.total_gpus,
            "totalMemoryGiB": self.total_memory_gib,
            "timeLimit": self.time_limit,
            "partition": self.partition,
            "nodeNames": list(self.node_names),
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class SlurmPolicy:
    """Administrator-provided Slurm site limits.

    Per-Worker CPU, GPU, and memory sizing no longer belongs here. A Phase 3
    Resource Planner must provide an already concrete SlurmResourceRequest.
    """

    partition: str
    time_limit: str
    max_cpus: int
    max_gpus: int
    max_memory_gib: int
    max_nodes: int
    cpus_per_node: int
    gpus_per_node: int
    memory_gib_per_node: int
    allowed_partitions: tuple[str, ...] = ()
    excluded_partitions: tuple[str, ...] = ()
    excluded_nodes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        partition = self.partition
        if partition:
            _validate_partition(partition)
        _slurm_time_seconds(self.time_limit)

        for field_name in (
            "max_cpus",
            "max_memory_gib",
            "max_nodes",
            "cpus_per_node",
            "memory_gib_per_node",
        ):
            _require_int(getattr(self, field_name), name=field_name, minimum=1)
        for field_name in (
            "max_gpus",
            "gpus_per_node",
        ):
            _require_int(getattr(self, field_name), name=field_name, minimum=0)

        raw_allowed = self.allowed_partitions
        if not isinstance(raw_allowed, tuple):
            raise ValueError("allowed_partitions must be a tuple of partition names.")
        allowed = raw_allowed or ((partition,) if partition else ())
        if len(set(allowed)) != len(allowed):
            raise ValueError("allowed_partitions must not contain duplicates.")
        for index, candidate in enumerate(allowed):
            _validate_partition(candidate, name=f"allowed_partitions[{index}]")
        if partition and partition not in allowed:
            raise ValueError(
                f"partition {partition!r} is not present in allowed_partitions."
            )
        object.__setattr__(self, "allowed_partitions", allowed)

        for field_name in ("excluded_partitions", "excluded_nodes"):
            value = getattr(self, field_name)
            if not isinstance(value, tuple):
                raise ValueError(f"{field_name} must be a tuple.")
            if len(set(value)) != len(value):
                raise ValueError(f"{field_name} must not contain duplicates.")
            if any(not item or item != item.strip() for item in value):
                raise ValueError(f"{field_name} contains an empty or invalid name.")
        for index, candidate in enumerate(self.excluded_partitions):
            _validate_partition(candidate, name=f"excluded_partitions[{index}]")
        overlap = set(allowed).intersection(self.excluded_partitions)
        if overlap:
            raise ValueError(
                "Allowed and excluded Slurm partitions overlap: "
                + ", ".join(sorted(overlap))
                + "."
            )

    def resolve_partitions(self, discovered: Iterable[str]) -> tuple[str, ...]:
        """Apply operator overrides to partitions discovered from ``sinfo``."""

        discovered_names = tuple(dict.fromkeys(discovered))
        for index, candidate in enumerate(discovered_names):
            _validate_partition(candidate, name=f"discovered[{index}]")
        requested = (
            (self.partition,)
            if self.partition
            else (self.allowed_partitions or discovered_names)
        )
        missing = [name for name in requested if name not in discovered_names]
        if missing:
            raise ValueError(
                "Configured Slurm partition(s) were not reported by sinfo: "
                + ", ".join(missing)
                + "."
            )
        eligible = tuple(
            name for name in requested if name not in self.excluded_partitions
        )
        if not eligible:
            raise ValueError(
                "No Slurm partitions remain after applying the site exclusions."
            )
        return eligible

    def validate_request(self, request: SlurmResourceRequest) -> None:
        """Validate a concrete request emitted by the Resource Planner."""

        if request.partition in self.excluded_partitions or (
            self.allowed_partitions
            and request.partition not in self.allowed_partitions
        ):
            raise ValueError(
                f"partition {request.partition!r} is not allowed by site policy."
            )
        checks = (
            (request.nodes, self.max_nodes, "nodes"),
            (request.cpus, self.cpus_per_node, "cpus per node"),
            (request.gpus, self.gpus_per_node, "gpus per node"),
            (request.memory_gib, self.memory_gib_per_node, "memory GiB per node"),
            (request.total_cpus, self.max_cpus, "total cpus"),
            (request.total_gpus, self.max_gpus, "total gpus"),
            (request.total_memory_gib, self.max_memory_gib, "total memory GiB"),
        )
        for actual, maximum, label in checks:
            if actual > maximum:
                raise ValueError(f"Planned {label}={actual} exceeds site limit {maximum}.")


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
    if request.node_names:
        argv.append(f"--nodelist={','.join(request.node_names)}")

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
    "parse_sbatch_job_id",
    "parse_sbatch_submission",
    "parse_scontrol_job_record",
    "resolve_execution_directory",
    "validate_execution_id",
]
