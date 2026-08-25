"""Planner-aware ``dask-jobqueue`` integration for heterogeneous Slurm jobs.

One :class:`PlannedSLURMCluster` owns the service-node Scheduler for a workflow.
Each Resource Planner allocation becomes a distinct ``SLURMJob`` worker spec,
so profiles may use different partitions, nodes, CPU, memory, GPU and process
counts while retaining the lifecycle semantics of ``SLURMCluster``.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import shlex
from typing import Mapping, Sequence

from dask_jobqueue import SLURMCluster
from dask_jobqueue.slurm import SLURMJob
from distributed.deploy.spec import ProcessInterface

from core.resource_planner import SlurmAllocationPlan, SlurmJobRequirement


_JOB_ID_RE = re.compile(r"[1-9][0-9]*\Z")
_SAFE_JOB_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_SAFE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z")


def _absolute_file(value: Path | str, *, name: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        raise ValueError(f"{name} must be an absolute regular non-symlink file.")
    return path


def _absolute_executable(value: Path | str, *, name: str) -> Path:
    """Return the canonical executable behind an absolute path.

    POSIX virtual environments normally expose ``bin/python`` as a symbolic
    link.  Rejecting that standard layout makes a valid uv-created environment
    unusable on Slurm hosts.  Resolve the link strictly, then validate the
    canonical target instead of weakening validation for launcher/request
    files, which must remain non-symlinks.
    """

    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute executable file.")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"{name} must resolve to an existing executable file.") from exc
    if not resolved.is_file():
        raise ValueError(f"{name} must resolve to a regular executable file.")
    return resolved


def _absolute_directory(value: Path | str, *, name: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or not path.is_dir() or path.is_symlink():
        raise ValueError(f"{name} must be an absolute non-symlink directory.")
    return path


@dataclass(frozen=True, slots=True)
class PlannedSlurmWorkerSpec:
    allocation_id: str
    submission_token: str
    options: Mapping[str, object]

    def __post_init__(self) -> None:
        if _SAFE_JOB_NAME_RE.fullmatch(self.allocation_id) is None:
            raise ValueError(f"Invalid Slurm allocation id: {self.allocation_id!r}.")
        if _SAFE_TOKEN_RE.fullmatch(self.submission_token) is None:
            raise ValueError("Invalid Slurm submission token.")


@dataclass(frozen=True, slots=True)
class SubmittedSlurmJob:
    allocation_id: str
    job_id: str
    submission_token: str


class PlannedSLURMJob(SLURMJob):
    """A ``SLURMJob`` whose payload is WorkFlow's validated Worker launcher."""

    def __init__(
        self,
        *args,
        allocation_id: str | None = None,
        submission_token: str | None = None,
        launcher_script: str | None = None,
        request_path: str | None = None,
        submit_command: str | None = None,
        cancel_command: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.allocation_id = allocation_id
        self.submission_token = submission_token
        if submit_command:
            self.submit_command = submit_command
        if cancel_command:
            self.cancel_command = cancel_command
        if launcher_script is None and request_path is None:
            return
        if launcher_script is None or request_path is None:
            raise ValueError("launcher_script and request_path must be provided together.")
        launcher = _absolute_file(launcher_script, name="launcher_script")
        request = _absolute_file(request_path, name="request_path")
        self._command_template = (
            f"exec {shlex.quote(str(launcher))} {shlex.quote(str(request))}"
        )

    async def close(self) -> None:
        """Cancel through dask-jobqueue and publish a terminal SpecCluster state."""

        try:
            await SLURMJob.close(self)
        finally:
            await ProcessInterface.close(self)


class PlannedSLURMCluster(SLURMCluster):
    """One on-demand Scheduler with heterogeneous planner-defined Slurm jobs."""

    job_cls = PlannedSLURMJob

    def submitted_job_records(self) -> tuple[SubmittedSlurmJob, ...]:
        candidates = list(self.workers.values())
        candidates.extend(tuple(getattr(self, "_created", ()) or ()))
        records: dict[str, SubmittedSlurmJob] = {}
        for candidate in candidates:
            if not isinstance(candidate, PlannedSLURMJob):
                continue
            allocation_id = str(candidate.allocation_id or "")
            submission_token = str(candidate.submission_token or "")
            job_id = str(candidate.job_id or "")
            if (
                _SAFE_JOB_NAME_RE.fullmatch(allocation_id) is not None
                and _SAFE_TOKEN_RE.fullmatch(submission_token) is not None
                and _JOB_ID_RE.fullmatch(job_id) is not None
            ):
                records[allocation_id] = SubmittedSlurmJob(
                    allocation_id=allocation_id,
                    job_id=job_id,
                    submission_token=submission_token,
                )
        return tuple(records[name] for name in sorted(records))

    def submit_planned_jobs(
        self,
        specs: Sequence[PlannedSlurmWorkerSpec],
    ) -> tuple[SubmittedSlurmJob, ...]:
        if self.worker_spec or self.workers:
            raise RuntimeError("This SLURMCluster already has Worker jobs.")
        if not specs:
            raise ValueError("At least one planned Slurm Worker job is required.")
        worker_spec: dict[str, dict[str, object]] = {}
        for spec in specs:
            if spec.allocation_id in worker_spec:
                raise ValueError(
                    f"Duplicate planned Slurm allocation {spec.allocation_id!r}."
                )
            worker_spec[spec.allocation_id] = {
                "cls": PlannedSLURMJob,
                "options": dict(spec.options),
            }
        self.worker_spec.update(worker_spec)
        self.sync(self._correct_state)

        records = {
            record.allocation_id: record
            for record in self.submitted_job_records()
        }
        result: list[SubmittedSlurmJob] = []
        for allocation_id in worker_spec:
            if allocation_id not in records:
                raise RuntimeError(
                    f"SLURMCluster did not return a valid job id for {allocation_id!r}."
                )
            result.append(records[allocation_id])
        return tuple(result)

    def stop_planned_jobs(self) -> None:
        """Scale Worker jobs to zero while leaving the local Scheduler alive."""

        self.scale(jobs=0)
        self.sync(self._correct_state)


def build_planned_slurm_worker_spec(
    plan: SlurmAllocationPlan,
    job: SlurmJobRequirement,
    *,
    execution_id: str,
    submission_token: str,
    request_path: Path,
    launcher_script: Path,
    project_root: Path,
    run_directory: Path,
    python_executable: Path,
    sbatch_executable: str,
    scancel_executable: str,
    interface: str | None,
    protocol: str | None,
    security: object,
) -> PlannedSlurmWorkerSpec:
    """Translate one Resource Planner job into one heterogeneous worker spec."""

    launcher = _absolute_file(launcher_script, name="launcher_script")
    request = _absolute_file(request_path, name="request_path")
    root = _absolute_directory(project_root, name="project_root")
    logs = _absolute_directory(run_directory, name="run_directory")
    python = _absolute_executable(python_executable, name="python_executable")
    allocation_tag = hashlib.sha256(
        job.allocation_id.encode("utf-8")
    ).hexdigest()[:10]
    job_name = f"wf-{execution_id[:24]}-{allocation_tag}"
    if _SAFE_JOB_NAME_RE.fullmatch(job_name) is None:
        raise ValueError(f"Generated Slurm job name is invalid: {job_name!r}.")

    directives = [
        "--nodes=1",
        "--ntasks-per-node=1",
        f"--nodelist={job.node}",
        "--export=NONE",
        f"--comment={submission_token}",
        "--signal=B:TERM@90",
        f"--chdir={root}",
    ]
    if job.gpu:
        directives.append(f"--gres=gpu:{job.gpu}")

    options: dict[str, object] = {
        "allocation_id": job.allocation_id,
        "submission_token": submission_token,
        "launcher_script": str(launcher),
        "request_path": str(request),
        "submit_command": sbatch_executable,
        "cancel_command": scancel_executable,
        "queue": job.partition,
        "cores": job.cpu,
        "memory": f"{job.memory_gib}GiB",
        "processes": job.processes,
        "nanny": False,
        "walltime": plan.time_limit,
        "job_cpu": job.cpu,
        "job_mem": f"{job.memory_gib}G",
        "job_name": job_name,
        "job_extra_directives": directives,
        "log_directory": str(logs),
        "shebang": "#!/bin/bash",
        "python": str(python),
        "worker_extra_args": [],
        "interface": interface,
        "protocol": protocol,
        "security": security,
        "config_name": "slurm",
    }
    return PlannedSlurmWorkerSpec(
        allocation_id=job.allocation_id,
        submission_token=submission_token,
        options=options,
    )


__all__ = [
    "PlannedSLURMCluster",
    "PlannedSLURMJob",
    "PlannedSlurmWorkerSpec",
    "SubmittedSlurmJob",
    "build_planned_slurm_worker_spec",
]
