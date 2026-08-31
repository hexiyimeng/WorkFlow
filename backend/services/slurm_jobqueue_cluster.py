"""Planner-aware ``dask-jobqueue`` integration for heterogeneous Slurm jobs.

One :class:`PlannedSLURMCluster` owns the service-node Scheduler for a workflow.
Each Resource Planner allocation becomes a distinct standard ``SLURMJob``
worker spec, so profiles may use different partitions, nodes, CPU, memory, GPU
and process counts while retaining SLURMCluster's Worker command and lifecycle.
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
from core.worker_ownership import (
    execution_ownership_resource,
    submission_ownership_resource,
)


_JOB_ID_RE = re.compile(r"[1-9][0-9]*\Z")
_SAFE_JOB_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_SAFE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z")


def _absolute_executable(value: Path | str, *, name: str) -> Path:
    """Validate an absolute executable while preserving its venv path.

    POSIX virtual environments normally expose ``bin/python`` as a symbolic
    link.  Rejecting that standard layout makes a valid uv-created environment
    unusable on Slurm hosts.  Resolve the link strictly, then validate the
    canonical target.  Return the original ``.venv/bin/python`` path so Python
    retains virtual-environment discovery instead of launching through the
    resolved base interpreter path.
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
    return path


def _absolute_directory(value: Path | str, *, name: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or not path.is_dir() or path.is_symlink():
        raise ValueError(f"{name} must be an absolute non-symlink directory.")
    return path


def _worker_command_security(security: object) -> object | None:
    """Return only a complete encrypted Security object for Worker CLI use.

    ``Client.security`` is a truthy ``Security`` instance even for plain TCP.
    dask-jobqueue interprets every truthy object as TLS-enabled and serializes
    its worker fields verbatim, turning absent values into command arguments
    such as ``--tls-ca-file None``. Plain TCP must therefore pass ``None`` to
    ``SLURMJob``; encrypted mode must fail before submission if incomplete.
    """

    if security is None or not bool(getattr(security, "require_encryption", False)):
        return None
    tls_config = getattr(security, "get_tls_config_for_role", None)
    if not callable(tls_config):
        raise ValueError("Encrypted Dask security does not expose Worker TLS config.")
    values = dict(tls_config("worker"))
    missing = sorted(
        name for name, value in values.items()
        if name != "ciphers" and not value
    )
    if missing:
        raise ValueError(
            "Encrypted Dask Worker TLS config is incomplete: "
            + ", ".join(missing)
            + "."
        )
    return security


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
    """A standard ``SLURMJob`` carrying immutable planner ownership metadata."""

    def __init__(
        self,
        *args,
        allocation_id: str | None = None,
        submission_token: str | None = None,
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

    async def close(self) -> None:
        """Cancel through dask-jobqueue and publish a terminal SpecCluster state."""

        try:
            # A heterogeneous correction may create several Job objects while
            # one sbatch call fails.  Only the objects with a validated Slurm
            # job id may invoke dask-jobqueue's scancel path.
            if _JOB_ID_RE.fullmatch(str(self.job_id or "")) is not None:
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
    project_root: Path,
    runtime_directory: Path,
    run_directory: Path,
    python_executable: Path,
    sbatch_executable: str,
    scancel_executable: str,
    interface: str | None,
    protocol: str | None,
    security: object,
    worker_port_range: str,
    nanny_port_range: str,
) -> PlannedSlurmWorkerSpec:
    """Translate one planner job into a standard SLURMJob Worker command."""

    root = _absolute_directory(project_root, name="project_root")
    runtime = _absolute_directory(runtime_directory, name="runtime_directory")
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

    threads_per_worker = job.cpu // job.processes
    if threads_per_worker <= 0 or threads_per_worker * job.processes != job.cpu:
        raise ValueError(
            f"Planned job {job.allocation_id!r} CPU count must divide exactly "
            "across its Worker processes."
        )
    worker_resources = dict(job.logical_resources)
    worker_resources[execution_ownership_resource(execution_id)] = 1
    worker_resources[submission_ownership_resource(submission_token)] = 1
    resources = ",".join(
        f"{name}={amount:g}" for name, amount in sorted(worker_resources.items())
    )
    backend = root / "backend"
    fallback_scratch = logs / f"worker-scratch-{allocation_tag}"
    token_hash = hashlib.sha256(submission_token.encode("utf-8")).hexdigest()
    prologue = [
        "set -euo pipefail",
        "umask 077",
        f"export PATH={shlex.quote(str(python.parent))}:/usr/local/bin:/usr/bin:/bin",
        f"export PYTHONPATH={shlex.quote(str(backend))}",
        "export PYTHONUNBUFFERED=1",
        "export WORKFLOW_DASK_WORKER_PROCESS=1",
        f"export WORKFLOW_EXECUTION_ID={shlex.quote(execution_id)}",
        f"export WORKFLOW_SUBMISSION_TOKEN_HASH={shlex.quote(token_hash)}",
        f"export WORKFLOW_WORKER_PROFILE={shlex.quote(job.profile)}",
        f"export WORKFLOW_WORKER_ROLE={'gpu' if job.gpu else 'cpu'}",
        f"export OMP_NUM_THREADS={threads_per_worker}",
        f"export MKL_NUM_THREADS={threads_per_worker}",
        f"export OPENBLAS_NUM_THREADS={threads_per_worker}",
        f"export NUMEXPR_NUM_THREADS={threads_per_worker}",
        f"export WorkFlow_MODELS_DIR={shlex.quote(str(runtime / 'models'))}",
        f"export CELLPOSE_LOCAL_MODELS_PATH={shlex.quote(str(runtime / 'models' / 'cellpose'))}",
        f"WORKFLOW_FALLBACK_SCRATCH={shlex.quote(str(fallback_scratch))}",
        'export WORKFLOW_DASK_LOCAL_DIRECTORY="${SLURM_TMPDIR:-$WORKFLOW_FALLBACK_SCRATCH}"',
        "mkdir -p \"$WORKFLOW_DASK_LOCAL_DIRECTORY\"",
    ]
    if job.gpu:
        prologue.extend((
            'if [[ -z "${CUDA_VISIBLE_DEVICES:-}" || "${CUDA_VISIBLE_DEVICES}" == *,* ]]; then '
            'echo "SLURMCluster GPU Worker must receive exactly one CUDA device" >&2; exit 2; fi',
            'export WORKFLOW_LOCAL_GPU_ID="${CUDA_VISIBLE_DEVICES}"',
            'export WORKFLOW_PHYSICAL_GPU_ID="${SLURMD_NODENAME:-${HOSTNAME}}:${CUDA_VISIBLE_DEVICES}"',
        ))
    else:
        prologue.extend((
            'export CUDA_VISIBLE_DEVICES=""',
            'export WORKFLOW_LOCAL_GPU_ID=""',
            'export WORKFLOW_PHYSICAL_GPU_ID=""',
        ))

    options: dict[str, object] = {
        "allocation_id": job.allocation_id,
        "submission_token": submission_token,
        "submit_command": sbatch_executable,
        "cancel_command": scancel_executable,
        "queue": job.partition,
        "cores": job.cpu,
        "memory": f"{job.memory_gib}GiB",
        "processes": job.processes,
        "nanny": True,
        "walltime": plan.time_limit,
        "job_cpu": job.cpu,
        "job_mem": f"{job.memory_gib}G",
        "job_name": job_name,
        "job_extra_directives": directives,
        "log_directory": str(logs),
        "shebang": "#!/bin/bash",
        "python": str(python),
        "worker_command": "distributed.cli.dask_worker",
        "worker_extra_args": [
            "--resources", resources,
            "--preload", "services.slurm_worker_preload",
            "--worker-port", worker_port_range,
            "--nanny-port", nanny_port_range,
            "--no-dashboard",
        ],
        "death_timeout": 120,
        "local_directory": '"$WORKFLOW_DASK_LOCAL_DIRECTORY"',
        "job_script_prologue": prologue,
        "interface": interface,
        "protocol": protocol,
        "security": _worker_command_security(security),
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
