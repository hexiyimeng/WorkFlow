"""One-shot WorkFlow execution runner for a Slurm compute-node allocation.

The long-lived control plane writes one immutable schema-v1 request into a
private runtime directory.  This module validates that request again inside
the allocation, executes the existing workflow executor, and spools broadcast
events for the control plane without requiring a WebSocket on the compute
node.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import threading
from typing import Any, Mapping, Sequence
import uuid

from core.state_manager import ExecutionStatus, state_manager
from core.slurm_execution import validate_execution_id
from core.window_execution import (
    ExecutionConfig,
    parse_execution_config,
    require_window_recovery_location,
)
from core.workflow_resources import (
    build_workflow_resource_plan,
    ensure_executable_resource_plan,
)
from services.dask_service import dask_service
from services.executor import (
    execute_graph,
    find_execution_roots,
    validate_graph_acyclic,
    validate_graph_structure,
    validate_graph_types,
)
from services.plugin_loader import load_all_plugins


logger = logging.getLogger("WorkFlow.SlurmRunner")

REQUEST_SCHEMA_VERSION = 1
EVENTS_FILENAME = "events.jsonl"
RESULT_FILENAME = "result.json"
CANCEL_MARKER_FILENAME = "cancel.requested"

_REQUEST_FIELDS = frozenset({
    "schemaVersion",
    "executionId",
    "graph",
    "executionConfig",
    "resourcePlan",
    "codeRevision",
    "submittedAt",
    "eventPath",
    "resultPath",
})
_RESOURCE_PLAN_FIELDS = frozenset({"cpuWorkers", "gpuWorkers"})
_CANCEL_MARKER_FIELDS = frozenset({
    "schemaVersion",
    "executionId",
    "jobId",
    "requestedAt",
})
_TERMINAL_STATUSES = frozenset({
    ExecutionStatus.SUCCEEDED,
    ExecutionStatus.FAILED,
    ExecutionStatus.CANCELLED,
    ExecutionStatus.INTERRUPTED,
})
_EXIT_CODES = {
    ExecutionStatus.SUCCEEDED: 0,
    ExecutionStatus.FAILED: 1,
    ExecutionStatus.CANCELLED: 130,
    ExecutionStatus.INTERRUPTED: 143,
}


class RunnerValidationError(ValueError):
    """The immutable runner request or its allocation is invalid."""


@dataclass(frozen=True)
class RunnerRequest:
    execution_id: str
    graph: dict[str, Any]
    execution_config: ExecutionConfig
    cpu_workers: int
    gpu_workers: int
    code_revision: str | None
    submitted_at: str
    event_path: str
    result_path: str

    @property
    def resource_plan(self) -> dict[str, int]:
        return {
            "cpuWorkers": self.cpu_workers,
            "gpuWorkers": self.gpu_workers,
        }


@dataclass(frozen=True)
class RunnerPaths:
    request: Path
    runtime_directory: Path
    execution_directory: Path
    events: Path
    result: Path
    cancel_marker: Path


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _absolute_path(value: str | os.PathLike[str], *, name: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise RunnerValidationError(f"{name} must be an absolute path: {path}")
    return path


def _is_symlink(path: Path) -> bool:
    try:
        return stat.S_ISLNK(path.lstat().st_mode)
    except FileNotFoundError:
        return False


def _require_regular_file(path: Path, *, name: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise RunnerValidationError(f"{name} does not exist: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise RunnerValidationError(f"{name} must not be a symbolic link: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise RunnerValidationError(f"{name} must be a regular file: {path}")


def _require_output_absent(path: Path, *, name: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(metadata.st_mode):
        raise RunnerValidationError(f"{name} must not be a symbolic link: {path}")
    raise RunnerValidationError(
        f"{name} already exists; refusing to mix records from two runners: {path}"
    )


def _validate_runner_input_paths(
    request_path: str | os.PathLike[str],
    runtime_directory: str | os.PathLike[str],
) -> tuple[Path, Path]:
    """Validate the immutable input file and configured shared runtime root."""
    request = _absolute_path(request_path, name="REQUEST_PATH")
    runtime = _absolute_path(runtime_directory, name="RUNTIME_DIR")

    try:
        runtime_metadata = runtime.lstat()
    except FileNotFoundError as exc:
        raise RunnerValidationError(f"RUNTIME_DIR does not exist: {runtime}") from exc
    if stat.S_ISLNK(runtime_metadata.st_mode):
        raise RunnerValidationError(f"RUNTIME_DIR must not be a symbolic link: {runtime}")
    if not stat.S_ISDIR(runtime_metadata.st_mode):
        raise RunnerValidationError(f"RUNTIME_DIR must be a directory: {runtime}")

    _require_regular_file(request, name="REQUEST_PATH")
    try:
        request.resolve(strict=True).relative_to(runtime.resolve(strict=True))
    except ValueError as exc:
        raise RunnerValidationError(
            "REQUEST_PATH must be contained by RUNTIME_DIR."
        ) from exc

    execution_directory = request.parent
    execution_metadata = execution_directory.lstat()
    if stat.S_ISLNK(execution_metadata.st_mode):
        raise RunnerValidationError(
            f"The per-execution directory must not be a symbolic link: "
            f"{execution_directory}"
        )
    if not stat.S_ISDIR(execution_metadata.st_mode):
        raise RunnerValidationError(
            f"The per-execution path must be a directory: {execution_directory}"
        )
    return request, runtime


def validate_runner_paths(
    request_path: str | os.PathLike[str],
    runtime_directory: str | os.PathLike[str],
    *,
    event_path: str | os.PathLike[str],
    result_path: str | os.PathLike[str],
) -> RunnerPaths:
    """Validate declared output files in the request's execution directory."""
    request, runtime = _validate_runner_input_paths(
        request_path,
        runtime_directory,
    )
    execution_directory = request.parent
    events = _absolute_path(event_path, name="eventPath")
    result = _absolute_path(result_path, name="resultPath")
    expected_parent = os.path.abspath(os.fspath(execution_directory))
    if os.path.abspath(os.fspath(events.parent)) != expected_parent:
        raise RunnerValidationError(
            "eventPath must be directly inside REQUEST_PATH.parent."
        )
    if os.path.abspath(os.fspath(result.parent)) != expected_parent:
        raise RunnerValidationError(
            "resultPath must be directly inside REQUEST_PATH.parent."
        )
    if events.name != EVENTS_FILENAME:
        raise RunnerValidationError(f"eventPath filename must be {EVENTS_FILENAME!r}.")
    if result.name != RESULT_FILENAME:
        raise RunnerValidationError(f"resultPath filename must be {RESULT_FILENAME!r}.")
    if events == result:
        raise RunnerValidationError("eventPath and resultPath must be different files.")

    cancel_marker = execution_directory / CANCEL_MARKER_FILENAME
    _require_output_absent(events, name=EVENTS_FILENAME)
    _require_output_absent(result, name=RESULT_FILENAME)

    return RunnerPaths(
        request=request,
        runtime_directory=runtime,
        execution_directory=execution_directory,
        events=events,
        result=result,
        cancel_marker=cancel_marker,
    )


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RunnerValidationError(f"Duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _reject_nonfinite_json_constant(value: str) -> None:
    raise RunnerValidationError(f"Non-finite JSON number is not allowed: {value}")


def _read_request_json(path: Path) -> Mapping[str, Any]:
    flags = os.O_RDONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise RunnerValidationError(
                    f"REQUEST_PATH must be a regular file: {path}"
                )
        except BaseException:
            os.close(descriptor)
            raise
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            payload = json.load(
                handle,
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_nonfinite_json_constant,
            )
    except RunnerValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RunnerValidationError(f"Cannot read schema-v1 request JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise RunnerValidationError("Runner request must be a JSON object.")
    return payload


def _strict_nonnegative_integer(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RunnerValidationError(f"{name} must be a non-negative integer.")
    return value


def parse_runner_request(payload: Mapping[str, Any]) -> RunnerRequest:
    fields = frozenset(payload)
    missing = sorted(_REQUEST_FIELDS - fields)
    unknown = sorted(fields - _REQUEST_FIELDS)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing fields: {missing}")
        if unknown:
            details.append(f"unknown fields: {unknown}")
        raise RunnerValidationError(
            "Invalid schema-v1 runner request (" + "; ".join(details) + ")."
        )
    if (
        type(payload["schemaVersion"]) is not int
        or payload["schemaVersion"] != REQUEST_SCHEMA_VERSION
    ):
        raise RunnerValidationError(
            f"Unsupported runner request schemaVersion {payload['schemaVersion']!r}; "
            f"expected {REQUEST_SCHEMA_VERSION}."
        )

    execution_id = payload["executionId"]
    try:
        execution_id = validate_execution_id(execution_id)
    except ValueError as exc:
        raise RunnerValidationError(str(exc).replace("execution_id", "executionId")) from exc

    graph = payload["graph"]
    if not isinstance(graph, dict):
        raise RunnerValidationError("graph must be a JSON object.")

    raw_config = payload["executionConfig"]
    if not isinstance(raw_config, Mapping):
        raise RunnerValidationError("executionConfig must be a JSON object.")
    try:
        execution_config = require_window_recovery_location(
            parse_execution_config(raw_config)
        )
    except ValueError as exc:
        raise RunnerValidationError(str(exc)) from exc
    # Requests are produced by the control plane.  Requiring the canonical
    # representation prevents ignored aliases or recovery-only extras from
    # changing meaning between submission and execution.
    if dict(raw_config) != execution_config.to_dict():
        raise RunnerValidationError(
            "executionConfig is not in the canonical schema-v1 representation."
        )

    raw_plan = payload["resourcePlan"]
    if not isinstance(raw_plan, Mapping):
        raise RunnerValidationError("resourcePlan must be a JSON object.")
    if frozenset(raw_plan) != _RESOURCE_PLAN_FIELDS:
        raise RunnerValidationError(
            "resourcePlan must contain exactly cpuWorkers and gpuWorkers."
        )
    cpu_workers = _strict_nonnegative_integer(
        raw_plan["cpuWorkers"],
        name="resourcePlan.cpuWorkers",
    )
    gpu_workers = _strict_nonnegative_integer(
        raw_plan["gpuWorkers"],
        name="resourcePlan.gpuWorkers",
    )
    if cpu_workers + gpu_workers == 0:
        raise RunnerValidationError("resourcePlan must request at least one Worker.")

    code_revision = payload["codeRevision"]
    if code_revision is not None and (
        not isinstance(code_revision, str)
        or re.fullmatch(r"[0-9a-fA-F]{40}", code_revision) is None
    ):
        raise RunnerValidationError(
            "codeRevision must be a 40-character Git commit hash or null."
        )
    if isinstance(code_revision, str):
        code_revision = code_revision.lower()
    submitted_at = payload["submittedAt"]
    if not isinstance(submitted_at, str) or not submitted_at.strip():
        raise RunnerValidationError("submittedAt must be a non-empty string.")
    event_path = payload["eventPath"]
    if not isinstance(event_path, str) or not event_path:
        raise RunnerValidationError("eventPath must be a non-empty string.")
    result_path = payload["resultPath"]
    if not isinstance(result_path, str) or not result_path:
        raise RunnerValidationError("resultPath must be a non-empty string.")

    return RunnerRequest(
        execution_id=execution_id,
        graph=graph,
        execution_config=execution_config,
        cpu_workers=cpu_workers,
        gpu_workers=gpu_workers,
        code_revision=code_revision,
        submitted_at=submitted_at,
        event_path=event_path,
        result_path=result_path,
    )


def _leading_nonnegative_integer(value: str | None) -> int | None:
    match = re.match(r"\s*(\d+)", value or "")
    return int(match.group(1)) if match is not None else None


def _allocated_cpu_count(environment: Mapping[str, str]) -> int:
    for name in (
        "SLURM_CPUS_PER_TASK",
        "SLURM_CPUS_ON_NODE",
        "SLURM_JOB_CPUS_PER_NODE",
    ):
        count = _leading_nonnegative_integer(environment.get(name))
        if count is not None:
            return count
    raise RunnerValidationError(
        "Slurm did not expose an allocated CPU count in "
        "SLURM_CPUS_PER_TASK, SLURM_CPUS_ON_NODE, or SLURM_JOB_CPUS_PER_NODE."
    )


def _allocated_gpu_count(environment: Mapping[str, str]) -> int:
    mask = environment.get("CUDA_VISIBLE_DEVICES")
    if mask is not None:
        normalized = mask.strip()
        if normalized.lower() in {"", "-1", "nodevfiles", "none"}:
            return 0
        devices = tuple(item.strip() for item in normalized.split(","))
        if any(not item for item in devices) or len(set(devices)) != len(devices):
            raise RunnerValidationError(
                f"CUDA_VISIBLE_DEVICES is not a valid unique device list: {mask!r}."
            )
        return len(devices)

    # Some Slurm installations do not configure device cgroups.  In that
    # case accept only an explicit numeric allocation variable; never inspect
    # every physical GPU on the host as that could escape the allocation.
    for name in ("SLURM_GPUS_ON_NODE", "SLURM_GPUS_PER_NODE"):
        count = _leading_nonnegative_integer(environment.get(name))
        if count is not None:
            return count
    return 0


def validate_slurm_allocation(
    request: RunnerRequest,
    environment: Mapping[str, str] | None = None,
) -> str:
    env = os.environ if environment is None else environment
    job_id = _require_slurm_job_id(env)

    allocated_cpus = _allocated_cpu_count(env)
    minimum_cpus = request.cpu_workers + request.gpu_workers
    if allocated_cpus < minimum_cpus:
        raise RunnerValidationError(
            f"Slurm allocation has {allocated_cpus} CPU(s), but the validated "
            f"resource plan needs at least {minimum_cpus} "
            f"({request.cpu_workers} CPU Worker(s) + "
            f"{request.gpu_workers} GPU Worker(s))."
        )

    allocated_gpus = _allocated_gpu_count(env)
    if allocated_gpus < request.gpu_workers:
        raise RunnerValidationError(
            f"Slurm allocation exposes {allocated_gpus} GPU(s), but the validated "
            f"resource plan needs {request.gpu_workers} GPU Worker(s)."
        )
    return job_id


def _require_slurm_job_id(environment: Mapping[str, str]) -> str:
    job_id = environment.get("SLURM_JOB_ID", "").strip()
    if not job_id:
        raise RunnerValidationError(
            "SLURM_JOB_ID is required; the compute runner must run inside a Slurm allocation."
        )
    return job_id


def validate_requested_resource_plan(request: RunnerRequest) -> None:
    """Recompute the graph-derived plan; never trust the submitter's count."""
    validate_graph_structure(request.graph)
    validate_graph_acyclic(request.graph)
    validate_graph_types(request.graph)
    execution_roots = find_execution_roots(request.graph)
    if not execution_roots:
        raise RunnerValidationError(
            "The submitted graph has no terminal execution root."
        )
    computed = ensure_executable_resource_plan(
        build_workflow_resource_plan(request.graph, execution_roots)
    )
    expected = {
        "cpuWorkers": computed.cpu_workers,
        "gpuWorkers": computed.gpu_workers,
    }
    if request.resource_plan != expected:
        raise RunnerValidationError(
            "Submitted resourcePlan does not match the graph-derived plan: "
            f"submitted={request.resource_plan}, computed={expected}."
        )


def validate_code_revision(request: RunnerRequest) -> None:
    """Reject a queued job if the shared checkout changed after submission."""
    if request.code_revision is None:
        return
    project_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ("git", "-C", str(project_root), "rev-parse", "HEAD"),
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            shell=False,
        )
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
        raise RunnerValidationError(
            "Cannot verify the submitted WorkFlow code revision."
        ) from exc
    current = result.stdout.strip().lower()
    if (
        result.returncode != 0
        or current != request.code_revision
        or status.returncode != 0
        or bool(status.stdout.strip())
    ):
        raise RunnerValidationError(
            "The shared WorkFlow checkout changed after this job was submitted; "
            "refusing to execute a different code snapshot."
        )


class JsonlEventWriter:
    """Synchronous, single-writer JSONL observer for one execution."""

    def __init__(self, path: Path, *, job_id: str):
        self.path = path
        self.job_id = job_id
        self._lock = threading.RLock()
        self._sequence = 0
        self.last_terminal_message: str | None = None

        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        self._handle = os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="\n",
        )

    @staticmethod
    def _is_terminal(payload: Mapping[str, Any]) -> bool:
        return (
            payload.get("type") in {"execution_finished", "done", "runner_failed"}
            or payload.get("status") in _TERMINAL_STATUSES
        )

    def write_event(self, execution_id: str, payload: Mapping[str, Any]) -> None:
        with self._lock:
            sequence = self._sequence + 1
            record = dict(payload)
            record["executionId"] = execution_id
            record["sequence"] = sequence
            record["timestamp"] = _utc_timestamp()
            record["jobId"] = self.job_id
            encoded = json.dumps(
                record,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            self._handle.write(encoded + "\n")
            self._handle.flush()
            if self._is_terminal(record):
                os.fsync(self._handle.fileno())
                message = record.get("message")
                if isinstance(message, str) and message:
                    self.last_terminal_message = message
            self._sequence = sequence

    def __call__(self, execution_id: str, payload: dict[str, Any]) -> None:
        self.write_event(execution_id, payload)

    def close(self) -> None:
        with self._lock:
            if self._handle.closed:
                return
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()


def _atomic_write_result(path: Path, payload: Mapping[str, Any]) -> None:
    if _is_symlink(path):
        raise RunnerValidationError(f"{RESULT_FILENAME} must not be a symbolic link: {path}")
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
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


def _cleanup_job_scratch(
    paths: RunnerPaths,
    *,
    job_id: str,
    environment: Mapping[str, str],
) -> None:
    raw_root = environment.get("WorkFlow_JOB_SCRATCH_ROOT", "").strip()
    if not raw_root:
        return
    scratch = Path(raw_root)
    if not scratch.is_absolute() or scratch == Path(scratch.anchor):
        raise RunnerValidationError("WorkFlow_JOB_SCRATCH_ROOT is unsafe.")
    if scratch.is_symlink():
        raise RunnerValidationError("Job scratch must not be a symbolic link.")

    fallback = paths.runtime_directory / "jobs" / job_id / "scratch"
    allowed = os.path.abspath(os.fspath(scratch)) == os.path.abspath(os.fspath(fallback))
    slurm_tmp = environment.get("SLURM_TMPDIR", "").strip()
    if slurm_tmp:
        slurm_tmp_path = Path(slurm_tmp)
        expected_tmp = slurm_tmp_path / f"workflow-{job_id}"
        allowed = allowed or (
            slurm_tmp_path.is_absolute()
            and not slurm_tmp_path.is_symlink()
            and os.path.abspath(os.fspath(scratch))
            == os.path.abspath(os.fspath(expected_tmp))
        )
    if not allowed:
        raise RunnerValidationError(
            "Job scratch is outside the runner-owned Slurm scratch locations."
        )
    if not scratch.exists():
        return
    metadata = scratch.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RunnerValidationError("Job scratch must be a real directory.")
    shutil.rmtree(scratch)


def _read_cancel_marker(
    path: Path,
    *,
    execution_id: str,
    job_id: str,
) -> bool:
    """Return whether a strict, matching cancellation marker is present.

    Absence means Slurm initiated the termination (for example timeout or
    preemption).  Once a marker exists, however, it is an immutable protocol
    record: unsafe file types, malformed JSON, schema drift, and identifiers
    for another execution/job are all hard failures rather than permission to
    continue computing.
    """
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(metadata.st_mode):
        raise RunnerValidationError(
            f"{CANCEL_MARKER_FILENAME} must not be a symbolic link: {path}"
        )
    if not stat.S_ISREG(metadata.st_mode):
        raise RunnerValidationError(
            f"{CANCEL_MARKER_FILENAME} must be a regular file: {path}"
        )

    flags = os.O_RDONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        try:
            opened_metadata = os.fstat(descriptor)
            if not stat.S_ISREG(opened_metadata.st_mode):
                raise RunnerValidationError(
                    f"{CANCEL_MARKER_FILENAME} must be a regular file: {path}"
                )
            # On platforms exposing stable inode/device identities, ensure
            # the lstat-checked path was not swapped before it was opened.
            if (
                metadata.st_ino
                and opened_metadata.st_ino
                and (
                    metadata.st_ino != opened_metadata.st_ino
                    or metadata.st_dev != opened_metadata.st_dev
                )
            ):
                raise RunnerValidationError(
                    f"{CANCEL_MARKER_FILENAME} changed while it was being read."
                )
        except BaseException:
            os.close(descriptor)
            raise
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            payload = json.load(
                handle,
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_nonfinite_json_constant,
            )
    except RunnerValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RunnerValidationError(
            f"Cannot read strict {CANCEL_MARKER_FILENAME} JSON: {exc}"
        ) from exc

    if not isinstance(payload, Mapping):
        raise RunnerValidationError(
            f"{CANCEL_MARKER_FILENAME} must be a JSON object."
        )
    fields = frozenset(payload)
    missing = sorted(_CANCEL_MARKER_FIELDS - fields)
    unknown = sorted(fields - _CANCEL_MARKER_FIELDS)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing fields: {missing}")
        if unknown:
            details.append(f"unknown fields: {unknown}")
        raise RunnerValidationError(
            f"Invalid schema-v1 {CANCEL_MARKER_FILENAME} ("
            + "; ".join(details)
            + ")."
        )
    if type(payload["schemaVersion"]) is not int or payload["schemaVersion"] != 1:
        raise RunnerValidationError(
            f"Unsupported {CANCEL_MARKER_FILENAME} schemaVersion "
            f"{payload['schemaVersion']!r}; expected 1."
        )
    if payload["executionId"] != execution_id:
        raise RunnerValidationError(
            f"{CANCEL_MARKER_FILENAME} executionId does not match this request."
        )
    if payload["jobId"] != job_id:
        raise RunnerValidationError(
            f"{CANCEL_MARKER_FILENAME} jobId does not match this Slurm allocation."
        )
    requested_at = payload["requestedAt"]
    if not isinstance(requested_at, str) or not requested_at.strip():
        raise RunnerValidationError(
            f"{CANCEL_MARKER_FILENAME}.requestedAt must be a non-empty string."
        )
    return True


class _SigtermController:
    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        execution_id: str,
        job_id: str,
        task: asyncio.Task[Any],
        cancel_marker: Path,
    ):
        self.loop = loop
        self.execution_id = execution_id
        self.job_id = job_id
        self.task = task
        self.cancel_marker = cancel_marker
        self.validation_error: RunnerValidationError | None = None
        self._uses_loop_handler = False
        self._previous_handler: Any = None

    def request_termination(self) -> None:
        if self.task.done():
            return
        try:
            explicitly_cancelled = _read_cancel_marker(
                self.cancel_marker,
                execution_id=self.execution_id,
                job_id=self.job_id,
            )
        except RunnerValidationError as exc:
            self.validation_error = exc
            logger.error("Rejecting invalid cancellation marker: %s", exc)
            state_manager.set_execution_status(
                self.execution_id,
                ExecutionStatus.FAILED,
                release_active=False,
            )
            self.task.cancel()
            return
        if explicitly_cancelled:
            if state_manager.cancel_execution(self.execution_id):
                logger.info(
                    "Explicit cancellation marker accepted for execution %s.",
                    self.execution_id,
                )
                return
        # Slurm timeout/preemption has no explicit user marker.  Leave the
        # session RUNNING while cancelling so execute_graph records the
        # durable recovery state as interrupted rather than cancelled.
        self.task.cancel()

    def install(self) -> None:
        try:
            self.loop.add_signal_handler(signal.SIGTERM, self.request_termination)
            self._uses_loop_handler = True
            return
        except (NotImplementedError, RuntimeError):
            pass

        self._previous_handler = signal.getsignal(signal.SIGTERM)

        def handler(_signum, _frame) -> None:
            self.loop.call_soon_threadsafe(self.request_termination)

        signal.signal(signal.SIGTERM, handler)

    def restore(self) -> None:
        if self._uses_loop_handler:
            self.loop.remove_signal_handler(signal.SIGTERM)
        elif self._previous_handler is not None:
            signal.signal(signal.SIGTERM, self._previous_handler)


def _terminal_message(status: str, writer: JsonlEventWriter) -> str:
    if writer.last_terminal_message:
        return writer.last_terminal_message
    return {
        ExecutionStatus.SUCCEEDED: "Workflow Finished Successfully",
        ExecutionStatus.FAILED: "Workflow execution failed",
        ExecutionStatus.CANCELLED: "Execution Cancelled",
        ExecutionStatus.INTERRUPTED: "Execution Interrupted by Slurm",
    }.get(status, f"Runner ended in unexpected state {status!r}")


async def _run_validated_request(
    request: RunnerRequest,
    paths: RunnerPaths,
    *,
    environment: Mapping[str, str] | None = None,
) -> tuple[str, str, int, str]:
    env = os.environ if environment is None else environment
    job_id = _require_slurm_job_id(env)
    writer = JsonlEventWriter(paths.events, job_id=job_id)
    observer_id = state_manager.register_broadcast_observer(
        writer,
        execution_id=request.execution_id,
    )
    status = ExecutionStatus.FAILED
    message = "Workflow execution failed"
    execution_task: asyncio.Task[Any] | None = None
    sigterm: _SigtermController | None = None
    execution_started = False

    try:
        writer.write_event(request.execution_id, {
            "type": "runner_started",
            "status": ExecutionStatus.RUNNING,
            "message": "Slurm compute runner started",
        })
        if _read_cancel_marker(
            paths.cancel_marker,
            execution_id=request.execution_id,
            job_id=job_id,
        ):
            status = ExecutionStatus.CANCELLED
            message = "Execution cancelled before compute initialization"
            writer.write_event(request.execution_id, {
                "type": "execution_finished",
                "status": status,
                "message": message,
            })
            return status, message, _EXIT_CODES[status], job_id
        validate_code_revision(request)
        load_all_plugins()
        validate_requested_resource_plan(request)
        validate_slurm_allocation(request, env)

        state_manager.start_execution(request.execution_id)
        execution_started = True
        execution_task = asyncio.create_task(
            execute_graph(
                request.graph,
                request.execution_id,
                request.execution_config,
            )
        )
        if not state_manager.attach_execution_task(
            request.execution_id,
            execution_task,
        ):
            execution_task.cancel()
            await asyncio.gather(execution_task, return_exceptions=True)
            raise RuntimeError("Failed to bind the compute runner execution task.")

        sigterm = _SigtermController(
            loop=asyncio.get_running_loop(),
            execution_id=request.execution_id,
            job_id=job_id,
            task=execution_task,
            cancel_marker=paths.cancel_marker,
        )
        sigterm.install()
        try:
            await execution_task
        except asyncio.CancelledError:
            session = state_manager.get_execution(request.execution_id)
            if sigterm is not None and sigterm.validation_error is not None:
                message = f"RunnerValidationError: {sigterm.validation_error}"
                writer.write_event(request.execution_id, {
                    "type": "runner_failed",
                    "status": ExecutionStatus.FAILED,
                    "message": message,
                })
            elif session is not None and session.status == ExecutionStatus.CANCELLING:
                state_manager.set_execution_status(
                    request.execution_id,
                    ExecutionStatus.CANCELLED,
                    release_active=False,
                )
            elif session is not None and session.status == ExecutionStatus.RUNNING:
                state_manager.set_execution_status(
                    request.execution_id,
                    ExecutionStatus.INTERRUPTED,
                    release_active=False,
                )
        except Exception as exc:
            logger.exception("Unhandled exception escaped execute_graph")
            session = state_manager.get_execution(request.execution_id)
            if session is not None and not ExecutionStatus.is_finished(session.status):
                state_manager.set_execution_status(
                    request.execution_id,
                    ExecutionStatus.FAILED,
                    release_active=False,
                )
            message = f"{type(exc).__name__}: {exc}"
            writer.write_event(request.execution_id, {
                "type": "runner_failed",
                "status": ExecutionStatus.FAILED,
                "message": message,
            })

        session = state_manager.get_execution(request.execution_id)
        if session is None or session.status not in _TERMINAL_STATUSES:
            raise RuntimeError(
                "execute_graph returned without a terminal local execution state."
            )
        status = session.status
        if writer.last_terminal_message:
            message = writer.last_terminal_message
        elif status != ExecutionStatus.FAILED:
            message = _terminal_message(status, writer)
    except Exception as exc:
        logger.exception("Slurm compute runner failed")
        status = ExecutionStatus.FAILED
        message = f"{type(exc).__name__}: {exc}"
        writer.write_event(request.execution_id, {
            "type": "runner_failed",
            "status": status,
            "message": message,
        })
    finally:
        if sigterm is not None:
            sigterm.restore()
        if execution_task is not None and not execution_task.done():
            execution_task.cancel()
            await asyncio.gather(execution_task, return_exceptions=True)
        state_manager.clear_active_execution(request.execution_id)
        if execution_started:
            try:
                await asyncio.to_thread(dask_service.stop_cluster)
            except Exception:
                logger.exception("Failed to stop the compute-node Dask cluster")
        state_manager.remove_broadcast_observer(observer_id)
        writer.close()

    exit_code = _EXIT_CODES.get(status, 1)
    return status, message, exit_code, job_id


async def run_execution_request(
    request_path: str | os.PathLike[str],
    runtime_directory: str | os.PathLike[str],
    *,
    environment: Mapping[str, str] | None = None,
) -> int:
    """Validate, execute, and atomically publish the terminal result."""
    validated_request_path, validated_runtime = _validate_runner_input_paths(
        request_path,
        runtime_directory,
    )
    fallback_result_path = validated_request_path.parent / RESULT_FILENAME
    _require_output_absent(fallback_result_path, name=RESULT_FILENAME)
    try:
        execution_id = validate_execution_id(validated_request_path.parent.name)
    except ValueError:
        execution_id = ""
    job_id = (os.environ if environment is None else environment).get(
        "SLURM_JOB_ID",
        "",
    ).strip()
    paths: RunnerPaths | None = None

    try:
        raw_request = _read_request_json(validated_request_path)
        request = parse_runner_request(raw_request)
        paths = validate_runner_paths(
            validated_request_path,
            validated_runtime,
            event_path=request.event_path,
            result_path=request.result_path,
        )
        if request.execution_id != execution_id:
            raise RunnerValidationError(
                "executionId must match the per-execution directory name."
            )
        status, message, exit_code, job_id = await _run_validated_request(
            request,
            paths,
            environment=environment,
        )
        execution_id = request.execution_id
    except Exception as exc:
        logger.exception("Runner request rejected before execution")
        status = ExecutionStatus.FAILED
        message = f"{type(exc).__name__}: {exc}"
        exit_code = _EXIT_CODES[ExecutionStatus.FAILED]

    result = {
        "schemaVersion": REQUEST_SCHEMA_VERSION,
        "executionId": execution_id,
        "jobId": job_id,
        "status": status,
        "message": message,
        "finishedAt": _utc_timestamp(),
        "runnerHost": socket.gethostname(),
        "exitCode": exit_code,
    }
    result_path = paths.result if paths is not None else fallback_result_path
    cleanup_paths = paths or RunnerPaths(
        request=validated_request_path,
        runtime_directory=validated_runtime,
        execution_directory=validated_request_path.parent,
        events=validated_request_path.parent / EVENTS_FILENAME,
        result=fallback_result_path,
        cancel_marker=validated_request_path.parent / CANCEL_MARKER_FILENAME,
    )
    try:
        _atomic_write_result(result_path, result)
    finally:
        try:
            _cleanup_job_scratch(
                cleanup_paths,
                job_id=job_id,
                environment=os.environ if environment is None else environment,
            )
        except Exception:
            logger.exception("Failed to clean the runner-owned job scratch directory")
    return exit_code


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute one immutable WorkFlow request inside Slurm.",
    )
    parser.add_argument("request_path", metavar="REQUEST_PATH")
    parser.add_argument("runtime_directory", metavar="RUNTIME_DIR")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    try:
        return asyncio.run(
            run_execution_request(args.request_path, args.runtime_directory)
        )
    except Exception as exc:
        logger.critical("Compute runner aborted before result publication: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
