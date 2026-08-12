from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any

import psutil
import pytest

from core import window_execution
from core.window_execution import (
    ACTIVE_LOCK_SCHEMA_VERSION,
    ActiveExecutionLock,
    ExecutionLayout,
    RecoveryLockError,
    _active_lock_owner_state,
    cleanup_stale_active_lock,
)


def _foreign_lock_payload(*, slurm_job_id: str | None = "4321") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schemaVersion": ACTIVE_LOCK_SCHEMA_VERSION,
        "executionId": "execution-1",
        "pid": 1234,
        "hostname": "compute-node-foreign",
        "processCreateTime": 12345.0,
        "lockId": "lock-1",
        "createdAt": "2026-08-09T00:00:00+00:00",
    }
    if slurm_job_id is not None:
        payload["slurmJobId"] = slurm_job_id
    return payload


def _completed_process(
    argv: list[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(argv, returncode, stdout, stderr)


def test_active_lock_records_current_slurm_job_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLURM_JOB_ID", " 4321 ")
    layout = ExecutionLayout(tmp_path / "recovery")

    with ActiveExecutionLock(layout, "execution-1"):
        payload = json.loads(layout.lock_path.read_text(encoding="utf-8"))
        assert payload["slurmJobId"] == "4321"

    assert not layout.lock_path.exists()


def test_active_lock_without_slurm_environment_preserves_existing_format(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    layout = ExecutionLayout(tmp_path / "recovery")

    with ActiveExecutionLock(layout, "execution-1"):
        payload = json.loads(layout.lock_path.read_text(encoding="utf-8"))
        assert "slurmJobId" not in payload


def test_invalid_slurm_job_id_refuses_to_create_unrecoverable_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLURM_JOB_ID", "4321; scancel 9")
    layout = ExecutionLayout(tmp_path / "recovery")

    with pytest.raises(RecoveryLockError, match="positive decimal"):
        ActiveExecutionLock(layout, "execution-1").acquire()

    assert not layout.lock_path.exists()


def test_foreign_lock_is_alive_when_squeue_lists_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        assert kwargs["shell"] is False
        assert kwargs["timeout"] == window_execution.SLURM_COMMAND_TIMEOUT_SECONDS
        return _completed_process(argv, stdout="RUNNING\n")

    monkeypatch.setattr(window_execution.subprocess, "run", fake_run)

    state = _active_lock_owner_state(
        _foreign_lock_payload(),
        lock_path=tmp_path / "active.lock",
    )

    assert state == "alive"
    assert len(calls) == 1
    assert calls[0][0] == "squeue"
    assert calls[0][calls[0].index("--jobs") + 1] == "4321"


@pytest.mark.parametrize(
    "terminal_state",
    ["COMPLETED", "FAILED", "OUT_OF_MEMORY", "CANCELLED by 1000", "TIMEOUT+"],
)
def test_foreign_lock_is_stale_only_after_sacct_reports_terminal_allocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal_state: str,
) -> None:
    def fake_run(argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if argv[0] == "squeue":
            return _completed_process(argv, stdout="")
        return _completed_process(
            argv,
            stdout=f"4321|{terminal_state}|\n4321.batch|COMPLETED|\n",
        )

    monkeypatch.setattr(window_execution.subprocess, "run", fake_run)

    assert _active_lock_owner_state(
        _foreign_lock_payload(),
        lock_path=tmp_path / "active.lock",
    ) == "stale"


@pytest.mark.parametrize(
    "queue_result,accounting_output",
    [
        (_completed_process(["squeue"], returncode=1, stderr="denied"), ""),
        (_completed_process(["squeue"], stdout=""), ""),
        (_completed_process(["squeue"], stdout=""), "4321|RUNNING|\n"),
        (_completed_process(["squeue"], stdout=""), "4321.batch|FAILED|\n"),
        (_completed_process(["squeue"], stdout=""), "4321|UNKNOWN|\n"),
    ],
)
def test_foreign_lock_remains_unknown_without_authoritative_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    queue_result: subprocess.CompletedProcess[str],
    accounting_output: str,
) -> None:
    def fake_run(argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if argv[0] == "squeue":
            return queue_result
        return _completed_process(argv, stdout=accounting_output)

    monkeypatch.setattr(window_execution.subprocess, "run", fake_run)

    assert _active_lock_owner_state(
        _foreign_lock_payload(),
        lock_path=tmp_path / "active.lock",
    ) == "unknown"


def test_scheduler_query_timeout_keeps_foreign_lock_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(argv, 3.0)

    monkeypatch.setattr(window_execution.subprocess, "run", fake_run)

    assert _active_lock_owner_state(
        _foreign_lock_payload(),
        lock_path=tmp_path / "active.lock",
    ) == "unknown"


def test_foreign_non_slurm_lock_is_never_reclaimed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_run(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("scheduler must not be queried without slurmJobId")

    monkeypatch.setattr(window_execution.subprocess, "run", unexpected_run)

    assert _active_lock_owner_state(
        _foreign_lock_payload(slurm_job_id=None),
        lock_path=tmp_path / "active.lock",
    ) == "unknown"


def test_local_dead_process_still_uses_existing_pid_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _foreign_lock_payload()
    payload["hostname"] = "local-node"
    monkeypatch.setattr(window_execution, "_normalized_hostname", lambda: "local-node")

    def missing_process(_pid: int) -> psutil.Process:
        raise psutil.NoSuchProcess(_pid)

    monkeypatch.setattr(window_execution.psutil, "Process", missing_process)
    monkeypatch.setattr(
        window_execution.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("local locks must not query Slurm"),
    )

    assert _active_lock_owner_state(
        payload,
        lock_path=tmp_path / "active.lock",
    ) == "stale"


def test_cleanup_removes_only_scheduler_confirmed_terminal_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ExecutionLayout(tmp_path / "recovery")
    layout.control_directory.mkdir(parents=True)
    layout.lock_path.write_text(
        json.dumps(_foreign_lock_payload()),
        encoding="utf-8",
    )

    def terminal_run(argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if argv[0] == "squeue":
            return _completed_process(argv, stdout="")
        return _completed_process(argv, stdout="4321|NODE_FAIL|\n")

    monkeypatch.setattr(window_execution.subprocess, "run", terminal_run)

    assert cleanup_stale_active_lock(layout) is True
    assert not layout.lock_path.exists()


def test_cleanup_keeps_lock_when_scheduler_query_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ExecutionLayout(tmp_path / "recovery")
    layout.control_directory.mkdir(parents=True)
    layout.lock_path.write_text(
        json.dumps(_foreign_lock_payload()),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        window_execution.subprocess,
        "run",
        lambda argv, **_kwargs: _completed_process(argv, returncode=1),
    )

    assert cleanup_stale_active_lock(layout) is False
    assert layout.lock_path.exists()
