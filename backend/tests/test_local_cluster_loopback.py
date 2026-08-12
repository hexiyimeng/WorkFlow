from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest

from services import dask_service as dask_service_module
from services.dask_service import (
    DaskService,
    LOCAL_CLUSTER_HOST,
    _configure_worker_signal_handling,
    _force_kill_cluster_workers_sync,
    build_local_cluster_specs,
)


def test_every_same_host_cluster_process_is_bound_to_loopback(tmp_path: Path) -> None:
    scheduler_spec, worker_specs = build_local_cluster_specs(
        cpu_workers=2,
        gpu_ids=("2", "7"),
        cpu_memory_limit="1GiB",
        gpu_memory_limit="2GiB",
        local_directory=str(tmp_path),
        dashboard_address=":0",
    )

    assert scheduler_spec["options"]["host"] == LOCAL_CLUSTER_HOST
    assert worker_specs
    assert {
        spec["options"]["host"] for spec in worker_specs.values()
    } == {LOCAL_CLUSTER_HOST}


def test_windows_worker_ignores_python_and_native_console_ctrl_c(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setenv("WORKFLOW_DASK_WORKER_PROCESS", "1")
    monkeypatch.setattr(dask_service_module.os, "name", "nt")
    monkeypatch.setattr(signal, "signal", lambda *args: calls.append(args))
    monkeypatch.setattr(
        dask_service_module,
        "_set_windows_console_ctrl_c_ignore_flag",
        lambda: calls.append("native"),
    )

    _configure_worker_signal_handling()

    assert calls == ["native", (signal.SIGINT, signal.SIG_IGN)]


def test_native_ctrl_c_protection_survives_python_signal_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setenv("WORKFLOW_DASK_WORKER_PROCESS", "1")
    monkeypatch.setattr(dask_service_module.os, "name", "nt")
    monkeypatch.setattr(
        dask_service_module,
        "_set_windows_console_ctrl_c_ignore_flag",
        lambda: calls.append("native"),
    )

    def _reject_python_signal(*_args):
        raise ValueError("signal only works in main thread")

    monkeypatch.setattr(signal, "signal", _reject_python_signal)

    _configure_worker_signal_handling()

    assert calls == ["native"]


def test_emergency_cluster_cleanup_kills_every_nanny_child_process() -> None:
    calls: list[tuple[str, float | None]] = []

    class _AsyncProcess:
        alive = True

        async def kill(self):
            calls.append(("kill", None))
            self.alive = False

        async def join(self, timeout):
            calls.append(("join", timeout))

        def is_alive(self):
            return self.alive

    class _WorkerProcess:
        process = _AsyncProcess()

    class _Nanny:
        process = _WorkerProcess()

        async def close(self, *, timeout, reason):
            calls.append(("nanny-close", timeout))
            raise TimeoutError(reason)

    class _Cluster:
        workers = {"a": _Nanny(), "b": _Nanny()}

        def sync(self, function, *args, **kwargs):
            import asyncio

            kwargs.pop("callback_timeout")
            return asyncio.run(function(*args, **kwargs))

    _force_kill_cluster_workers_sync(_Cluster(), timeout=2.0)

    assert calls == [
        ("nanny-close", 1.6),
        ("kill", None),
        ("join", 2.0),
        ("nanny-close", 1.6),
        ("kill", None),
        ("join", 2.0),
    ]


def test_stop_cluster_reports_graceful_failure_after_emergency_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    class _Client:
        def close(self, *, timeout):
            calls.append(("client-close", timeout))
            raise TimeoutError("client hung")

    class _Cluster:
        def close(self, *, timeout):
            calls.append(("cluster-close", timeout))
            raise TimeoutError("cluster hung")

    service = object.__new__(DaskService)
    service.client = _Client()
    service.cluster = _Cluster()
    service.active_cpu_workers = 4
    service.active_gpu_workers = 8
    service.active_gpu_ids = tuple(str(index) for index in range(8))
    service._cluster_poisoned = False
    monkeypatch.setattr(
        dask_service_module,
        "_cluster_worker_processes_alive",
        lambda cluster: True,
    )
    monkeypatch.setattr(
        dask_service_module,
        "_force_kill_cluster_workers_sync",
        lambda cluster, *, timeout: calls.append(("force-kill", cluster, timeout)),
    )

    assert service.stop_cluster() is False

    assert service.client is None
    assert service.cluster is None
    assert service.active_cpu_workers == 0
    assert service.active_gpu_workers == 0
    assert service.active_gpu_ids == ()
    assert service._cluster_poisoned is False
    assert [entry[0] for entry in calls] == [
        "client-close",
        "cluster-close",
        "force-kill",
    ]
    assert calls[0][1] <= 10.0
    assert calls[1][1] > 40.0


def test_stop_cluster_retains_poisoned_handles_when_child_exit_is_unconfirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        def close(self, *, timeout):
            raise TimeoutError("client hung")

    class _Cluster:
        def close(self, *, timeout):
            raise TimeoutError("cluster hung")

    service = object.__new__(DaskService)
    client = _Client()
    cluster = _Cluster()
    service.client = client
    service.cluster = cluster
    service.active_cpu_workers = 4
    service.active_gpu_workers = 8
    service.active_gpu_ids = tuple(str(index) for index in range(8))
    service._cluster_poisoned = False
    monkeypatch.setattr(
        dask_service_module,
        "_cluster_worker_processes_alive",
        lambda candidate: True,
    )

    def _fail_emergency_cleanup(candidate, *, timeout):
        raise TimeoutError("worker survived")

    monkeypatch.setattr(
        dask_service_module,
        "_force_kill_cluster_workers_sync",
        _fail_emergency_cleanup,
    )

    with pytest.raises(RuntimeError, match="poisoned"):
        service.stop_cluster()

    assert service.client is client
    assert service.cluster is cluster
    assert service._cluster_poisoned is True
    assert service.active_cpu_workers == 0
    assert service.active_gpu_workers == 0
    assert service.active_gpu_ids == ()


def test_real_eight_gpu_six_cpu_nannies_validate_over_loopback() -> None:
    """Exercise the failing Windows topology without importing CUDA/PyTorch."""
    backend_dir = Path(__file__).resolve().parents[1]
    probe = Path(__file__).with_name("test_support_local_cluster_loopback_probe.py")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(backend_dir)
    completed = subprocess.run(
        [sys.executable, str(probe)],
        cwd=backend_dir,
        env=environment,
        capture_output=True,
        text=True,
        timeout=300.0,
        check=False,
    )
    output = "\n".join((completed.stdout, completed.stderr))
    assert completed.returncode == 0, output
    result_lines = [
        line for line in completed.stdout.splitlines()
        if line.startswith("PROBE_RESULT=")
    ]
    assert result_lines, output
    result = json.loads(result_lines[-1].split("=", 1)[1])

    assert result["schedulerAddress"].startswith("tcp://127.0.0.1:")
    assert len(result["workerAddresses"]) == 14
    assert result["validatedCpuWorkers"] == 6
    assert result["validatedGpuWorkers"] == 8
    assert all(
        address.startswith("tcp://127.0.0.1:")
        for address in result["workerAddresses"]
    )
    roles = [entry["role"] for entry in result["taskResults"].values()]
    assert roles.count("cpu") == 6
    assert roles.count("gpu") == 8
    gpu_masks = sorted(
        entry["cudaVisibleDevices"]
        for entry in result["taskResults"].values()
        if entry["role"] == "gpu"
    )
    assert gpu_masks == [str(index) for index in range(8)]
