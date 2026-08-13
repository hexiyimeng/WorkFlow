from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest
from distributed.core import Status

from services import dask_service as dask_service_module
from services.dask_service import (
    DaskService,
    LOCAL_CLUSTER_HOST,
    WorkflowClient,
    _add_worker_spec_batch,
    _cluster_nanny_shutdown_confirmed,
    _cleanup_failed_workflow_clients,
    _configure_worker_signal_handling,
    _control_loop_shutdown_confirmed,
    _force_kill_cluster_workers_sync,
    _force_stop_control_loop,
    _nanny_shutdown_confirmed,
    _provision_worker_specs_in_batches,
    _worker_batch_start_timeout,
    _worker_registration_timeout,
    _worker_start_batch_size,
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


@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_worker_batch_size_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("WorkFlow_DASK_WORKER_START_BATCH_SIZE", value)

    with pytest.raises(ValueError, match="positive integer"):
        _worker_start_batch_size(8)


def test_worker_batch_defaults_to_concurrent_topology_and_honors_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WorkFlow_DASK_WORKER_START_BATCH_SIZE", raising=False)
    assert _worker_start_batch_size(11) == 11

    monkeypatch.setenv("WorkFlow_DASK_WORKER_START_BATCH_SIZE", "4")
    assert _worker_start_batch_size(11) == 4

    monkeypatch.setenv("WorkFlow_DASK_WORKER_START_BATCH_SIZE", "32")
    assert _worker_start_batch_size(11) == 11


def test_worker_startup_timeout_policy_supports_slow_large_topology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WorkFlow_DASK_WORKER_BATCH_START_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("WorkFlow_DASK_WORKER_REGISTRATION_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS", raising=False)

    assert _worker_batch_start_timeout() == 180.0
    assert _worker_registration_timeout() == 60.0
    assert dask_service_module._cluster_start_timeout() == 600.0


def test_seven_healthy_slow_batches_can_exceed_old_120_second_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = {"now": 0.0}
    observed_workers: dict[str, object] = {}

    class _Cluster:
        def sync(self, function, candidate, batch, **kwargs):
            assert function is _add_worker_spec_batch
            assert candidate is self
            assert kwargs["callback_timeout"] > 50.0
            clock["now"] += 50.0
            observed_workers.update({name: {} for name in batch})

    class _Client:
        def wait_for_workers(self, count, *, timeout):
            assert len(observed_workers) == count
            assert timeout > 0

    monkeypatch.setattr(
        dask_service_module.time,
        "monotonic",
        lambda: clock["now"],
    )
    monkeypatch.setattr(
        dask_service_module,
        "get_fresh_scheduler_info",
        lambda client, *, timeout: {"workers": dict(observed_workers)},
    )
    worker_specs = {
        f"worker-{index}": {"cls": object, "options": {}}
        for index in range(14)
    }

    _provision_worker_specs_in_batches(
        _Cluster(),
        _Client(),
        worker_specs,
        deadline=600.0,
        batch_size=2,
        batch_timeout=180.0,
        registration_timeout=60.0,
    )

    assert clock["now"] == 350.0
    assert len(observed_workers) == 14


def test_slow_worker_start_gets_fresh_registration_and_identity_budgets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = {"now": 0.0}
    observed_workers: dict[str, object] = {}
    observed_timeouts: list[tuple[str, float]] = []

    class _Cluster:
        def sync(self, function, candidate, batch, **kwargs):
            assert function is _add_worker_spec_batch
            assert candidate is self
            assert kwargs["start_timeout"] == pytest.approx(180.0)
            clock["now"] = 179.5
            observed_workers.update({name: {} for name in batch})

    class _Client:
        def wait_for_workers(self, count, *, timeout):
            observed_timeouts.append(("registration", timeout))
            assert len(observed_workers) == count
            clock["now"] += 10.0

    def fresh_info(_client, *, timeout):
        observed_timeouts.append(("identity", timeout))
        return {"workers": dict(observed_workers)}

    monkeypatch.setattr(
        dask_service_module.time,
        "monotonic",
        lambda: clock["now"],
    )
    monkeypatch.setattr(
        dask_service_module,
        "get_fresh_scheduler_info",
        fresh_info,
    )

    _provision_worker_specs_in_batches(
        _Cluster(),
        _Client(),
        {"gpu-0": {"cls": object, "options": {}}},
        deadline=600.0,
        batch_size=1,
        batch_timeout=180.0,
        registration_timeout=60.0,
    )

    assert observed_timeouts == [
        ("registration", pytest.approx(60.0)),
        ("identity", pytest.approx(60.0)),
    ]


def test_nanny_start_failure_preserves_name_exception_and_exit_diagnostics() -> None:
    async def scenario() -> None:
        class _AsyncProcess:
            pid = 4321
            exitcode = 7

            def is_alive(self):
                return False

        class _WorkerProcess:
            process = _AsyncProcess()

        class _Nanny:
            def __init__(self, scheduler_address, **options):
                self.name = options["name"]
                self.status = Status.init
                self.address = "tcp://127.0.0.1:1"
                self.worker_address = ""
                self.process = _WorkerProcess()
                self._startup_lock = asyncio.Lock()

            async def start(self):
                self.status = Status.failed
                try:
                    raise OSError("spawn exploded")
                except OSError as cause:
                    raise RuntimeError("Nanny failed to start") from cause

        class _Scheduler:
            address = "tcp://127.0.0.1:1"
            contact_address = address

        class _Cluster:
            scheduler = _Scheduler()
            worker_spec: dict[str, object] = {}
            workers: dict[str, object] = {}
            _created: set[object] = set()

        with pytest.raises(RuntimeError) as error:
            await _add_worker_spec_batch(
                _Cluster(),
                {"gpu-3": {"cls": _Nanny, "options": {}}},
                start_timeout=1.0,
                cleanup_timeout=1.0,
            )

        message = str(error.value)
        assert "gpu-3" in message
        assert "RuntimeError" in message
        assert "Nanny failed to start" in message
        assert "OSError" in message
        assert "spawn exploded" in message
        assert "4321" in message
        assert "7" in message

    asyncio.run(scenario())


def test_starting_nanny_without_child_is_not_shutdown_confirmed() -> None:
    class _Nanny:
        status = Status.starting
        process = None

    nanny = _Nanny()

    assert _nanny_shutdown_confirmed(nanny) is False
    assert _cluster_nanny_shutdown_confirmed(
        type("Cluster", (), {"workers": {}, "_created": {nanny}})()
    ) is False


def test_closed_nanny_is_not_confirmed_until_startup_lock_is_released() -> None:
    async def scenario() -> None:
        lock = asyncio.Lock()
        await lock.acquire()
        nanny = type(
            "Nanny",
            (),
            {"status": Status.closed, "process": None, "_startup_lock": lock},
        )()
        assert _nanny_shutdown_confirmed(nanny) is False
        lock.release()
        assert _nanny_shutdown_confirmed(nanny) is True

    asyncio.run(scenario())


def test_cancelled_worker_batch_rolls_back_specs_and_closes_nanny() -> None:
    async def scenario() -> None:
        started = asyncio.Event()

        class _Nanny:
            instances: list[_Nanny] = []

            def __init__(self, scheduler_address, **options):
                self.scheduler_address = scheduler_address
                self.options = options
                self.status = Status.init
                self.process = None
                self._startup_lock = asyncio.Lock()
                self.__class__.instances.append(self)

            async def start(self):
                async with self._startup_lock:
                    self.status = Status.starting
                    started.set()
                    await asyncio.Event().wait()

            async def close(self, *, timeout, reason):
                self.status = Status.closed

        class _Scheduler:
            address = "tcp://127.0.0.1:1"
            contact_address = address

        class _Cluster:
            scheduler = _Scheduler()
            worker_spec: dict[str, object] = {}
            workers: dict[str, object] = {}
            _created: set[object] = set()

        cluster = _Cluster()
        task = asyncio.create_task(
            _add_worker_spec_batch(
                cluster,
                {"gpu-0": {"cls": _Nanny, "options": {}}},
                start_timeout=30.0,
                cleanup_timeout=1.0,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cluster.worker_spec == {}
        assert cluster.workers == {}
        assert _Nanny.instances[0].status == Status.closed
        assert _nanny_shutdown_confirmed(_Nanny.instances[0]) is True
        assert cluster._workflow_pending_worker_starts == {}

    asyncio.run(scenario())


def test_synchronous_batch_timeout_waits_for_transactional_rollback() -> None:
    class _LoopCluster:
        def __init__(self):
            from distributed.utils import LoopRunner

            self._loop_runner = LoopRunner()
            self._loop_runner.start()
            self.loop = self._loop_runner.loop
            self.worker_spec: dict[str, object] = {}
            self.workers: dict[str, object] = {}
            self._created: set[object] = set()
            self.scheduler = type(
                "Scheduler",
                (),
                {
                    "address": "tcp://127.0.0.1:1",
                    "contact_address": "tcp://127.0.0.1:1",
                },
            )()

        def sync(self, function, *args, **kwargs):
            from distributed.utils import sync

            callback_timeout = kwargs.pop("callback_timeout")
            return sync(
                self.loop,
                function,
                *args,
                callback_timeout=callback_timeout,
                **kwargs,
            )

    class _Nanny:
        instances: list[_Nanny] = []

        def __init__(self, scheduler_address, **options):
            self.status = Status.init
            self.process = None
            self._startup_lock = asyncio.Lock()
            self.__class__.instances.append(self)

        async def start(self):
            async with self._startup_lock:
                self.status = Status.starting
                await asyncio.Event().wait()

        async def close(self, *, timeout, reason):
            self.status = Status.closed

    cluster = _LoopCluster()
    try:
        with pytest.raises(TimeoutError):
            cluster.sync(
                _add_worker_spec_batch,
                cluster,
                {"gpu-0": {"cls": _Nanny, "options": {}}},
                start_timeout=0.05,
                cleanup_timeout=0.2,
                callback_timeout=1.0,
            )

        assert cluster.worker_spec == {}
        assert cluster.workers == {}
        assert cluster._workflow_pending_worker_starts == {}
        assert _Nanny.instances[0].status == Status.closed
        assert _nanny_shutdown_confirmed(_Nanny.instances[0]) is True
    finally:
        cluster._loop_runner.stop()


def test_control_loop_must_stop_before_runtime_handle_is_discarded() -> None:
    class _LoopRunner:
        started = True

        def is_started(self):
            return self.started

        def stop(self, *, timeout):
            assert timeout == 2.0
            self.started = False

    owner = type("Owner", (), {"_loop_runner": _LoopRunner()})()

    assert _control_loop_shutdown_confirmed(owner) is False
    _force_stop_control_loop(owner, timeout=2.0, label="test")
    assert _control_loop_shutdown_confirmed(owner) is True


def test_looprunner_started_flag_does_not_hide_a_lingering_control_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Thread:
        alive = True

        def is_alive(self):
            return self.alive

        def join(self, timeout):
            pass

    thread = _Thread()

    class _LoopRunner:
        _all_loops = {}
        _loop = object()
        _loop_thread = thread
        started = True

        def is_started(self):
            return self.started

        def stop(self, *, timeout):
            # Mirrors Distributed 2026.3: the public started flag is cleared
            # and the thread reference is dropped before a bounded join
            # timeout is reported.
            self.started = False
            self._loop_thread = None
            raise TimeoutError("join timed out")

    class _ClientOwner:
        status = "closing"
        _loop_runner = _LoopRunner()

    owner = _ClientOwner()
    monkeypatch.setattr(dask_service_module, "Client", _ClientOwner)

    with pytest.raises(RuntimeError, match="control thread remained active"):
        _force_stop_control_loop(owner, timeout=0.1, label="test")
    assert _control_loop_shutdown_confirmed(owner) is False

    thread.alive = False
    # A later cleanup pass owns the captured thread and can finalize the owner
    # without invoking the already-detached LoopRunner again.
    _force_stop_control_loop(owner, timeout=0.1, label="test")
    assert _control_loop_shutdown_confirmed(owner) is True
    assert not hasattr(owner, "_workflow_lingering_loop_thread")
    assert owner.status == "closed"


def test_failed_independent_client_connection_does_not_leak_ioloop_thread() -> None:
    import threading
    from distributed.utils import LoopRunner

    baseline_loops = len(LoopRunner._all_loops)
    baseline_threads = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name == "IO loop"
    }

    with pytest.raises((OSError, TimeoutError)):
        WorkflowClient(
            "tcp://127.0.0.1:1",
            timeout=0.2,
            set_as_default=False,
        )

    assert len(LoopRunner._all_loops) == baseline_loops
    assert {
        thread.ident
        for thread in threading.enumerate()
        if thread.name == "IO loop"
    } == baseline_threads


def test_public_close_timeout_with_live_detached_thread_poisons_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Thread:
        alive = True

        def is_alive(self):
            return self.alive

        def join(self, timeout):
            pass

    thread = _Thread()

    class _LoopRunner:
        _all_loops = {}
        _loop = object()
        _loop_thread = thread
        started = True

        def is_started(self):
            return self.started

        def stop(self, *, timeout):
            self.started = False
            self._loop_thread = None
            raise TimeoutError("join timed out")

    class _Client:
        status = "running"
        _loop_runner = _LoopRunner()

        def close(self, *, timeout):
            self.status = "closing"
            self._loop_runner.stop(timeout=timeout)

    class _Cluster:
        def close(self, *, timeout):
            pass

    monkeypatch.setattr(dask_service_module, "Client", _Client)
    service = object.__new__(DaskService)
    client = _Client()
    cluster = _Cluster()
    service.client = client
    service.cluster = cluster
    service.active_cpu_workers = 1
    service.active_gpu_workers = 0
    service.active_gpu_ids = ()
    service._cluster_poisoned = False

    with pytest.raises(RuntimeError, match="poisoned"):
        service.stop_cluster()

    assert service.client is client
    assert service.cluster is cluster
    assert service._cluster_poisoned is True
    assert client._workflow_lingering_loop_thread is thread


def test_failed_client_lingering_loop_blocks_replacement_until_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Thread:
        alive = True

        def is_alive(self):
            return self.alive

        def join(self, timeout):
            pass

    class _LoopRunner:
        def is_started(self):
            return False

    failed_client = object.__new__(WorkflowClient)
    failed_client.status = "closed"
    failed_client._loop_runner = _LoopRunner()
    failed_client._workflow_lingering_loop_thread = _Thread()
    monkeypatch.setattr(WorkflowClient, "_failed_start_clients", [failed_client])

    with pytest.raises(RuntimeError, match="replacement cluster startup is blocked"):
        _cleanup_failed_workflow_clients()

    failed_client._workflow_lingering_loop_thread.alive = False
    _cleanup_failed_workflow_clients()
    assert WorkflowClient._failed_start_clients == []


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


def test_emergency_cluster_cleanup_kills_children_but_rejects_nonterminal_nannies() -> None:
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
        status = Status.closing

        async def close(self, *, timeout, reason):
            calls.append(("nanny-close", timeout))
            raise TimeoutError(reason)

    class _Cluster:
        workers = {"a": _Nanny(), "b": _Nanny()}

        def sync(self, function, *args, **kwargs):
            import asyncio

            kwargs.pop("callback_timeout")
            return asyncio.run(function(*args, **kwargs))

    with pytest.raises(RuntimeError, match="non-terminal"):
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
    shutdown_state = {"confirmed": False}
    monkeypatch.setattr(
        dask_service_module,
        "_cluster_nanny_shutdown_confirmed",
        lambda cluster: shutdown_state["confirmed"],
    )

    def _confirm_emergency_cleanup(cluster, *, timeout):
        calls.append(("force-kill", cluster, timeout))
        shutdown_state["confirmed"] = True

    monkeypatch.setattr(
        dask_service_module,
        "_force_kill_cluster_workers_sync",
        _confirm_emergency_cleanup,
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
        "_cluster_nanny_shutdown_confirmed",
        lambda candidate: False,
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
    environment.pop("WorkFlow_DASK_WORKER_START_BATCH_SIZE", None)
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
    assert result["startupEvents"] == [
        "startup_requested",
        "client_connected_before_workers",
        "workers_ready",
    ]
    assert result["clientOwnsCluster"] is False
    assert result["independentLoops"] is True
    assert len(result["registeredBatches"]) == 1
    assert len(result["registeredBatches"][0]) == 14
    roles = [entry["role"] for entry in result["taskResults"]]
    assert roles.count("cpu") == 6
    assert roles.count("gpu") == 8
    assert len({entry["address"] for entry in result["taskResults"]}) == 14
    cpu_entries = [
        entry for entry in result["taskResults"] if entry["role"] == "cpu"
    ]
    assert all(entry["cudaVisibleDevices"] == "" for entry in cpu_entries)
    assert all(entry["resources"] == {"CPU": 1} for entry in cpu_entries)
    gpu_entries = [
        entry for entry in result["taskResults"] if entry["role"] == "gpu"
    ]
    assert all(entry["resources"] == {"GPU": 1} for entry in gpu_entries)
    assert all(
        entry["physicalGpuId"] == entry["cudaVisibleDevices"]
        for entry in gpu_entries
    )
    gpu_masks = sorted(
        entry["cudaVisibleDevices"]
        for entry in gpu_entries
    )
    assert gpu_masks == [str(index) for index in range(8)]
