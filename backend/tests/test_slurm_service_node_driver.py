import asyncio
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

from core.slurm_execution import SlurmPolicy
from core.state_manager import ExecutionStatus, state_manager
from services import slurm_execution_service as module


def _config(tmp_path: Path) -> module.SlurmRuntimeConfig:
    root = tmp_path / "checkout"
    script = root / "deploy" / "hpc" / "slurm" / "workflow_workers.sbatch"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/bash\n", encoding="utf-8")
    (root / "backend").mkdir()
    (root / "backend" / "main.py").write_text("", encoding="utf-8")
    runtime = tmp_path / "runtime"
    execution_root = runtime / "jobs"
    execution_root.mkdir(parents=True)
    cli = tmp_path / "cli"
    cli.mkdir()
    executables = {}
    for name in ("squeue", "scontrol"):
        path = cli / name
        path.write_text("#!/bin/sh\n", encoding="utf-8")
        path.chmod(0o755)
        executables[name] = str(path.resolve())
    policy = SlurmPolicy(
        partition="compute",
        time_limit="01:00:00",
        base_cpus=1,
        cpus_per_cpu_worker=1,
        cpus_per_gpu_worker=1,
        base_memory_gib=1,
        memory_gib_per_cpu_worker=2,
        memory_gib_per_gpu_worker=4,
        max_cpu_workers=8,
        max_gpu_workers=8,
        max_cpus=32,
        max_gpus=8,
        max_memory_gib=64,
        max_nodes=4,
        cpus_per_node=8,
        gpus_per_node=2,
        memory_gib_per_node=16,
        allowed_partitions=("compute",),
    )
    return module.SlurmRuntimeConfig(
        runtime_directory=runtime.resolve(),
        execution_root=execution_root.resolve(),
        execution_script=script.resolve(),
        project_root=root.resolve(),
        policy=policy,
        sbatch_executable="sbatch",
        squeue_executable=executables["squeue"],
        sacct_executable=None,
        scontrol_executable=executables["scontrol"],
        scancel_executable="scancel",
        poll_interval_seconds=0.001,
        result_grace_seconds=0.001,
        cancel_grace_seconds=1.0,
        scheduler_host="mn02.internal",
        scheduler_port=8786,
        worker_port_range="20000:20015",
        nanny_port_range="20100:20115",
        worker_start_timeout_seconds=10.0,
    )


def test_driver_runs_on_service_node_and_slurm_job_contains_only_workers(
    tmp_path, monkeypatch
):
    state_manager._init_state()
    config = _config(tmp_path)
    plan = SimpleNamespace(cpu_workers=1, gpu_workers=3)
    graph = {"terminal": {"type": "Output", "inputs": {}}}
    monkeypatch.setattr(
        module,
        "_authoritative_graph_and_plan",
        lambda value, selected: (value, plan),
    )
    monkeypatch.setattr(
        module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    monkeypatch.setattr(module, "_git_revision", lambda root: "a" * 40)
    monkeypatch.setattr(
        module.SlurmExecutionService,
        "_worker_security_payload",
        staticmethod(lambda selected: None),
    )

    events = []
    client = SimpleNamespace(
        scheduler=SimpleNamespace(address="tcp://mn02.internal:8786")
    )
    monkeypatch.setattr(
        module.dask_service,
        "start_external_scheduler",
        lambda **kwargs: events.append(("scheduler", kwargs)) or client,
    )
    monkeypatch.setattr(
        module.dask_service,
        "activate_external_workers",
        lambda **kwargs: events.append(("workers", kwargs)),
    )
    monkeypatch.setattr(
        module.dask_service,
        "stop_cluster",
        lambda: events.append(("scheduler_closed", None)) or True,
    )

    async def execute_locally(value, execution_id, selected, **kwargs):
        assert os.environ["WorkFlow_SLURM_WORKER_JOB_ID"] == "123"
        events.append(("driver", kwargs))
        await kwargs["external_cleanup_barrier"]()
        # Models executor's active.lock release point: it must occur only
        # after Slurm terminal proof and Scheduler close.
        events.append(("recovery_lock_released", None))
        state_manager.set_execution_status(
            execution_id, ExecutionStatus.SUCCEEDED, release_active=False
        )

    monkeypatch.setattr(module, "execute_graph_on_service_node", execute_locally)
    service = module.SlurmExecutionService()
    monkeypatch.setattr(
        service,
        "_run_command",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, "123\n", ""),
    )

    async def terminate(**kwargs):
        events.append(("worker_terminal", kwargs["job_id"]))
        return "CANCELLED", "0:15", "c001,c002"

    monkeypatch.setattr(
        service,
        "_wait_for_worker_allocation_running",
        lambda **kwargs: asyncio.sleep(0),
    )
    monkeypatch.setattr(service, "_wait_for_worker_allocation_terminal", terminate)
    state_manager.start_execution("execution-1")

    result = asyncio.run(
        service.execute_graph(graph, "execution-1", {"mode": "full_graph"})
    )

    assert result == "execution-1"
    assert [name for name, _ in events] == [
        "scheduler",
        "workers",
        "driver",
        "worker_terminal",
        "scheduler_closed",
        "recovery_lock_released",
    ]
    request = json.loads(
        (config.execution_root / "execution-1" / "request.json").read_text(
            encoding="utf-8"
        )
    )
    assert request["schemaVersion"] == 2
    assert request["allowInsecure"] is True
    assert request["workerMemoryGiB"] == {"cpu": 2, "gpu": 4}
    assert request["schedulerAddress"] == "tcp://mn02.internal:8786"
    assert request["resourcePlan"]["nodes"] == 2
    assert request["resourcePlan"]["gpuWorkersByNode"] == [2, 1]
    assert "graph" not in request
    assert "executionConfig" not in request
    assert config.execution_script.name == "workflow_workers.sbatch"
    assert "WorkFlow_SLURM_WORKER_JOB_ID" not in os.environ
    assert state_manager.active_execution_id is None


def test_restart_reconciliation_cancels_orphan_workers_without_executing_graph(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    run = config.execution_root / "orphan"
    run.mkdir()
    record = {
        "schemaVersion": 2,
        "executionId": "orphan",
        "jobId": "456",
        "cluster": None,
        "state": "driver_running",
        "submissionToken": "wf:orphan:token",
        "resources": config.policy.resource_request(
            cpu_workers=1, gpu_workers=0
        ).to_dict(),
    }
    (run / "job.json").write_text(json.dumps(record), encoding="utf-8")
    monkeypatch.setattr(
        module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    service = module.SlurmExecutionService()
    calls = []

    async def terminate(**kwargs):
        calls.append(kwargs["job_id"])
        return "CANCELLED", "0:15", "c001"

    monkeypatch.setattr(service, "_wait_for_worker_allocation_terminal", terminate)
    monkeypatch.setattr(
        module,
        "execute_graph_on_service_node",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("reconcile must not resume a graph")
        ),
    )

    assert asyncio.run(service.reconcile_active_job()) is None
    assert calls == ["456"]
    updated = json.loads((run / "job.json").read_text(encoding="utf-8"))
    assert updated["state"] == ExecutionStatus.INTERRUPTED


def test_worker_terminal_barrier_retries_scancel_and_requires_terminal_proof(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    service = module.SlurmExecutionService()
    calls = []

    async def cancel(*args, **kwargs):
        calls.append("cancel")
        if calls.count("cancel") == 1:
            raise module.SlurmSubmissionError("temporary controller outage")

    async def terminal(*args, **kwargs):
        calls.append("terminal")
        return True, ("CANCELLED", "0:15", "c001")

    monkeypatch.setattr(service, "_send_cancel", cancel)
    monkeypatch.setattr(service, "_query_terminal_state", terminal)

    result = asyncio.run(service._wait_for_worker_allocation_terminal(
        config=config,
        execution_id="barrier",
        job_id="789",
        cluster=None,
        submission_token="wf:barrier:token",
    ))

    assert result == ("CANCELLED", "0:15", "c001")
    assert calls == ["cancel", "cancel", "terminal"]


def test_worker_registration_clock_starts_only_after_allocation_runs(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    service = module.SlurmExecutionService()
    states = iter((
        (True, ("PENDING", "", "Resources")),
        (True, ("PENDING", "", "Priority")),
        (True, ("RUNNING", "c001,c002", "")),
    ))
    observed = []

    async def queue_state(*args, **kwargs):
        return next(states)

    async def broadcast(*args, **kwargs):
        observed.append(args[3])

    monkeypatch.setattr(service, "_query_queue_state", queue_state)
    monkeypatch.setattr(service, "_broadcast_job_state", broadcast)
    asyncio.run(service._wait_for_worker_allocation_running(
        config=config,
        execution_id="queued-1",
        job_id="900",
        cluster=None,
        submission_token="wf:queued:one",
        resource_request=config.policy.resource_request(
            cpu_workers=1, gpu_workers=0
        ),
    ))

    assert observed == ["PENDING", "PENDING", "RUNNING"]
