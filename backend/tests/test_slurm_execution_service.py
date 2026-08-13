import asyncio
import json
from pathlib import Path
import subprocess
import threading
from types import SimpleNamespace

import pytest

from core.slurm_execution import SlurmPolicy
from core.state_manager import ExecutionStatus, state_manager
from services import slurm_execution_service as service_module


def _policy() -> SlurmPolicy:
    return SlurmPolicy(
        partition="compute",
        time_limit="01:00:00",
        base_cpus=1,
        cpus_per_cpu_worker=1,
        cpus_per_gpu_worker=2,
        base_memory_gib=4,
        memory_gib_per_cpu_worker=8,
        memory_gib_per_gpu_worker=64,
        max_cpu_workers=16,
        max_gpu_workers=8,
        max_cpus=64,
        max_gpus=8,
        max_memory_gib=512,
        allowed_partitions=("compute",),
    )


def _runtime_config(tmp_path: Path) -> service_module.SlurmRuntimeConfig:
    root = tmp_path / "checkout"
    script = root / "deploy" / "hpc" / "slurm" / "workflow_execution.sbatch"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/bash\n", encoding="utf-8")
    backend = root / "backend"
    backend.mkdir()
    (backend / "main.py").write_text("", encoding="utf-8")
    runtime = tmp_path / "runtime"
    execution_root = runtime / "jobs"
    execution_root.mkdir(parents=True)
    cli_directory = tmp_path / "slurm-cli"
    cli_directory.mkdir()
    squeue = cli_directory / "squeue"
    sacct = cli_directory / "sacct"
    scontrol = cli_directory / "scontrol"
    for executable in (squeue, sacct, scontrol):
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o755)
    return service_module.SlurmRuntimeConfig(
        runtime_directory=runtime.resolve(),
        execution_root=execution_root.resolve(),
        execution_script=script.resolve(),
        project_root=root.resolve(),
        policy=_policy(),
        sbatch_executable="sbatch",
        squeue_executable=str(squeue.resolve()),
        sacct_executable=str(sacct.resolve()),
        scontrol_executable=str(scontrol.resolve()),
        scancel_executable="scancel",
        poll_interval_seconds=0.001,
        result_grace_seconds=0.001,
        cancel_grace_seconds=1.0,
    )


@pytest.fixture(autouse=True)
def _reset_state_manager():
    state_manager._init_state()
    yield
    state_manager._init_state()


def test_runtime_config_finds_checkout_above_deploy_directory(tmp_path):
    config = _runtime_config(tmp_path)
    environment = {
        "WorkFlow_SLURM_RUNTIME_DIR": str(config.runtime_directory),
        "WorkFlow_SLURM_EXECUTION_SCRIPT": str(config.execution_script),
        "WorkFlow_SLURM_SQUEUE": config.squeue_executable,
        "WorkFlow_SLURM_SACCT": config.sacct_executable,
        "WorkFlow_SLURM_SCONTROL": config.scontrol_executable,
    }

    loaded = service_module.SlurmRuntimeConfig.from_environment(environment)

    assert loaded.project_root == config.project_root
    assert loaded.execution_root == config.execution_root
    assert loaded.squeue_executable == config.squeue_executable
    assert loaded.sacct_executable == config.sacct_executable
    assert loaded.scontrol_executable == config.scontrol_executable


def test_runtime_config_rejects_missing_status_command(tmp_path):
    config = _runtime_config(tmp_path)
    environment = {
        "WorkFlow_SLURM_RUNTIME_DIR": str(config.runtime_directory),
        "WorkFlow_SLURM_EXECUTION_SCRIPT": str(config.execution_script),
        "WorkFlow_SLURM_SQUEUE": str(tmp_path / "missing-squeue"),
        "WorkFlow_SLURM_SACCT": config.sacct_executable,
        "WorkFlow_SLURM_SCONTROL": config.scontrol_executable,
    }

    with pytest.raises(ValueError, match="WorkFlow_SLURM_SQUEUE"):
        service_module.SlurmRuntimeConfig.from_environment(environment)


def test_slurm_policy_from_environment_is_pure_and_honors_limits(tmp_path):
    absent_runtime = tmp_path / "must-not-be-created"
    policy = service_module.slurm_policy_from_environment({
        "WorkFlow_SLURM_RUNTIME_DIR": str(absent_runtime),
        "WorkFlow_SLURM_PARTITION": "gpu-old",
        "WorkFlow_SLURM_ALLOWED_PARTITIONS": "gpu-old,cpu-old",
        "WorkFlow_SLURM_MAX_CPU_WORKERS": "12",
        "WorkFlow_SLURM_MAX_GPU_WORKERS": "4",
        "WorkFlow_SLURM_MAX_GPUS": "4",
        "WorkFlow_SLURM_GPU_WORKER_MEMORY_GIB": "48",
    })

    assert policy.partition == "gpu-old"
    assert policy.allowed_partitions == ("gpu-old", "cpu-old")
    assert policy.max_cpu_workers == 12
    assert policy.max_gpu_workers == 4
    assert policy.max_gpus == 4
    assert policy.memory_gib_per_gpu_worker == 48
    assert not absent_runtime.exists()


def test_runtime_directory_rejects_filesystem_root():
    root = Path(Path.cwd().anchor)
    with pytest.raises(ValueError, match="filesystem root"):
        service_module._absolute_directory(str(root), name="runtime")


def test_squeue_failure_is_not_reported_as_job_absence(tmp_path, monkeypatch):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    monkeypatch.setattr(
        service,
        "_run_command",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=args[0], returncode=1, stdout="", stderr="controller unavailable"
        ),
    )

    succeeded, row = asyncio.run(service._query_queue_state(config, "123"))

    assert succeeded is False
    assert row is None


def test_squeue_requires_one_exact_root_job_row(tmp_path, monkeypatch):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    outputs = iter([
        "123|wf:token:one|RUNNING|c001|None\n",
        "123.batch|wf:token:one|RUNNING|c001|None\n",
        "123|wf:token:one|RUNNING|c001|None\n123|wf:token:one|PENDING||Resources\n",
    ])
    monkeypatch.setattr(
        service,
        "_run_command",
        lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, next(outputs), ""
        ),
    )

    async def scenario():
        assert await service._query_queue_state(
            config, "123", submission_token="wf:token:one"
        ) == (
            True,
            ("RUNNING", "c001", "None"),
        )
        assert await service._query_queue_state(config, "123") == (False, None)
        assert await service._query_queue_state(config, "123") == (False, None)

    asyncio.run(scenario())


def test_squeue_submission_token_mismatch_is_unknown(tmp_path, monkeypatch):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    monkeypatch.setattr(
        service,
        "_run_command",
        lambda argv, **kwargs: subprocess.CompletedProcess(
            argv,
            0,
            "123|wf:another:execution|RUNNING|c001|None\n",
            "",
        ),
    )

    assert asyncio.run(service._query_queue_state(
        config,
        "123",
        submission_token="wf:expected:execution",
    )) == (False, None)


def test_cancel_refuses_reused_job_id_with_different_submission_token(
    tmp_path,
    monkeypatch,
):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    commands = []

    def command(argv, **kwargs):
        commands.append(tuple(argv))
        return subprocess.CompletedProcess(
            argv,
            0,
            "123|wf:other:run|RUNNING|c001|None\n",
            "",
        )

    monkeypatch.setattr(service, "_run_command", command)

    with pytest.raises(service_module.SlurmSubmissionError, match="ownership"):
        asyncio.run(service._send_cancel(
            config,
            "123",
            whole_job=True,
            submission_token="wf:expected:run",
        ))

    assert all(command[0] != "scancel" for command in commands)


def test_slurm_cli_environment_removes_option_overrides():
    cleaned = service_module._slurm_cli_environment({
        "PATH": "/usr/bin",
        "HOME": "/home/test",
        "LANG": "C.UTF-8",
        "SBATCH_GRES": "gpu:8",
        "SBATCH_EXCLUSIVE": "1",
        "SCANCEL_CLUSTERS": "wrong",
        "SQUEUE_FORMAT": "%all",
        "SLURM_CLUSTERS": "other-cluster",
        "UNRELATED_SECRET": "do-not-forward",
    })

    assert cleaned == {
        "PATH": "/usr/bin",
        "HOME": "/home/test",
        "LANG": "C.UTF-8",
    }


def test_cancel_argv_targets_local_batch_first_without_federation(
    tmp_path,
    monkeypatch,
):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    commands = []

    def command(argv, *, timeout):
        commands.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(service, "_run_command", command)

    async def owned_job(*args, **kwargs):
        return True, ("RUNNING", "c001", "")

    monkeypatch.setattr(service, "_query_queue_state", owned_job)

    async def scenario():
        await service._send_cancel(
            config,
            "123",
            whole_job=False,
            cluster="alpha",
        )
        await service._send_cancel(
            config,
            "123",
            whole_job=True,
            cluster="alpha",
        )

    asyncio.run(scenario())

    assert commands == [
        ("scancel", "--batch", "--signal=TERM", "123"),
        ("scancel", "123"),
    ]


def test_accounting_normalizes_cancelled_by_uid(tmp_path, monkeypatch):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    monkeypatch.setattr(
        service,
        "_run_command",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            "123|CANCELLED by 1000|0:15|c002\n",
            "",
        ),
    )

    assert asyncio.run(
        service._query_accounting_state(config, "123")
    ) == (True, ("CANCELLED", "0:15", "c002"))


def test_runtime_config_allows_missing_optional_sacct(tmp_path):
    config = _runtime_config(tmp_path)
    empty_path = tmp_path / "without-accounting"
    empty_path.mkdir()
    environment = {
        "PATH": str(empty_path),
        "WorkFlow_SLURM_RUNTIME_DIR": str(config.runtime_directory),
        "WorkFlow_SLURM_EXECUTION_SCRIPT": str(config.execution_script),
        "WorkFlow_SLURM_SQUEUE": config.squeue_executable,
        "WorkFlow_SLURM_SCONTROL": config.scontrol_executable,
    }

    loaded = service_module.SlurmRuntimeConfig.from_environment(environment)

    assert loaded.sacct_executable is None


def test_runtime_config_explicit_empty_sacct_disables_autodiscovery(tmp_path):
    config = _runtime_config(tmp_path)
    environment = {
        "PATH": str(Path(config.sacct_executable).parent),
        "WorkFlow_SLURM_RUNTIME_DIR": str(config.runtime_directory),
        "WorkFlow_SLURM_EXECUTION_SCRIPT": str(config.execution_script),
        "WorkFlow_SLURM_SQUEUE": config.squeue_executable,
        "WorkFlow_SLURM_SCONTROL": config.scontrol_executable,
        "WorkFlow_SLURM_SACCT": "",
    }

    loaded = service_module.SlurmRuntimeConfig.from_environment(environment)

    assert loaded.sacct_executable is None


def test_scontrol_returns_only_exact_terminal_root_job(tmp_path, monkeypatch):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    outputs = iter([
        "JobId=123 JobState=NODE_FAIL ExitCode=1:0 NodeList=c001\n",
        "JobId=123.batch JobState=FAILED ExitCode=1:0 NodeList=c001\n",
        "JobId=123 JobState=RUNNING ExitCode=0:0 NodeList=c001\n",
        "JobId=123 JobState=FAILED\nJobId=123 JobState=FAILED\n",
    ])
    monkeypatch.setattr(
        service,
        "_run_command",
        lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, next(outputs), ""
        ),
    )

    async def scenario():
        assert await service._query_scontrol_state(config, "123") == (
            True,
            ("NODE_FAIL", "1:0", "c001"),
        )
        assert await service._query_scontrol_state(config, "123") == (False, None)
        assert await service._query_scontrol_state(config, "123") == (True, None)
        assert await service._query_scontrol_state(config, "123") == (False, None)

    asyncio.run(scenario())


def test_terminal_query_falls_back_when_slurmdbd_is_unavailable(
    tmp_path,
    monkeypatch,
):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    commands = []

    def command(argv, **kwargs):
        commands.append(tuple(argv))
        if argv[0] == config.sacct_executable:
            return subprocess.CompletedProcess(
                argv,
                1,
                "",
                "Problem talking to the database: Connection refused",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            "JobId=123 JobState=COMPLETED ExitCode=0:0 NodeList=c001\n",
            "",
        )

    monkeypatch.setattr(service, "_run_command", command)

    assert asyncio.run(service._query_terminal_state(config, "123")) == (
        True,
        ("COMPLETED", "0:0", "c001"),
    )
    assert commands[0][0] == config.scontrol_executable


def test_relay_rejects_event_from_another_job(tmp_path):
    event_path = tmp_path / "events.jsonl"
    event_path.write_text(
        json.dumps({
            "executionId": "execution-1",
            "jobId": "999",
            "sequence": 1,
            "type": "log",
            "message": "wrong job",
        }) + "\n",
        encoding="utf-8",
    )
    service = service_module.SlurmExecutionService()

    with pytest.raises(RuntimeError, match="another Slurm job"):
        asyncio.run(service._relay_events(
            execution_id="execution-1",
            job_id="123",
            event_path=event_path,
            cursor=service_module._EventCursor(),
        ))


def test_relay_commits_each_valid_record_before_a_corrupt_line(
    tmp_path,
    monkeypatch,
):
    event_path = tmp_path / "events.jsonl"
    first = {
        "executionId": "execution-1",
        "jobId": "123",
        "sequence": 1,
        "type": "log",
        "message": "first",
    }
    first_line = (json.dumps(first) + "\n").encode("utf-8")
    event_path.write_bytes(first_line + b"{not-json}\n")
    applied = []
    broadcast = []

    monkeypatch.setattr(
        service_module,
        "_apply_external_event",
        lambda execution_id, event: applied.append((execution_id, dict(event))),
    )

    async def record_broadcast(execution_id, event):
        broadcast.append((execution_id, dict(event)))

    monkeypatch.setattr(state_manager, "broadcast", record_broadcast)
    service = service_module.SlurmExecutionService()
    cursor = service_module._EventCursor()

    with pytest.raises(RuntimeError, match="Corrupt Slurm event stream"):
        asyncio.run(service._relay_events(
            execution_id="execution-1",
            job_id="123",
            event_path=event_path,
            cursor=cursor,
        ))

    assert [entry[1]["sequence"] for entry in applied] == [1]
    assert [entry[1]["sequence"] for entry in broadcast] == [1]
    assert cursor.offset == len(first_line)
    assert cursor.last_sequence == 1

    second = {**first, "sequence": 2, "message": "second"}
    event_path.write_bytes(first_line + (json.dumps(second) + "\n").encode("utf-8"))
    assert asyncio.run(service._relay_events(
        execution_id="execution-1",
        job_id="123",
        event_path=event_path,
        cursor=cursor,
    )) == 1
    assert [entry[1]["sequence"] for entry in broadcast] == [1, 2]


def test_valid_terminal_result_is_not_blocked_by_corrupt_complete_event(
    tmp_path,
    monkeypatch,
):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "execution-terminal-result"
    run_directory.mkdir()
    valid_event = {
        "executionId": "execution-terminal-result",
        "jobId": "123",
        "sequence": 1,
        "type": "log",
        "message": "valid progress",
    }
    (run_directory / service_module.EVENTS_FILENAME).write_bytes(
        (json.dumps(valid_event) + "\n").encode("utf-8") + b"{not-json}\n"
    )
    expected_result = {
        "schemaVersion": 1,
        "executionId": "execution-terminal-result",
        "jobId": "123",
        "status": ExecutionStatus.SUCCEEDED,
        "message": "done",
    }
    (run_directory / service_module.RESULT_FILENAME).write_text(
        json.dumps(expected_result),
        encoding="utf-8",
    )
    applied = []
    broadcast = []
    monkeypatch.setattr(
        service_module,
        "_apply_external_event",
        lambda execution_id, event: applied.append((execution_id, dict(event))),
    )
    monkeypatch.setattr(state_manager, "add_log", lambda *args, **kwargs: None)

    async def record_broadcast(execution_id, event):
        broadcast.append((execution_id, dict(event)))

    monkeypatch.setattr(state_manager, "broadcast", record_broadcast)
    service = service_module.SlurmExecutionService()
    request = config.policy.resource_request(cpu_workers=1, gpu_workers=0)

    result = asyncio.run(service._monitor_job(
        config=config,
        execution_id="execution-terminal-result",
        job_id="123",
        run_directory=run_directory,
        resource_request=request,
    ))

    assert result == expected_result
    assert [item[1]["sequence"] for item in applied] == [1]
    assert [
        item[1]["sequence"]
        for item in broadcast
        if "sequence" in item[1]
    ] == [1]
    warnings = [item[1] for item in broadcast if item[1]["type"] == "warning"]
    assert len(warnings) == 1
    assert "quarantined" in warnings[0]["message"]


def test_monitor_retry_preserves_event_cursor_and_does_not_rebroadcast(
    tmp_path,
    monkeypatch,
):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "execution-monitor-cursor"
    run_directory.mkdir()
    valid_event = {
        "executionId": "execution-monitor-cursor",
        "jobId": "123",
        "sequence": 1,
        "type": "log",
        "message": "broadcast exactly once",
    }
    (run_directory / service_module.EVENTS_FILENAME).write_bytes(
        (json.dumps(valid_event) + "\n").encode("utf-8") + b"{not-json}\n"
    )
    expected_result = {
        "schemaVersion": 1,
        "executionId": "execution-monitor-cursor",
        "jobId": "123",
        "status": ExecutionStatus.SUCCEEDED,
        "message": "done",
    }
    result_path = run_directory / service_module.RESULT_FILENAME
    applied = []
    broadcast = []
    monkeypatch.setattr(
        service_module,
        "_apply_external_event",
        lambda execution_id, event: applied.append((execution_id, dict(event))),
    )
    monkeypatch.setattr(state_manager, "add_log", lambda *args, **kwargs: None)

    async def record_broadcast(execution_id, event):
        payload = dict(event)
        broadcast.append((execution_id, payload))
        if payload.get("type") == "warning" and not result_path.exists():
            result_path.write_text(json.dumps(expected_result), encoding="utf-8")

    monkeypatch.setattr(state_manager, "broadcast", record_broadcast)
    service = service_module.SlurmExecutionService()
    request = config.policy.resource_request(cpu_workers=1, gpu_workers=0)

    result = asyncio.run(service._monitor_until_terminal(
        config=config,
        execution_id="execution-monitor-cursor",
        job_id="123",
        run_directory=run_directory,
        resource_request=request,
    ))

    assert result == expected_result
    assert [item[1]["sequence"] for item in applied] == [1]
    assert [
        item[1]["sequence"]
        for item in broadcast
        if "sequence" in item[1]
    ] == [1]
    assert len([item for item in broadcast if item[1]["type"] == "warning"]) == 1


def test_monitor_rejects_result_from_another_job(tmp_path):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "execution-1"
    run_directory.mkdir()
    (run_directory / service_module.RESULT_FILENAME).write_text(
        json.dumps({
            "schemaVersion": 1,
            "executionId": "execution-1",
            "jobId": "999",
            "status": "succeeded",
        }),
        encoding="utf-8",
    )
    service = service_module.SlurmExecutionService()
    request = config.policy.resource_request(cpu_workers=1, gpu_workers=0)

    with pytest.raises(RuntimeError, match="another Slurm job"):
        asyncio.run(service._monitor_job(
            config=config,
            execution_id="execution-1",
            job_id="123",
            run_directory=run_directory,
            resource_request=request,
        ))


def test_monitor_fails_after_confirmed_squeue_absence_without_history_or_result(
    tmp_path,
    monkeypatch,
):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "execution-purged-job"
    run_directory.mkdir()
    service = service_module.SlurmExecutionService()
    queue_queries = 0

    async def absent(*args, **kwargs):
        nonlocal queue_queries
        queue_queries += 1
        return True, None

    async def no_history(*args, **kwargs):
        return False, None

    monkeypatch.setattr(service, "_query_queue_state", absent)
    monkeypatch.setattr(service, "_query_terminal_state", no_history)

    result = asyncio.run(service._monitor_job(
        config=config,
        execution_id="execution-purged-job",
        job_id="123",
        run_directory=run_directory,
        resource_request=config.policy.resource_request(
            cpu_workers=1,
            gpu_workers=0,
        ),
    ))

    assert queue_queries >= 2
    assert result["status"] == ExecutionStatus.FAILED
    assert result["jobId"] == "123"
    assert "disappeared from squeue" in result["message"]


def test_accounting_rejects_conflicting_duplicate_root_rows(tmp_path, monkeypatch):
    service = service_module.SlurmExecutionService()
    config = _runtime_config(tmp_path)
    outputs = iter([
        "123|COMPLETED|0:0|c001\n123.batch|FAILED|1:0|c001\n",
        "123|COMPLETED|0:0|c001\n123|RUNNING|0:0|c001\n",
    ])
    monkeypatch.setattr(
        service,
        "_run_command",
        lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, next(outputs), ""
        ),
    )

    async def scenario():
        assert await service._query_accounting_state(config, "123") == (
            True,
            ("COMPLETED", "0:0", "c001"),
        )
        assert await service._query_accounting_state(config, "123") == (True, None)

    asyncio.run(scenario())


def test_monitor_query_failure_never_starts_absence_grace(
    tmp_path,
    monkeypatch,
):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "execution-query-failure"
    run_directory.mkdir()
    expected_result = {
        "schemaVersion": 1,
        "executionId": "execution-query-failure",
        "jobId": "123",
        "status": ExecutionStatus.SUCCEEDED,
        "message": "runner eventually published",
    }
    service = service_module.SlurmExecutionService()
    queries = 0

    async def failed_query(*args, **kwargs):
        nonlocal queries
        queries += 1
        if queries == 2:
            (run_directory / service_module.RESULT_FILENAME).write_text(
                json.dumps(expected_result),
                encoding="utf-8",
            )
        return False, None

    async def must_not_query_history(*args, **kwargs):
        raise AssertionError("failed squeue must not be treated as absence")

    monkeypatch.setattr(service, "_query_queue_state", failed_query)
    monkeypatch.setattr(service, "_query_terminal_state", must_not_query_history)

    result = asyncio.run(service._monitor_job(
        config=config,
        execution_id="execution-query-failure",
        job_id="123",
        run_directory=run_directory,
        resource_request=config.policy.resource_request(
            cpu_workers=1,
            gpu_workers=0,
        ),
    ))

    assert queries == 2
    assert result == expected_result


@pytest.mark.parametrize("with_sacct", [True, False])
def test_execute_submits_graph_derived_resources_and_worker_memory(
    tmp_path,
    monkeypatch,
    with_sacct,
):
    config = _runtime_config(tmp_path)
    if not with_sacct:
        config = service_module.SlurmRuntimeConfig(
            **{**config.__dict__, "sacct_executable": None}
        )
    graph = {"terminal": {"type": "TestOutput", "inputs": {}}}
    plan = SimpleNamespace(cpu_workers=2, gpu_workers=1)
    monkeypatch.setattr(
        service_module,
        "_authoritative_graph_and_plan",
        lambda selected_graph, selected_config: (selected_graph, plan),
    )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    monkeypatch.setattr(service_module, "_git_revision", lambda root: "a" * 40)

    captured = {}
    service = service_module.SlurmExecutionService()

    def submit(argv, *, timeout):
        captured["argv"] = tuple(argv)
        return subprocess.CompletedProcess(argv, 0, "123;test-cluster\n", "")

    async def monitor(**kwargs):
        return {
            "schemaVersion": 1,
            "executionId": "execution-1",
            "jobId": "123",
            "status": ExecutionStatus.SUCCEEDED,
            "message": "done",
            "finishedAt": "2026-08-09T00:00:01Z",
        }

    monkeypatch.setattr(service, "_run_command", submit)
    monkeypatch.setattr(service, "_monitor_job", monitor)
    state_manager.start_execution("execution-1")

    asyncio.run(service.execute_graph(graph, "execution-1", {"mode": "full_graph"}))

    argv = captured["argv"]
    assert "--cpus-per-task=5" in argv
    assert "--mem=84G" in argv
    assert "--gres=gpu:1" in argv
    assert any(item.startswith("--comment=wf:") for item in argv)
    assert argv[-7:] == (
        str(config.execution_root / "execution-1" / "request.json"),
        str(config.runtime_directory),
        "8",
        "64",
        config.squeue_executable,
        config.sacct_executable or "-",
        config.scontrol_executable,
    )
    payload = json.loads(
        (config.execution_root / "execution-1" / "request.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["resourcePlan"] == {"cpuWorkers": 2, "gpuWorkers": 1}
    assert payload["eventPath"] == str(
        config.execution_root / "execution-1" / "events.jsonl"
    )
    assert payload["resultPath"] == str(
        config.execution_root / "execution-1" / "result.json"
    )
    assert payload["graph"] == graph
    job_record = json.loads(
        (config.execution_root / "execution-1" / "job.json").read_text(
            encoding="utf-8"
        )
    )
    assert job_record["state"] == ExecutionStatus.SUCCEEDED


def test_cancel_during_sbatch_waits_for_job_id_then_cancels(tmp_path, monkeypatch):
    config = _runtime_config(tmp_path)
    graph = {"terminal": {"type": "TestOutput", "inputs": {}}}
    plan = SimpleNamespace(cpu_workers=1, gpu_workers=0)
    monkeypatch.setattr(
        service_module,
        "_authoritative_graph_and_plan",
        lambda selected_graph, selected_config: (selected_graph, plan),
    )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    monkeypatch.setattr(service_module, "_git_revision", lambda root: "b" * 40)

    entered = threading.Event()
    release = threading.Event()
    cancelled_jobs = []
    service = service_module.SlurmExecutionService()

    def delayed_submit(argv, *, timeout):
        entered.set()
        assert release.wait(timeout=5)
        return subprocess.CompletedProcess(argv, 0, "456\n", "")

    async def send_cancel(
        config_arg,
        job_id,
        *,
        whole_job,
        cluster=None,
        submission_token=None,
    ):
        cancelled_jobs.append((job_id, whole_job))

    async def queue_state(*args, **kwargs):
        return True, ("RUNNING", "c001", "")

    async def monitor(**kwargs):
        return {
            "schemaVersion": 1,
            "executionId": "execution-2",
            "jobId": "456",
            "status": ExecutionStatus.CANCELLED,
            "message": "cancelled",
        }

    monkeypatch.setattr(service, "_run_command", delayed_submit)
    monkeypatch.setattr(service, "_send_cancel", send_cancel)
    monkeypatch.setattr(service, "_query_queue_state", queue_state)
    monkeypatch.setattr(service, "_monitor_job", monitor)

    async def scenario():
        state_manager.start_execution("execution-2")
        task = asyncio.create_task(
            service.execute_graph(graph, "execution-2", {"mode": "full_graph"})
        )
        assert state_manager.attach_execution_task("execution-2", task)
        assert await asyncio.to_thread(entered.wait, 5)
        assert state_manager.cancel_execution("execution-2")
        # Let the first cancellation reach the uninterruptible sbatch harvest,
        # then issue the repeated Stop action that previously orphaned a job.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert state_manager.cancel_execution("execution-2")
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert cancelled_jobs == [("456", False)]
    job_record = json.loads(
        (config.execution_root / "execution-2" / "job.json").read_text(
            encoding="utf-8"
        )
    )
    assert job_record["jobId"] == "456"
    assert job_record["state"] == ExecutionStatus.CANCELLED


@pytest.mark.parametrize("failure_kind", ("nonzero", "oserror"))
def test_explicit_sbatch_failure_is_terminal_across_restart(
    tmp_path,
    monkeypatch,
    failure_kind,
):
    config = _runtime_config(tmp_path)
    graph = {"terminal": {"type": "TestOutput", "inputs": {}}}
    plan = SimpleNamespace(cpu_workers=1, gpu_workers=0)
    monkeypatch.setattr(
        service_module,
        "_authoritative_graph_and_plan",
        lambda selected_graph, selected_config: (selected_graph, plan),
    )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    monkeypatch.setattr(service_module, "_git_revision", lambda root: "c" * 40)

    def reject_submission(argv, *, timeout):
        if failure_kind == "oserror":
            raise OSError("sbatch executable is unavailable")
        return subprocess.CompletedProcess(
            argv,
            1,
            "",
            "Batch job submission failed: invalid partition",
        )

    service = service_module.SlurmExecutionService()
    monkeypatch.setattr(service, "_run_command", reject_submission)
    execution_id = f"explicit-failure-{failure_kind}"
    state_manager.start_execution(execution_id)

    with pytest.raises(service_module.SlurmSubmissionError):
        asyncio.run(
            service.execute_graph(graph, execution_id, {"mode": "full_graph"})
        )

    job_path = config.execution_root / execution_id / "job.json"
    failed_record = json.loads(job_path.read_text(encoding="utf-8"))
    assert failed_record["jobId"] is None
    assert failed_record["state"] == ExecutionStatus.FAILED
    assert failed_record["finishedAt"]
    assert "submissionToken" in failed_record

    # Model a new control-plane process: no in-memory session survives, and
    # reconciliation must ignore this durable terminal record without asking
    # Slurm whether a token might still be active.
    state_manager._init_state()
    restarted_service = service_module.SlurmExecutionService()

    async def must_not_query_scheduler(*args, **kwargs):
        raise AssertionError("terminal rejected submissions must not be queried")

    monkeypatch.setattr(
        restarted_service,
        "_query_job_by_submission_token",
        must_not_query_scheduler,
    )
    assert asyncio.run(restarted_service.reconcile_active_job()) is None


def test_pre_submission_failure_finishes_session_and_releases_active_slot(
    monkeypatch,
):
    service = service_module.SlurmExecutionService()
    state_manager.start_execution("execution-preflight-error")
    monkeypatch.setattr(
        service_module,
        "_authoritative_graph_and_plan",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ValueError("invalid graph resource declaration")
        ),
    )

    with pytest.raises(ValueError, match="invalid graph"):
        asyncio.run(service.execute_graph(
            {},
            "execution-preflight-error",
            {"mode": "full_graph"},
        ))

    session = state_manager.get_execution("execution-preflight-error")
    assert session is not None
    assert session.status == ExecutionStatus.FAILED
    assert state_manager.active_execution_id is None


def test_runner_revision_validation_rejects_changed_checkout(monkeypatch):
    from services import slurm_execution_runner as runner

    request = runner.parse_runner_request({
        "schemaVersion": 1,
        "executionId": "execution-3",
        "graph": {},
        "executionConfig": {"mode": "full_graph"},
        "resourcePlan": {"cpuWorkers": 1, "gpuWorkers": 0},
        "codeRevision": "c" * 40,
        "submittedAt": "2026-08-09T00:00:00Z",
        "eventPath": str((Path.cwd() / "events.jsonl").resolve()),
        "resultPath": str((Path.cwd() / "result.json").resolve()),
    })
    responses = iter((
        subprocess.CompletedProcess((), 0, "d" * 40 + "\n", ""),
        subprocess.CompletedProcess((), 0, "", ""),
    ))
    monkeypatch.setattr(runner.subprocess, "run", lambda *args, **kwargs: next(responses))

    with pytest.raises(runner.RunnerValidationError, match="changed"):
        runner.validate_code_revision(request)


def test_monitor_error_retries_and_keeps_active_lease(tmp_path, monkeypatch):
    config = _runtime_config(tmp_path)
    graph = {"terminal": {"type": "TestOutput", "inputs": {}}}
    plan = SimpleNamespace(cpu_workers=1, gpu_workers=0)
    monkeypatch.setattr(
        service_module,
        "_authoritative_graph_and_plan",
        lambda selected_graph, selected_config: (selected_graph, plan),
    )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    monkeypatch.setattr(service_module, "_git_revision", lambda root: "e" * 40)
    service = service_module.SlurmExecutionService()
    monitor_retried = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    def submit(argv, *, timeout):
        return subprocess.CompletedProcess(argv, 0, "333\n", "")

    async def monitor(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("temporary shared filesystem outage")
        monitor_retried.set()
        await release.wait()
        return {
            "schemaVersion": 1,
            "executionId": "execution-monitor-retry",
            "jobId": "333",
            "status": ExecutionStatus.SUCCEEDED,
            "message": "done",
        }

    monkeypatch.setattr(service, "_run_command", submit)
    monkeypatch.setattr(service, "_monitor_job", monitor)

    async def scenario():
        state_manager.start_execution("execution-monitor-retry")
        task = asyncio.create_task(service.execute_graph(
            graph,
            "execution-monitor-retry",
            {"mode": "full_graph"},
        ))
        assert state_manager.attach_execution_task("execution-monitor-retry", task)
        await monitor_retried.wait()
        session = state_manager.get_execution("execution-monitor-retry")
        assert session is not None and session.status == ExecutionStatus.RUNNING
        assert state_manager.active_execution_id == "execution-monitor-retry"
        with pytest.raises(RuntimeError, match="Another execution"):
            state_manager.start_execution("must-be-rejected")
        release.set()
        assert await task == "execution-monitor-retry"

    asyncio.run(scenario())


def test_failed_scancel_cannot_terminalize_running_job(tmp_path, monkeypatch):
    config = _runtime_config(tmp_path)
    config = service_module.SlurmRuntimeConfig(
        **{
            **config.__dict__,
            "cancel_grace_seconds": 0.01,
            "poll_interval_seconds": 0.001,
        }
    )
    service = service_module.SlurmExecutionService()
    state_manager.start_execution("execution-cancel-stuck")
    session = state_manager.get_execution("execution-cancel-stuck")
    assert session is not None
    session.status = ExecutionStatus.CANCELLING
    monitor_started = asyncio.Event()

    async def queue_state(*args, **kwargs):
        return True, ("RUNNING", "c001", "")

    async def send_cancel(*args, **kwargs):
        raise service_module.SlurmSubmissionError("scheduler rejected scancel")

    async def monitor(**kwargs):
        monitor_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(service, "_query_queue_state", queue_state)
    monkeypatch.setattr(service, "_send_cancel", send_cancel)
    monkeypatch.setattr(service, "_monitor_until_terminal", monitor)

    async def scenario():
        task = asyncio.create_task(service._cancel_and_wait_for_terminal(
            config=config,
            execution_id="execution-cancel-stuck",
                job_id="444",
                cluster=None,
                submission_token="wf:cancel:stuck",
            run_directory=config.execution_root,
            resource_request=config.policy.resource_request(
                cpu_workers=1,
                gpu_workers=0,
            ),
        ))
        await monitor_started.wait()
        await asyncio.sleep(0.03)
        assert not task.done()
        assert session.status == ExecutionStatus.CANCELLING
        assert state_manager.active_execution_id == "execution-cancel-stuck"
        service.request_monitor_detach("execution-cancel-stuck")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_reconcile_reattaches_one_durable_submitted_job(tmp_path, monkeypatch):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "execution-4"
    run_directory.mkdir()
    request = config.policy.resource_request(cpu_workers=1, gpu_workers=0)
    (run_directory / "job.json").write_text(
        json.dumps({
            "schemaVersion": 1,
            "executionId": "execution-4",
            "jobId": "789",
            "cluster": "test-cluster",
            "state": "submitted",
            "submissionToken": "wf:execution:four",
            "resources": request.to_dict(),
            "submittedAt": "2026-08-09T00:00:00Z",
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    monitor_started = asyncio.Event()
    service = service_module.SlurmExecutionService()

    async def monitor(**kwargs):
        monitor_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(service, "_monitor_job", monitor)

    async def scenario():
        assert await service.reconcile_active_job() == "execution-4"
        await monitor_started.wait()
        session = state_manager.get_execution("execution-4")
        assert session is not None
        assert state_manager.active_execution_id == "execution-4"
        assert session.task is not None
        session.task.cancel()
        await asyncio.gather(session.task, return_exceptions=True)

    asyncio.run(scenario())

    assert state_manager.active_execution_id is None


def test_reconcile_ignores_multiple_terminal_cancelled_records(tmp_path, monkeypatch):
    config = _runtime_config(tmp_path)
    request = config.policy.resource_request(cpu_workers=1, gpu_workers=0)
    for execution_id, job_id in (("cancelled-1", "801"), ("cancelled-2", "802")):
        run_directory = config.execution_root / execution_id
        run_directory.mkdir()
        (run_directory / "job.json").write_text(
            json.dumps({
                "schemaVersion": 1,
                "executionId": execution_id,
                "jobId": job_id,
                "cluster": None,
                "state": ExecutionStatus.CANCELLED,
                "resources": request.to_dict(),
                "submittedAt": "2026-08-09T00:00:00Z",
            }),
            encoding="utf-8",
        )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )

    assert asyncio.run(
        service_module.SlurmExecutionService().reconcile_active_job()
    ) is None


def test_reconcile_fails_closed_for_unresolved_ambiguous_submission(
    tmp_path,
    monkeypatch,
):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "ambiguous-1"
    run_directory.mkdir()
    request = config.policy.resource_request(cpu_workers=1, gpu_workers=0)
    (run_directory / "job.json").write_text(
        json.dumps({
            "schemaVersion": 1,
            "executionId": "ambiguous-1",
            "jobId": None,
            "state": "submitting",
            "submissionToken": "wf:token:one",
            "resources": request.to_dict(),
            "submittedAt": "2026-08-09T00:00:00Z",
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    service = service_module.SlurmExecutionService()

    async def no_match(*args, **kwargs):
        return True, None

    monkeypatch.setattr(service, "_query_job_by_submission_token", no_match)

    with pytest.raises(service_module.SlurmSubmissionError, match="refusing"):
        asyncio.run(service.reconcile_active_job())


def test_ambiguous_submission_recovers_fast_finished_runner_result(
    tmp_path,
    monkeypatch,
):
    config = _runtime_config(tmp_path)
    execution_id = "fast-finished-1"
    run_directory = config.execution_root / execution_id
    run_directory.mkdir()
    (run_directory / service_module.RESULT_FILENAME).write_text(
        json.dumps({
            "schemaVersion": service_module.REQUEST_SCHEMA_VERSION,
            "executionId": execution_id,
            "jobId": "902",
            "status": ExecutionStatus.SUCCEEDED,
        }),
        encoding="utf-8",
    )
    service = service_module.SlurmExecutionService()

    async def must_not_query_squeue(*args, **kwargs):
        raise AssertionError("a durable runner result must win over squeue")

    monkeypatch.setattr(
        service,
        "_query_job_by_submission_token",
        must_not_query_squeue,
    )

    assert asyncio.run(service._recover_ambiguous_submission(
        config=config,
        execution_id=execution_id,
        submission_token="wf:token:finished",
    )) == "902"


def test_reconcile_recovers_job_id_from_submission_token(tmp_path, monkeypatch):
    config = _runtime_config(tmp_path)
    run_directory = config.execution_root / "ambiguous-2"
    run_directory.mkdir()
    request = config.policy.resource_request(cpu_workers=1, gpu_workers=0)
    (run_directory / "job.json").write_text(
        json.dumps({
            "schemaVersion": 1,
            "executionId": "ambiguous-2",
            "jobId": None,
            "state": "submitting",
            "submissionToken": "wf:token:two",
            "resources": request.to_dict(),
            "submittedAt": "2026-08-09T00:00:00Z",
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service_module.SlurmRuntimeConfig,
        "from_environment",
        classmethod(lambda cls: config),
    )
    service = service_module.SlurmExecutionService()
    monitor_started = asyncio.Event()

    async def found(*args, **kwargs):
        return True, ("901", "RUNNING")

    async def monitor(**kwargs):
        monitor_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(service, "_query_job_by_submission_token", found)
    monkeypatch.setattr(service, "_monitor_until_terminal", monitor)

    async def scenario():
        assert await service.reconcile_active_job() == "ambiguous-2"
        await monitor_started.wait()
        session = state_manager.get_execution("ambiguous-2")
        assert session is not None and session.task is not None
        service.request_monitor_detach("ambiguous-2")
        session.task.cancel()
        await asyncio.gather(session.task, return_exceptions=True)

    asyncio.run(scenario())

    recovered = json.loads(
        (run_directory / "job.json").read_text(encoding="utf-8")
    )
    assert recovered["jobId"] == "901"
    assert recovered["state"] == "submitted"
