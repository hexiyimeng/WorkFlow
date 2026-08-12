import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.state_manager import ExecutionStatus, state_manager
from services import slurm_execution_runner as runner


def _request_payload(
    *,
    execution_id="execution-1",
    cpu_workers=1,
    gpu_workers=0,
    directory=None,
):
    execution_directory = Path.cwd() if directory is None else Path(directory)
    return {
        "schemaVersion": 1,
        "executionId": execution_id,
        "graph": {
            "output": {
                "type": "TestOutput",
                "inputs": {},
            },
        },
        "executionConfig": {"mode": "full_graph"},
        "resourcePlan": {
            "cpuWorkers": cpu_workers,
            "gpuWorkers": gpu_workers,
        },
        "codeRevision": None,
        "submittedAt": "2026-08-09T00:00:00Z",
        "eventPath": str((execution_directory / "events.jsonl").resolve()),
        "resultPath": str((execution_directory / "result.json").resolve()),
    }


def _write_request(directory: Path, payload=None) -> Path:
    path = directory / "request.json"
    path.write_text(
        json.dumps(payload or _request_payload(directory=directory)),
        encoding="utf-8",
    )
    return path


def _write_cancel_marker(
    directory: Path,
    *,
    execution_id="execution-1",
    job_id="800",
    **updates,
) -> Path:
    payload = {
        "schemaVersion": 1,
        "executionId": execution_id,
        "jobId": job_id,
        "requestedAt": "2026-08-09T00:00:01Z",
    }
    payload.update(updates)
    path = directory / runner.CANCEL_MARKER_FILENAME
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_parse_request_rejects_unknown_fields_and_noncanonical_config():
    payload = _request_payload()
    payload["unexpected"] = True
    with pytest.raises(runner.RunnerValidationError, match="unknown fields"):
        runner.parse_runner_request(payload)

    payload = _request_payload()
    payload["executionConfig"] = {"mode": "full_graph", "ignored": True}
    with pytest.raises(runner.RunnerValidationError, match="canonical"):
        runner.parse_runner_request(payload)

    payload = _request_payload()
    payload["schemaVersion"] = True
    with pytest.raises(runner.RunnerValidationError, match="schemaVersion"):
        runner.parse_runner_request(payload)


def test_request_json_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "request.json"
    path.write_text('{"schemaVersion":1,"schemaVersion":1}', encoding="utf-8")
    with pytest.raises(runner.RunnerValidationError, match="Duplicate JSON"):
        runner._read_request_json(path)


def test_invalid_request_schema_publishes_specific_fallback_result(tmp_path):
    execution_directory = tmp_path / "jobs" / "execution-invalid-schema"
    execution_directory.mkdir(parents=True)
    payload = _request_payload(
        execution_id="execution-invalid-schema",
        directory=execution_directory,
    )
    payload["unexpected"] = True
    request_path = _write_request(execution_directory, payload)

    exit_code = asyncio.run(runner.run_execution_request(
        request_path,
        tmp_path,
        environment={"SLURM_JOB_ID": "777"},
    ))

    assert exit_code == 1
    result = json.loads(
        (execution_directory / "result.json").read_text(encoding="utf-8")
    )
    assert result["executionId"] == "execution-invalid-schema"
    assert result["jobId"] == "777"
    assert "unknown fields" in result["message"]


def test_runner_paths_must_be_absolute_regular_files(tmp_path, monkeypatch):
    execution_directory = tmp_path / "requests" / "execution-1"
    execution_directory.mkdir(parents=True)
    request_path = _write_request(execution_directory)
    paths = runner.validate_runner_paths(
        request_path,
        tmp_path,
        event_path=execution_directory / "events.jsonl",
        result_path=execution_directory / "result.json",
    )
    assert paths.events == execution_directory / "events.jsonl"
    assert paths.result == execution_directory / "result.json"
    assert paths.runtime_directory == tmp_path

    monkeypatch.chdir(execution_directory)
    with pytest.raises(runner.RunnerValidationError, match="absolute"):
        runner.validate_runner_paths(
            "request.json",
            tmp_path,
            event_path=execution_directory / "events.jsonl",
            result_path=execution_directory / "result.json",
        )

    with pytest.raises(runner.RunnerValidationError, match="eventPath"):
        runner.validate_runner_paths(
            request_path,
            tmp_path,
            event_path=tmp_path / "events.jsonl",
            result_path=execution_directory / "result.json",
        )


def test_runner_rejects_symlink_request_when_supported(tmp_path):
    target_directory = tmp_path / "target"
    target_directory.mkdir()
    target = _write_request(target_directory)
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    link = runtime / "request.json"
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("Creating symlinks is not permitted on this platform")

    with pytest.raises(runner.RunnerValidationError, match="symbolic link"):
        runner.validate_runner_paths(
            link,
            runtime,
            event_path=runtime / "events.jsonl",
            result_path=runtime / "result.json",
        )


def test_runner_rejects_request_outside_shared_runtime(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    request_path = _write_request(outside)

    with pytest.raises(runner.RunnerValidationError, match="contained"):
        runner.validate_runner_paths(
            request_path,
            runtime,
            event_path=outside / "events.jsonl",
            result_path=outside / "result.json",
        )


def test_allocation_validation_counts_cpu_and_visible_gpu_workers():
    request = runner.parse_runner_request(
        _request_payload(cpu_workers=2, gpu_workers=2)
    )
    job_id = runner.validate_slurm_allocation(request, {
        "SLURM_JOB_ID": "501",
        "SLURM_CPUS_PER_TASK": "4",
        "CUDA_VISIBLE_DEVICES": "GPU-a,GPU-b",
    })
    assert job_id == "501"

    with pytest.raises(runner.RunnerValidationError, match="needs at least 4"):
        runner.validate_slurm_allocation(request, {
            "SLURM_JOB_ID": "501",
            "SLURM_CPUS_PER_TASK": "3",
            "CUDA_VISIBLE_DEVICES": "GPU-a,GPU-b",
        })
    with pytest.raises(runner.RunnerValidationError, match="needs 2 GPU"):
        runner.validate_slurm_allocation(request, {
            "SLURM_JOB_ID": "501",
            "SLURM_CPUS_PER_TASK": "4",
            "CUDA_VISIBLE_DEVICES": "GPU-a",
        })


def test_resource_plan_is_recomputed_and_mismatch_rejected(monkeypatch):
    request = runner.parse_runner_request(_request_payload(cpu_workers=1))
    monkeypatch.setattr(runner, "validate_graph_structure", lambda graph: None)
    monkeypatch.setattr(runner, "validate_graph_acyclic", lambda graph: None)
    monkeypatch.setattr(runner, "validate_graph_types", lambda graph: None)
    monkeypatch.setattr(runner, "find_execution_roots", lambda graph: ["output"])
    monkeypatch.setattr(
        runner,
        "build_workflow_resource_plan",
        lambda graph, roots: SimpleNamespace(),
    )
    monkeypatch.setattr(
        runner,
        "ensure_executable_resource_plan",
        lambda plan: SimpleNamespace(cpu_workers=2, gpu_workers=0),
    )

    with pytest.raises(runner.RunnerValidationError, match="does not match"):
        runner.validate_requested_resource_plan(request)


def test_jsonl_writer_uses_monotonic_sequence_and_fsyncs_terminal_event(
    tmp_path,
    monkeypatch,
):
    fsync_calls = []
    monkeypatch.setattr(runner.os, "fsync", lambda fd: fsync_calls.append(fd))
    path = tmp_path / "events.jsonl"
    writer = runner.JsonlEventWriter(path, job_id="700")
    writer.write_event("exec", {"type": "progress", "progress": 10})
    assert fsync_calls == []
    writer.write_event("exec", {
        "type": "execution_finished",
        "status": "succeeded",
        "message": "done",
    })
    assert len(fsync_calls) == 1
    writer.close()

    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [record["sequence"] for record in records] == [1, 2]
    assert all(record["jobId"] == "700" for record in records)
    assert all(record["executionId"] == "exec" for record in records)
    assert records[1]["message"] == "done"


def test_full_runner_writes_events_and_atomic_terminal_result(tmp_path, monkeypatch):
    execution_directory = tmp_path / "requests" / "execution-1"
    execution_directory.mkdir(parents=True)
    request_path = _write_request(execution_directory)
    monkeypatch.setattr(runner, "load_all_plugins", lambda: (True, [], []))
    monkeypatch.setattr(runner, "validate_requested_resource_plan", lambda request: None)
    monkeypatch.setattr(runner.dask_service, "stop_cluster", lambda: None)

    async def fake_execute(_graph, execution_id, _config):
        state_manager.set_execution_status(
            execution_id,
            ExecutionStatus.SUCCEEDED,
            release_active=False,
        )
        await state_manager.broadcast(execution_id, {
            "type": "execution_finished",
            "status": "succeeded",
            "message": "test workflow finished",
        })
        await state_manager.broadcast(execution_id, {
            "type": "done",
            "status": "succeeded",
            "message": "done",
        })

    monkeypatch.setattr(runner, "execute_graph", fake_execute)
    state_manager.clear_state()
    exit_code = asyncio.run(runner.run_execution_request(
        request_path,
        tmp_path,
        environment={
            "SLURM_JOB_ID": "800",
            "SLURM_CPUS_PER_TASK": "1",
            "CUDA_VISIBLE_DEVICES": "",
        },
    ))

    assert exit_code == 0
    events = [
        json.loads(line)
        for line in (execution_directory / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [event["sequence"] for event in events] == [1, 2, 3]
    assert [event["type"] for event in events] == [
        "runner_started",
        "execution_finished",
        "done",
    ]
    result = json.loads(
        (execution_directory / "result.json").read_text(encoding="utf-8")
    )
    assert result["schemaVersion"] == 1
    assert result["executionId"] == "execution-1"
    assert result["jobId"] == "800"
    assert result["status"] == "succeeded"
    assert result["message"] == "done"
    assert result["exitCode"] == 0


def test_existing_matching_cancel_marker_stops_before_plugins_or_dask(
    tmp_path,
    monkeypatch,
):
    execution_directory = tmp_path / "requests" / "execution-1"
    execution_directory.mkdir(parents=True)
    request_path = _write_request(execution_directory)
    _write_cancel_marker(execution_directory)

    forbidden_calls = []
    monkeypatch.setattr(
        runner,
        "load_all_plugins",
        lambda: forbidden_calls.append("plugins"),
    )
    monkeypatch.setattr(
        runner,
        "execute_graph",
        lambda *_args, **_kwargs: forbidden_calls.append("execute_graph"),
    )
    monkeypatch.setattr(
        runner.dask_service,
        "stop_cluster",
        lambda: forbidden_calls.append("dask"),
    )
    state_manager.clear_state()

    exit_code = asyncio.run(runner.run_execution_request(
        request_path,
        tmp_path,
        environment={
            "SLURM_JOB_ID": "800",
            "SLURM_CPUS_PER_TASK": "1",
            "CUDA_VISIBLE_DEVICES": "",
        },
    ))

    assert exit_code == 130
    assert forbidden_calls == []
    events = [
        json.loads(line)
        for line in (execution_directory / "events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert [event["type"] for event in events] == [
        "runner_started",
        "execution_finished",
    ]
    assert events[-1]["status"] == ExecutionStatus.CANCELLED
    result = json.loads(
        (execution_directory / "result.json").read_text(encoding="utf-8")
    )
    assert result["status"] == ExecutionStatus.CANCELLED
    assert result["jobId"] == "800"


@pytest.mark.parametrize(
    "marker_payload, error_fragment",
    [
        ({"schemaVersion": 1}, "missing fields"),
        (
            {
                "schemaVersion": 1,
                "executionId": "execution-1",
                "jobId": "800",
                "requestedAt": "now",
                "unexpected": True,
            },
            "unknown fields",
        ),
        (
            {
                "schemaVersion": 1,
                "executionId": "another-execution",
                "jobId": "800",
                "requestedAt": "now",
            },
            "executionId does not match",
        ),
        (
            {
                "schemaVersion": 1,
                "executionId": "execution-1",
                "jobId": "801",
                "requestedAt": "now",
            },
            "jobId does not match",
        ),
    ],
)
def test_invalid_existing_cancel_marker_fails_without_computing(
    tmp_path,
    monkeypatch,
    marker_payload,
    error_fragment,
):
    execution_directory = tmp_path / "requests" / "execution-1"
    execution_directory.mkdir(parents=True)
    request_path = _write_request(execution_directory)
    (execution_directory / runner.CANCEL_MARKER_FILENAME).write_text(
        json.dumps(marker_payload),
        encoding="utf-8",
    )
    forbidden_calls = []
    monkeypatch.setattr(
        runner,
        "load_all_plugins",
        lambda: forbidden_calls.append("plugins"),
    )
    monkeypatch.setattr(
        runner,
        "execute_graph",
        lambda *_args, **_kwargs: forbidden_calls.append("execute_graph"),
    )
    monkeypatch.setattr(
        runner.dask_service,
        "stop_cluster",
        lambda: forbidden_calls.append("dask"),
    )
    state_manager.clear_state()

    exit_code = asyncio.run(runner.run_execution_request(
        request_path,
        tmp_path,
        environment={"SLURM_JOB_ID": "800"},
    ))

    assert exit_code == 1
    assert forbidden_calls == []
    result = json.loads(
        (execution_directory / "result.json").read_text(encoding="utf-8")
    )
    assert result["status"] == ExecutionStatus.FAILED
    assert error_fragment in result["message"]


def test_malformed_cancel_marker_json_fails_without_computing(tmp_path, monkeypatch):
    execution_directory = tmp_path / "requests" / "execution-1"
    execution_directory.mkdir(parents=True)
    request_path = _write_request(execution_directory)
    (execution_directory / runner.CANCEL_MARKER_FILENAME).write_text(
        '{"schemaVersion":1,',
        encoding="utf-8",
    )
    forbidden_calls = []
    monkeypatch.setattr(
        runner,
        "load_all_plugins",
        lambda: forbidden_calls.append("plugins"),
    )
    monkeypatch.setattr(
        runner,
        "execute_graph",
        lambda *_args, **_kwargs: forbidden_calls.append("execute_graph"),
    )
    monkeypatch.setattr(
        runner.dask_service,
        "stop_cluster",
        lambda: forbidden_calls.append("dask"),
    )

    exit_code = asyncio.run(runner.run_execution_request(
        request_path,
        tmp_path,
        environment={"SLURM_JOB_ID": "800"},
    ))

    assert exit_code == 1
    assert forbidden_calls == []
    result = json.loads(
        (execution_directory / "result.json").read_text(encoding="utf-8")
    )
    assert result["status"] == ExecutionStatus.FAILED
    assert "Cannot read strict cancel.requested JSON" in result["message"]


def test_cancel_marker_must_be_regular_non_symlink_file(tmp_path, monkeypatch):
    execution_directory = tmp_path / "requests" / "execution-1"
    execution_directory.mkdir(parents=True)
    request_path = _write_request(execution_directory)
    marker = execution_directory / runner.CANCEL_MARKER_FILENAME
    marker.mkdir()
    monkeypatch.setattr(runner.dask_service, "stop_cluster", lambda: None)

    exit_code = asyncio.run(runner.run_execution_request(
        request_path,
        tmp_path,
        environment={"SLURM_JOB_ID": "800"},
    ))

    assert exit_code == 1
    result = json.loads(
        (execution_directory / "result.json").read_text(encoding="utf-8")
    )
    assert result["status"] == ExecutionStatus.FAILED
    assert "must be a regular file" in result["message"]


def test_sigterm_marker_distinguishes_cancel_from_interruption(tmp_path):
    async def scenario():
        state_manager.clear_state()
        state_manager.start_execution("cancelled")
        cancel_task = asyncio.create_task(asyncio.sleep(60))
        assert state_manager.attach_execution_task("cancelled", cancel_task)
        marker = _write_cancel_marker(
            tmp_path,
            execution_id="cancelled",
            job_id="900",
        )
        cancel_controller = runner._SigtermController(
            loop=asyncio.get_running_loop(),
            execution_id="cancelled",
            job_id="900",
            task=cancel_task,
            cancel_marker=marker,
        )
        cancel_controller.request_termination()
        assert state_manager.get_execution("cancelled").status == "cancelling"
        await asyncio.gather(cancel_task, return_exceptions=True)

        state_manager.clear_state()
        state_manager.start_execution("interrupted")
        interrupt_task = asyncio.create_task(asyncio.sleep(60))
        assert state_manager.attach_execution_task("interrupted", interrupt_task)
        interrupt_controller = runner._SigtermController(
            loop=asyncio.get_running_loop(),
            execution_id="interrupted",
            job_id="901",
            task=interrupt_task,
            cancel_marker=tmp_path / "missing.cancel.requested",
        )
        interrupt_controller.request_termination()
        assert state_manager.get_execution("interrupted").status == "running"
        await asyncio.gather(interrupt_task, return_exceptions=True)
        state_manager.clear_state()

    asyncio.run(scenario())


def test_sigterm_with_crossed_marker_fails_execution(tmp_path):
    async def scenario():
        state_manager.clear_state()
        state_manager.start_execution("execution-1")
        task = asyncio.create_task(asyncio.sleep(60))
        assert state_manager.attach_execution_task("execution-1", task)
        marker = _write_cancel_marker(tmp_path, job_id="different-job")
        controller = runner._SigtermController(
            loop=asyncio.get_running_loop(),
            execution_id="execution-1",
            job_id="expected-job",
            task=task,
            cancel_marker=marker,
        )

        controller.request_termination()

        assert controller.validation_error is not None
        assert "jobId does not match" in str(controller.validation_error)
        assert state_manager.get_execution("execution-1").status == "failed"
        await asyncio.gather(task, return_exceptions=True)
        state_manager.clear_state()

    asyncio.run(scenario())


def test_runner_cleans_only_its_fallback_job_scratch(tmp_path):
    runtime = tmp_path / "runtime"
    execution_directory = runtime / "jobs" / "execution-cleanup"
    execution_directory.mkdir(parents=True)
    request = execution_directory / "request.json"
    request.write_text("{}", encoding="utf-8")
    scratch = runtime / "jobs" / "501" / "scratch"
    scratch.mkdir(parents=True)
    (scratch / "spill.bin").write_bytes(b"spill")
    paths = runner.RunnerPaths(
        request=request,
        runtime_directory=runtime,
        execution_directory=execution_directory,
        events=execution_directory / "events.jsonl",
        result=execution_directory / "result.json",
        cancel_marker=execution_directory / "cancel.requested",
    )

    runner._cleanup_job_scratch(
        paths,
        job_id="501",
        environment={"WorkFlow_JOB_SCRATCH_ROOT": str(scratch)},
    )

    assert not scratch.exists()
    assert execution_directory.exists()
    assert request.exists()


def test_runner_refuses_to_clean_arbitrary_scratch_path(tmp_path):
    runtime = tmp_path / "runtime"
    execution_directory = runtime / "jobs" / "execution-cleanup"
    execution_directory.mkdir(parents=True)
    outside = tmp_path / "do-not-delete"
    outside.mkdir()
    paths = runner.RunnerPaths(
        request=execution_directory / "request.json",
        runtime_directory=runtime,
        execution_directory=execution_directory,
        events=execution_directory / "events.jsonl",
        result=execution_directory / "result.json",
        cancel_marker=execution_directory / "cancel.requested",
    )

    with pytest.raises(runner.RunnerValidationError, match="outside"):
        runner._cleanup_job_scratch(
            paths,
            job_id="501",
            environment={"WorkFlow_JOB_SCRATCH_ROOT": str(outside)},
        )

    assert outside.exists()
