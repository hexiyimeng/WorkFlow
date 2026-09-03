from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from core.workflow_resources import (
    WorkerProfileRequirement,
    WorkflowResourcePlan,
)
from services import execution_dispatcher
from services import slurm_execution_service as slurm_service_module


def test_failed_metadata_preflight_still_returns_profile_requirements(
    monkeypatch,
) -> None:
    plan = WorkflowResourcePlan(nodes=(WorkerProfileRequirement(
        node_id="zarr_writer",
        node_type="ZarrWriter",
        display_name="Zarr Writer",
        worker_profile="cpu-writer",
    ),))

    monkeypatch.setattr(execution_dispatcher, "validate_graph_structure", lambda graph: None)
    monkeypatch.setattr(execution_dispatcher, "validate_graph_acyclic", lambda graph: None)
    monkeypatch.setattr(execution_dispatcher, "validate_graph_types", lambda graph: None)
    monkeypatch.setattr(
        execution_dispatcher,
        "find_execution_roots",
        lambda graph: ["zarr_writer"],
    )
    monkeypatch.setattr(
        execution_dispatcher,
        "build_workflow_resource_plan",
        lambda graph, roots: plan,
    )

    async def fail_preflight(graph, execution_config):
        raise ValueError(
            "Terminal OUTPUT node 'zarr_writer' input 'output_path' "
            "must be an absolute path."
        )

    monkeypatch.setattr(
        execution_dispatcher,
        "preflight_graph_locally",
        fail_preflight,
    )

    result = asyncio.run(
        execution_dispatcher.preflight_graph(
            {"zarr_writer": {"type": "ZarrWriter", "inputs": {}}},
            {"mode": "full_graph"},
            worker_profiles=[],
            worker_pools=[],
        )
    )

    assert result["resourcesSatisfied"] is False
    assert result["requiredResources"]["requiredWorkerProfiles"] == {
        "cpu-writer": 1,
    }
    assert "absolute path" in result["resourceError"]
    assert result["preflightError"]["type"] == "ValueError"


def test_preflight_filters_unrequired_profiles_before_validation(monkeypatch) -> None:
    plan = WorkflowResourcePlan(nodes=(WorkerProfileRequirement(
        node_id="cellpose",
        node_type="Cellpose",
        display_name="Cellpose",
        worker_profile="gpu-cellpose",
    ),))

    monkeypatch.setattr(execution_dispatcher, "validate_graph_structure", lambda graph: None)
    monkeypatch.setattr(execution_dispatcher, "validate_graph_acyclic", lambda graph: None)
    monkeypatch.setattr(execution_dispatcher, "validate_graph_types", lambda graph: None)
    monkeypatch.setattr(execution_dispatcher, "find_execution_roots", lambda graph: ["cellpose"])
    monkeypatch.setattr(
        execution_dispatcher,
        "build_workflow_resource_plan",
        lambda graph, roots: plan,
    )
    monkeypatch.setattr(
        execution_dispatcher,
        "uses_slurm_execution_backend",
        lambda: False,
    )

    async def successful_preflight(graph, execution_config):
        return {
            "windowable": True,
            "requiredResources": plan.to_preflight_dict(),
        }

    monkeypatch.setattr(
        execution_dispatcher,
        "preflight_graph_locally",
        successful_preflight,
    )

    result = asyncio.run(execution_dispatcher.preflight_graph(
        {"cellpose": {"type": "Cellpose", "inputs": {}}},
        {"mode": "full_graph"},
        worker_profiles=[
            {
                "name": "cpu-general",
                "physical_resources": {"cpu": 8, "memory": "32GB", "gpu": 1},
                "logical_resources": {"cpu-general": 1, "CPU": 8, "GPU": 1},
                "capabilities": ["cpu-general"],
                "threads": 8,
            },
            {
                "name": "gpu-cellpose",
                "physical_resources": {"cpu": 4, "memory": "32GB", "gpu": 1},
                "logical_resources": {"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
                "capabilities": ["gpu-cellpose"],
                "threads": 4,
            },
        ],
        worker_pools=[
            {"profile": "cpu-general", "processes": 1, "scale": 1},
            {"profile": "gpu-cellpose", "processes": 1, "scale": 2},
        ],
    ))

    assert result["resourcesSatisfied"] is True
    assert result["resourceError"] is None


def test_durable_slurm_snapshot_survives_process_local_state_loss(
    monkeypatch,
    tmp_path,
) -> None:
    execution_id = "12345678-1234-1234-1234-123456789abc"
    execution_root = tmp_path / "jobs"
    run_directory = execution_root / execution_id
    run_directory.mkdir(parents=True)
    (run_directory / "job.json").write_text(
        json.dumps({
            "schemaVersion": slurm_service_module.JOB_SCHEMA_VERSION,
            "executionId": execution_id,
            "state": "failed",
            "submittedAt": "2026-09-02T18:19:13+00:00",
            "finishedAt": "2026-09-03T10:29:45+00:00",
            "message": "example terminal failure",
            "recoveryDirectory": None,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        slurm_service_module.SlurmRuntimeConfig,
        "from_environment",
        lambda environment=None: SimpleNamespace(execution_root=execution_root),
    )

    snapshot = slurm_service_module.SlurmExecutionService().get_durable_execution_snapshot(
        execution_id
    )

    assert snapshot is not None
    assert snapshot["status"] == "failed"
    assert snapshot["durable"] is True
    assert snapshot["message"] == "example terminal failure"
    assert snapshot["createdAt"] == 1_788_373_153_000
    assert snapshot["finishedAt"] == 1_788_431_385_000
