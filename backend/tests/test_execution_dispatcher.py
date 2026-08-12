import asyncio
from types import SimpleNamespace

from services import execution_dispatcher as dispatcher


def test_slurm_preflight_validates_policy_without_submitting(monkeypatch):
    async def fake_preflight(graph, execution_config):
        return {
            "windowable": True,
            "requiredResources": {"cpuWorkers": 2, "gpuWorkers": 1},
            "resourcesSatisfied": None,
            "resourceError": None,
        }

    class Policy:
        def resource_request(self, *, cpu_workers, gpu_workers):
            assert (cpu_workers, gpu_workers) == (2, 1)
            return SimpleNamespace(to_dict=lambda: {
                "cpuWorkers": 2,
                "gpuWorkers": 1,
                "nodes": 1,
                "cpus": 4,
                "gpus": 1,
                "memoryGiB": 84,
                "timeLimit": "01:00:00",
                "partition": "compute",
            })

    monkeypatch.setattr(dispatcher, "preflight_graph_locally", fake_preflight)
    monkeypatch.setattr(dispatcher, "uses_slurm_execution_backend", lambda: True)
    monkeypatch.setattr(
        dispatcher,
        "slurm_policy_from_environment",
        lambda: Policy(),
    )

    result = asyncio.run(dispatcher.preflight_graph({}, {"mode": "full_graph"}))

    assert result["resourcesSatisfied"] is True
    assert result["resourceError"] is None
    assert result["slurmRequest"]["gpus"] == 1


def test_slurm_preflight_rejects_graph_beyond_policy(monkeypatch):
    async def fake_preflight(graph, execution_config):
        return {
            "windowable": True,
            "requiredResources": {"cpuWorkers": 1, "gpuWorkers": 9},
        }

    class Policy:
        def resource_request(self, *, cpu_workers, gpu_workers):
            raise ValueError("gpu_workers=9 exceeds policy limit 8")

    monkeypatch.setattr(dispatcher, "preflight_graph_locally", fake_preflight)
    monkeypatch.setattr(dispatcher, "uses_slurm_execution_backend", lambda: True)
    monkeypatch.setattr(
        dispatcher,
        "slurm_policy_from_environment",
        lambda: Policy(),
    )

    try:
        asyncio.run(dispatcher.preflight_graph({}, {"mode": "full_graph"}))
    except ValueError as exc:
        assert "policy limit 8" in str(exc)
    else:
        raise AssertionError("Slurm preflight must reject an excessive Graph plan")


def test_local_backend_dispatches_to_existing_executor(monkeypatch):
    calls = []

    async def fake_local(graph, execution_id, execution_config):
        calls.append((graph, execution_id, execution_config))
        return execution_id

    async def unexpected_slurm(*_args, **_kwargs):
        raise AssertionError("Slurm service must not run for the local backend")

    monkeypatch.setattr(dispatcher, "uses_slurm_execution_backend", lambda: False)
    monkeypatch.setattr(dispatcher, "execute_graph_locally", fake_local)
    monkeypatch.setattr(
        dispatcher.slurm_execution_service,
        "execute_graph",
        unexpected_slurm,
    )

    result = asyncio.run(
        dispatcher.execute_graph({"nodes": []}, "local-run", None)
    )

    assert result == "local-run"
    assert calls == [({"nodes": []}, "local-run", None)]


def test_slurm_backend_dispatches_to_control_plane_service(monkeypatch):
    calls = []

    async def unexpected_local(*_args, **_kwargs):
        raise AssertionError("Local executor must not run on the Slurm control plane")

    async def fake_slurm(graph, execution_id, execution_config):
        calls.append((graph, execution_id, execution_config))
        return execution_id

    monkeypatch.setattr(dispatcher, "uses_slurm_execution_backend", lambda: True)
    monkeypatch.setattr(dispatcher, "execute_graph_locally", unexpected_local)
    monkeypatch.setattr(
        dispatcher.slurm_execution_service,
        "execute_graph",
        fake_slurm,
    )

    config = {"mode": "full_graph"}
    result = asyncio.run(
        dispatcher.execute_graph({"nodes": []}, "slurm-run", config)
    )

    assert result == "slurm-run"
    assert calls == [({"nodes": []}, "slurm-run", config)]


def test_reconcile_is_a_noop_for_local_backend(monkeypatch):
    calls = []
    monkeypatch.setattr(dispatcher, "uses_slurm_execution_backend", lambda: False)
    monkeypatch.setattr(
        dispatcher.slurm_execution_service,
        "reconcile_active_job",
        lambda: calls.append(True),
    )

    result = asyncio.run(dispatcher.reconcile_execution_backend())

    assert result is None
    assert calls == []


def test_reconcile_delegates_for_slurm_backend(monkeypatch):
    async def reconcile():
        return "execution-existing"

    monkeypatch.setattr(dispatcher, "uses_slurm_execution_backend", lambda: True)
    monkeypatch.setattr(
        dispatcher.slurm_execution_service,
        "reconcile_active_job",
        reconcile,
    )

    assert (
        asyncio.run(dispatcher.reconcile_execution_backend())
        == "execution-existing"
    )
