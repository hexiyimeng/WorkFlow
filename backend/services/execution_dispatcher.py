"""Select the execution backend without changing the WebSocket contract."""

from __future__ import annotations

from typing import Any

from core.window_execution import ExecutionConfig
from services.executor import (
    execute_graph as execute_graph_locally,
    preflight_graph as preflight_graph_locally,
)
from services.slurm_execution_service import (
    slurm_policy_from_environment,
    slurm_execution_service,
    uses_slurm_execution_backend,
)


async def preflight_graph(
    graph: dict[str, Any],
    execution_config: ExecutionConfig | dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run read-only graph preflight for the selected execution backend.

    A Slurm control plane has no local Dask cluster to compare with.  Instead,
    validate the Graph-derived Worker counts against the configured single-node
    Slurm policy without creating a runtime directory or submitting a job.
    """
    result = await preflight_graph_locally(graph, execution_config)
    if not uses_slurm_execution_backend():
        return result

    required = result.get("requiredResources")
    if not isinstance(required, dict):
        raise ValueError("Preflight did not return a valid resource plan.")
    policy = slurm_policy_from_environment()
    request = policy.resource_request(
        cpu_workers=required.get("cpuWorkers"),
        gpu_workers=required.get("gpuWorkers"),
    )
    result["resourcesSatisfied"] = True
    result["resourceError"] = None
    result["slurmRequest"] = request.to_dict()
    return result


async def execute_graph(
    graph: dict[str, Any],
    execution_id: str,
    execution_config: ExecutionConfig | dict[str, Any] | None = None,
) -> str:
    """Run locally or submit to Slurm according to the process configuration."""
    if uses_slurm_execution_backend():
        return await slurm_execution_service.execute_graph(
            graph,
            execution_id,
            execution_config,
        )
    return await execute_graph_locally(
        graph,
        execution_id,
        execution_config,
    )


async def reconcile_execution_backend() -> str | None:
    """Reattach durable remote monitoring after a control-plane restart."""
    if not uses_slurm_execution_backend():
        return None
    return await slurm_execution_service.reconcile_active_job()


def detach_execution_backend(execution_id: str) -> None:
    """Detach a remote monitor during process shutdown without cancelling it."""
    if uses_slurm_execution_backend():
        slurm_execution_service.request_monitor_detach(execution_id)


__all__ = [
    "execute_graph",
    "detach_execution_backend",
    "preflight_graph",
    "reconcile_execution_backend",
    "uses_slurm_execution_backend",
]
