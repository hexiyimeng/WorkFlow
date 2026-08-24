"""Select the execution backend without changing the WebSocket contract."""

from __future__ import annotations

import os
from typing import Any

from core.cluster_inventory import ClusterInventoryService
from core.resource_planner import plan_workflow_resources
from core.worker_pool import parse_worker_pools
from core.worker_profiles import parse_worker_profiles
from core.window_execution import ExecutionConfig
from core.workflow_resources import build_workflow_resource_plan
from services.executor import (
    execute_graph as execute_graph_locally,
    find_execution_roots,
    preflight_graph as preflight_graph_locally,
    validate_graph_acyclic,
    validate_graph_structure,
    validate_graph_types,
)
from services.slurm_execution_service import (
    slurm_policy_from_environment,
    slurm_execution_service,
    uses_slurm_execution_backend,
    validate_allocation_plan_policy,
)


async def preflight_graph(
    graph: dict[str, Any],
    execution_config: ExecutionConfig | dict[str, Any] | None = None,
    *,
    worker_profiles: object = None,
    worker_pools: object = None,
) -> dict[str, Any]:
    """Run read-only graph preflight for the selected execution backend.

    A Slurm control plane has no active Dask cluster during preflight. Instead,
    validate Worker Profiles and Pools against live multi-node inventory
    without creating a Scheduler, directory, or Slurm job.
    """
    # Resource requirements depend only on graph structure and registered Node
    # declarations. Discover them before validating environment-specific file
    # paths or lazy output metadata, so the settings UI can still configure
    # Profiles when another node field needs correction.
    validate_graph_structure(graph)
    validate_graph_acyclic(graph)
    validate_graph_types(graph)
    execution_roots = find_execution_roots(graph)
    if not execution_roots:
        raise ValueError("No terminal execution root was found in the workflow graph.")
    workflow_plan = build_workflow_resource_plan(graph, execution_roots)
    try:
        result = await preflight_graph_locally(graph, execution_config)
    except Exception as exc:
        message = str(exc) or type(exc).__name__
        return {
            "windowable": False,
            "output_shape": None,
            "outputShape": None,
            "ndim": None,
            "outputs": [],
            "reason": message,
            "requiredResources": workflow_plan.to_preflight_dict(),
            "availableResources": None,
            "resourcesSatisfied": False,
            "resourceError": message,
            "allocationPlan": None,
            "preflightError": {
                "type": type(exc).__name__,
                "message": message,
            },
        }
    required = result.get("requiredResources")
    if not isinstance(required, dict):
        raise ValueError("Preflight did not return a valid resource plan.")
    if worker_profiles is None or worker_pools is None:
        result["resourcesSatisfied"] = False
        result["resourceError"] = (
            "Configure every required Worker Profile and Worker Pool before "
            "running on Slurm."
        )
        result["allocationPlan"] = None
        return result

    try:
        profiles = parse_worker_profiles(worker_profiles)
        pools = parse_worker_pools(worker_pools)
        profile_by_name = {profile.name: profile for profile in profiles}
        pool_by_name = {pool.profile: pool for pool in pools}
        required_names = set(required.get("requiredWorkerProfiles", {}))
        missing_profiles = sorted(required_names - set(profile_by_name))
        missing_pools = sorted(required_names - set(pool_by_name))
        if missing_profiles:
            raise ValueError(
                "Configure Worker Profile(s): " + ", ".join(missing_profiles) + "."
            )
        if missing_pools:
            raise ValueError(
                "Configure Worker Pool(s): " + ", ".join(missing_pools) + "."
            )
        for name in required_names:
            pool_by_name[name].validate_profile(profile_by_name[name])
        if not uses_slurm_execution_backend():
            result["resourcesSatisfied"] = True
            result["resourceError"] = None
            result["allocationPlan"] = None
            return result
        policy = slurm_policy_from_environment()
        inventory = ClusterInventoryService(
            scontrol_executable=os.getenv("WorkFlow_SLURM_SCONTROL", "scontrol")
        ).load()
        allocation = plan_workflow_resources(
            workflow_plan,
            profiles,
            pools,
            inventory,
            partition=policy.partition,
            time_limit=policy.time_limit,
        )
        validate_allocation_plan_policy(allocation, policy)
    except (ValueError, RuntimeError) as exc:
        result["resourcesSatisfied"] = False
        result["resourceError"] = str(exc)
        result["allocationPlan"] = None
        return result
    result["resourcesSatisfied"] = True
    result["resourceError"] = None
    result["allocationPlan"] = allocation.to_dict()
    return result


async def execute_graph(
    graph: dict[str, Any],
    execution_id: str,
    execution_config: ExecutionConfig | dict[str, Any] | None = None,
    *,
    worker_profiles: object = None,
    worker_pools: object = None,
) -> str:
    """Run locally or submit to Slurm according to the process configuration."""
    if uses_slurm_execution_backend():
        return await slurm_execution_service.execute_graph(
            graph,
            execution_id,
            execution_config,
            worker_profiles=worker_profiles,
            worker_pools=worker_pools,
        )
    return await execute_graph_locally(
        graph,
        execution_id,
        execution_config,
        worker_profiles=worker_profiles,
        worker_pools=worker_pools,
    )


async def reconcile_execution_backend() -> str | None:
    """Cancel orphan Workers left by a dead service-node Driver."""
    if not uses_slurm_execution_backend():
        return None
    return await slurm_execution_service.reconcile_active_job()


def detach_execution_backend(execution_id: str) -> None:
    """Deprecated: a local Driver cannot detach while remote Workers continue."""
    del execution_id


__all__ = [
    "execute_graph",
    "detach_execution_backend",
    "preflight_graph",
    "reconcile_execution_backend",
    "uses_slurm_execution_backend",
]
