from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from core.registry import NODE_CLASS_MAPPINGS
from core.worker_profiles import normalize_worker_profile, resolve_worker_profile


@dataclass(frozen=True, slots=True)
class WorkerProfileRequirement:
    node_id: str
    node_type: str
    display_name: str
    worker_profile: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "worker_profile",
            normalize_worker_profile(
                self.worker_profile,
                owner=f"Worker profile requirement for node {self.node_id!r}",
            ),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "nodeId": self.node_id,
            "nodeType": self.node_type,
            "displayName": self.display_name,
            "workerProfile": self.worker_profile,
        }


@dataclass(frozen=True, slots=True)
class WorkflowResourcePlan:
    """Backend-neutral profile requirements for a reachable workflow.

    Each count is the number of reachable nodes declaring that profile. It is
    intentionally not a Worker-pool size. The Resource Planner turns these
    requirements plus browser-supplied Pools into concrete scheduler requests.
    """

    nodes: tuple[WorkerProfileRequirement, ...]

    @property
    def required_worker_profiles(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for node in self.nodes:
            counts[node.worker_profile] = counts.get(node.worker_profile, 0) + 1
        return dict(sorted(counts.items()))

    def requires_worker_profile(self, profile: str) -> bool:
        normalized = normalize_worker_profile(profile, owner="WorkflowResourcePlan")
        return any(node.worker_profile == normalized for node in self.nodes)

    @property
    def is_mixed(self) -> bool:
        return len(self.required_worker_profiles) > 1

    def to_preflight_dict(self) -> dict[str, object]:
        return {
            "requiredWorkerProfiles": self.required_worker_profiles,
            "profileRequirements": [node.to_dict() for node in self.nodes],
        }


def _reachable_node_ids(
    graph: Mapping[str, Mapping[str, object]],
    execution_roots: Sequence[str],
) -> set[str]:
    reachable: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in reachable:
            return
        node_data = graph.get(node_id)
        if node_data is None:
            raise ValueError(
                f"Cannot build resource plan for unknown node {node_id!r}."
            )
        reachable.add(node_id)
        raw_inputs = node_data.get("inputs") or {}
        if not isinstance(raw_inputs, Mapping):
            raise ValueError(f"Node {node_id!r} inputs must be an object.")
        for value in raw_inputs.values():
            if isinstance(value, list) and len(value) == 2:
                visit(str(value[0]))

    for root_id in execution_roots:
        visit(str(root_id))
    return reachable


def build_workflow_resource_plan(
    graph: Mapping[str, Mapping[str, object]],
    execution_roots: Sequence[str],
    *,
    node_mappings: Mapping[str, type] | None = None,
) -> WorkflowResourcePlan:
    """Collect profile requirements for only the terminal-reachable graph."""

    mappings = NODE_CLASS_MAPPINGS if node_mappings is None else node_mappings
    requirements: list[WorkerProfileRequirement] = []
    for node_id in sorted(_reachable_node_ids(graph, execution_roots)):
        node_data = graph[node_id]
        node_type = str(node_data.get("type", ""))
        node_cls = mappings.get(node_type)
        if node_cls is None:
            raise ValueError(
                f"Cannot build resource plan: node type {node_type!r} is not registered."
            )
        requirements.append(WorkerProfileRequirement(
            node_id=node_id,
            node_type=node_type,
            display_name=str(getattr(node_cls, "DISPLAY_NAME", node_type)),
            worker_profile=resolve_worker_profile(node_cls),
        ))
    return WorkflowResourcePlan(nodes=tuple(requirements))


def ensure_executable_resource_plan(plan: WorkflowResourcePlan) -> WorkflowResourcePlan:
    """Validate profile declarations without inventing a Worker topology."""

    if not isinstance(plan, WorkflowResourcePlan):
        raise TypeError("plan must be a WorkflowResourcePlan.")
    return plan


def validate_workflow_resource_plan(
    plan: WorkflowResourcePlan,
    cluster_summary: object,
) -> None:
    """Validate placement capabilities, never profile counts as Worker counts."""

    profile_slots = getattr(cluster_summary, "worker_profile_slots", None)
    if profile_slots is None:
        return
    missing = sorted(
        profile for profile in plan.required_worker_profiles
        if float(profile_slots.get(profile, 0) or 0) < 1
    )
    if missing:
        raise RuntimeError(
            "No active Dask Worker advertises required Profile(s): "
            + ", ".join(missing)
            + "."
        )


__all__ = [
    "WorkerProfileRequirement",
    "WorkflowResourcePlan",
    "build_workflow_resource_plan",
    "ensure_executable_resource_plan",
    "validate_workflow_resource_plan",
]
