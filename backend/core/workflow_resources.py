from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from core.execution_resources import (
    ExecutionResource,
    normalize_execution_resource,
    normalize_execution_workers,
    resolve_execution_resource,
    resolve_execution_workers,
)
from core.registry import NODE_CLASS_MAPPINGS


@dataclass(frozen=True)
class ResourceNodeRequirement:
    node_id: str
    node_type: str
    display_name: str
    resource: ExecutionResource
    workers: int | None = None

    def __post_init__(self) -> None:
        owner = f"Resource requirement for node {self.node_id!r}"
        resource = normalize_execution_resource(self.resource, owner=owner)
        workers_value = self.workers
        if workers_value is None:
            workers_value = 0 if resource == "any" else 1
        workers = normalize_execution_workers(
            workers_value,
            owner=owner,
        )
        object.__setattr__(self, "resource", resource)
        object.__setattr__(self, "workers", workers)

    def to_dict(self) -> dict[str, object]:
        return {
            "nodeId": self.node_id,
            "nodeType": self.node_type,
            "displayName": self.display_name,
            "resource": self.resource,
            "workers": self.workers,
        }


@dataclass(frozen=True)
class WorkflowResourcePlan:
    nodes: tuple[ResourceNodeRequirement, ...]
    requires_cpu: bool
    requires_gpu: bool
    cpu_node_ids: tuple[str, ...]
    gpu_node_ids: tuple[str, ...]
    any_node_ids: tuple[str, ...] = ()
    cpu_workers: int = 0
    gpu_workers: int = 0

    def __post_init__(self) -> None:
        for field_name in ("cpu_workers", "gpu_workers"):
            value = getattr(self, field_name)
            if type(value) is not int or value < 0:
                raise ValueError(
                    f"WorkflowResourcePlan.{field_name} must be a "
                    f"non-negative integer, got {value!r}."
                )

        declared_cpu_ids = tuple(
            node.node_id for node in self.nodes if node.resource == "cpu"
        )
        declared_gpu_ids = tuple(
            node.node_id for node in self.nodes if node.resource == "gpu"
        )
        declared_any_ids = tuple(
            node.node_id for node in self.nodes if node.resource == "any"
        )
        cpu_node_ids = tuple(
            dict.fromkeys((*self.cpu_node_ids, *declared_cpu_ids))
        )
        gpu_node_ids = tuple(
            dict.fromkeys((*self.gpu_node_ids, *declared_gpu_ids))
        )
        any_node_ids = tuple(
            dict.fromkeys((*self.any_node_ids, *declared_any_ids))
        )
        declared_cpu = sum(
            int(node.workers)
            for node in self.nodes
            if node.resource in {"any", "cpu"}
        )
        declared_gpu = sum(
            int(node.workers)
            for node in self.nodes
            if node.resource == "gpu"
        )
        object.__setattr__(self, "cpu_node_ids", cpu_node_ids)
        object.__setattr__(self, "gpu_node_ids", gpu_node_ids)
        object.__setattr__(self, "any_node_ids", any_node_ids)
        object.__setattr__(
            self,
            "cpu_workers",
            max(self.cpu_workers, declared_cpu),
        )
        object.__setattr__(
            self,
            "gpu_workers",
            max(self.gpu_workers, declared_gpu),
        )
        object.__setattr__(
            self,
            "requires_cpu",
            bool(self.requires_cpu or cpu_node_ids or self.cpu_workers > 0),
        )
        object.__setattr__(
            self,
            "requires_gpu",
            bool(self.requires_gpu or gpu_node_ids or self.gpu_workers > 0),
        )

    @property
    def is_mixed(self) -> bool:
        return self.requires_cpu and self.requires_gpu

    def to_preflight_dict(self) -> dict[str, object]:
        return {
            "requiresCpu": self.requires_cpu,
            "requiresGpu": self.requires_gpu,
            "cpuWorkers": self.cpu_workers,
            "gpuWorkers": self.gpu_workers,
            "cpuNodes": [
                node.to_dict()
                for node in self.nodes
                if node.resource == "cpu"
            ],
            "gpuNodes": [
                node.to_dict()
                for node in self.nodes
                if node.resource == "gpu"
            ],
            "anyNodes": [
                node.to_dict()
                for node in self.nodes
                if node.resource == "any"
            ],
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
    """Resolve logical resources for only the terminal-reachable graph."""

    mappings = NODE_CLASS_MAPPINGS if node_mappings is None else node_mappings
    requirements: list[ResourceNodeRequirement] = []
    for node_id in sorted(_reachable_node_ids(graph, execution_roots)):
        node_data = graph[node_id]
        node_type = str(node_data.get("type", ""))
        node_cls = mappings.get(node_type)
        if node_cls is None:
            raise ValueError(
                f"Cannot build resource plan: node type {node_type!r} is not registered."
            )
        resource = resolve_execution_resource(node_cls)
        workers = resolve_execution_workers(node_cls)
        requirements.append(ResourceNodeRequirement(
            node_id=node_id,
            node_type=node_type,
            display_name=str(getattr(node_cls, "DISPLAY_NAME", node_type)),
            resource=resource,
            workers=workers,
        ))

    nodes = tuple(requirements)
    cpu_node_ids = tuple(node.node_id for node in nodes if node.resource == "cpu")
    gpu_node_ids = tuple(node.node_id for node in nodes if node.resource == "gpu")
    any_node_ids = tuple(node.node_id for node in nodes if node.resource == "any")
    cpu_workers = sum(
        int(node.workers)
        for node in nodes
        if node.resource in {"any", "cpu"}
    )
    gpu_workers = sum(
        int(node.workers) for node in nodes if node.resource == "gpu"
    )
    return WorkflowResourcePlan(
        nodes=nodes,
        requires_cpu=bool(cpu_node_ids or cpu_workers),
        requires_gpu=bool(gpu_node_ids),
        cpu_node_ids=cpu_node_ids,
        gpu_node_ids=gpu_node_ids,
        any_node_ids=any_node_ids,
        cpu_workers=cpu_workers,
        gpu_workers=gpu_workers,
    )


def validate_workflow_resource_plan(
    plan: WorkflowResourcePlan,
    cluster_summary: object,
) -> None:
    cpu_slots = float(getattr(cluster_summary, "total_cpu_slots", 0.0))
    gpu_slots = float(getattr(cluster_summary, "total_gpu_slots", 0.0))
    required_cpu_slots = max(plan.cpu_workers, int(plan.requires_cpu))
    required_gpu_slots = max(plan.gpu_workers, int(plan.requires_gpu))
    if plan.requires_cpu and cpu_slots < required_cpu_slots:
        availability = (
            "no Worker with resource CPU=1 is available"
            if cpu_slots < 1
            else f"only {cpu_slots:g} CPU Worker slot(s) are available"
        )
        raise RuntimeError(
            f"The workflow requires {required_cpu_slots} CPU Worker slot(s) with "
            f"resource CPU=1, but {availability}."
        )
    if plan.requires_gpu and gpu_slots < required_gpu_slots:
        availability = (
            "no GPU Worker is available"
            if gpu_slots < 1
            else f"only {gpu_slots:g} GPU Worker slot(s) are available"
        )
        raise RuntimeError(
            f"The workflow requires {required_gpu_slots} GPU Worker slot(s) with "
            f"resource GPU=1, but {availability}. CPU fallback is not supported."
        )
