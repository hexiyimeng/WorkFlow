from __future__ import annotations

from typing import Literal, cast


ExecutionResource = Literal["any", "cpu", "gpu"]

CPU_RESOURCE: dict[str, float] = {"CPU": 1.0}
GPU_RESOURCE: dict[str, float] = {"GPU": 1.0}


def normalize_execution_resource(
    value: object,
    *,
    owner: str = "Node",
) -> ExecutionResource:
    """Validate a logical execution resource without resolving a device ID."""

    normalized = str(value).strip().lower()
    if normalized not in {"any", "cpu", "gpu"}:
        raise ValueError(
            f"{owner}.EXECUTION_RESOURCE must be 'any', 'cpu', or 'gpu', "
            f"got {normalized!r}."
        )
    return cast(ExecutionResource, normalized)


def resolve_execution_resource(node_cls: type) -> ExecutionResource:
    """Return the declared resource, defaulting unconstrained nodes to any."""

    return normalize_execution_resource(
        getattr(node_cls, "EXECUTION_RESOURCE", "any"),
        owner=getattr(node_cls, "__name__", str(node_cls)),
    )


def normalize_execution_workers(
    value: object,
    *,
    owner: str = "Node",
) -> int:
    """Validate a node's requested Worker-pool capacity."""

    if type(value) is not int or value < 0:
        raise ValueError(
            f"{owner}.EXECUTION_WORKERS must be a non-negative integer, "
            f"got {value!r}."
        )
    return value


def resolve_execution_workers(node_cls: type) -> int:
    """Return how many Workers this node contributes to its resource pool."""

    owner = getattr(node_cls, "__name__", str(node_cls))
    resource = resolve_execution_resource(node_cls)
    return normalize_execution_workers(
        getattr(
            node_cls,
            "EXECUTION_WORKERS",
            0 if resource == "any" else 1,
        ),
        owner=owner,
    )


def dask_resources_for_execution_resource(
    execution_resource: object,
) -> dict[str, float]:
    resource = normalize_execution_resource(
        execution_resource,
        owner="NodeRuntime",
    )
    if resource == "gpu":
        return dict(GPU_RESOURCE)
    if resource == "cpu":
        return dict(CPU_RESOURCE)
    return {}


def dask_resources_for_node(node_cls: type) -> dict[str, float]:
    return dask_resources_for_execution_resource(
        resolve_execution_resource(node_cls)
    )


def dask_annotation_kwargs(
    node_cls: type,
    node_id: str | None,
) -> dict[str, object]:
    """Build the exact annotations used for layers created by one node."""

    resource = resolve_execution_resource(node_cls)
    annotations: dict[str, object] = {
        "brainflow_node_id": node_id,
        "execution_resource": resource,
    }
    resources = dask_resources_for_execution_resource(resource)
    if resources:
        annotations["resources"] = resources
    return annotations
