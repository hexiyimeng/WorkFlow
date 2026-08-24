from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from core.cluster_inventory import ClusterInventory, ClusterNode
from core.worker_pool import WorkerPool
from core.worker_profiles import WorkerProfile
from core.workflow_resources import WorkflowResourcePlan


class ResourcePlanningError(ValueError):
    """A valid profile/pool request cannot be placed on the inventory."""


@dataclass(frozen=True, slots=True)
class SlurmJobRequirement:
    allocation_id: str
    profile: str
    node: str
    workers: int
    processes: int
    threads: int
    cpu: int
    memory_gib: int
    gpu: int
    logical_resources: Mapping[str, float]

    def to_dict(self) -> dict[str, object]:
        return {
            "allocationId": self.allocation_id,
            "profile": self.profile,
            "node": self.node,
            "workers": self.workers,
            "processes": self.processes,
            "threads": self.threads,
            "slurm": {
                "nodes": 1,
                "cpus": self.cpu,
                "memoryGiB": self.memory_gib,
                "gpus": self.gpu,
                "nodelist": [self.node],
            },
            "logicalResources": dict(self.logical_resources),
        }


@dataclass(frozen=True, slots=True)
class NodeAllocation:
    node: str
    workers: Mapping[str, int]
    cpu: int
    memory_gib: int
    gpu: int
    jobs: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "node": self.node,
            "workers": dict(sorted(self.workers.items())),
            "cpu": self.cpu,
            "memoryGiB": self.memory_gib,
            "gpu": self.gpu,
            "jobs": list(self.jobs),
        }


@dataclass(frozen=True, slots=True)
class SlurmAllocationPlan:
    partition: str
    time_limit: str
    required_worker_profiles: Mapping[str, int]
    worker_counts: Mapping[str, int]
    jobs: tuple[SlurmJobRequirement, ...]
    nodes: tuple[NodeAllocation, ...]

    @property
    def total_workers(self) -> int:
        return sum(self.worker_counts.values())

    @property
    def total_cpu(self) -> int:
        return sum(job.cpu for job in self.jobs)

    @property
    def total_gpu(self) -> int:
        return sum(job.gpu for job in self.jobs)

    @property
    def total_memory_gib(self) -> int:
        return sum(job.memory_gib for job in self.jobs)

    def to_dict(self) -> dict[str, object]:
        return {
            "partition": self.partition,
            "timeLimit": self.time_limit,
            "requiredWorkerProfiles": dict(self.required_worker_profiles),
            "workerCounts": dict(self.worker_counts),
            "totalWorkers": self.total_workers,
            "totalCpu": self.total_cpu,
            "totalGpu": self.total_gpu,
            "totalMemoryGiB": self.total_memory_gib,
            "jobs": [job.to_dict() for job in self.jobs],
            "nodes": [node.to_dict() for node in self.nodes],
        }


@dataclass(slots=True)
class _NodeCapacity:
    node: ClusterNode
    cpu_used: int = 0
    memory_gib_used: int = 0
    gpu_used: int = 0

    def fits(self, *, cpu: int, memory_gib: int, gpu: int) -> bool:
        return (
            self.cpu_used + cpu <= self.node.cpu
            and self.memory_gib_used + memory_gib <= self.node.memory_gib
            and self.gpu_used + gpu <= self.node.gpu
        )

    def score(self, *, cpu: int, memory_gib: int, gpu: int) -> tuple[float, float, float, str]:
        cpu_fraction = (self.cpu_used + cpu) / max(1, self.node.cpu)
        memory_fraction = (self.memory_gib_used + memory_gib) / max(1, self.node.memory_gib)
        gpu_fraction = (
            (self.gpu_used + gpu) / self.node.gpu
            if self.node.gpu
            else (0.0 if gpu == 0 else float("inf"))
        )
        # Best-fit keeps large contiguous resources available on unused nodes.
        return (-max(cpu_fraction, memory_fraction, gpu_fraction), -gpu_fraction, -memory_fraction, self.node.name)


def _index_unique(items: Sequence[object], *, key, label: str) -> dict[str, object]:
    result: dict[str, object] = {}
    for item in items:
        name = key(item)
        if name in result:
            raise ResourcePlanningError(f"Duplicate {label} for {name!r}.")
        result[name] = item
    return result


def plan_workflow_resources(
    workflow: WorkflowResourcePlan,
    profiles: Sequence[WorkerProfile],
    pools: Sequence[WorkerPool],
    inventory: ClusterInventory,
    *,
    partition: str,
    time_limit: str,
) -> SlurmAllocationPlan:
    if not workflow.required_worker_profiles:
        raise ResourcePlanningError("The workflow has no Worker Profile requirements.")
    profile_by_name = _index_unique(profiles, key=lambda item: item.name, label="Worker Profile")
    pool_by_profile = _index_unique(pools, key=lambda item: item.profile, label="Worker Pool")

    required_names = set(workflow.required_worker_profiles)
    missing_profiles = sorted(required_names - set(profile_by_name))
    missing_pools = sorted(required_names - set(pool_by_profile))
    if missing_profiles:
        raise ResourcePlanningError(
            "Configure Worker Profile(s): " + ", ".join(missing_profiles) + "."
        )
    if missing_pools:
        raise ResourcePlanningError(
            "Configure Worker Pool(s): " + ", ".join(missing_pools) + "."
        )

    candidates = [
        _NodeCapacity(node=node)
        for node in sorted(inventory.for_partition(partition), key=lambda item: item.name)
    ]
    if not candidates:
        raise ResourcePlanningError(
            f"No available Slurm node was reported for partition {partition!r}."
        )

    units: list[tuple[WorkerProfile, WorkerPool, int, int, int, int]] = []
    worker_counts: dict[str, int] = {}
    for name in sorted(required_names):
        profile = profile_by_name[name]
        pool = pool_by_profile[name]
        assert isinstance(profile, WorkerProfile)
        assert isinstance(pool, WorkerPool)
        pool.validate_profile(profile)
        physical = profile.physical_resources
        job_cpu = physical.cpu * pool.processes
        job_memory = physical.memory_gib * pool.processes
        job_gpu = physical.gpu * pool.processes
        worker_counts[name] = pool.worker_count
        for scale_index in range(pool.scale):
            units.append((profile, pool, scale_index, job_cpu, job_memory, job_gpu))

    units.sort(
        key=lambda item: (
            -item[5],
            -item[4],
            -item[3],
            item[0].name,
            item[2],
        )
    )

    jobs: list[SlurmJobRequirement] = []
    node_jobs: dict[str, list[SlurmJobRequirement]] = {}
    for profile, pool, scale_index, cpu, memory_gib, gpu in units:
        fitting = [
            capacity for capacity in candidates
            if capacity.fits(cpu=cpu, memory_gib=memory_gib, gpu=gpu)
        ]
        if not fitting:
            raise ResourcePlanningError(
                f"Cannot place Worker Pool {profile.name!r} instance "
                f"{scale_index + 1}/{pool.scale}: it needs CPU={cpu}, "
                f"memory={memory_gib}GiB, GPU={gpu}. Inventory capacity is exhausted."
            )
        selected = min(
            fitting,
            key=lambda capacity: capacity.score(cpu=cpu, memory_gib=memory_gib, gpu=gpu),
        )
        selected.cpu_used += cpu
        selected.memory_gib_used += memory_gib
        selected.gpu_used += gpu
        allocation_id = f"{profile.name}-{scale_index + 1}"
        job = SlurmJobRequirement(
            allocation_id=allocation_id,
            profile=profile.name,
            node=selected.node.name,
            workers=pool.processes,
            processes=pool.processes,
            threads=profile.threads,
            cpu=cpu,
            memory_gib=memory_gib,
            gpu=gpu,
            logical_resources=profile.logical_resources,
        )
        jobs.append(job)
        node_jobs.setdefault(selected.node.name, []).append(job)

    node_allocations = tuple(
        NodeAllocation(
            node=node_name,
            workers={
                profile_name: sum(job.workers for job in planned if job.profile == profile_name)
                for profile_name in sorted({job.profile for job in planned})
            },
            cpu=sum(job.cpu for job in planned),
            memory_gib=sum(job.memory_gib for job in planned),
            gpu=sum(job.gpu for job in planned),
            jobs=tuple(job.allocation_id for job in planned),
        )
        for node_name, planned in sorted(node_jobs.items())
    )
    return SlurmAllocationPlan(
        partition=partition,
        time_limit=time_limit,
        required_worker_profiles=workflow.required_worker_profiles,
        worker_counts=dict(sorted(worker_counts.items())),
        jobs=tuple(sorted(jobs, key=lambda item: item.allocation_id)),
        nodes=node_allocations,
    )


__all__ = [
    "NodeAllocation",
    "ResourcePlanningError",
    "SlurmAllocationPlan",
    "SlurmJobRequirement",
    "plan_workflow_resources",
]
