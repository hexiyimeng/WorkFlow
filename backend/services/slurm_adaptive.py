"""Profile-scoped Dask Adaptive; Slurm job counts include pending requests.

All task state is read from the existing Scheduler. There is no second task
queue, dependency resolver, or submission path for workflow tasks.
"""
from __future__ import annotations

import math
import hashlib
import time
from collections.abc import Callable, Sequence

from distributed.deploy import Adaptive

from core.worker_pool import WorkerPool
from services.slurm_jobqueue_cluster import PlannedSlurmWorkerSpec


def profile_target_jobs(scheduler, pool: WorkerPool, target_duration: float,
                        slots_per_worker: int = 1) -> int:
    """Apply workload/duration and memory heuristics within one resource profile.

    Each task consumes one profile token. CPU capacity follows Worker threads;
    GPU capacity is limited by both threads and devices. Use configured capacity
    even while Workers are pending/restarting.
    Scheduler.processing also contains work already queued at a worker.
    """
    tasks = [task for task in scheduler.tasks.values()
             if task.state in {"processing", "queued", "no-worker"}
             and (task.resource_restrictions or {}).get(pool.profile, 0) > 0]
    duration = sum(scheduler._get_prefix_duration(task.prefix) for task in tasks)
    task_slots = min(len(tasks), math.ceil(duration / target_duration))
    task_workers = math.ceil(task_slots / slots_per_worker)
    workers = [worker for worker in scheduler.workers.values()
               if worker.resources.get(pool.profile, 0) >= 1]
    # Avoid retiring capacity which is still needed for retained intermediate data.
    memory_workers = 0
    if workers:
        limit = sum(worker.memory_limit for worker in workers)
        used = sum(worker.nbytes for worker in workers)
        if limit:
            memory_workers = math.ceil(used / (0.6 * limit / len(workers)))
    target = math.ceil(max(task_workers, memory_workers) / pool.processes)
    return max(pool.minimum_jobs, min(pool.maximum_jobs, target))


class ProfileAdaptive(Adaptive):
    """Adaptive tokens are whole Slurm jobs, not individual worker processes."""

    def __init__(self, cluster, pool: WorkerPool,
                 baseline_names: Sequence[Sequence[str]],
                 spec_factory: Callable[[str, str], PlannedSlurmWorkerSpec],
                 token_prefix: str, *, slots_per_worker: int = 1, **kwargs):
        self.pool = pool
        self.slots_per_worker = slots_per_worker
        self.baseline_names = {
            f"baseline:{pool.profile}:{index}": tuple(names)
            for index, names in enumerate(baseline_names)
        }
        self.elastic_names: dict[str, tuple[str, ...]] = {}
        self.spec_factory = spec_factory
        self.token_prefix = token_prefix
        self.serial = 0
        self.retry_after = 0.0
        self.failure: BaseException | None = None
        super().__init__(cluster, minimum=pool.minimum_jobs,
                         maximum=pool.maximum_jobs, **kwargs)

    @property
    def plan(self):
        return set(self.baseline_names) | {
            key for key in self.elastic_names if key in self.cluster.worker_spec
        }

    @property
    def requested(self):
        return set(self.baseline_names) | {
            key for key in self.elastic_names
            if key in self.cluster.workers and self.cluster.workers[key].job_id
        }

    @property
    def observed(self):
        names = {worker.name for worker in self.cluster.scheduler.workers.values()}
        return {key for key, members in
                {**self.baseline_names, **self.elastic_names}.items()
                if set(members).issubset(names)}

    async def target(self):
        return profile_target_jobs(self.cluster.scheduler, self.pool, self.target_duration,
                                   self.slots_per_worker)

    async def workers_to_close(self, target):
        workers = {worker.name: worker for worker in self.cluster.scheduler.workers.values()}
        candidates = [key for key, names in self.elastic_names.items()
                      if key in self.plan and key in self.observed
                      and all(not workers[name].processing for name in names)]
        candidates.sort(key=lambda key: sum(workers[n].nbytes for n in self.elastic_names[key]))
        # AdaptiveCore handles requested-but-not-observed jobs first.
        pending = len((self.requested - self.observed) - set(self.baseline_names))
        return candidates[:max(0, len(self.plan) - target - pending)]

    async def recommendations(self, target):
        result = await super().recommendations(target)
        if result.get("status") == "down":
            # Even partial baseline registration must never make it a pending
            # elastic candidate in AdaptiveCore.
            result["workers"] = [key for key in result["workers"]
                                 if key in self.elastic_names]
        return result

    async def scale_up(self, n):
        if time.monotonic() < self.retry_after:
            return
        for _ in range(max(0, min(n, self.pool.maximum_jobs) - len(self.plan))):
            self.serial += 1
            name = f"{self.pool.profile}-elastic-{self.serial}"
            token = f"{self.token_prefix}:e:{hashlib.sha256(name.encode()).hexdigest()[:16]}"
            names = (name,) if self.pool.processes == 1 else tuple(
                f"{name}-{index}" for index in range(self.pool.processes))
            self.elastic_names[name] = names
            try:
                spec = self.spec_factory(name, token)
                await self.cluster.add_job_specs((spec,))
            except Exception as exc:
                # sbatch errors can be ambiguous: stop growth and let the service
                # reconcile the durable ownership journal before any retry.
                self.failure = exc
                self.stop()
                raise

    async def scale_down(self, workers):
        for key in workers:
            if key not in self.elastic_names or key not in self.cluster.worker_spec:
                continue
            names = set(self.elastic_names[key])
            live = {address: worker for address, worker in self.cluster.scheduler.workers.items()
                    if worker.name in names}
            if any(worker.processing for worker in live.values()):
                continue
            if live:
                retired = await self.cluster.scheduler.retire_workers(
                    workers=list(live), close_workers=True, remove=True)
                if not set(live).issubset(retired):
                    continue
            self.cluster.worker_spec.pop(key, None)
            await self.cluster._correct_state()

    async def allocation_ended(self, allocation_id: str):
        """A failed elastic allocation does not cancel the baseline workflow."""
        self.cluster.worker_spec.pop(allocation_id, None)
        await self.cluster._correct_state()
        self.retry_after = time.monotonic() + 60


def start_profile_adaptives(cluster, pools, baseline_specs, spec_factory, token_prefix):
    """Run on the owning Cluster's IOLoop."""
    baseline_key = baseline_specs[0].allocation_id
    baseline_names = {pool.profile: [] for pool in pools}
    profile_slots = {}
    for index, spec in enumerate(baseline_specs):
        # Factory already knows the Profile; find it from the plan's allocation ID.
        profile = next(pool.profile for pool in pools
                       if spec.allocation_id.rsplit("-", 1)[0] == pool.profile)
        processes = int(spec.options["processes"])
        profile_slots[profile] = spec.slots_per_worker
        name = f"{baseline_key}-het-{index}"
        names = (name,) if processes == 1 else tuple(f"{name}-{i}" for i in range(processes))
        baseline_names[profile].append(names)
    cluster.profile_adaptives = [
        ProfileAdaptive(cluster, pool, baseline_names[pool.profile],
                        lambda name, token, profile=pool.profile: spec_factory(profile, name, token),
                        token_prefix, slots_per_worker=profile_slots[pool.profile])
        for pool in pools
    ]
