"""Verify profile routing and concurrency on real Dask Worker thread pools."""
import asyncio
import time
from types import SimpleNamespace

import dask
import pytest
from distributed import Client, Scheduler, Worker, get_worker

from core.worker_profiles import (
    PhysicalResources, WorkerProfile, dask_resources_for_node,
)
from core.worker_pool import WorkerPool
from services.dask_service import WorkerDevicePlugin
from services.slurm_adaptive import profile_target_jobs


def timed_task(value):
    started = time.monotonic()
    time.sleep(0.1)
    return started, time.monotonic(), get_worker().name


@pytest.mark.parametrize("gpu, expected", [(0, 4), (1, 1)])
def test_real_worker_profile_routing_and_thread_device_concurrency(gpu, expected):
    profile = WorkerProfile("GPU" if gpu else "CPU", PhysicalResources(4, 4, gpu), {}, 4)
    node_cls = type("Node", (), {"required_worker_profile": profile.name})

    async def run():
        async with Scheduler(dashboard=False) as scheduler:
            async with (
                Worker(scheduler.address, name="selected", nthreads=4,
                       resources=dict(profile.logical_resources), memory_limit=0,
                       dashboard_address=None),
                Worker(scheduler.address, name="other-type", nthreads=4,
                       resources={"CPU": 4} if gpu else {"GPU": 1}, memory_limit=0,
                       dashboard_address=None),
                Client(scheduler.address, asynchronous=True) as client,
            ):
                with dask.annotate(resources=dask_resources_for_node(node_cls)):
                    graph = [dask.delayed(timed_task, pure=False)(i) for i in range(8)]
                return await client.gather(client.compute(graph))

    results = asyncio.run(run())
    assert {name for _, _, name in results} == {"selected"}
    events = sorted([(start, 1) for start, _, _ in results]
                    + [(end, -1) for _, end, _ in results])
    active = peak = 0
    for _, change in events:
        active += change
        peak = max(peak, active)
    assert peak == expected


def test_adaptive_counts_thread_slots_per_worker_and_processes_per_job():
    pool = WorkerPool("CPU", 1, 10, processes=2)
    scheduler = SimpleNamespace(
        tasks={i: SimpleNamespace(state="queued", prefix="read",
                                 resource_restrictions={"CPU": 1})
               for i in range(17)},
        workers={}, _get_prefix_duration=lambda prefix: 10,
    )
    # Includes pending/restarting pools with no registered Worker: 8 slots/Job.
    assert profile_target_jobs(scheduler, pool, 5, slots_per_worker=4) == 3
    scheduler.tasks.pop(16)
    assert profile_target_jobs(scheduler, pool, 5, slots_per_worker=4) == 2
    scheduler.tasks = {0: next(iter(scheduler.tasks.values()))}
    assert profile_target_jobs(scheduler, pool, 5, slots_per_worker=4) == 1
    # Retained data may still need two Jobs even without runnable tasks.
    scheduler.tasks = {}
    scheduler.workers = {
        str(i): SimpleNamespace(resources={"CPU": 4},
                                memory_limit=1000, nbytes=900)
        for i in range(2)
    }
    assert profile_target_jobs(scheduler, pool, 5, slots_per_worker=4) == 2


def test_worker_plugin_accepts_cpu_thread_capacity(monkeypatch):
    monkeypatch.setenv("WORKFLOW_WORKER_PROFILE", "CPU")
    monkeypatch.setenv("WORKFLOW_WORKER_ROLE", "cpu")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    worker = SimpleNamespace(state=SimpleNamespace(
        nthreads=4, total_resources={"CPU": 4}))
    WorkerDevicePlugin().setup(worker)
    assert worker.worker_role == "cpu"
    worker.state.total_resources["CPU"] = 1
    with pytest.raises(RuntimeError, match="thread/device slots"):
        WorkerDevicePlugin().setup(worker)


def test_capability_only_profile_derives_cpu_slots():
    profile = WorkerProfile.from_dict({
        "name": "CPU", "capabilities": ["CPU"],
        "physical_resources": {"cpu": 8, "memory": "4GB", "gpu": 0},
    })
    assert profile.logical_resources[profile.name] == profile.threads == 8


def test_only_two_worker_types_and_disjoint_resources():
    assert WorkerProfile("CPU", PhysicalResources(8, 4), {}, 8).logical_resources == {"CPU": 8}
    assert WorkerProfile("GPU", PhysicalResources(4, 8, 1), {}, 4).logical_resources == {"GPU": 1}
    with pytest.raises(ValueError, match="CPU or GPU"):
        WorkerProfile("reader", PhysicalResources(8, 4), {}, 8)
    with pytest.raises(ValueError, match="only GPU"):
        WorkerProfile("GPU", PhysicalResources(4, 8, 1), {"CPU": 4, "GPU": 1}, 4)


def test_builtin_reader_writer_and_compute_share_cpu_pool():
    from core.worker_profiles import resolve_worker_profile
    from nodes.ome_zarr_reader import OMEZarrReader
    from nodes.zarr_writer_node import ZarrWriter
    from nodes.cellpose_node import Cellpose

    assert resolve_worker_profile(OMEZarrReader) == resolve_worker_profile(ZarrWriter) == "CPU"
    assert resolve_worker_profile(Cellpose) == "GPU"
