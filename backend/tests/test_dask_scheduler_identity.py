from __future__ import annotations

from services.dask_service import (
    CPU_RESOURCE_NAME,
    GPU_RESOURCE_NAME,
    DaskService,
    get_fresh_scheduler_info,
)
from services.memory_monitor import query_worker_memory


def _worker_info(role: str, gpu_id: str | None = None) -> dict:
    if role == "cpu":
        resources = {CPU_RESOURCE_NAME: 1.0}
        startup = {
            "workerRole": "cpu",
            "assignedDevice": "cpu",
            "cudaVisibleDevices": "",
            "physicalGpuId": None,
            "resources": resources,
        }
    else:
        resources = {GPU_RESOURCE_NAME: 1.0}
        startup = {
            "workerRole": "gpu",
            "assignedDevice": "cuda:0",
            "cudaVisibleDevices": gpu_id,
            "physicalGpuId": gpu_id,
            "resources": resources,
        }
    return {
        "resources": resources,
        "workflowDevice": startup,
    }


def _fourteen_worker_identity() -> dict:
    workers = {
        f"tcp://127.0.0.1:{50000 + index}": _worker_info("cpu")
        for index in range(6)
    }
    workers.update(
        {
            f"tcp://127.0.0.1:{51000 + index}": _worker_info("gpu", str(index))
            for index in range(8)
        }
    )
    return {
        "address": "tcp://127.0.0.1:49999",
        "workers": workers,
    }


class _CacheRacingClient:
    """Model Distributed's five-Worker shared scheduler_info cache race."""

    def __init__(self):
        self.callback_timeouts: list[float | None] = []
        full_identity = _fourteen_worker_identity()
        self.truncated_identity = {
            **full_identity,
            "workers": dict(list(full_identity["workers"].items())[:5]),
        }

        class _Scheduler:
            async def identity(inner_self, *, n_workers):
                assert n_workers == -1
                return full_identity

        self.scheduler = _Scheduler()

    def scheduler_info(self, *, n_workers):
        assert n_workers == -1
        return self.truncated_identity

    def sync(self, function, **kwargs):
        import asyncio

        self.callback_timeouts.append(kwargs.pop("callback_timeout", None))
        return asyncio.run(function(**kwargs))


def test_direct_scheduler_identity_ignores_five_worker_client_cache() -> None:
    client = _CacheRacingClient()

    assert len(client.scheduler_info(n_workers=-1)["workers"]) == 5
    assert len(get_fresh_scheduler_info(client)["workers"]) == 14


def test_topology_validation_uses_atomic_fourteen_worker_identity() -> None:
    client = _CacheRacingClient()
    service = object.__new__(DaskService)

    summary = service.validate_cluster_topology(
        expected_cpu_workers=6,
        expected_gpu_workers=8,
        expected_gpu_ids=tuple(str(index) for index in range(8)),
        client=client,
    )

    assert len(summary.cpu_workers) == 6
    assert len(summary.gpu_workers) == 8
    assert summary.total_cpu_slots == 6.0
    assert summary.total_gpu_slots == 8.0


def test_direct_scheduler_identity_forwards_rpc_timeout() -> None:
    client = _CacheRacingClient()

    get_fresh_scheduler_info(client, timeout=3.25)

    assert client.callback_timeouts == [3.25]


def test_memory_diagnostics_bound_the_direct_scheduler_rpc() -> None:
    client = _CacheRacingClient()

    result = query_worker_memory(client, timeout_seconds=2.5)

    assert len(result) == 14
    assert client.callback_timeouts == [2.5]
