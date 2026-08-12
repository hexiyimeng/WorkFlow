"""Subprocess entry point for a real multi-Nanny loopback smoke test.

The filename follows this repository's test-file allowlist so it is versioned,
but ``__test__ = False`` keeps pytest from treating the helper as a test module.
Windows multiprocessing spawn must be launched from an importable file guarded
by ``if __name__ == '__main__'``.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import tempfile

from distributed import get_worker

from services import dask_service as dask_service_module
from services.dask_service import (
    CPU_RESOURCE_NAME,
    GPU_RESOURCE_NAME,
    DaskService,
    _worker_resources,
)


__test__ = False


def _task_process_contract() -> dict[str, object]:
    worker = get_worker()
    return {
        "address": str(worker.address),
        "role": os.environ.get("WORKFLOW_WORKER_ROLE", ""),
        "cudaVisibleDevices": os.environ.get("CUDA_VISIBLE_DEVICES", ""),
        "physicalGpuId": os.environ.get("WORKFLOW_PHYSICAL_GPU_ID", ""),
        "resources": _worker_resources(worker),
    }


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="workflow-loopback-probe-") as local_dir:
        startup_events = ["startup_requested"]
        registered_batches: list[tuple[str, ...]] = []
        original_provision = dask_service_module._provision_worker_specs_in_batches
        original_add_batch = dask_service_module._add_worker_spec_batch

        async def observed_add_batch(cluster, worker_specs, **kwargs):
            registered_batches.append(tuple(worker_specs))
            return await original_add_batch(cluster, worker_specs, **kwargs)

        def observed_provision(cluster, client, worker_specs, **kwargs):
            startup_events.append("client_connected_before_workers")
            assert not dask_service_module.get_fresh_scheduler_info(
                client,
                timeout=30.0,
            )["workers"]
            result = original_provision(
                cluster,
                client,
                worker_specs,
                **kwargs,
            )
            startup_events.append("workers_ready")
            return result

        dask_service_module._detect_cuda_for_cluster = lambda: (True, 8)
        dask_service_module._get_dask_local_dir = lambda: local_dir
        dask_service_module._compute_worker_memory_limit = (
            lambda *_args, **_kwargs: "512MiB"
        )
        dask_service_module._provision_worker_specs_in_batches = observed_provision
        dask_service_module._add_worker_spec_batch = observed_add_batch
        dask_service_module.config.GPU_IDS = None
        dask_service_module.config.DASHBOARD_ADDRESS = ":0"
        service = DaskService()
        service.client = None
        service.cluster = None
        service._cluster_poisoned = False
        try:
            client = service.start_cluster(
                cpu_workers=6,
                gpu_workers=8,
            )
            scheduler_info = dask_service_module.get_fresh_scheduler_info(
                client,
                timeout=30.0,
            )
            worker_addresses = tuple(sorted(scheduler_info["workers"]))
            # Exercise the exact production validation path at a topology
            # larger than Distributed's five-Worker Client cache default.
            validation = service.validate_cluster_topology(
                expected_cpu_workers=6,
                expected_gpu_workers=8,
                expected_gpu_ids=tuple(str(index) for index in range(8)),
                client=client,
                rpc_timeout=30.0,
            )
            worker_resources = {
                address: dict(info.get("resources", {}) or {})
                for address, info in scheduler_info["workers"].items()
            }
            gpu_futures = [
                client.submit(
                    _task_process_contract,
                    workers=[address],
                    allow_other_workers=False,
                    resources={GPU_RESOURCE_NAME: 1},
                    pure=False,
                )
                for address, resources in worker_resources.items()
                if resources.get(GPU_RESOURCE_NAME) == 1
            ]
            cpu_futures = [
                client.submit(
                    _task_process_contract,
                    workers=[address],
                    allow_other_workers=False,
                    resources={CPU_RESOURCE_NAME: 1},
                    pure=False,
                )
                for address, resources in worker_resources.items()
                if resources.get(CPU_RESOURCE_NAME) == 1
            ]
            task_results = [
                future.result(timeout=30.0)
                for future in (*gpu_futures, *cpu_futures)
            ]
            payload = {
                "schedulerAddress": scheduler_info["address"],
                "workerAddresses": worker_addresses,
                "taskResults": task_results,
                "startupEvents": startup_events,
                "registeredBatches": registered_batches,
                "clientOwnsCluster": client.cluster is not None,
                "independentLoops": client.loop is not service.cluster.loop,
                "validatedCpuWorkers": len(validation.cpu_workers),
                "validatedGpuWorkers": len(validation.gpu_workers),
            }
            print("PROBE_RESULT=" + json.dumps(payload, sort_keys=True), flush=True)
        finally:
            service.stop_cluster()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
