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

from dask.distributed import Client, SpecCluster

from services.dask_service import build_local_cluster_specs


__test__ = False


def _task_process_contract() -> dict[str, str]:
    return {
        "role": os.environ.get("WORKFLOW_WORKER_ROLE", ""),
        "cudaVisibleDevices": os.environ.get("CUDA_VISIBLE_DEVICES", ""),
    }


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="workflow-loopback-probe-") as local_dir:
        scheduler_spec, worker_specs = build_local_cluster_specs(
            cpu_workers=4,
            gpu_ids=tuple(str(index) for index in range(8)),
            cpu_memory_limit="512MiB",
            gpu_memory_limit="512MiB",
            local_directory=local_dir,
            dashboard_address=":0",
            worker_start_timeout=180.0,
        )
        cluster = None
        client = None
        try:
            cluster = SpecCluster(
                workers=worker_specs,
                scheduler=scheduler_spec,
                asynchronous=False,
                silence_logs="warning",
                name="WorkFlow loopback regression probe",
            )
            client = Client(cluster, timeout=180.0)
            client.wait_for_workers(12, timeout=180.0)
            scheduler_info = client.scheduler_info(n_workers=-1)
            worker_addresses = tuple(sorted(scheduler_info["workers"]))
            task_futures = {
                address: client.submit(
                    _task_process_contract,
                    workers=[address],
                    allow_other_workers=False,
                    pure=False,
                )
                for address in worker_addresses
            }
            task_results = {
                address: future.result(timeout=30.0)
                for address, future in task_futures.items()
            }
            payload = {
                "schedulerAddress": scheduler_info["address"],
                "workerAddresses": worker_addresses,
                "taskResults": task_results,
            }
            print("PROBE_RESULT=" + json.dumps(payload, sort_keys=True), flush=True)
        finally:
            if client is not None:
                client.close(timeout=30.0)
            if cluster is not None:
                cluster.close(timeout=30.0)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
