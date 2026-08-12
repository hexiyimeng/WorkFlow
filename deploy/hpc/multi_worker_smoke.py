from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = PROJECT_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

CPU_RESOURCE_NAME = "CPU"
GPU_RESOURCE_NAME = "GPU"
WORK_ITEMS = 131_072


def _sum_of_squares(item_count: int) -> int:
    return item_count * (item_count + 1) * (2 * item_count + 1) // 6


def _worker_computation(
    *,
    ordinal: int,
    expected_role: str,
    validate_cuda: bool,
) -> dict[str, Any]:
    """Run one pinned task and return process, placement, and compute evidence."""
    from distributed import get_worker

    worker = get_worker()
    resources = {
        str(name): float(value)
        for name, value in dict(worker.state.total_resources).items()
    }
    role = os.environ.get("WORKFLOW_WORKER_ROLE", "")
    if role != expected_role:
        raise RuntimeError(
            f"Task {ordinal} expected a {expected_role} Worker, got {role!r}."
        )

    values = np.arange(1, WORK_ITEMS + 1, dtype=np.int64)
    cpu_checksum = int(np.dot(values, values))
    expected_checksum = _sum_of_squares(WORK_ITEMS)
    if cpu_checksum != expected_checksum:
        raise RuntimeError(
            f"Task {ordinal} produced CPU checksum {cpu_checksum}, "
            f"expected {expected_checksum}."
        )

    cuda_summary: dict[str, Any] | None = None
    if validate_cuda:
        import torch

        if not torch.cuda.is_available():
            raise RuntimeError(
                f"GPU Worker task {ordinal}: torch.cuda.is_available() is false."
            )
        if torch.cuda.device_count() != 1:
            raise RuntimeError(
                f"GPU Worker task {ordinal}: expected one isolated logical CUDA "
                f"device, found {torch.cuda.device_count()} "
                f"(CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES')!r})."
            )
        torch.cuda.set_device(0)
        cuda_values = torch.arange(
            1,
            WORK_ITEMS + 1,
            dtype=torch.int64,
            device="cuda:0",
        )
        cuda_checksum = int(torch.sum(cuda_values * cuda_values).item())
        torch.cuda.synchronize()
        if cuda_checksum != expected_checksum:
            raise RuntimeError(
                f"GPU Worker task {ordinal} produced CUDA checksum {cuda_checksum}, "
                f"expected {expected_checksum}."
            )
        cuda_summary = {
            "validated": True,
            "checksum": cuda_checksum,
            "logicalDeviceCount": int(torch.cuda.device_count()),
            "deviceName": str(torch.cuda.get_device_name(0)),
            "torchVersion": str(torch.__version__),
            "torchCudaVersion": str(torch.version.cuda),
        }

    return {
        "ordinal": ordinal,
        "workerAddress": str(worker.address),
        "workerName": str(worker.name),
        "host": platform.node(),
        "pid": os.getpid(),
        "role": role,
        "resources": resources,
        "cudaVisibleDevices": os.environ.get("CUDA_VISIBLE_DEVICES", ""),
        "physicalGpuId": os.environ.get("WORKFLOW_PHYSICAL_GPU_ID", ""),
        "cpuChecksum": cpu_checksum,
        "cuda": cuda_summary,
    }


def _nonnegative_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _leading_integer(value: str | None) -> int | None:
    if not value:
        return None
    digits = []
    for character in value.strip():
        if not character.isdigit():
            break
        digits.append(character)
    return int("".join(digits)) if digits else None


def _validate_slurm_context(
    *,
    cpu_workers: int,
    gpu_workers: int,
    allow_non_slurm: bool,
) -> dict[str, Any]:
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id and not allow_non_slurm:
        raise RuntimeError(
            "This smoke test must run inside a Slurm allocation. Submit it with "
            "deploy/hpc/submit_multi_worker_smoke.sh; use --allow-non-slurm only "
            "for a local CPU-only developer test."
        )
    if allow_non_slurm and gpu_workers:
        raise ValueError("--allow-non-slurm supports CPU-only developer tests.")

    allocated_nodes = _leading_integer(
        os.environ.get("SLURM_JOB_NUM_NODES") or os.environ.get("SLURM_NNODES")
    )
    if allocated_nodes is not None and allocated_nodes != 1:
        raise RuntimeError(
            f"Expected exactly one allocated compute node, got {allocated_nodes}."
        )

    allocated_cpus = _leading_integer(
        os.environ.get("SLURM_CPUS_PER_TASK")
        or os.environ.get("SLURM_CPUS_ON_NODE")
        or os.environ.get("SLURM_JOB_CPUS_PER_NODE")
    )
    required_cpus = cpu_workers + gpu_workers
    if job_id and allocated_cpus is not None and allocated_cpus < required_cpus:
        raise RuntimeError(
            f"Slurm allocated {allocated_cpus} CPU(s), but {required_cpus} Worker "
            "processes were requested."
        )

    visible_devices = tuple(
        item.strip()
        for item in os.environ.get("CUDA_VISIBLE_DEVICES", "").split(",")
        if item.strip() and item.strip() != "-1"
    )
    if gpu_workers and len(visible_devices) != gpu_workers:
        raise RuntimeError(
            f"Slurm job requested {gpu_workers} GPU Worker(s), but the allocation "
            f"exposes {len(visible_devices)} CUDA device(s): {visible_devices!r}."
        )

    return {
        "validated": bool(job_id),
        "jobId": job_id,
        "jobName": os.environ.get("SLURM_JOB_NAME"),
        "nodeList": os.environ.get("SLURM_JOB_NODELIST"),
        "allocatedNodes": allocated_nodes,
        "allocatedCpus": allocated_cpus,
        "cudaVisibleDevices": list(visible_devices),
    }


def _write_result(path: Path, payload: dict[str, Any]) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, destination)


def _run_smoke(
    *,
    cpu_workers: int,
    gpu_workers: int,
    task_timeout: float,
    allow_non_slurm: bool,
) -> dict[str, Any]:
    if cpu_workers + gpu_workers < 2:
        raise ValueError(
            "A multi-Worker smoke test requires at least two total Workers."
        )
    slurm = _validate_slurm_context(
        cpu_workers=cpu_workers,
        gpu_workers=gpu_workers,
        allow_non_slurm=allow_non_slurm,
    )

    from services.dask_service import (
        CPU_RESOURCE_NAME as runtime_cpu_resource,
        GPU_RESOURCE_NAME as runtime_gpu_resource,
        dask_service,
        get_fresh_scheduler_info,
    )
    from core.workflow_resources import (
        WorkflowResourcePlan,
        validate_workflow_resource_plan,
    )

    if runtime_cpu_resource != CPU_RESOURCE_NAME:
        raise RuntimeError("Unexpected runtime CPU resource name.")
    if runtime_gpu_resource != GPU_RESOURCE_NAME:
        raise RuntimeError("Unexpected runtime GPU resource name.")

    started = time.monotonic()
    client = None
    summary: dict[str, Any] | None = None
    try:
        client = dask_service.start_cluster(
            cpu_workers=cpu_workers,
            gpu_workers=gpu_workers,
        )
        scheduler_info = get_fresh_scheduler_info(client, timeout=task_timeout)
        worker_info = dict(scheduler_info.get("workers", {}))
        expected_total = cpu_workers + gpu_workers
        if len(worker_info) != expected_total:
            raise RuntimeError(
                f"Scheduler registered {len(worker_info)} Workers; "
                f"expected {expected_total}."
            )

        worker_addresses = tuple(sorted(str(address) for address in worker_info))
        if len(set(worker_addresses)) != expected_total:
            raise RuntimeError("Dask Worker addresses are not unique.")
        if not all(
            address.startswith("tcp://127.0.0.1:") for address in worker_addresses
        ):
            raise RuntimeError(
                f"One or more same-node Worker addresses are not loopback: "
                f"{worker_addresses!r}."
            )

        active_gpu_ids = tuple(dask_service.active_gpu_ids)
        if len(active_gpu_ids) != gpu_workers or len(set(active_gpu_ids)) != gpu_workers:
            raise RuntimeError(
                f"Runtime selected invalid GPU identities: {active_gpu_ids!r}."
            )
        topology = dask_service.validate_cluster_topology(
            expected_cpu_workers=cpu_workers,
            expected_gpu_workers=gpu_workers,
            expected_gpu_ids=active_gpu_ids,
            client=client,
            rpc_timeout=task_timeout,
        )
        resource_plan = WorkflowResourcePlan(
            nodes=(),
            requires_cpu=cpu_workers > 0,
            requires_gpu=gpu_workers > 0,
            cpu_node_ids=(),
            gpu_node_ids=(),
            cpu_workers=cpu_workers,
            gpu_workers=gpu_workers,
        )
        validate_workflow_resource_plan(resource_plan, topology)

        placements: list[tuple[str, str]] = []
        for address in worker_addresses:
            resources = dict(worker_info[address].get("resources", {}) or {})
            if float(resources.get(CPU_RESOURCE_NAME, 0) or 0) == 1.0:
                placements.append((address, "cpu"))
            elif float(resources.get(GPU_RESOURCE_NAME, 0) or 0) == 1.0:
                placements.append((address, "gpu"))
            else:
                raise RuntimeError(
                    f"Worker {address} has no unambiguous CPU/GPU role: {resources!r}."
                )

        futures = []
        for ordinal, (address, role) in enumerate(placements):
            resource_name = CPU_RESOURCE_NAME if role == "cpu" else GPU_RESOURCE_NAME
            futures.append(
                (
                    address,
                    client.submit(
                        _worker_computation,
                        ordinal=ordinal,
                        expected_role=role,
                        validate_cuda=(role == "gpu"),
                        workers=[address],
                        allow_other_workers=False,
                        resources={resource_name: 1},
                        pure=False,
                    ),
                )
            )

        task_results = []
        for expected_address, future in futures:
            result = future.result(timeout=task_timeout)
            if result["workerAddress"] != expected_address:
                raise RuntimeError(
                    f"Pinned task ran on {result['workerAddress']}, "
                    f"expected {expected_address}."
                )
            task_results.append(result)

        observed_hosts = {str(result["host"]) for result in task_results}
        if observed_hosts != {platform.node()}:
            raise RuntimeError(
                f"Workers did not all run on the allocated compute node: "
                f"driver={platform.node()!r}, workers={sorted(observed_hosts)!r}."
            )
        observed_pids = {int(result["pid"]) for result in task_results}
        if len(observed_pids) != expected_total:
            raise RuntimeError(
                "Each Dask Worker must be a distinct process; observed PIDs "
                f"were {sorted(observed_pids)!r}."
            )
        if {str(result["workerAddress"]) for result in task_results} != set(
            worker_addresses
        ):
            raise RuntimeError("Pinned computations did not cover every Worker.")

        cpu_results = [result for result in task_results if result["role"] == "cpu"]
        gpu_results = [result for result in task_results if result["role"] == "gpu"]
        if len(cpu_results) != cpu_workers or len(gpu_results) != gpu_workers:
            raise RuntimeError("Observed task roles do not match the requested topology.")
        if any(result["cudaVisibleDevices"] for result in cpu_results):
            raise RuntimeError("A CPU Worker unexpectedly retained CUDA visibility.")
        if any(
            result["physicalGpuId"] != result["cudaVisibleDevices"]
            for result in gpu_results
        ):
            raise RuntimeError("A GPU Worker CUDA mask does not match its assigned GPU.")
        if any(result["cuda"] is None for result in gpu_results):
            raise RuntimeError("A GPU Worker did not execute the CUDA validation kernel.")

        summary = {
            "status": "passed",
            "host": platform.node(),
            "hostFqdn": socket.getfqdn(),
            "requested": {
                "cpuWorkers": cpu_workers,
                "gpuWorkers": gpu_workers,
            },
            "resourcePlan": resource_plan.to_preflight_dict(),
            "schedulerAddress": str(scheduler_info.get("address", "")),
            "workerAddresses": list(worker_addresses),
            "workerProcessIds": sorted(observed_pids),
            "validatedCpuWorkers": len(topology.cpu_workers),
            "validatedGpuWorkers": len(topology.gpu_workers),
            "cudaComputeValidated": bool(gpu_workers),
            "tasks": task_results,
            "slurm": slurm,
            "elapsedSecondsBeforeShutdown": round(time.monotonic() - started, 3),
        }
    finally:
        if dask_service.client is not None or dask_service.cluster is not None:
            graceful = dask_service.stop_cluster()
            if not graceful:
                raise RuntimeError(
                    "Worker computations passed, but Dask required emergency cleanup."
                )

    assert summary is not None
    summary["clusterShutdown"] = "graceful"
    summary["elapsedSeconds"] = round(time.monotonic() - started, 3)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Start a real multi-process WorkFlow Dask cluster inside one Slurm "
            "compute-node allocation and run one pinned computation per Worker."
        )
    )
    parser.add_argument("--cpu-workers", type=_nonnegative_integer, required=True)
    parser.add_argument("--gpu-workers", type=_nonnegative_integer, required=True)
    parser.add_argument("--result-file", type=Path, required=True)
    parser.add_argument("--task-timeout", type=_positive_float, default=180.0)
    parser.add_argument(
        "--allow-non-slurm",
        action="store_true",
        help="Permit a CPU-only local developer test (never validates CUDA/Slurm).",
    )
    args = parser.parse_args()

    summary = _run_smoke(
        cpu_workers=args.cpu_workers,
        gpu_workers=args.gpu_workers,
        task_timeout=args.task_timeout,
        allow_non_slurm=args.allow_non_slurm,
    )
    _write_result(args.result_file, summary)
    print("__WORKFLOW_MULTI_WORKER_RESULT__", flush=True)
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    main()
