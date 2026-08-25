from __future__ import annotations

import subprocess
from types import SimpleNamespace

import distributed

from core.cluster_inventory import (
    ClusterInventory,
    ClusterInventoryService,
    parse_scontrol_show_node,
    parse_sinfo_partitions,
)
from core.resource_planner import plan_workflow_resources
from core.worker_pool import WorkerPool
from core.worker_profiles import (
    CPU_GENERAL_PROFILE,
    PhysicalResources,
    WorkerProfile,
    resolve_worker_profile,
    worker_logical_resources,
)
from nodes.base.block_map import BlockContextFactory
from services.dask_service import (
    build_local_profile_cluster_specs,
    cluster_resource_summary_from_scheduler_info,
)
from services.executor import _resolve_max_in_flight_windows
from services.slurm_execution_service import (
    _worker_job_launcher_plan,
    _worker_job_request,
    slurm_policy_from_environment,
)
from core.workflow_resources import (
    build_workflow_resource_plan,
)


class CpuNode:
    DISPLAY_NAME = "CPU"
    required_worker_profile = "cpu-reader"


class GpuNode:
    DISPLAY_NAME = "GPU"
    required_worker_profile = "gpu-cellpose"


class DefaultProfileNode:
    pass


def test_node_without_profile_uses_cpu_general_default() -> None:
    assert resolve_worker_profile(DefaultProfileNode) == CPU_GENERAL_PROFILE


def test_workflow_plan_counts_reachable_worker_profiles() -> None:
    graph = {
        "cpu-a": {"type": "Cpu", "inputs": {}},
        "gpu": {"type": "Gpu", "inputs": {"image": ["cpu-a", 0]}},
        "cpu-b": {"type": "Cpu", "inputs": {"mask": ["gpu", 0]}},
        "unreachable": {"type": "Gpu", "inputs": {}},
    }

    plan = build_workflow_resource_plan(
        graph,
        ["cpu-b"],
        node_mappings={"Cpu": CpuNode, "Gpu": GpuNode},
    )

    assert plan.required_worker_profiles == {
        "cpu-reader": 2,
        "gpu-cellpose": 1,
    }
    preflight = plan.to_preflight_dict()
    assert preflight["requiredWorkerProfiles"] == plan.required_worker_profiles
    assert "cpuWorkers" not in preflight
    assert "gpuWorkers" not in preflight


def test_inventory_parses_gpu_from_gres_not_partition() -> None:
    inventory = parse_scontrol_show_node(
        "NodeName=c001 CPUTot=96 RealMemory=1024000 "
        "Gres=gpu:a100:4(S:0-3),gpu:a40:2(S:4-5) "
        "State=IDLE Partitions=compute\n"
        "NodeName=c002 CPUTot=64 RealMemory=512000 Gres=(null) "
        "State=IDLE Partitions=gpu\n"
    )
    assert inventory.nodes[0].gpu == 6
    assert inventory.nodes[1].gpu == 0


def test_sinfo_discovers_all_partitions_and_default_without_selecting_it() -> None:
    partitions, default_partition = parse_sinfo_partitions(
        "gpu\ncompute*\ncontrol\ntao\n"
    )
    assert partitions == ("gpu", "compute", "control", "tao")
    assert default_partition == "compute"


def test_inventory_service_queries_sinfo_before_node_resources() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(argv, **_kwargs):
        calls.append(tuple(argv))
        if argv[0] == "sinfo-test":
            return subprocess.CompletedProcess(argv, 0, "gpu\ncompute*\ntao\n", "")
        return subprocess.CompletedProcess(
            argv,
            0,
            "NodeName=c001 CPUTot=96 RealMemory=1028000 "
            "Gres=gpu:4 State=IDLE Partitions=compute\n",
            "",
        )

    inventory = ClusterInventoryService(
        sinfo_executable="sinfo-test",
        scontrol_executable="scontrol-test",
        command_runner=runner,
    ).load()

    assert [call[0] for call in calls] == ["sinfo-test", "scontrol-test"]
    assert inventory.partition_names == ("gpu", "compute", "tao")
    assert inventory.default_partition == "compute"


def test_planner_uses_multiple_discovered_partitions_but_excludes_control() -> None:
    workflow = build_workflow_resource_plan(
        {"gpu": {"type": "Gpu", "inputs": {}}},
        ["gpu"],
        node_mappings={"Gpu": GpuNode},
    )
    profile = WorkerProfile(
        name="gpu-cellpose",
        physical_resources=PhysicalResources(cpu=4, memory_gib=32, gpu=1),
        logical_resources={"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
        threads=1,
    )
    inventory_nodes = parse_scontrol_show_node(
        "NodeName=aio CPUTot=80 RealMemory=1031000 Gres=gpu:4 State=IDLE Partitions=gpu\n"
        "NodeName=c001 CPUTot=96 RealMemory=1028000 Gres=gpu:4 State=IDLE Partitions=compute\n"
        "NodeName=mn02 CPUTot=80 RealMemory=1031000 Gres=gpu:8 State=IDLE Partitions=control\n"
        "NodeName=t001 CPUTot=40 RealMemory=500000 Gres=gpu:2 State=IDLE Partitions=tao\n"
        "NodeName=t002 CPUTot=40 RealMemory=500000 Gres=gpu:2 State=DOWN Partitions=tao\n"
    )
    inventory = ClusterInventory(
        nodes=inventory_nodes.nodes,
        partitions=("gpu", "compute", "control", "tao"),
        default_partition="compute",
    )
    policy = slurm_policy_from_environment({})
    partitions = policy.resolve_partitions(inventory.partition_names)
    assert partitions == ("gpu", "compute", "tao")

    allocation = plan_workflow_resources(
        workflow,
        [profile],
        [WorkerPool(profile="gpu-cellpose", processes=1, scale=10)],
        inventory,
        partitions=partitions,
        time_limit="01:00:00",
    )

    assert set(allocation.partitions) == {"gpu", "compute", "tao"}
    assert {node.node for node in allocation.nodes} == {"aio", "c001", "t001"}
    assert all(job.partition != "control" for job in allocation.jobs)
    assert all(
        _worker_job_request(allocation, job).partition == job.partition
        for job in allocation.jobs
    )


def test_planner_places_eight_gpu_jobs_on_two_real_nodes() -> None:
    plan = build_workflow_resource_plan(
        {"gpu": {"type": "Gpu", "inputs": {}}},
        ["gpu"],
        node_mappings={"Gpu": GpuNode},
    )
    profile = WorkerProfile(
        name="gpu-cellpose",
        physical_resources=PhysicalResources(cpu=4, memory_gib=32, gpu=1),
        logical_resources={"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
        threads=1,
    )
    pool = WorkerPool(profile="gpu-cellpose", processes=1, scale=8)
    inventory = parse_scontrol_show_node(
        "NodeName=c001 CPUTot=96 RealMemory=1024000 Gres=gpu:4 State=IDLE Partitions=compute\n"
        "NodeName=c003 CPUTot=96 RealMemory=1024000 Gres=gpu:4 State=IDLE Partitions=compute\n"
    )
    allocation = plan_workflow_resources(
        plan,
        [profile],
        [pool],
        inventory,
        partition="compute",
        time_limit="01:00:00",
    )
    assert allocation.worker_counts == {"gpu-cellpose": 8}
    assert {node.node: node.workers for node in allocation.nodes} == {
        "c001": {"gpu-cellpose": 4},
        "c003": {"gpu-cellpose": 4},
    }
    assert len(allocation.jobs) == 8
    assert all(job.workers == 1 and job.gpu == 1 for job in allocation.jobs)
    requests = [_worker_job_request(allocation, job) for job in allocation.jobs]
    assert all(request.nodes == 1 and request.gpus == 1 for request in requests)
    assert [request.node_names[0] for request in requests].count("c001") == 4
    assert [request.node_names[0] for request in requests].count("c003") == 4
    first_launcher_plan = _worker_job_launcher_plan(allocation, allocation.jobs[0])
    assert len(first_launcher_plan["jobs"]) == 1
    assert first_launcher_plan["totalWorkers"] == 1


def test_cpu_pool_scale_multiplies_processes_without_changing_requirements() -> None:
    plan = build_workflow_resource_plan(
        {"reader": {"type": "Cpu", "inputs": {}}},
        ["reader"],
        node_mappings={"Cpu": CpuNode},
    )
    profile = WorkerProfile(
        name="cpu-reader",
        physical_resources=PhysicalResources(cpu=8, memory_gib=32, gpu=0),
        logical_resources={"cpu-reader": 1, "CPU": 8},
        threads=8,
    )
    pool = WorkerPool(profile="cpu-reader", processes=4, scale=5)
    inventory = parse_scontrol_show_node(
        "NodeName=c001 CPUTot=160 RealMemory=1048576 Gres=(null) "
        "State=IDLE Partitions=compute\n"
    )
    allocation = plan_workflow_resources(
        plan,
        [profile],
        [pool],
        inventory,
        partition="compute",
        time_limit="01:00:00",
    )
    assert allocation.required_worker_profiles == {"cpu-reader": 1}
    assert allocation.worker_counts == {"cpu-reader": 20}
    assert len(allocation.jobs) == 5
    assert all(job.workers == 4 and job.cpu == 32 for job in allocation.jobs)


def test_gpu_pool_rejects_multiple_processes_per_job() -> None:
    profile = WorkerProfile(
        name="gpu-cellpose",
        physical_resources=PhysicalResources(cpu=4, memory_gib=32, gpu=1),
        logical_resources={"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
        threads=1,
    )
    pool = WorkerPool(profile="gpu-cellpose", processes=2, scale=1)
    try:
        pool.validate_profile(profile)
    except ValueError as exc:
        assert "processes=1" in str(exc)
    else:
        raise AssertionError("GPU Worker Pools must reject processes > 1")


def test_local_worker_specs_use_profile_pool_counts_and_capabilities(tmp_path) -> None:
    reader = WorkerProfile(
        name="cpu-reader",
        physical_resources=PhysicalResources(cpu=8, memory_gib=32, gpu=0),
        logical_resources={"cpu-reader": 1, "CPU": 8},
        threads=8,
    )
    cellpose = WorkerProfile(
        name="gpu-cellpose",
        physical_resources=PhysicalResources(cpu=4, memory_gib=32, gpu=1),
        logical_resources={"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
        threads=1,
    )
    _, specs = build_local_profile_cluster_specs(
        profiles={reader.name: reader, cellpose.name: cellpose},
        pools={
            reader.name: WorkerPool(profile=reader.name, processes=4, scale=5),
            cellpose.name: WorkerPool(profile=cellpose.name, processes=1, scale=2),
        },
        gpu_ids=("0", "1"),
        local_directory=str(tmp_path),
        dashboard_address=":0",
        worker_start_timeout=30,
    )
    assert len(specs) == 22
    assert sum(
        spec["options"]["resources"].get("cpu-reader", 0) == 1
        for spec in specs.values()
    ) == 20
    gpu_specs = [
        spec for spec in specs.values()
        if spec["options"]["resources"].get("gpu-cellpose") == 1
    ]
    assert [spec["options"]["env"]["CUDA_VISIBLE_DEVICES"] for spec in gpu_specs] == ["0", "1"]


def test_worker_resources_use_distributed_2026_state_api() -> None:
    worker = SimpleNamespace(
        state=SimpleNamespace(total_resources={
            "gpu-cellpose": 1,
            "GPU": 1,
            "CPU": 4,
        }),
        resources={},
    )

    assert worker_logical_resources(worker) == {
        "gpu-cellpose": 1.0,
        "GPU": 1.0,
        "CPU": 4.0,
    }


def test_block_runtime_accepts_profile_from_worker_total_resources(monkeypatch) -> None:
    worker = SimpleNamespace(
        name="gpu-cellpose-0",
        state=SimpleNamespace(total_resources={
            "gpu-cellpose": 1,
            "GPU": 1,
            "CPU": 4,
        }),
        resources={},
        worker_role="gpu",
        assigned_gpu="cuda:0",
    )
    monkeypatch.setattr(distributed, "get_worker", lambda: worker)

    assert BlockContextFactory().resolve_device_hint("gpu-cellpose") == "cuda:0"


def test_profile_worker_role_counts_are_disjoint() -> None:
    summary = cluster_resource_summary_from_scheduler_info({
        "address": "tcp://127.0.0.1:8786",
        "workers": {
            "tcp://127.0.0.1:1": {
                "resources": {"cpu-reader": 1, "CPU": 8},
            },
            "tcp://127.0.0.1:2": {
                "resources": {"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
            },
        },
    })

    assert summary.cpu_workers == ("tcp://127.0.0.1:1",)
    assert summary.gpu_workers == ("tcp://127.0.0.1:2",)
    assert summary.total_cpu_slots == 12
    assert summary.worker_profile_slots == {
        "cpu-reader": 1,
        "gpu-cellpose": 1,
    }


def test_window_concurrency_defaults_to_one() -> None:
    assert _resolve_max_in_flight_windows(
        None,
        resource_plan=object(),
        cluster_summary=object(),
    ) == 1
