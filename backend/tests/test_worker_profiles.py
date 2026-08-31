from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import distributed
import pytest
from dask_jobqueue import SLURMCluster
from distributed.security import Security

from core.cluster_inventory import (
    ClusterInventory,
    ClusterInventoryService,
    parse_scontrol_show_node,
    parse_sinfo_partitions,
    parse_sinfo_nodes,
)
from core.resource_planner import ResourcePlanningError, plan_workflow_resources
from core.platform import rewrite_dashboard_url
from core.worker_pool import WorkerPool
from core.worker_profiles import (
    CPU_GENERAL_PROFILE,
    PhysicalResources,
    WorkerProfile,
    resolve_worker_profile,
    worker_logical_resources,
)
from core.worker_ownership import (
    execution_ownership_resource,
    submission_ownership_resource,
)
from nodes.base.block_map import BlockContextFactory
from services.dask_service import (
    DaskService,
    build_local_profile_cluster_specs,
    cluster_resource_summary_from_scheduler_info,
    validate_external_worker_ownership,
)
from services.executor import _resolve_max_in_flight_windows
from services.slurm_execution_service import (
    SlurmExecutionService,
    _loopback_dashboard_address,
    _worker_job_request,
    slurm_policy_from_environment,
)
from services.slurm_jobqueue_cluster import (
    PlannedSLURMCluster,
    PlannedSLURMJob,
    build_planned_slurm_worker_spec,
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


class BananaNode:
    required_worker_profile = "banana"


class AppleNode:
    required_worker_profile = "apple"


class OrangeNode:
    required_worker_profile = "orange"


def test_slurm_dashboard_must_remain_on_service_node_loopback() -> None:
    assert _loopback_dashboard_address(None) == "127.0.0.1:8787"
    assert _loopback_dashboard_address("127.0.0.1:18787") == "127.0.0.1:18787"
    for unsafe in ("0.0.0.0:8787", "mn02:8787", "127.0.0.1:80"):
        try:
            _loopback_dashboard_address(unsafe)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Unsafe Dashboard binding was accepted: {unsafe}")


def test_dashboard_browser_host_override_preserves_dask_status_path() -> None:
    assert rewrite_dashboard_url(
        "http://127.0.0.1:8787/status",
        "127.0.0.1:18787",
    ) == "http://127.0.0.1:18787/status"


def test_node_without_profile_uses_cpu_general_default() -> None:
    assert resolve_worker_profile(DefaultProfileNode) == CPU_GENERAL_PROFILE


def test_legacy_profile_threads_migrate_to_slurmcluster_cpu_contract() -> None:
    profile = WorkerProfile.from_dict({
        "name": "gpu-cellpose",
        "physical_resources": {"cpu": 4, "memory": "32GB", "gpu": 1},
        "logical_resources": {"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
        "capabilities": ["gpu-cellpose"],
        "threads": 1,
    })
    assert profile.threads == 4


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
        "NodeName=c001 CPUTot=96 CPUAlloc=16 RealMemory=1024000 AllocMem=32768 "
        "Gres=gpu:a100:4(S:0-3),gpu:a40:2(S:4-5) "
        "AllocTRES=cpu=16,mem=32G,gres/gpu=2 "
        "State=IDLE Partitions=compute\n"
        "NodeName=c002 CPUTot=64 RealMemory=512000 Gres=(null) "
        "State=IDLE Partitions=gpu\n"
    )
    assert inventory.nodes[0].cpu == 80
    assert inventory.nodes[0].memory_mib == 991232
    assert inventory.nodes[0].gpu == 4
    assert inventory.nodes[0].gpu_total == 6
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
            return subprocess.CompletedProcess(
                argv,
                0,
                "c001|compute*|16/80/0/96|MIXED\n"
                "c001|tao|16/80/0/96|MIXED\n",
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            "NodeName=c001 CPUTot=96 CPUAlloc=16 RealMemory=1028000 AllocMem=32768 "
            "Gres=gpu:4 AllocTRES=cpu=16,mem=32G,gres/gpu=1 "
            "State=MIXED Partitions=compute,tao\n",
            "",
        )

    inventory = ClusterInventoryService(
        sinfo_executable="sinfo-test",
        scontrol_executable="scontrol-test",
        command_runner=runner,
    ).load()

    assert [call[0] for call in calls] == ["sinfo-test", "scontrol-test"]
    assert calls[0][1:] == ("--Node", "--noheader", "--format=%N|%P|%C|%T")
    assert inventory.partition_names == ("compute", "tao")
    assert inventory.default_partition == "compute"
    assert inventory.nodes[0].cpu == 80
    assert inventory.nodes[0].gpu == 3
    assert inventory.nodes[0].partitions == ("compute", "tao")


def test_sinfo_node_snapshot_tracks_each_partition_and_idle_cpu() -> None:
    nodes, partitions, default_partition = parse_sinfo_nodes(
        "t001|tao*|20/76/0/96|MIXED\n"
        "t001|compute|20/76/0/96|MIXED\n"
        "mn02|mn|2/78/0/80|MIXED\n"
    )
    assert partitions == ("tao", "compute", "mn")
    assert default_partition == "tao"
    assert nodes[0].partitions == ("tao", "compute")
    assert nodes[0].cpu_idle == 76


def test_planner_uses_multiple_discovered_partitions_but_excludes_management() -> None:
    workflow = build_workflow_resource_plan(
        {"gpu": {"type": "Gpu", "inputs": {}}},
        ["gpu"],
        node_mappings={"Gpu": GpuNode},
    )
    profile = WorkerProfile(
        name="gpu-cellpose",
        physical_resources=PhysicalResources(cpu=4, memory_gib=32, gpu=1),
        logical_resources={"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
        threads=4,
    )
    inventory_nodes = parse_scontrol_show_node(
        "NodeName=aio CPUTot=80 RealMemory=1031000 Gres=gpu:4 State=IDLE Partitions=gpu\n"
        "NodeName=c001 CPUTot=96 RealMemory=1028000 Gres=gpu:4 State=IDLE Partitions=compute\n"
        "NodeName=mn02 CPUTot=80 RealMemory=1031000 Gres=gpu:8 State=IDLE Partitions=mn\n"
        "NodeName=mn01 CPUTot=80 RealMemory=1031000 Gres=gpu:8 State=IDLE Partitions=control\n"
        "NodeName=t001 CPUTot=40 RealMemory=500000 Gres=gpu:2 State=IDLE Partitions=tao\n"
        "NodeName=t002 CPUTot=40 RealMemory=500000 Gres=gpu:2 State=DOWN Partitions=tao\n"
    )
    inventory = ClusterInventory(
        nodes=inventory_nodes.nodes,
        partitions=("gpu", "compute", "mn", "control", "tao"),
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
    assert all(job.partition not in {"mn", "control"} for job in allocation.jobs)
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
        threads=4,
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


def test_planned_slurm_job_uses_jobqueue_with_exact_planner_directives(
    tmp_path: Path,
) -> None:
    workflow = build_workflow_resource_plan(
        {"gpu": {"type": "Gpu", "inputs": {}}},
        ["gpu"],
        node_mappings={"Gpu": GpuNode},
    )
    profile = WorkerProfile(
        name="gpu-cellpose",
        physical_resources=PhysicalResources(cpu=4, memory_gib=32, gpu=1),
        logical_resources={"gpu-cellpose": 1, "CPU": 4, "GPU": 1},
        threads=4,
    )
    inventory = parse_scontrol_show_node(
        "NodeName=t001 CPUTot=40 RealMemory=385349 "
        "Gres=gpu:2 State=IDLE Partitions=tao\n"
    )
    allocation = plan_workflow_resources(
        workflow,
        [profile],
        [WorkerPool(profile="gpu-cellpose", processes=1, scale=1)],
        inventory,
        partition="tao",
        time_limit="01:00:00",
    )
    spec = build_planned_slurm_worker_spec(
        allocation,
        allocation.jobs[0],
        execution_id="12345678-1234-1234-1234-123456789abc",
        submission_token="wf:abcdef:1",
        project_root=tmp_path,
        runtime_directory=tmp_path,
        run_directory=tmp_path,
        python_executable=Path(sys.executable),
        sbatch_executable="/usr/bin/sbatch",
        scancel_executable="/usr/bin/scancel",
        interface=None,
        protocol="tcp://",
        security=Security(),
        worker_port_range="20000:20100",
        nanny_port_range="20101:20200",
    )
    job = PlannedSLURMJob(
        "tcp://mn02:8786",
        name=spec.allocation_id,
        **spec.options,
    )
    script = job.job_script()

    assert issubclass(PlannedSLURMCluster, SLURMCluster)
    assert "#SBATCH -p tao" in script
    assert "#SBATCH --nodelist=t001" in script
    assert "#SBATCH --gres=gpu:1" in script
    assert "#SBATCH --comment=wf:abcdef:1" in script
    assert "distributed.cli.dask_worker" in script
    assert "--nthreads 4" in script
    assert "--nanny" in script
    assert "services.slurm_worker_preload" in script
    assert "slurm_worker_launcher" not in script
    assert "workflow_workers.sbatch" not in script
    assert "--tls-" not in script
    assert "--tls-ca-file None" not in script
    assert execution_ownership_resource(
        "12345678-1234-1234-1234-123456789abc"
    ) in script
    assert submission_ownership_resource("wf:abcdef:1") in script
    assert "wf:abcdef:1=1" not in script


def test_slurmcluster_derives_worker_threads_from_cores_and_processes(
    tmp_path: Path,
) -> None:
    workflow = build_workflow_resource_plan(
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
    allocation = plan_workflow_resources(
        workflow,
        [profile],
        [WorkerPool(profile="cpu-reader", processes=4, scale=1)],
        parse_scontrol_show_node(
            "NodeName=t001 CPUTot=40 RealMemory=500000 Gres=(null) "
            "State=IDLE Partitions=tao\n"
        ),
        partition="tao",
        time_limit="01:00:00",
    )
    spec = build_planned_slurm_worker_spec(
        allocation,
        allocation.jobs[0],
        execution_id="12345678-1234-1234-1234-123456789abc",
        submission_token="wf:abcdef:1",
        project_root=tmp_path,
        runtime_directory=tmp_path,
        run_directory=tmp_path,
        python_executable=Path(sys.executable),
        sbatch_executable="/usr/bin/sbatch",
        scancel_executable="/usr/bin/scancel",
        interface=None,
        protocol="tcp://",
        security=None,
        worker_port_range="20000:20100",
        nanny_port_range="20101:20200",
    )
    assert spec.options["cores"] == 32
    assert spec.options["processes"] == 4
    script = PlannedSLURMJob(
        "tcp://mn02:8786",
        name=spec.allocation_id,
        **spec.options,
    ).job_script()
    assert "--nthreads 8" in script
    assert "--nworkers 4" in script


def test_unplaceable_profile_rejects_complete_plan_before_any_submission() -> None:
    workflow = build_workflow_resource_plan(
        {
            "banana": {"type": "Banana", "inputs": {}},
            "apple": {"type": "Apple", "inputs": {}},
            "orange": {"type": "Orange", "inputs": {}},
        },
        ["banana", "apple", "orange"],
        node_mappings={
            "Banana": BananaNode,
            "Apple": AppleNode,
            "Orange": OrangeNode,
        },
    )
    profiles = [
        WorkerProfile(
            name="banana",
            physical_resources=PhysicalResources(cpu=20, memory_gib=8, gpu=0),
            logical_resources={"banana": 1, "CPU": 20},
            threads=20,
        ),
        WorkerProfile(
            name="apple",
            physical_resources=PhysicalResources(cpu=10, memory_gib=8, gpu=0),
            logical_resources={"apple": 1, "CPU": 10},
            threads=10,
        ),
        WorkerProfile(
            name="orange",
            physical_resources=PhysicalResources(cpu=4, memory_gib=8, gpu=1),
            logical_resources={"orange": 1, "CPU": 4, "GPU": 1},
            threads=4,
        ),
    ]
    pools = [
        WorkerPool(
            profile=item.name,
            processes=1,
            scale=2 if item.name == "orange" else 1,
        )
        for item in profiles
    ]
    inventory = parse_scontrol_show_node(
        "NodeName=t001 CPUTot=30 RealMemory=262144 Gres=(null) "
        "State=IDLE Partitions=tao\n"
    )

    with pytest.raises(ResourcePlanningError, match="orange"):
        plan_workflow_resources(
            workflow,
            profiles,
            pools,
            inventory,
            partitions=("tao",),
            time_limit="01:00:00",
        )

    execution_source = inspect.getsource(SlurmExecutionService._execute_graph_impl)
    assert execution_source.index("_plan_slurm_allocation") < execution_source.index(
        "start_slurm_jobqueue_scheduler"
    )


def test_active_slurm_execution_submits_workers_through_slurmcluster() -> None:
    source = inspect.getsource(SlurmExecutionService._execute_graph_impl)
    assert "start_slurm_jobqueue_scheduler" in source
    assert "submit_slurm_jobqueue_workers" in source
    assert "build_sbatch_argv" not in source
    assert source.index('"type": "dashboard_ready"') < source.index(
        "activate_external_worker_profiles"
    )
    profile_activation = inspect.signature(
        DaskService.activate_external_worker_profiles
    ).parameters
    assert "submission_tokens" in profile_activation


def test_worker_registration_detects_an_exited_slurm_job_immediately(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service = SlurmExecutionService()

    async def missing_from_queue(*_args, **_kwargs):
        return True, None

    async def failed_in_controller(*_args, **_kwargs):
        return True, ("FAILED", "127:0", "t001")

    monkeypatch.setattr(service, "_query_queue_state", missing_from_queue)
    monkeypatch.setattr(service, "_query_terminal_state", failed_in_controller)
    request = SimpleNamespace()

    with pytest.raises(
        RuntimeError,
        match=r"57093.*FAILED.*127:0.*57093\.err",
    ):
        asyncio.run(service._assert_worker_allocations_alive(
            config=SimpleNamespace(),
            submitted_jobs=(("57093", None, "wf:test:1", request),),
            run_directory=tmp_path,
        ))


def test_legacy_squeue_invalid_job_id_is_an_authoritative_absence(
    monkeypatch,
) -> None:
    service = SlurmExecutionService()
    monkeypatch.setattr(service, "_run_command", lambda *_args, **_kwargs: (
        subprocess.CompletedProcess(
            args=("squeue",),
            returncode=1,
            stdout="",
            stderr="slurm_load_jobs error: Invalid job id specified\n",
        )
    ))

    result = asyncio.run(service._query_queue_state(
        SimpleNamespace(squeue_executable="squeue"),
        "57093",
    ))

    assert result == (True, None)


def test_worker_cleanup_accepts_confirmed_purged_legacy_job(
    monkeypatch,
) -> None:
    service = SlurmExecutionService()

    async def no_terminal_record(*_args, **_kwargs):
        return False, None

    async def absent_from_queue(*_args, **_kwargs):
        return True, None

    async def cancel_already_absent(*_args, **_kwargs):
        return None

    monkeypatch.setattr(service, "_query_terminal_state", no_terminal_record)
    monkeypatch.setattr(service, "_query_queue_state", absent_from_queue)
    monkeypatch.setattr(service, "_send_cancel", cancel_already_absent)

    terminal = asyncio.run(service._wait_for_worker_allocation_terminal(
        config=SimpleNamespace(
            poll_interval_seconds=0.001,
            result_grace_seconds=0.002,
        ),
        execution_id="12345678-1234-1234-1234-123456789abc",
        job_id="57093",
        cluster=None,
        submission_token="wf:test:1",
    ))

    assert terminal == ("PURGED", "", "")


def test_planned_slurmcluster_owns_submit_and_scale_down_lifecycle(
    monkeypatch,
) -> None:
    submitted = 0
    cancelled: list[str] = []

    async def fake_submit(self, _script_filename):
        nonlocal submitted
        submitted += 1
        return f"Submitted batch job {55000 + submitted}"

    async def fake_close(job_id, _cancel_command):
        cancelled.append(str(job_id))

    monkeypatch.setattr(PlannedSLURMJob, "_submit_job", fake_submit)
    monkeypatch.setattr(PlannedSLURMJob, "_close_job", staticmethod(fake_close))
    cluster = PlannedSLURMCluster(
        n_workers=0,
        queue="compute",
        cores=4,
        memory="4GiB",
        processes=1,
        nanny=True,
        walltime="01:00:00",
        worker_extra_args=[],
        scheduler_options={
            "host": "127.0.0.1",
            "port": 0,
            "dashboard": False,
            "dashboard_address": None,
        },
        asynchronous=False,
        name="WorkFlow-SLURMCluster-test",
    )
    try:
        from services.slurm_jobqueue_cluster import PlannedSlurmWorkerSpec

        spec = PlannedSlurmWorkerSpec(
            allocation_id="gpu-cellpose-1",
            submission_token="wf:abcdef:1",
            options={
                "allocation_id": "gpu-cellpose-1",
                "submission_token": "wf:abcdef:1",
                "queue": "compute",
                "cores": 4,
                "memory": "4GiB",
                "processes": 1,
                "nanny": True,
                "walltime": "01:00:00",
                "worker_extra_args": [],
            },
        )
        records = cluster.submit_planned_jobs((spec,))
        assert [(item.allocation_id, item.job_id) for item in records] == [
            ("gpu-cellpose-1", "55001")
        ]
        assert set(cluster.workers) == {"gpu-cellpose-1"}

        cluster.stop_planned_jobs()
        assert cluster.workers == {}
        assert cancelled == ["55001"]
    finally:
        cluster.close(timeout=10)


def test_partial_slurm_submission_remains_discoverable_for_complete_rollback(
    monkeypatch,
) -> None:
    cancelled: list[str] = []

    async def fake_submit(self, _script_filename):
        if self.allocation_id == "orange-1":
            raise RuntimeError("simulated Slurm race")
        return "Submitted batch job 55201"

    async def fake_close(job_id, _cancel_command):
        cancelled.append(str(job_id))

    monkeypatch.setattr(PlannedSLURMJob, "_submit_job", fake_submit)
    monkeypatch.setattr(PlannedSLURMJob, "_close_job", staticmethod(fake_close))
    cluster = PlannedSLURMCluster(
        n_workers=0,
        queue="tao",
        cores=4,
        memory="4GiB",
        processes=1,
        nanny=True,
        walltime="01:00:00",
        worker_extra_args=[],
        scheduler_options={
            "host": "127.0.0.1",
            "port": 0,
            "dashboard": False,
            "dashboard_address": None,
        },
        asynchronous=False,
        name="WorkFlow-SLURMCluster-partial-test",
    )
    try:
        from services.slurm_jobqueue_cluster import PlannedSlurmWorkerSpec

        def spec(name: str) -> PlannedSlurmWorkerSpec:
            return PlannedSlurmWorkerSpec(
                allocation_id=name,
                submission_token=f"wf:abcdef:{name}",
                options={
                    "allocation_id": name,
                    "submission_token": f"wf:abcdef:{name}",
                    "queue": "tao",
                    "cores": 4,
                    "memory": "4GiB",
                    "processes": 1,
                    "nanny": True,
                    "walltime": "01:00:00",
                    "worker_extra_args": [],
                },
            )

        with pytest.raises(RuntimeError, match="simulated Slurm race"):
            cluster.submit_planned_jobs((spec("banana-1"), spec("orange-1")))

        records = cluster.submitted_job_records()
        assert [(item.allocation_id, item.job_id) for item in records] == [
            ("banana-1", "55201")
        ]
        cluster.stop_planned_jobs()
        assert cancelled == ["55201"]
    finally:
        cluster.close(timeout=10)


def test_dask_service_owns_planned_slurmcluster_scheduler_and_cleanup(
    monkeypatch,
    tmp_path: Path,
) -> None:
    async def fake_submit(self, _script_filename):
        return "Submitted batch job 55100"

    async def fake_close(_job_id, _cancel_command):
        return None

    monkeypatch.setenv("WorkFlow_DASK_ALLOW_INSECURE_CLUSTER", "1")
    monkeypatch.setattr(PlannedSLURMJob, "_submit_job", fake_submit)
    monkeypatch.setattr(PlannedSLURMJob, "_close_job", staticmethod(fake_close))
    service = DaskService()
    template = SimpleNamespace(
        partition="compute",
        cpu=4,
        memory_gib=4,
        processes=1,
    )
    from services.slurm_jobqueue_cluster import PlannedSlurmWorkerSpec

    service.start_slurm_jobqueue_scheduler(
        host="127.0.0.1",
        port=0,
        dashboard_address="127.0.0.1:0",
        template_job=template,
        time_limit="01:00:00",
        shared_temp_directory=str(tmp_path),
        python_executable=sys.executable,
    )
    try:
        records = service.submit_slurm_jobqueue_workers((
            PlannedSlurmWorkerSpec(
                allocation_id="cpu-reader-1",
                submission_token="wf:abcdef:1",
                options={
                    "allocation_id": "cpu-reader-1",
                    "submission_token": "wf:abcdef:1",
                    "queue": "compute",
                    "cores": 4,
                    "memory": "4GiB",
                    "processes": 1,
                    "nanny": True,
                    "walltime": "01:00:00",
                    "worker_extra_args": [],
                },
            ),
        ))
        assert records[0].job_id == "55100"
        service.stop_slurm_jobqueue_workers()
    finally:
        assert service.stop_cluster() is True


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
        threads=4,
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
        threads=4,
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


def test_external_worker_ownership_uses_registered_hidden_resources() -> None:
    execution_id = "12345678-1234-1234-1234-123456789abc"
    submission_token = "wf:abcdef:1"
    scheduler_info = {
        "address": "tcp://mn02:8786",
        "workers": {
            "tcp://t001:20000": {
                "resources": {
                    "gpu-cellpose": 1,
                    "CPU": 4,
                    "GPU": 1,
                    execution_ownership_resource(execution_id): 1,
                    submission_ownership_resource(submission_token): 1,
                },
            },
        },
    }

    validate_external_worker_ownership(
        scheduler_info,
        execution_id=execution_id,
        submission_tokens=(submission_token,),
    )
    summary = cluster_resource_summary_from_scheduler_info(scheduler_info)
    assert summary.worker_profile_slots == {"gpu-cellpose": 1}


def test_external_worker_ownership_rejects_another_execution() -> None:
    submission_token = "wf:abcdef:1"
    scheduler_info = {
        "workers": {
            "tcp://t001:20000": {
                "resources": {
                    execution_ownership_resource("another-execution"): 1,
                    submission_ownership_resource(submission_token): 1,
                },
            },
        },
    }

    with pytest.raises(RuntimeError, match="execution ownership mismatch"):
        validate_external_worker_ownership(
            scheduler_info,
            execution_id="expected-execution",
            submission_tokens=(submission_token,),
        )


def test_window_concurrency_defaults_to_one() -> None:
    assert _resolve_max_in_flight_windows(
        None,
        resource_plan=object(),
        cluster_summary=object(),
    ) == 1
