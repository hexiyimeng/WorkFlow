import asyncio
import json
from pathlib import Path

import pytest

from services import slurm_worker_launcher as launcher


def _payload(runtime: Path, **updates):
    payload = {
        "schemaVersion": 2,
        "executionId": "execution-1",
        "submissionToken": "wf:0123456789abcdef:0123456789abcdef",
        "codeRevision": None,
        "schedulerAddress": "tcp://mn02.cluster:43123",
        "resourcePlan": {
            "cpuWorkers": 3,
            "gpuWorkers": 6,
            "nodes": 2,
            "cpus": 8,
            "gpus": 4,
            "memoryGiB": 300,
            "totalCpus": 16,
            "totalGpus": 8,
            "totalMemoryGiB": 600,
            "cpuWorkersByNode": [2, 1],
            "gpuWorkersByNode": [4, 2],
            "timeLimit": "01:00:00",
            "partition": "compute",
        },
        "runtimeDirectory": str(runtime.resolve()),
        "security": None,
        "allowInsecure": True,
        "workerMemoryGiB": {"cpu": 8, "gpu": 64},
        "networkInterface": None,
        "workerPortRange": "20000:20015",
        "nannyPortRange": "20100:20115",
    }
    payload.update(updates)
    return payload


def _environment(**updates):
    result = {
        "SLURM_JOB_ID": "900",
        "SLURM_NNODES": "2",
        "SLURM_NTASKS": "2",
        "SLURM_PROCID": "1",
        "SLURM_LOCALID": "0",
        "SLURM_NODEID": "1",
        "SLURM_CPUS_PER_TASK": "8",
        "SLURM_MEM_PER_NODE": "131072",
        "CUDA_VISIBLE_DEVICES": "0,1,2,3",
    }
    result.update(updates)
    return result


def test_schema_v2_requires_exact_layout_and_homogeneous_gres_is_maximum(tmp_path):
    request = launcher.parse_worker_launcher_request(_payload(tmp_path))

    assert request.resource_plan.node_count == 2
    assert request.resource_plan.gpus_per_allocated_node == 4
    assert request.resource_plan.cpu_workers_by_node == (2, 1)
    assert request.resource_plan.gpu_workers_by_node == (4, 2)

    invalid = _payload(tmp_path)
    invalid["resourcePlan"]["gpuWorkers"] = 7
    with pytest.raises(launcher.WorkerLauncherValidationError, match="sum exactly"):
        launcher.parse_worker_launcher_request(invalid)

    invalid = _payload(tmp_path)
    invalid["unexpected"] = True
    with pytest.raises(launcher.WorkerLauncherValidationError, match="unknown fields"):
        launcher.parse_worker_launcher_request(invalid)


@pytest.mark.parametrize(
    "address",
    (
        "tcp://127.0.0.1:8786",
        "tcp://localhost:8786",
        "tcp://0.0.0.0:8786",
        "udp://mn02:8786",
        "tcp://mn02",
        "tcp://user@mn02:8786",
    ),
)
def test_scheduler_address_must_be_compute_reachable_tcp(tmp_path, address):
    with pytest.raises(launcher.WorkerLauncherValidationError, match="schedulerAddress"):
        launcher.parse_worker_launcher_request(
            _payload(tmp_path, schedulerAddress=address)
        )


def test_request_file_must_live_below_real_runtime(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    job = runtime / "jobs" / "execution-1"
    job.mkdir(parents=True)
    request_path = job / "workers.json"
    request_path.write_text(json.dumps(_payload(runtime)), encoding="utf-8")

    parsed = launcher._read_request(request_path.resolve())
    assert parsed.execution_id == "execution-1"

    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(_payload(runtime)), encoding="utf-8")
    with pytest.raises(launcher.WorkerLauncherValidationError, match="below runtimeDirectory"):
        launcher._read_request(outside.resolve())


def test_tls_security_is_complete_shared_and_uses_tls_scheduler(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    security_directory = runtime / "security"
    security_directory.mkdir()
    security = {}
    for field, filename in (
        ("tlsCaFile", "ca.pem"),
        ("tlsCertFile", "worker.pem"),
        ("tlsKeyFile", "worker.key"),
    ):
        path = security_directory / filename
        path.write_text("test", encoding="utf-8")
        security[field] = str(path.resolve())
    job = runtime / "jobs" / "execution-1"
    job.mkdir(parents=True)
    request_path = job / "workers.json"
    request_path.write_text(
        json.dumps(
            _payload(
                runtime,
                schedulerAddress="tls://mn02.cluster:43123",
                security=security,
                allowInsecure=False,
            )
        ),
        encoding="utf-8",
    )

    request = launcher._read_request(request_path.resolve())
    allocation = launcher.validate_slurm_worker_allocation(
        request, _environment(), hostname="c002"
    )
    options = launcher.build_nanny_options(
        request,
        allocation,
        environment=_environment(
            SLURM_TMPDIR=str((tmp_path / "scratch").resolve()),
        ),
    )
    assert all(option["security"].require_encryption for option in options)

    incomplete = _payload(
        runtime,
        schedulerAddress="tls://mn02.cluster:43123",
        security={"tlsCaFile": security["tlsCaFile"]},
        allowInsecure=False,
    )
    with pytest.raises(launcher.WorkerLauncherValidationError, match="contain exactly"):
        launcher.parse_worker_launcher_request(incomplete)


@pytest.mark.parametrize(
    "updates, message",
    (
        ({"allowInsecure": 1}, "must be a boolean"),
        ({"allowInsecure": False}, "security=null requires"),
        (
            {
                "schedulerAddress": "tls://mn02.cluster:43123",
                "security": {
                    "tlsCaFile": "C:/ca.pem",
                    "tlsCertFile": "C:/worker.pem",
                    "tlsKeyFile": "C:/worker.key",
                },
                "allowInsecure": True,
            },
            "TLS security requires",
        ),
    ),
)
def test_allow_insecure_is_exact_and_consistent_with_tls(tmp_path, updates, message):
    with pytest.raises(launcher.WorkerLauncherValidationError, match=message):
        launcher.parse_worker_launcher_request(_payload(tmp_path, **updates))


def test_rank_selects_uneven_tail_and_allows_only_deliberate_extra_gpus(tmp_path):
    request = launcher.parse_worker_launcher_request(_payload(tmp_path))
    allocation = launcher.validate_slurm_worker_allocation(
        request,
        _environment(),
        hostname="c002",
    )

    assert allocation.rank == 1
    assert allocation.cpu_workers == 1
    assert allocation.gpu_workers == 2
    assert allocation.visible_gpu_ids == ("0", "1", "2", "3")
    assert allocation.selected_gpu_ids == ("0", "1")

    with pytest.raises(launcher.WorkerLauncherValidationError, match="needs 2"):
        launcher.validate_slurm_worker_allocation(
            request,
            _environment(CUDA_VISIBLE_DEVICES="0"),
            hostname="c002",
        )


@pytest.mark.parametrize(
    "updates, message",
    (
        ({"SLURM_NTASKS": "3"}, "one Worker launcher"),
        ({"SLURM_LOCALID": "1"}, "one task per node"),
        ({"SLURM_NODEID": "0"}, "one task per node"),
        ({"SLURM_CPUS_PER_TASK": "2"}, "needs at least 3"),
    ),
)
def test_slurm_layout_validation_is_fail_closed(tmp_path, updates, message):
    request = launcher.parse_worker_launcher_request(_payload(tmp_path))
    with pytest.raises(launcher.WorkerLauncherValidationError, match=message):
        launcher.validate_slurm_worker_allocation(
            request,
            _environment(**updates),
            hostname="c002",
        )


def test_nanny_options_isolate_roles_and_use_cluster_global_gpu_ids(tmp_path):
    request = launcher.parse_worker_launcher_request(_payload(tmp_path))
    allocation = launcher.validate_slurm_worker_allocation(
        request,
        _environment(),
        hostname="c002",
    )
    options = launcher.build_nanny_options(
        request,
        allocation,
        environment=_environment(
            SLURM_TMPDIR=str((tmp_path / "scratch").resolve()),
        ),
    )

    assert len(options) == 3
    cpu, gpu0, gpu1 = options
    assert cpu["resources"] == {"CPU": 1}
    assert cpu["env"]["CUDA_VISIBLE_DEVICES"] == ""
    assert cpu["memory_limit"] == "8GB"
    assert gpu0["resources"] == {"GPU": 1}
    assert gpu0["env"]["CUDA_VISIBLE_DEVICES"] == "0"
    assert gpu0["env"]["WORKFLOW_PHYSICAL_GPU_ID"] == "c002:0"
    assert gpu0["env"]["WORKFLOW_LOCAL_GPU_ID"] == "0"
    assert gpu0["memory_limit"] == "64GB"
    assert gpu1["env"]["CUDA_VISIBLE_DEVICES"] == "1"
    assert gpu1["env"]["WORKFLOW_PHYSICAL_GPU_ID"] == "c002:1"
    assert all(option["worker_port"] == "20000:20015" for option in options)
    assert all(option["port"] == "20100:20115" for option in options)
    assert all(option["scheduler_ip"] == "tcp://mn02.cluster:43123" for option in options)
    assert all(option["host"] == "c002" for option in options)
    assert all(
        str((tmp_path / "scratch").resolve()) in option["local_directory"]
        for option in options
    )


def test_immutable_network_interface_overrides_compute_hostname(tmp_path):
    request = launcher.parse_worker_launcher_request(
        _payload(tmp_path, networkInterface="ib0")
    )
    allocation = launcher.validate_slurm_worker_allocation(
        request, _environment(), hostname="c002"
    )
    options = launcher.build_nanny_options(request, allocation, environment=_environment())
    assert all(option["interface"] == "ib0" for option in options)
    assert all("host" not in option for option in options)


@pytest.mark.parametrize(
    "worker_memory, message",
    (
        ({"cpu": 8}, "exactly"),
        ({"cpu": 0, "gpu": 64}, "positive"),
        ({"cpu": 8, "gpu": True}, "non-negative integer"),
        ({"cpu": 8, "gpu": 128}, "exceeds resourcePlan"),
    ),
)
def test_worker_memory_limits_are_immutable_role_specific_values(
    tmp_path, worker_memory, message
):
    with pytest.raises(launcher.WorkerLauncherValidationError, match=message):
        launcher.parse_worker_launcher_request(
            _payload(tmp_path, workerMemoryGiB=worker_memory)
        )


@pytest.mark.parametrize(
    "updates, message",
    (
        ({"workerPortRange": "20000"}, "form START:END"),
        ({"workerPortRange": "100:200"}, "unprivileged"),
        ({"workerPortRange": "20000:20001"}, "needs at least 6"),
        ({"nannyPortRange": "20010:20030"}, "must not overlap"),
    ),
)
def test_worker_and_nanny_port_ranges_are_strict(tmp_path, updates, message):
    with pytest.raises(launcher.WorkerLauncherValidationError, match=message):
        launcher.parse_worker_launcher_request(_payload(tmp_path, **updates))


def test_nannies_start_concurrently_and_term_closes_all(tmp_path):
    request = launcher.parse_worker_launcher_request(_payload(tmp_path))
    allocation = launcher.validate_slurm_worker_allocation(
        request,
        _environment(),
        hostname="c002",
    )
    stop = asyncio.Event()
    created = []
    all_created = asyncio.Event()
    release_start = asyncio.Event()

    class FakeNanny:
        def __init__(self, **options):
            self.options = options
            self.worker_address = f"tcp://c002:{40000 + len(created)}"
            self.closed = False
            self._finished = asyncio.Event()
            created.append(self)
            if len(created) == 3:
                all_created.set()

        async def start(self):
            await release_start.wait()

        async def finished(self):
            await self._finished.wait()

        async def close(self, *, timeout, reason):
            assert timeout == 120
            assert reason == "slurm-worker-launcher-stop"
            self.closed = True
            self._finished.set()

    async def scenario():
        task = asyncio.create_task(
            launcher.run_worker_launcher(
                request,
                allocation,
                environment=_environment(
                    WorkFlow_DASK_LOCAL_DIR=str((tmp_path / "scratch").resolve()),
                ),
                stop_event=stop,
                nanny_factory=FakeNanny,
            )
        )
        await asyncio.wait_for(all_created.wait(), timeout=1)
        # Every constructor exists before any start() is allowed to finish.
        assert len(created) == 3
        release_start.set()
        await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(scenario())
    assert all(nanny.closed for nanny in created)


def test_worker_holder_is_dynamic_worker_only_and_slurm_19_compatible():
    script = (
        Path(__file__).resolve().parents[2]
        / "deploy"
        / "hpc"
        / "slurm"
        / "workflow_workers.sbatch"
    ).read_text(encoding="utf-8")
    directives = tuple(
        line.strip()
        for line in script.splitlines()
        if line.lstrip().startswith("#SBATCH")
    )

    assert not any("--nodes" in line for line in directives)
    assert not any("--ntasks" in line for line in directives)
    assert not any("--gres" in line for line in directives)
    assert "--ntasks-per-node=1" in script
    assert "--distribution=block" in script
    assert '--gres="gpu:$GPUS_PER_NODE"' in script
    assert 'exec srun "${SRUN_ARGS[@]}"' in script
    assert "services.slurm_worker_launcher" in script
    assert "services.slurm_execution_runner" not in script
    assert "execute_graph" not in script
    assert "127.0.0.1" not in script
