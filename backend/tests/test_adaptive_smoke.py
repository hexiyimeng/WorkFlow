"""Exercise the real smoke-test driver, replacing only Slurm with local workers."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import sys
import time
import pytest


@pytest.mark.parametrize("with_gpu", [False, True])
def test_smoke_driver_checks_worker_types_and_cleans_up(monkeypatch, tmp_path, with_gpu):
    from distributed import Worker, get_worker
    from core.cluster_inventory import ClusterInventoryService, parse_scontrol_show_node
    from services.slurm_execution_service import SlurmRuntimeConfig, slurm_policy_from_environment
    from services.slurm_jobqueue_cluster import PlannedSLURMJob, BaselineSLURMJob

    root = Path(__file__).resolve().parents[2]
    module_spec = importlib.util.spec_from_file_location("adaptive_smoke_test", root / "deploy/hpc/adaptive_smoke.py")
    module = importlib.util.module_from_spec(module_spec)
    monkeypatch.setitem(sys.modules, module_spec.name, module)
    module_spec.loader.exec_module(module)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = SlurmRuntimeConfig(
        runtime_directory=tmp_path, execution_root=tmp_path / "jobs", project_root=root,
        policy=slurm_policy_from_environment({}), sbatch_executable="sbatch", squeue_executable="squeue",
        sacct_executable=None, sinfo_executable="sinfo", scontrol_executable="scontrol",
        scancel_executable="scancel", poll_interval_seconds=1, result_grace_seconds=1,
        cancel_grace_seconds=10, scheduler_host="127.0.0.1", scheduler_port=port,
        dashboard_address="127.0.0.1:0")
    monkeypatch.setattr(SlurmRuntimeConfig, "from_environment", classmethod(lambda cls: config))
    inventory = parse_scontrol_show_node(
        "NodeName=c1 CPUTot=16 CPUAlloc=0 RealMemory=65536 AllocMem=0 "
        "Gres=gpu:2 State=IDLE Partitions=compute\n")
    monkeypatch.setattr(ClusterInventoryService, "load", lambda self: inventory)
    monkeypatch.setenv("WorkFlow_DASK_ALLOW_INSECURE_CLUSTER", "1")
    monkeypatch.setattr(sys, "argv", ["adaptive_smoke.py", "--run", "--timeout", "60"]
                        + (["--gpu"] if with_gpu else []))
    allocations = {}

    async def submit(self, filename):
        job_id = str(80000 + len(allocations))
        members = self.components if isinstance(self, BaselineSLURMJob) else [self]
        allocation = {"token": self.submission_token, "workers": [], "active": True}
        allocations[job_id] = allocation
        for member in members:
            profile = "GPU" if "GPU=1" in member._command_template else "CPU"
            worker = await Worker(self.scheduler, name=member.name, nthreads=1,
                                  resources={profile: 1}, memory_limit=0, dashboard_address=None)
            allocation["workers"].append(worker)
        return f"Submitted batch job {job_id}"

    async def cancel(job_id, command):
        for worker in allocations[job_id]["workers"]:
            await worker.close()
        allocations[job_id]["active"] = False

    def command(*args):
        assert args[0] == "squeue", args
        return "\n".join(f"{job_id}|{a['token']}|RUNNING|c1" for job_id, a in allocations.items() if a["active"])

    def task(value, profile, delay):
        time.sleep(0.2)
        return {"value": value, "profile": profile, "worker": get_worker().name}

    monkeypatch.setattr(PlannedSLURMJob, "_submit_job", submit)
    monkeypatch.setattr("services.slurm_jobqueue_cluster.detect_heterogeneous_directive",
                        lambda command: "hetjob")
    monkeypatch.setattr(PlannedSLURMJob, "_close_job", staticmethod(cancel))
    monkeypatch.setattr(module, "command", command)
    monkeypatch.setattr(module, "smoke_task", task)
    assert module.main() == 0
    report = json.loads(next((tmp_path / "test-runs").glob("*/result.json")).read_text())
    assert report["status"] == "PASS" and report["cleanupConfirmed"]
    assert {phase["profile"] for phase in report["phases"]} == (
        {"CPU", "GPU"} if with_gpu else {"CPU"})
    assert all(phase["extraExecuted"] and phase["shrunk"] for phase in report["phases"])
    assert all(not allocation["active"] for allocation in allocations.values())
