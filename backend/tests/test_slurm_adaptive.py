from __future__ import annotations

import asyncio
import json
import time
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from core.cluster_inventory import parse_scontrol_show_node
from core.resource_planner import plan_workflow_resources
from core.worker_pool import WorkerPool
from core.worker_profiles import PhysicalResources, WorkerProfile
from core.workflow_resources import build_workflow_resource_plan
from services.slurm_adaptive import ProfileAdaptive, profile_target_jobs
from services.slurm_execution_service import SlurmExecutionService, _queue_wait_seconds
from services.slurm_jobqueue_cluster import (
    BaselineSLURMJob, PlannedSLURMCluster, PlannedSLURMJob, PlannedSlurmWorkerSpec,
)


@pytest.fixture(autouse=True)
def slurm_version_without_cli(monkeypatch):
    monkeypatch.setattr("services.slurm_jobqueue_cluster.detect_heterogeneous_directive",
                        lambda command: "hetjob")


def spec(name, profile="CPU", processes=1, token="wf:test:baseline"):
    return PlannedSlurmWorkerSpec(name, token, {
        "allocation_id": name, "submission_token": token,
        "queue": "gpu" if profile == "GPU" else "compute",
        "cores": 4 * processes, "memory": f"{4 * processes}GiB", "processes": processes,
        "nanny": True, "walltime": "01:00:00",
        "job_extra_directives": [f"--comment={token}"] +
                                (["--gres=gpu:1"] if profile == "GPU" else []),
        "job_script_prologue": [f"export WORKFLOW_WORKER_PROFILE={profile}"],
        "worker_extra_args": ["--resources", f"{profile}={1 if profile == 'GPU' else 4}"],
    }, slots_per_worker=1 if profile == "GPU" else 4)


def cluster():
    return PlannedSLURMCluster(
        n_workers=0, queue="compute", cores=4, memory="4GiB", processes=1,
        scheduler_options={"host": "127.0.0.1", "port": 0, "dashboard": False,
                           "dashboard_address": None})


def test_hetjob_submits_once_and_tracks_all_worker_names(monkeypatch, tmp_path):
    scripts, cancelled = [], []

    async def submit(self, filename):
        scripts.append(Path(filename).read_text())
        return "Submitted batch job 70100"

    async def cancel(job_id, command):
        cancelled.append(job_id)

    monkeypatch.setattr(PlannedSLURMJob, "_submit_job", submit)
    monkeypatch.setattr(PlannedSLURMJob, "_close_job", staticmethod(cancel))
    c = cluster()
    try:
        c.configure_journal(tmp_path)
        records = c.submit_baseline((spec("CPU-1", processes=2),
                                     spec("GPU-1", "GPU")))
        assert len(scripts) == 1
        script = scripts[0]
        assert script.count("#SBATCH hetjob") == 1
        assert "srun --het-group=0" in script and "srun --het-group=1" in script
        assert "#SBATCH -p compute" in script and "#SBATCH -p gpu" in script
        assert "--gres=gpu:1" in script
        assert "--cpus-per-task=8 --mem=8192M --gres=none" in script
        assert "--cpus-per-task=4 --mem=4096M --gres=gpu:1" in script
        assert "wait -n" in script and "trap " in script
        assert c.plan == {"CPU-1-het-0-0", "CPU-1-het-0-1", "CPU-1-het-1"}
        assert len(records) == 1 and records[0].job_id == "70100"
        journal = json.loads((tmp_path / "CPU-1.json").read_text())
        assert journal["jobId"] == "70100"
        c.stop_planned_jobs()
        assert cancelled == ["70100"]
        assert c.submitted_job_records() == records  # retirement doesn't erase ownership
    finally:
        c.close(timeout=10)


def test_incremental_submission_and_whole_multiworker_job_removal(monkeypatch, tmp_path):
    submitted = []
    cancelled = []

    async def submit(self, filename):
        # Ownership is durable before the external mutation.
        assert json.loads((tmp_path / f"{self.allocation_id}.json").read_text())["jobId"] is None
        submitted.append(self.allocation_id)
        return f"Submitted batch job {70200 + len(submitted)}"

    async def cancel(job_id, command):
        cancelled.append(job_id)

    monkeypatch.setattr(PlannedSLURMJob, "_submit_job", submit)
    monkeypatch.setattr(PlannedSLURMJob, "_close_job", staticmethod(cancel))
    c = cluster()
    try:
        c.configure_journal(tmp_path)
        c.submit_baseline((spec("CPU-1"),))
        c.submit_planned_jobs((spec("CPU-elastic-1", processes=2, token="wf:test:e1"),))
        assert len(submitted) == 2
        assert {"CPU-elastic-1-0", "CPU-elastic-1-1"}.issubset(c.plan)
        c.sync(c.scale_down, ["CPU-elastic-1-0", "CPU-elastic-1-1"])
        c.sync(c._correct_state)
        assert set(c.workers) == {"CPU-1"}
        assert cancelled == ["70202"]
        c.stop_planned_jobs()
        assert set(cancelled) == {"70201", "70202"}
    finally:
        c.close(timeout=10)


def task(profile, duration=10, state="queued"):
    return NS(state=state, resource_restrictions={profile: 1}, prefix=duration)


def worker(name, profile="GPU", busy=False, memory=0):
    return NS(name=name, resources={profile: 1}, memory_limit=1000,
              nbytes=memory, nthreads=8, processing={"t"} if busy else set())


def scheduler(tasks=(), workers=()):
    return NS(tasks=dict(enumerate(tasks)), workers={w.name: w for w in workers},
              _get_prefix_duration=lambda duration: duration)


def test_target_uses_profile_tasks_and_resource_slots_not_cpu_threads():
    pool = WorkerPool("GPU", 1, 8)
    s = scheduler([task("GPU") for _ in range(4)] +
                  [task("CPU") for _ in range(100)] +
                  [task("GPU", state="waiting") for _ in range(100)],
                  [worker("base")])
    assert profile_target_jobs(s, pool, 5) == 4
    s.tasks = {0: task("GPU", duration=10000, state="processing")}
    assert profile_target_jobs(s, pool, 5) == 1  # one long task isn't parallelizable
    s.tasks = {0: task("GPU", state="no-worker")}
    assert profile_target_jobs(s, pool, 5) == 1


def test_target_rounds_to_whole_jobs_and_respects_memory_and_bounds():
    pool = WorkerPool("CPU", 1, 3, processes=2)
    s = scheduler([task("CPU") for _ in range(5)])
    assert profile_target_jobs(s, pool, 5) == 3
    s.tasks = {}
    s.workers = {"w": worker("w", "CPU", memory=900)}
    assert profile_target_jobs(s, pool, 5) == 1
    with pytest.raises(ValueError, match="maximum_jobs"):
        WorkerPool("CPU", 2, 1)


def test_pending_jobs_count_toward_target_and_baseline_is_never_retired():
    async def run():
        c = NS(worker_spec={"elastic": {}}, workers={"elastic": NS(job_id="70300")},
               scheduler=scheduler(workers=[worker("base")]))
        adaptive = ProfileAdaptive(c, WorkerPool("GPU", 1, 4),
                                   [("base",)], lambda *args: None, "wf:test", interval=0,
                                   wait_count=2)
        adaptive.elastic_names["elastic"] = ("elastic",)
        assert adaptive.plan == adaptive.requested == {"baseline:GPU:0", "elastic"}
        assert await adaptive.recommendations(2) == {"status": "same"}
        assert await adaptive.recommendations(1) == {"status": "same"}
        assert await adaptive.recommendations(1) == {"status": "down", "workers": ["elastic"]}
        c.scheduler.workers = {}  # a restarting baseline worker stays protected
        result = await adaptive.recommendations(1)
        assert all(not name.startswith("baseline:") for name in result.get("workers", []))
    asyncio.run(run())


def test_retirement_refusal_keeps_job_and_success_releases_only_elastic():
    async def run():
        corrected = []
        s = scheduler(workers=[worker("base"), worker("elastic")])
        async def retire(**kwargs):
            return {}
        async def correct():
            corrected.append(True)
        s.retire_workers = retire
        c = NS(worker_spec={"baseline": {}, "elastic": {}}, workers={},
               scheduler=s, _correct_state=correct)
        a = ProfileAdaptive(c, WorkerPool("GPU", 1, 4),
                            [("base",)], lambda *args: None, "wf:test", interval=0)
        a.elastic_names["elastic"] = ("elastic",)
        await a.scale_down(["baseline", "elastic"])
        assert set(c.worker_spec) == {"baseline", "elastic"}
        async def allow(**kwargs):
            return {"elastic": {}}
        s.retire_workers = allow
        await a.scale_down(["baseline", "elastic"])
        assert set(c.worker_spec) == {"baseline"} and corrected == [True]
    asyncio.run(run())


def test_busy_inventory_can_queue_minimum_without_pinning_nodes():
    class Node:
        required_worker_profile = "GPU"
    workflow = build_workflow_resource_plan({"n": {"type": "Gpu", "inputs": {}}}, ["n"],
                                            node_mappings={"Gpu": Node})
    profile = WorkerProfile("GPU", PhysicalResources(4, 8, 1),
                            {"GPU": 1, "GPU": 1}, threads=4)
    inventory = parse_scontrol_show_node(
        "NodeName=g01 CPUTot=8 CPUAlloc=8 RealMemory=32000 AllocMem=32000 "
        "Gres=gpu:1 GresUsed=gpu:1 State=ALLOCATED Partitions=gpu\n")
    plan = plan_workflow_resources(workflow, [profile], [WorkerPool("GPU", 1, 4)],
                                   inventory, time_limit="01:00:00")
    assert len(plan.jobs) == 1 and plan.jobs[0].node == ""
    assert plan.jobs[0].to_dict()["slurm"]["nodelist"] == []
    assert _queue_wait_seconds({}) == 0  # peak queues aren't aborted after five minutes
    assert _queue_wait_seconds({"WorkFlow_SLURM_QUEUE_START_TIMEOUT_SECONDS": "0.0"}) == 0
    for value in ("-1", "nan", "inf"):
        with pytest.raises(ValueError):
            _queue_wait_seconds({"WorkFlow_SLURM_QUEUE_START_TIMEOUT_SECONDS": value})


def test_generated_baseline_scripts_parse_in_bash(tmp_path):
    git = shutil.which("git")
    git_bash = Path(git).parent.parent / "bin" / "bash.exe" if git else Path("missing")
    bash = str(git_bash) if git_bash.is_file() else shutil.which("bash")
    if not bash:
        pytest.skip("Bash is unavailable")
    for count in (1, 2):
        specs = [spec("CPU-1", processes=2), spec("GPU-1", "GPU")][:count]
        job = BaselineSLURMJob("tcp://127.0.0.1:8786", name="base", component_specs=specs,
                              **dict(specs[0].options))
        script = job.job_script()
        if count == 1:
            assert "--het-group" not in script and "#SBATCH hetjob" not in script
        path = tmp_path / f"baseline-{count}.sh"
        path.write_text(script, encoding="utf-8", newline="\n")
        result = subprocess.run([bash, "-n", path.as_posix()], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr


def test_heterogeneous_queue_query_and_token_recovery(monkeypatch):
    service = SlurmExecutionService()
    config = NS(squeue_executable="squeue")
    def command(argv, **kwargs):
        if "--format=%i|%k|%T" in argv:
            return NS(returncode=0, stdout="70400+0|wf:test|RUNNING\n70400+1|wf:test|RUNNING", stderr="")
        return NS(returncode=0, stdout="70400+0|wf:test|RUNNING|c1|None\n70400+1|wf:test|RUNNING|g1|None", stderr="")
    monkeypatch.setattr(service, "_run_command", command)
    async def run():
        assert await service._query_queue_state(config, "70400", submission_token="wf:test") == (
            True, ("RUNNING", "c1,g1", "None"))
        assert await service._query_job_by_submission_token(config, "wf:test") == (True, ("70400", "RUNNING"))
        assert await service._query_queue_state(config, "70400", submission_token="wrong") == (False, None)
    asyncio.run(run())


def test_native_graph_triggers_only_its_profile_and_shrinks_extras(monkeypatch, tmp_path):
    """Real Scheduler/Workers/Adaptive, with sbatch replaced by local workers."""
    import dask
    from distributed import Client, Worker
    from services.slurm_adaptive import start_profile_adaptives

    allocations, submitted, cancelled = {}, [], []

    async def submit(self, filename):
        job_id = str(71000 + len(submitted))
        submitted.append(self.allocation_id)
        members = self.components if isinstance(self, BaselineSLURMJob) else [self]
        allocations[job_id] = []
        for member in members:
            profile = "GPU" if "GPU=1" in member._command_template else "CPU"
            w = await Worker(self.scheduler, name=member.name, nthreads=4,
                             resources={profile: 1 if profile == "GPU" else 4}, memory_limit=0, dashboard_address=None)
            allocations[job_id].append(w)
        return f"Submitted batch job {job_id}"

    async def cancel(job_id, command):
        cancelled.append(job_id)
        for w in allocations[job_id]:
            await w.close()

    monkeypatch.setattr(PlannedSLURMJob, "_submit_job", submit)
    monkeypatch.setattr(PlannedSLURMJob, "_close_job", staticmethod(cancel))
    with dask.config.set({"distributed.adaptive.interval": "20ms",
                          "distributed.adaptive.target-duration": "100ms"}):
        c = cluster()
        client = Client(c)
        try:
            c.configure_journal(tmp_path)
            specs = (spec("CPU-1"), spec("GPU-1", "GPU"))
            c.submit_baseline(specs)
            client.wait_for_workers(2, timeout=10)
            pools = [WorkerPool("CPU", 1, 2), WorkerPool("GPU", 1, 3)]
            async def start():
                start_profile_adaptives(c, pools, specs,
                                       lambda profile, name, token: spec(name, profile, token=token),
                                       "wf:integration")
            c.sync(start)

            def work(x):
                import time
                time.sleep(0.08)
                return x + 1

            with dask.annotate(resources={"GPU": 1}):
                results = [dask.delayed(work, pure=False)(i) for i in range(24)]
            futures = client.compute(results)
            assert client.gather(futures) == list(range(1, 25))
            assert any(name.startswith("GPU-elastic-") for name in submitted)
            assert not any(name.startswith("CPU-elastic-") for name in submitted)
            client.cancel(futures)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                async def active_specs():
                    return set(c.worker_spec)
                if c.sync(active_specs) == {"CPU-1"}:
                    break
                time.sleep(0.05)
            assert c.sync(active_specs) == {"CPU-1"}
            assert "71000" not in cancelled
            c.stop_planned_jobs()
            assert "71000" in cancelled
        finally:
            c.stop_profile_adaptives()
            client.close()
            c.close(timeout=10)


def test_restart_recovers_elastic_and_ambiguous_jobs_from_journals(monkeypatch, tmp_path):
    from services.slurm_execution_service import SlurmRuntimeConfig, JOB_SCHEMA_VERSION
    service = SlurmExecutionService()
    run = tmp_path / "run"
    journals = run / "allocations"
    journals.mkdir(parents=True)
    (run / "job.json").write_text(json.dumps({
        "schemaVersion": JOB_SCHEMA_VERSION, "executionId": "run", "state": "driver_running",
        "jobIds": ["72000"], "clusters": [None], "submissionTokens": ["wf:base"],
        "pendingSubmissionTokens": [],
    }))
    (journals / "elastic.json").write_text(json.dumps({
        "allocationId": "elastic", "jobId": "72001", "submissionToken": "wf:elastic"}))
    (journals / "pending.json").write_text(json.dumps({
        "allocationId": "pending", "jobId": None, "submissionToken": "wf:pending"}))
    monkeypatch.setattr(SlurmRuntimeConfig, "from_environment", classmethod(lambda cls: NS(execution_root=tmp_path)))
    cancelled = []
    async def terminal(**kwargs):
        cancelled.append((kwargs["job_id"], kwargs["submission_token"]))
        return "CANCELLED", "0:0", ""
    async def find(config, token):
        assert token == "wf:pending"
        return True, ("72002", "PENDING")
    monkeypatch.setattr(service, "_wait_for_worker_allocation_terminal", terminal)
    monkeypatch.setattr(service, "_query_job_by_submission_token", find)
    asyncio.run(service.reconcile_active_job())
    assert set(cancelled) == {("72000", "wf:base"), ("72001", "wf:elastic"), ("72002", "wf:pending")}
    record = json.loads((run / "job.json").read_text())
    assert record["state"] == "interrupted" and len(record["clusters"]) == 3


def test_hetjob_leader_terminal_does_not_release_live_components(monkeypatch):
    service = SlurmExecutionService()
    async def leader(*args):
        return True, ("COMPLETED", "0:0", "c1")
    async def siblings(*args):
        return True, ("RUNNING", "g1", "None")
    monkeypatch.setattr(service, "_query_scontrol_state", leader)
    monkeypatch.setattr(service, "_query_queue_state", siblings)
    assert asyncio.run(service._query_terminal_state(NS(), "73000")) == (True, None)


def test_normal_driver_cleanup_does_not_trigger_allocation_failure(monkeypatch, tmp_path):
    import services.slurm_execution_service as module
    service = SlurmExecutionService()
    closing = False
    class Cluster:
        profile_adaptives = []
        worker_spec = {}
        @staticmethod
        def sync(function, *args):
            return asyncio.run(function(*args))
        @staticmethod
        def submitted_job_records():
            return ()
    monkeypatch.setattr(module.dask_service, "cluster", Cluster())
    async def cleanup():
        nonlocal closing
        closing = True
        await asyncio.sleep(0.03)
    async def driver(*args, **kwargs):
        await asyncio.sleep(0.02)
        await kwargs["external_cleanup_barrier"]()
    async def alive(**kwargs):
        if closing:
            raise RuntimeError("baseline cancelled by normal cleanup")
    monkeypatch.setattr(module, "execute_graph_on_service_node", driver)
    monkeypatch.setattr(service, "_assert_worker_allocations_alive", alive)
    config = NS(poll_interval_seconds=0.001, worker_start_timeout_seconds=1)
    asyncio.run(service._execute_with_adaptive_monitor(
        config, [], tmp_path, {}, "run", None, external_cleanup_barrier=cleanup))
    assert closing
