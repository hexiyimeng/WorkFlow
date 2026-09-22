#!/usr/bin/env python3
"""Opt-in real Slurm smoke test of baseline + profile Adaptive.

Run with the same environment and Python as the backend, from a shared checkout.
Without --run this only validates the plan. This is a control-plane smoke test,
not a Cellpose throughput benchmark or a production Scheduler sizing test.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, replace
import json
import getpass
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))


def command(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True,
                          timeout=30).stdout.strip()


def owned_queue(executable, token_prefix):
    """Match our unique Slurm comments; never cancel by username or job name."""
    rows = command(executable, "--user", getpass.getuser(), "--noheader", "--format=%i|%k|%T|%R")
    result = []
    for line in rows.splitlines():
        fields = line.split("|", 3)
        if len(fields) == 4 and fields[1].startswith(token_prefix + ":"):
            result.append(dict(zip(("id", "token", "state", "reason"), fields)))
    return result


def smoke_task(value, profile, delay):
    # Defined in __main__ so cloudpickle sends it by value to the workers.
    import os
    import socket
    import time
    from distributed import get_worker

    worker = get_worker()
    if worker.state.total_resources.get(profile, 0) < 1:
        raise RuntimeError(f"Task ran on the wrong profile: {profile}")
    cuda = None
    if profile == "GPU":
        import torch
        if torch.cuda.device_count() != 1:
            raise RuntimeError("Expected exactly one visible CUDA device")
        cuda = float((torch.ones(16, device="cuda") + 1).sum().item())
        if cuda != 32.0:
            raise RuntimeError("CUDA arithmetic failed")
    time.sleep(delay)
    return {"value": value, "profile": profile, "worker": worker.name,
            "host": socket.gethostname(), "cuda": cuda,
            "deviceMask": os.environ.get("CUDA_VISIBLE_DEVICES")}


def identity(value):
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Submit real Slurm jobs")
    parser.add_argument("--gpu", action="store_true", help="Also allocate/test real GPU workers")
    parser.add_argument("--timeout", type=int, default=600,
                        help="Seconds per registration/work phase; does not change production queue policy")
    args = parser.parse_args()
    if not 60 <= args.timeout <= 3600:
        parser.error("--timeout must be between 60 and 3600 seconds")

    import dask
    import distributed
    import dask_jobqueue
    import psutil
    from core.worker_pool import WorkerPool
    from core.worker_profiles import PhysicalResources, WorkerProfile
    from core.workflow_resources import WorkerProfileRequirement, WorkflowResourcePlan
    from services.dask_service import dask_service
    from services.slurm_execution_service import (
        SlurmRuntimeConfig, _plan_slurm_allocation, validate_allocation_plan_policy,
    )
    from services.slurm_jobqueue_cluster import build_planned_slurm_worker_spec

    config = SlurmRuntimeConfig.from_environment()
    # Cap test Jobs at 15 minutes, respecting any shorter site policy.
    from core.slurm_execution import _slurm_time_seconds
    if _slurm_time_seconds(config.policy.time_limit) > 900:
        config = replace(config, policy=replace(config.policy, time_limit="00:15:00"))
    names = ["CPU"] + (["GPU"] if args.gpu else [])
    profiles = [WorkerProfile(name, PhysicalResources(1, 8 if name == "GPU" else 4,
                                                   int(name == "GPU")), {}, 1)
                for name in names]
    pools = [WorkerPool(name, 1, 2) for name in names]
    workflow = WorkflowResourcePlan(tuple(
        WorkerProfileRequirement(name, "Smoke", name, name) for name in names))
    plan = _plan_slurm_allocation(workflow,
                                 worker_profiles=[p.to_dict() for p in profiles],
                                 worker_pools=[p.to_dict() for p in pools], config=config)
    validate_allocation_plan_policy(plan, config.policy)
    print(json.dumps({"plan": plan.to_dict(), "submit": args.run}, indent=2), flush=True)
    if not args.run:
        return 0

    execution_id = "smoke-" + uuid.uuid4().hex[:16]
    token_prefix = "wf:" + execution_id
    run = config.runtime_directory / "test-runs" / execution_id
    run.mkdir(parents=True, mode=0o700)
    report = {"status": "RUNNING", "run": str(run), "tokenPrefix": token_prefix,
              "versions": {"dask": dask.__version__, "distributed": distributed.__version__,
                           "jobqueue": dask_jobqueue.__version__},
              "plan": plan.to_dict(), "phases": [], "cleanupConfirmed": False}
    (run / "result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"RESULT_DIRECTORY={run}", flush=True)
    process = psutil.Process()
    process.cpu_percent()
    client = cluster = None
    samples = []
    baseline_names = set()

    def snapshot(label):
        async def scheduler_state():
            return {
                "workers": [{"name": w.name, "host": w.host,
                             "resources": dict(w.resources), "processing": len(w.processing)}
                            for w in cluster.scheduler.workers.values()],
                "pools": {a.pool.profile: {"planned": len(a.plan), "requested": len(a.requested),
                                           "observed": len(a.observed),
                                           "failure": str(a.failure) if a.failure else None}
                          for a in cluster.profile_adaptives},
                "tasks": len(cluster.scheduler.tasks),
            }
        started = time.monotonic()
        sample = cluster.sync(scheduler_state)
        sample.update(label=label, time=time.time(), schedulerRoundTripSeconds=time.monotonic()-started,
                      serviceProcessRss=process.memory_info().rss,
                      serviceProcessCpuPercent=process.cpu_percent(),
                      queue=owned_queue(config.squeue_executable, token_prefix))
        samples.append(sample)
        with (run / "samples.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(sample) + "\n")
        if any(pool["failure"] for pool in sample["pools"].values()):
            raise RuntimeError("Adaptive submission failed; see samples.jsonl")
        if baseline_names and not baseline_names.issubset({w["name"] for w in sample["workers"]}):
            raise RuntimeError("A baseline worker disappeared during the smoke test")
        return sample

    def wait_until(label, predicate, timeout):
        deadline = time.monotonic() + timeout
        while True:
            sample = snapshot(label)
            if predicate(sample):
                return sample
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{label}: deadline reached; inspect Slurm reasons in samples.jsonl")
            time.sleep(2)

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Received signal {signum}")

    old_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        client = dask_service.start_slurm_jobqueue_scheduler(
            host=config.scheduler_host, port=config.scheduler_port,
            dashboard_address=config.dashboard_address, template_job=plan.jobs[0],
            time_limit=plan.time_limit, shared_temp_directory=str(run),
            python_executable=sys.executable)
        cluster = dask_service.cluster
        templates = {job.profile: job for job in plan.jobs}

        def spec_factory(profile, name, token):
            return build_planned_slurm_worker_spec(
                plan, replace(templates[profile], allocation_id=name), execution_id=execution_id,
                submission_token=token, project_root=config.project_root,
                runtime_directory=config.runtime_directory, run_directory=run,
                python_executable=Path(sys.executable), sbatch_executable=config.sbatch_executable,
                scancel_executable=config.scancel_executable, scheduler_host=config.scheduler_host,
                scheduler_port=config.scheduler_port, protocol=cluster.scheduler_address.split(":", 1)[0],
                security=cluster.security, worker_port_range=config.worker_port_range,
                nanny_port_range=config.nanny_port_range)

        specs = tuple(spec_factory(job.profile, job.allocation_id, token_prefix + ":baseline")
                      for job in plan.jobs)
        records = dask_service.submit_slurm_baseline(specs, run / "allocations")
        report["baselineJobs"] = [asdict(record) for record in records]
        if len(records) != 1:
            raise RuntimeError("Minimum resources were not submitted as one Slurm job")
        sample = wait_until("baseline-registration", lambda s: len(s["workers"]) == len(names), args.timeout)
        baseline_names = {w["name"] for w in sample["workers"]}
        for name in names:
            if sum(w["resources"].get(name, 0) >= 1 for w in sample["workers"]) != 1:
                raise RuntimeError(f"Invalid baseline for {name}")
        # Use production Adaptive defaults, including target_duration.
        dask_service.start_slurm_adaptive(pools, specs, spec_factory, token_prefix)
        for profile in names:
            print(f"PHASE={profile}", flush=True)
            start = len(samples)
            before = {record.allocation_id for record in cluster.submitted_job_records()}
            # Native dependencies: one input -> parallel tasks -> one consumer.
            other = next((name for name in names if name != profile), profile)
            with dask.annotate(resources={other: 1}):
                seed = dask.delayed(identity)(7)
            with dask.annotate(resources={profile: 1}):
                tasks = [dask.delayed(smoke_task, pure=False)(seed, profile, 3) for _ in range(32)]
            with dask.annotate(resources={other: 1}):
                output = dask.delayed(identity)(tasks)
            future = client.compute(output, optimize_graph=False)
            wait_until(profile + "-work", lambda s: future.done(), args.timeout)
            values = future.result()
            if len(values) != 32 or any(v["value"] != 7 or v["profile"] != profile for v in values):
                raise RuntimeError("Native dependency graph returned incorrect results")
            future.release()
            wait_until(profile + "-shrink", lambda s: all(p["planned"] == p["observed"] == 1
                       for p in s["pools"].values()), min(args.timeout, 120))
            added = [asdict(record) for record in cluster.submitted_job_records()
                     if record.allocation_id not in before]
            if any(not record["allocation_id"].startswith(profile + "-elastic-") for record in added):
                raise RuntimeError("An unrelated profile expanded")
            expanded = any(s["pools"][profile]["observed"] > 1 for s in samples[start:])
            used_extra = any(v["worker"] not in baseline_names for v in values)
            report["phases"].append({"profile": profile, "extraJobs": added,
                                      "extraRegistered": expanded, "extraExecuted": used_extra,
                                      "baselineRetained": True, "shrunk": True, "results": values})
        report["status"] = ("PASS" if all(p["extraRegistered"] and p["extraExecuted"]
                                         for p in report["phases"]) else "INCONCLUSIVE")
    except BaseException as exc:
        report.update(status="INCONCLUSIVE" if isinstance(exc, TimeoutError) else "FAIL",
                      error=f"{type(exc).__name__}: {exc}")
    finally:
        # Retain evidence even when cancellation fails. Kill only this run's jobs.
        errors = []
        if cluster is not None:
            try:
                cluster.stop_profile_adaptives()
                report["jobs"] = [asdict(record) for record in cluster.submitted_job_records()]
                cluster.stop_planned_jobs()
            except BaseException as exc:
                errors.append(f"cluster-job-cleanup: {exc}")
        try:
            deadline = time.monotonic() + 120
            while True:
                rows = owned_queue(config.squeue_executable, token_prefix)
                if not rows:
                    report["cleanupConfirmed"] = True
                    break
                ids = sorted({row["id"].split("+", 1)[0] for row in rows})
                if any(not job_id.isdigit() for job_id in ids):
                    raise RuntimeError("Unexpected Slurm job id; refusing cancellation")
                command(config.scancel_executable, *ids)
                if time.monotonic() >= deadline:
                    raise TimeoutError("Test jobs remain in squeue after cancellation")
                time.sleep(2)
        except BaseException as exc:
            errors.append(f"cleanup: {exc}")
        try:
            if not dask_service.stop_cluster():
                errors.append("scheduler-close required emergency cleanup")
        except BaseException as exc:
            errors.append(f"scheduler-close: {exc}")
        signal.signal(signal.SIGTERM, old_term)
        if errors:
            report.update(status="FAIL", cleanupErrors=errors)
        if samples:
            report["serviceProcessPeakRss"] = max(s["serviceProcessRss"] for s in samples)
            report["serviceProcessPeakCpuPercent"] = max(s["serviceProcessCpuPercent"] for s in samples)
            report["schedulerMaxRoundTripSeconds"] = max(s["schedulerRoundTripSeconds"] for s in samples)
        (run / "result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"status": report["status"], "cleanupConfirmed": report["cleanupConfirmed"],
                          "result": str(run / "result.json")}), flush=True)
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
