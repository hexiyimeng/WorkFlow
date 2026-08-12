from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SMOKE = PROJECT_ROOT / "deploy" / "hpc" / "multi_worker_smoke.py"


def test_local_cpu_smoke_uses_two_real_worker_processes(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    environment = os.environ.copy()
    environment.update(
        {
            "WorkFlow_CUDA_MODE": "disabled",
            "WorkFlow_DASHBOARD_ADDRESS": ":0",
            "WorkFlow_DASK_LOCAL_DIR": str(tmp_path / "dask"),
            "WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS": "180",
            "WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS": "120",
            "WorkFlow_CPU_WORKER_MEMORY_LIMIT_GB": "1",
        }
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(SMOKE),
            "--cpu-workers",
            "2",
            "--gpu-workers",
            "0",
            "--allow-non-slurm",
            "--task-timeout",
            "60",
            "--result-file",
            str(result_path),
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=240.0,
        check=False,
    )
    output = "\n".join((completed.stdout, completed.stderr))
    assert completed.returncode == 0, output
    assert "__WORKFLOW_MULTI_WORKER_RESULT__" in completed.stdout

    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert result["status"] == "passed"
    assert result["requested"] == {"cpuWorkers": 2, "gpuWorkers": 0}
    assert result["resourcePlan"]["cpuWorkers"] == 2
    assert result["resourcePlan"]["gpuWorkers"] == 0
    assert result["validatedCpuWorkers"] == 2
    assert result["validatedGpuWorkers"] == 0
    assert result["cudaComputeValidated"] is False
    assert result["slurm"]["validated"] is False
    assert result["clusterShutdown"] == "graceful"
    assert len(set(result["workerAddresses"])) == 2
    assert len(set(result["workerProcessIds"])) == 2
    assert len(result["tasks"]) == 2
    assert {task["workerAddress"] for task in result["tasks"]} == set(
        result["workerAddresses"]
    )
    assert {task["role"] for task in result["tasks"]} == {"cpu"}
    assert all(task["resources"] == {"CPU": 1.0} for task in result["tasks"])
    assert all(task["cuda"] is None for task in result["tasks"])
    assert len({task["host"] for task in result["tasks"]}) == 1


def test_local_mode_refuses_to_claim_gpu_validation(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(SMOKE),
            "--cpu-workers",
            "1",
            "--gpu-workers",
            "1",
            "--allow-non-slurm",
            "--result-file",
            str(tmp_path / "result.json"),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )

    assert completed.returncode != 0
    assert "supports CPU-only developer tests" in completed.stderr
    assert not (tmp_path / "result.json").exists()
