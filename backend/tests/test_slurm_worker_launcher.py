from __future__ import annotations

from pathlib import Path

from services.slurm_worker_launcher import (
    build_nanny_options,
    parse_worker_launcher_request,
)


def test_launcher_builds_distinct_profile_workers_and_gpu_masks(tmp_path: Path) -> None:
    payload = {
        "schemaVersion": 3,
        "executionId": "phase3-test",
        "submissionToken": "wf:phase3:test",
        "schedulerAddress": "tcp://mn02:8786",
        "runtimeDirectory": str(tmp_path),
        "security": None,
        "allowInsecure": True,
        "networkInterface": None,
        "workerPortRange": "19000:19010",
        "nannyPortRange": "19100:19110",
        "allocationPlan": {
            "nodes": [{
                "node": "c001",
                "workers": {"gpu-cellpose": 2},
                "cpu": 8,
                "memoryGiB": 64,
                "gpu": 2,
                "jobs": ["gpu-cellpose-1", "gpu-cellpose-2"],
            }],
            "jobs": [
                {
                    "allocationId": f"gpu-cellpose-{index}",
                    "profile": "gpu-cellpose",
                    "node": "c001",
                    "workers": 1,
                    "processes": 1,
                    "threads": 1,
                    "slurm": {
                        "nodes": 1,
                        "cpus": 4,
                        "memoryGiB": 32,
                        "gpus": 1,
                        "nodelist": ["c001"],
                    },
                    "logicalResources": {
                        "gpu-cellpose": 1,
                        "CPU": 4,
                        "GPU": 1,
                    },
                }
                for index in (1, 2)
            ],
        },
    }
    request = parse_worker_launcher_request(payload)
    options = build_nanny_options(request, environment={
        "SLURMD_NODENAME": "c001",
        "SLURM_CPUS_ON_NODE": "8",
        "CUDA_VISIBLE_DEVICES": "2,5",
        "SLURM_TMPDIR": str(tmp_path / "scratch"),
    })

    assert len(options) == 2
    assert [option["env"]["CUDA_VISIBLE_DEVICES"] for option in options] == ["2", "5"]
    assert all(option["resources"]["gpu-cellpose"] == 1 for option in options)
    assert all(option["env"]["WORKFLOW_WORKER_PROFILE"] == "gpu-cellpose" for option in options)
