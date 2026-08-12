from pathlib import Path

import pytest

from core import config as config_module
from services import dask_service


def test_slurm_mem_per_node_is_mib() -> None:
    assert config_module._slurm_memory_limit_gb(
        {"SLURM_MEM_PER_NODE": "131072"}
    ) == pytest.approx(128.0)


def test_slurm_mem_per_cpu_uses_allocated_cpu_count() -> None:
    assert config_module._slurm_memory_limit_gb(
        {
            "SLURM_MEM_PER_CPU": "4096",
            "SLURM_JOB_CPUS_PER_NODE": "16(x2)",
        }
    ) == pytest.approx(64.0)


def test_slurm_memory_ignores_incomplete_or_invalid_values() -> None:
    assert config_module._slurm_memory_limit_gb(
        {"SLURM_MEM_PER_CPU": "4096"}
    ) is None
    assert config_module._slurm_memory_limit_gb(
        {"SLURM_MEM_PER_NODE": "not-a-number"}
    ) is None


def test_cgroup_memory_uses_smallest_finite_limit(tmp_path: Path) -> None:
    unlimited = tmp_path / "unlimited"
    unlimited.write_text("max\n", encoding="ascii")
    sentinel = tmp_path / "sentinel"
    sentinel.write_text(str(1 << 62), encoding="ascii")
    finite = tmp_path / "finite"
    finite.write_text(str(24 * 1024**3), encoding="ascii")

    assert config_module._cgroup_memory_limit_gb(
        (unlimited, sentinel, finite)
    ) == pytest.approx(24.0)


def test_effective_memory_is_smallest_available_ceiling(monkeypatch) -> None:
    monkeypatch.setattr(config_module, "_physical_memory_gb", lambda: 1000.0)
    monkeypatch.setattr(config_module, "_slurm_memory_limit_gb", lambda: 128.0)
    monkeypatch.setattr(config_module, "_cgroup_memory_limit_gb", lambda: 96.0)

    assert config_module._get_system_memory_gb() == pytest.approx(96.0)


def test_dask_role_limits_stay_within_slurm_allocation(monkeypatch) -> None:
    monkeypatch.setattr(dask_service, "_get_system_memory_gb", lambda: 128.0)
    total_weight = (
        2 * dask_service.CPU_WORKER_HOST_MEMORY_WEIGHT
        + dask_service.GPU_WORKER_HOST_MEMORY_WEIGHT
    )

    cpu_limit = dask_service._compute_worker_memory_limit(
        3,
        configured_limit_gb=0,
        allocation_weight=dask_service.CPU_WORKER_HOST_MEMORY_WEIGHT,
        total_allocation_weight=total_weight,
    )
    gpu_limit = dask_service._compute_worker_memory_limit(
        3,
        configured_limit_gb=0,
        allocation_weight=dask_service.GPU_WORKER_HOST_MEMORY_WEIGHT,
        total_allocation_weight=total_weight,
    )

    assert cpu_limit == "17.9GiB"
    assert gpu_limit == "53.7GiB"
    assert 2 * 17.9 + 53.7 <= 128.0 * 0.70


def test_slurm_cuda_mask_is_counted_without_importing_torch(monkeypatch) -> None:
    monkeypatch.setenv("WorkFlow_CUDA_MODE", "auto")
    monkeypatch.setenv("WorkFlow_TRUST_SLURM_CUDA_MASK", "1")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "GPU-a,GPU-b")

    assert dask_service._detect_cuda_for_cluster() == (True, 2)


def test_trusted_slurm_cuda_mask_requires_allocation_mask(monkeypatch) -> None:
    monkeypatch.setenv("WorkFlow_CUDA_MODE", "auto")
    monkeypatch.setenv("WorkFlow_TRUST_SLURM_CUDA_MASK", "1")
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    with pytest.raises(RuntimeError, match="requires CUDA_VISIBLE_DEVICES"):
        dask_service._detect_cuda_for_cluster()
