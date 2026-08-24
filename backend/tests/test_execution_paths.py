from __future__ import annotations

import os

import pytest

from core.execution_paths import normalize_execution_path
from nodes.zarr_writer_node import ZarrWriter


def test_slurm_backend_accepts_posix_output_path_on_any_control_plane_os(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WorkFlow_EXECUTION_BACKEND", "slurm")

    path = "/share/home/songzh/workflow-runtime/data/processed01.zarr"

    assert normalize_execution_path(path, name="output_path") == path
    assert ZarrWriter.validate_output_path(path) == path


def test_slurm_backend_rejects_windows_output_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WorkFlow_EXECUTION_BACKEND", "slurm")

    with pytest.raises(ValueError, match="Slurm/Linux"):
        normalize_execution_path(r"D:\results\output.zarr", name="output_path")


@pytest.mark.skipif(os.name != "nt", reason="Windows-local path semantics")
def test_local_windows_error_explains_how_to_use_slurm_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WorkFlow_EXECUTION_BACKEND", "local")

    with pytest.raises(ValueError, match="WorkFlow_EXECUTION_BACKEND=slurm"):
        normalize_execution_path("/share/results/output.zarr", name="output_path")
