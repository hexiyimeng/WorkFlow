"""Path validation for the platform that will execute a workflow."""

from __future__ import annotations

import ntpath
import os
import posixpath
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Mapping


_EXECUTION_BACKENDS = {"local", "slurm"}


def execution_backend_name(environment: Mapping[str, str] | None = None) -> str:
    env = os.environ if environment is None else environment
    value = str(env.get("WorkFlow_EXECUTION_BACKEND", "local")).strip().lower()
    if value not in _EXECUTION_BACKENDS:
        raise ValueError(
            "WorkFlow_EXECUTION_BACKEND must be 'local' or 'slurm', "
            f"got {value!r}."
        )
    return value


def execution_path_style(environment: Mapping[str, str] | None = None) -> str:
    """Return the path style used by the selected execution backend."""

    backend = execution_backend_name(environment)
    if backend == "slurm":
        return "posix"
    return "windows" if os.name == "nt" else "posix"


def normalize_execution_path(
    value: Any,
    *,
    name: str,
    environment: Mapping[str, str] | None = None,
) -> str:
    """Validate a path without interpreting it on the control-plane OS."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty absolute path.")
    raw = value.strip()
    if "\x00" in raw:
        raise ValueError(f"{name} contains a null byte.")

    backend = execution_backend_name(environment)
    style = execution_path_style(environment)
    if style == "posix":
        if not PurePosixPath(raw).is_absolute():
            backend_label = "Slurm/Linux" if backend == "slurm" else "Linux/POSIX"
            raise ValueError(
                f"{name} must be an absolute path for the selected {backend_label} "
                "execution backend, for example /share/results/output.zarr."
            )
        return posixpath.normpath(raw)

    if not PureWindowsPath(raw).is_absolute():
        raise ValueError(
            f"{name} must be an absolute path for the selected local Windows "
            "execution backend, for example "
            r"D:\results\output.zarr or \\server\share\output.zarr. "
            "A /share/... path is valid only when WorkFlow_EXECUTION_BACKEND=slurm."
        )
    return ntpath.normpath(raw)


def execution_path_suffix(
    value: str,
    environment: Mapping[str, str] | None = None,
) -> str:
    path_type = (
        PureWindowsPath
        if execution_path_style(environment) == "windows"
        else PurePosixPath
    )
    return path_type(value).suffix


def execution_paths_equal(
    first: str,
    second: str,
    environment: Mapping[str, str] | None = None,
) -> bool:
    if execution_path_style(environment) == "windows":
        return ntpath.normcase(ntpath.normpath(first)) == ntpath.normcase(
            ntpath.normpath(second)
        )
    return posixpath.normpath(first) == posixpath.normpath(second)
