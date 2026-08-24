from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import dask.array as da
import numpy as np

from core.registry import register_node
from nodes.base import ChunkPlanner


logger = logging.getLogger("WorkFlow.OMEZarrReader")


DEFAULT_AXES = {
    2: ("Y", "X"),
    3: ("Z", "Y", "X"),
    4: ("C", "Z", "Y", "X"),
    5: ("T", "C", "Z", "Y", "X"),
}


def _normalize_zarr_path(value: str) -> Path:
    raw = str(value or "").strip().strip('"').strip("'")
    if not raw:
        raise ValueError("file_path cannot be empty.")
    if "\x00" in raw:
        raise ValueError("file_path contains a null byte.")
    path = Path(raw).expanduser().resolve()
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Not a valid zarr directory: {path}")
    return path


def _normalize_axes(value: Any, ndim: int) -> tuple[str, ...]:
    axes = ChunkPlanner.normalize_axes(value)
    if axes is None:
        return tuple(
            DEFAULT_AXES.get(
                int(ndim),
                tuple(f"DIM_{index}" for index in range(int(ndim))),
            )
        )
    normalized = tuple(str(axis).upper() for axis in axes)
    if len(normalized) != int(ndim):
        raise ValueError(f"axes length {len(normalized)} does not match array ndim={ndim}.")
    return normalized


def _resolve_group_array(group, requested_path: str | None, multiscale_index: int, scale_level: int):
    multiscales = group.attrs.get("multiscales", []) or []
    selected_multiscale = None
    array_path = requested_path

    if array_path is None and isinstance(multiscales, list) and multiscales:
        if multiscale_index < 0 or multiscale_index >= len(multiscales):
            raise ValueError(
                f"multiscale_index={multiscale_index} is out of range for "
                f"{len(multiscales)} multiscale entries."
            )
        selected_multiscale = multiscales[multiscale_index]
        datasets = (
            selected_multiscale.get("datasets", [])
            if isinstance(selected_multiscale, dict)
            else []
        )
        if datasets:
            if scale_level < 0 or scale_level >= len(datasets):
                raise ValueError(
                    f"scale_level={scale_level} is out of range for "
                    f"{len(datasets)} dataset entries."
                )
            dataset = datasets[scale_level]
            if isinstance(dataset, dict):
                array_path = str(dataset.get("path") or "").strip("/") or None

    if array_path is None:
        if "0" in group:
            array_path = "0"
        else:
            array_paths = [
                str(name)
                for name, value in group.items()
                if hasattr(value, "shape") and hasattr(value, "dtype")
            ]
            array_path = array_paths[0] if len(array_paths) == 1 else None

    if array_path is None:
        raise ValueError(
            "Could not infer an array dataset from the zarr group. "
            "Provide array_path explicitly, for example '0' or 'labels/cells/0'."
        )
    return group[array_path], array_path


def _target_chunks_by_axes(
    *,
    shape: tuple[int, ...],
    native_chunks: tuple[int, ...],
    axes: tuple[str, ...],
    chunk_mode: str,
    chunk_z: int,
    chunk_y: int,
    chunk_x: int,
    keep_first_dim: bool,
) -> tuple[int, ...]:
    if str(chunk_mode).lower() == "native":
        return tuple(int(size) for size in native_chunks)

    chunks: list[int] = []
    fallback_chunks = native_chunks or tuple(min(64, int(size)) for size in shape)
    for index, (axis_name, extent) in enumerate(zip(axes, shape)):
        axis = str(axis_name).upper()
        extent = int(extent)
        if axis == "X":
            chunks.append(min(int(chunk_x), extent))
        elif axis == "Y":
            chunks.append(min(int(chunk_y), extent))
        elif axis == "Z":
            chunks.append(min(int(chunk_z), extent))
        elif axis == "C":
            chunks.append(extent)
        elif axis == "T":
            chunks.append(extent if keep_first_dim else 1)
        elif index == 0 and keep_first_dim:
            chunks.append(extent)
        else:
            chunks.append(min(int(fallback_chunks[index]), extent))
    return tuple(chunks)


@register_node("OMEZarrReader")
class OMEZarrReader:
    """Lazy reader for direct Zarr arrays and OME-NGFF multiscale datasets."""

    PREFLIGHT_SAFE = True
    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "Zarr / OME-Zarr Reader"
    required_worker_profile = "cpu-reader"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "file_path": ("STRING", {"default": ""}),
            },
            "optional": {
                "array_path": ("STRING", {"default": "", "multiline": False}),
                "multiscale_index": ("INT", {"default": 0, "min": 0, "max": 1024}),
                "scale_level": ("INT", {"default": 0, "min": 0, "max": 1024}),
                "axes": ("STRING", {"default": "", "multiline": False}),
                "chunk_mode": (["by_axes", "native"], {"default": "by_axes"}),
                "chunk_z": ("INT", {"default": 64, "min": 1, "max": 8192, "label": "Z Chunk Size"}),
                "chunk_y": ("INT", {"default": 64, "min": 1, "max": 8192, "label": "Y Chunk Size"}),
                "chunk_x": ("INT", {"default": 64, "min": 1, "max": 8192, "label": "X Chunk Size"}),
                "keep_first_dim": ("BOOLEAN", {"default": False, "label": "Keep First Dimension Intact"}),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[any]",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "load_zarr"

    def load_zarr(
        self,
        file_path,
        array_path="",
        multiscale_index=0,
        scale_level=0,
        axes="",
        chunk_mode="by_axes",
        chunk_z=64,
        chunk_y=64,
        chunk_x=64,
        keep_first_dim=False,
    ):
        root_path = _normalize_zarr_path(file_path)
        requested_path = str(array_path or "").strip().strip("/").strip()
        if "\x00" in requested_path:
            raise ValueError("array_path contains a null byte.")
        requested_path = requested_path.replace("\\", "/") or None

        import zarr

        selected_path = None
        try:
            z_arr = zarr.open_array(str(root_path), mode="r")
            array_store_path = root_path
            source_kind = "array"
            logger.info("[ZarrReader] Loaded direct array: %s", root_path)
        except Exception:
            group = zarr.open_group(str(root_path), mode="r")
            z_arr, selected_path = _resolve_group_array(
                group,
                requested_path,
                int(multiscale_index),
                int(scale_level),
            )
            array_store_path = root_path / selected_path
            source_kind = "group"
            logger.info("[ZarrReader] Loaded group array dataset=%s: %s", selected_path, root_path)

        shape = tuple(int(x) for x in z_arr.shape)
        ndim = int(z_arr.ndim)
        native_chunks = tuple(int(x) for x in (z_arr.chunks or tuple(min(64, size) for size in shape)))

        resolved_axes = _normalize_axes(axes, ndim)
        target_chunks = _target_chunks_by_axes(
            shape=shape,
            native_chunks=native_chunks,
            axes=resolved_axes,
            chunk_mode=str(chunk_mode or "by_axes"),
            chunk_z=int(chunk_z),
            chunk_y=int(chunk_y),
            chunk_x=int(chunk_x),
            keep_first_dim=bool(keep_first_dim),
        )

        total_gb = np.prod(shape) * z_arr.dtype.itemsize / (1024**3)
        logger.info(
            "[ZarrReader] kind=%s shape=%s dtype=%s size=%.2fGB axes=%s target_chunks=%s",
            source_kind,
            shape,
            z_arr.dtype,
            total_gb,
            resolved_axes,
            target_chunks,
        )

        dask_arr = da.from_zarr(str(array_store_path), chunks=target_chunks)
        return (dask_arr,)
