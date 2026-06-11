from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import dask.array as da
import numpy as np

from core.registry import register_node


logger = logging.getLogger("WorkFlow.OMEZarrReader")


DEFAULT_AXES = {
    2: ["Y", "X"],
    3: ["Z", "Y", "X"],
    4: ["C", "Z", "Y", "X"],
    5: ["T", "C", "Z", "Y", "X"],
}


def _sanitize_zarr_path(file_path: str, node_id: str) -> Path:
    raw = str(file_path or "").strip().strip('"').strip("'")
    if not raw:
        raise ValueError(f"[Node {node_id}] File path cannot be empty.")
    if "\x00" in raw:
        raise ValueError(f"[Node {node_id}] File path contains a null byte.")
    path = Path(raw).expanduser().resolve()
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"[Node {node_id}] Not a valid zarr directory: {path}")
    return path


def _axis_names_from_ngff_axes(axes: Any, ndim: int) -> list[str] | None:
    if not isinstance(axes, list) or len(axes) != ndim:
        return None
    names = []
    for axis in axes:
        if isinstance(axis, dict):
            names.append(str(axis.get("name", "")).upper())
        else:
            names.append(str(axis).upper())
    return names if all(names) else None


def _default_axes(ndim: int) -> list[str]:
    return list(DEFAULT_AXES.get(ndim, [f"DIM_{i}" for i in range(ndim)]))


def _target_chunks_by_axes(
    *,
    shape: tuple[int, ...],
    original_chunks: tuple[int, ...] | None,
    axes: list[str],
    chunk_z: int,
    chunk_y: int,
    chunk_x: int,
    keep_first_dim: bool,
) -> tuple[int, ...]:
    chunks = []
    original_chunks = original_chunks or tuple(min(64, int(s)) for s in shape)
    for index, (axis_name, extent) in enumerate(zip(axes, shape)):
        axis = str(axis_name).lower()
        extent = int(extent)
        if axis == "x":
            chunks.append(min(int(chunk_x), extent))
        elif axis == "y":
            chunks.append(min(int(chunk_y), extent))
        elif axis == "z":
            chunks.append(min(int(chunk_z), extent))
        elif axis == "c":
            chunks.append(extent)
        elif axis == "t":
            chunks.append(extent if keep_first_dim else 1)
        elif index == 0 and keep_first_dim:
            chunks.append(extent)
        else:
            fallback = original_chunks[index] if index < len(original_chunks) else min(64, extent)
            chunks.append(min(int(fallback), extent))
    return tuple(chunks)


@register_node("OMEZarrReader")
class OMEZarrReader:
    """
    Lazy OME-Zarr source node.

    Returns a Dask Array and metadata without computing the array. Chunk planning
    is based on OME-NGFF axes when available, otherwise defaults to YX, ZYX,
    CZYX, or TCZYX for 2D-5D arrays.
    """

    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "OME-Zarr Reader"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "file_path": ("STRING", {"default": ""}),
            },
            "optional": {
                "chunk_z": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Z Chunk Size"}),
                "chunk_y": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Y Chunk Size"}),
                "chunk_x": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "X Chunk Size"}),
                "keep_first_dim": ("BOOLEAN", {"default": False, "label": "Keep First Dimension Intact"}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY[any]", "DICT")
    RETURN_NAMES = ("dask_arr", "metadata")
    FUNCTION = "load_zarr"

    def load_zarr(
        self,
        file_path,
        chunk_z=64,
        chunk_y=64,
        chunk_x=64,
        keep_first_dim=False,
        callback=None,
        **kwargs,
    ):
        node_id = kwargs.get("_node_id", "unknown")
        root_path = _sanitize_zarr_path(file_path, node_id)

        import zarr

        multiscales = []
        dataset_path = None
        try:
            z_arr = zarr.open_array(str(root_path), mode="r")
            array_path = root_path
            logger.info("[ZarrReader] Loaded direct array: %s", root_path)
        except Exception:
            store = zarr.open_group(str(root_path), mode="r")
            multiscales = store.attrs.get("multiscales", []) or []
            dataset_path = "0"
            if multiscales:
                datasets = multiscales[0].get("datasets", [])
                if datasets:
                    dataset_path = datasets[0].get("path", "0")
            z_arr = store[dataset_path]
            array_path = root_path / dataset_path
            logger.info("[ZarrReader] Loaded array from group dataset=%s: %s", dataset_path, root_path)

        shape = tuple(int(x) for x in z_arr.shape)
        ndim = int(z_arr.ndim)
        original_chunks = tuple(int(x) for x in (z_arr.chunks or ()))
        axes = None
        voxel_size = [1.0] * ndim

        if multiscales:
            first_scale = multiscales[0]
            axes = _axis_names_from_ngff_axes(first_scale.get("axes"), ndim)
            datasets = first_scale.get("datasets", [])
            if datasets:
                for transform in datasets[0].get("coordinateTransformations", []):
                    if transform.get("type") == "scale":
                        scale = transform.get("scale")
                        if isinstance(scale, list) and len(scale) == ndim:
                            voxel_size = scale

        axes = axes or _default_axes(ndim)
        target_chunks = _target_chunks_by_axes(
            shape=shape,
            original_chunks=original_chunks,
            axes=axes,
            chunk_z=chunk_z,
            chunk_y=chunk_y,
            chunk_x=chunk_x,
            keep_first_dim=bool(keep_first_dim),
        )

        total_gb = np.prod(shape) * z_arr.dtype.itemsize / (1024**3)
        logger.info(
            "[ZarrReader] shape=%s dtype=%s size=%.2fGB axes=%s target_chunks=%s",
            shape,
            z_arr.dtype,
            total_gb,
            axes,
            target_chunks,
        )

        dask_arr = da.from_zarr(str(array_path), chunks=target_chunks)
        metadata = {
            "source_path": str(root_path),
            "shape": dask_arr.shape,
            "dtype": str(dask_arr.dtype),
            "chunks": dask_arr.chunksize,
            "ndim": dask_arr.ndim,
            "npartitions": dask_arr.npartitions,
            "voxel_size": voxel_size,
            "axes": axes,
            "original_chunks": original_chunks,
        }
        return (dask_arr, metadata)
