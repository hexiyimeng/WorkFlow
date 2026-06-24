from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from core.registry import register_node
from nodes.base import BaseMapBlocksNode


TOKEN_DTYPE = np.dtype("uint8")


def _normalize_output_path(value: str) -> str:
    raw = str(value or "").strip().strip('"').strip("'")
    if not raw:
        raise ValueError("ZarrWriter output_path cannot be empty.")
    if "\x00" in raw:
        raise ValueError("ZarrWriter output_path contains a null byte.")
    path = Path(raw).expanduser()
    if path.suffix.lower() != ".zarr":
        path = path.with_suffix(path.suffix + ".zarr") if path.suffix else Path(f"{path}.zarr")
    return str(path.resolve())


def _normalize_dataset_path(value: str | None) -> str:
    raw = str(value or "0").strip().strip("/")
    if not raw:
        return "0"
    if "\x00" in raw:
        raise ValueError("dataset_path contains a null byte.")
    return raw.replace("\\", "/")


def _normalize_voxel_size(value: Any, ndim: int) -> tuple[float, ...]:
    if value is None or value == "":
        return tuple(1.0 for _ in range(int(ndim)))
    if isinstance(value, str):
        values = tuple(part.strip() for part in value.split(",") if part.strip())
    elif isinstance(value, (list, tuple)):
        values = tuple(value)
    else:
        raise ValueError(f"Unsupported voxel_size {value!r}.")
    if len(values) != int(ndim):
        raise ValueError(f"voxel_size length {len(values)} does not match array ndim={ndim}.")
    return tuple(float(x) for x in values)


def _validate_regular_chunks(chunks: tuple[tuple[int, ...], ...]) -> None:
    for axis, axis_chunks in enumerate(chunks):
        if len(axis_chunks) <= 1:
            continue
        nominal = int(axis_chunks[0])
        for index, size in enumerate(axis_chunks[:-1]):
            if int(size) != nominal:
                raise ValueError(
                    "ZarrWriter requires regular chunks except possibly the final boundary chunk. "
                    f"Axis {axis} chunk {index} has size {size}, expected {nominal}."
                )


def _prepare_compressor(name: str):
    compressor_name = str(name or "default").strip().lower()
    if compressor_name in {"default", "zstd"}:
        import numcodecs

        return numcodecs.Zstd(level=3)
    if compressor_name == "blosc":
        import numcodecs

        return numcodecs.Blosc(cname="zstd", clevel=3, shuffle=numcodecs.Blosc.SHUFFLE)
    if compressor_name == "lz4":
        import numcodecs

        return numcodecs.LZ4(acceleration=1)
    if compressor_name == "none":
        return None
    raise ValueError(f"Unsupported compressor_name={name!r}.")


def _create_zarr_array(container, name: str | None, *, shape, chunks, dtype, compressor):
    kwargs = {
        "shape": tuple(int(x) for x in shape),
        "chunks": tuple(int(x) for x in chunks),
        "dtype": np.dtype(dtype),
        "overwrite": True,
    }
    if compressor is not None:
        kwargs["compressor"] = compressor
    try:
        if name is None:
            return container.create(**kwargs)
        return container.create_dataset(name, **kwargs)
    except TypeError:
        kwargs.pop("compressor", None)
        if name is None:
            return container.create(**kwargs)
        return container.create_dataset(name, **kwargs)


def _create_nested_dataset(group, dataset_path: str, *, shape, chunks, dtype, compressor):
    parts = [part for part in dataset_path.split("/") if part]
    if not parts:
        raise ValueError("dataset_path cannot be empty.")
    current = group
    for part in parts[:-1]:
        current = current.require_group(part)
    return _create_zarr_array(
        current,
        parts[-1],
        shape=shape,
        chunks=chunks,
        dtype=dtype,
        compressor=compressor,
    )


def _prepare_store(
    *,
    output_path: str,
    store_kind: str,
    dataset_path: str,
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
    dtype: np.dtype,
    axes: tuple[str, ...],
    voxel_size: tuple[float, ...],
    compressor_name: str,
    overwrite: bool,
    write_metadata: bool,
) -> None:
    import zarr

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"ZarrWriter output already exists: {path}")
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()

    compressor = _prepare_compressor(compressor_name)
    created_at = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    if store_kind == "array":
        arr = zarr.open(
            str(path),
            mode="w",
            shape=shape,
            chunks=chunks,
            dtype=dtype,
            compressor=compressor,
        )
        if write_metadata:
            arr.attrs["workflow_writer"] = {
                "node": "ZarrWriter",
                "store_kind": store_kind,
                "shape": shape,
                "chunks": chunks,
                "dtype": str(dtype),
                "axes": list(axes),
                "voxel_size": list(voxel_size),
                "created_at": created_at,
            }
        return

    if store_kind != "ome_zarr":
        raise ValueError(f"store_kind must be 'array' or 'ome_zarr', got {store_kind!r}.")

    group = zarr.open_group(str(path), mode="w")
    arr = _create_nested_dataset(
        group,
        dataset_path,
        shape=shape,
        chunks=chunks,
        dtype=dtype,
        compressor=compressor,
    )
    if write_metadata:
        group.attrs["multiscales"] = [{
            "version": "0.4",
            "name": path.stem,
            "datasets": [{
                "path": dataset_path,
                "coordinateTransformations": [{"type": "scale", "scale": list(voxel_size)}],
            }],
            "axes": [{"name": str(axis).lower()} for axis in axes],
        }]
        arr.attrs["workflow_writer"] = {
            "node": "ZarrWriter",
            "store_kind": store_kind,
            "shape": shape,
            "chunks": chunks,
            "dtype": str(dtype),
            "created_at": created_at,
        }


def write_zarr_block(array: np.ndarray, ctx=None) -> np.ndarray:
    if ctx is None:
        raise RuntimeError("ZarrWriter block requires a BlockContext.")
    resources = ctx.resources or {}
    output_path = resources.get("output_path")
    store_kind = resources.get("store_kind")
    dataset_path = resources.get("dataset_path")
    origin = ctx.chunk_origins[0] if ctx.chunk_origins else None
    if not output_path:
        raise RuntimeError("ZarrWriter output_path is missing from block resources.")
    if origin is None:
        raise RuntimeError("ZarrWriter requires block array-location metadata.")

    import zarr

    if store_kind == "ome_zarr":
        target = zarr.open_group(str(output_path), mode="r+")[dataset_path]
    else:
        target = zarr.open(str(output_path), mode="r+")
    region = tuple(slice(int(start), int(start) + int(length)) for start, length in zip(origin, array.shape))
    target[region] = array
    token_shape = ctx.output_chunk_shape or ((1,) * int(array.ndim))
    return np.ones(tuple(int(x) for x in token_shape), dtype=TOKEN_DTYPE)


@register_node("ZarrWriter")
class ZarrWriter(BaseMapBlocksNode):
    """Generic Zarr/OME-Zarr side-effect writer backed by map_blocks."""

    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "Zarr Writer"
    OUTPUT_NODE = True
    FAILURE_POLICY = "raise"
    SKIP_EMPTY_BLOCKS = False

    MAP_INPUTS = ["array"]
    PRIMARY_INPUT = "array"
    PROCESS_BLOCK = write_zarr_block
    ARRAY_AXES_BY_NDIM = {
        "array": {
            2: ("Y", "X"),
            3: ("Z", "Y", "X"),
            4: ("C", "Z", "Y", "X"),
            5: ("T", "C", "Z", "Y", "X"),
        }
    }
    MAP_BLOCKS_OUTPUT_SPEC = {
        "dtype": "uint8",
        "chunks": "token_chunks_from_primary",
        "enforce_ndim": True,
    }

    RETURN_TYPES = ("DASK_ARRAY[uint8]",)
    RETURN_NAMES = ("write_tokens",)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "array": ("DASK_ARRAY[any]",),
                "output_path": ("STRING", {"default": "output.zarr", "multiline": False}),
            },
            "optional": {
                "store_kind": (["array", "ome_zarr"], {"default": "array"}),
                "dataset_path": ("STRING", {"default": "0", "multiline": False}),
                "axes": ("STRING", {"default": "", "multiline": False}),
                "voxel_size": ("STRING", {"default": "", "multiline": False}),
                "compressor_name": (["default", "zstd", "blosc", "lz4", "none"], {"default": "default"}),
                "overwrite": ("BOOLEAN", {"default": True}),
                "write_metadata": ("BOOLEAN", {"default": True}),
            },
        }

    def preprocess(
        self,
        dask_arr=None,
        params: dict | None = None,
        runtime: dict | None = None,
    ) -> dict[str, Any] | None:
        if dask_arr is None:
            raise ValueError("ZarrWriter expects an input Dask Array, got None.")
        params = params or {}
        output_path = _normalize_output_path(params.get("output_path", "output.zarr"))
        store_kind = str(params.get("store_kind") or "array").strip().lower()
        dataset_path = _normalize_dataset_path(params.get("dataset_path", "0"))
        axes = tuple(
            str(axis).upper()
            for axis in ((getattr(self, "_axes_by_name", {}) or {}).get("array") or ())
        )
        if len(axes) != int(dask_arr.ndim):
            raise ValueError(
                f"ZarrWriter axes {axes!r} length does not match array ndim={dask_arr.ndim}."
            )
        voxel_size = _normalize_voxel_size(params.get("voxel_size"), int(dask_arr.ndim))
        chunks = tuple(
            int(axis_chunks[0]) if axis_chunks else 1
            for axis_chunks in dask_arr.chunks
        )

        _validate_regular_chunks(dask_arr.chunks)
        _prepare_store(
            output_path=output_path,
            store_kind=store_kind,
            dataset_path=dataset_path,
            shape=tuple(int(x) for x in dask_arr.shape),
            chunks=chunks,
            dtype=np.dtype(dask_arr.dtype),
            axes=axes,
            voxel_size=voxel_size,
            compressor_name=str(params.get("compressor_name") or "default"),
            overwrite=bool(params.get("overwrite", True)),
            write_metadata=bool(params.get("write_metadata", True)),
        )
        return {
            "output_path": output_path,
            "store_kind": store_kind,
            "dataset_path": dataset_path,
        }
