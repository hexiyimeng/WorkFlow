from __future__ import annotations

import hashlib
from itertools import product
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from core.registry import register_node
from nodes.base import BaseMapBlocksNode


TOKEN_DTYPE = np.dtype("uint8")
logger = logging.getLogger("WorkFlow.ZarrWriter")


def _normalize_output_path(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("ZarrWriter output_path must be a literal string.")
    raw = value.strip()
    if not raw:
        raise ValueError("ZarrWriter output_path cannot be empty.")
    if "\x00" in raw:
        raise ValueError("ZarrWriter output_path contains a null byte.")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        raise ValueError("ZarrWriter output_path must be an absolute path.")
    if path.suffix.lower() != ".zarr":
        raise ValueError(
            "ZarrWriter output_path must include the complete final '.zarr' suffix; "
            "the framework does not append file extensions."
        )
    return str(path.resolve())


def _normalize_dataset_path(value: str | None) -> str:
    raw = str(value or "0").strip().strip("/")
    if not raw:
        return "0"
    if "\x00" in raw:
        raise ValueError("dataset_path contains a null byte.")
    normalized = "/".join(
        part for part in raw.replace("\\", "/").split("/") if part
    )
    return normalized or "0"


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


def _zarr_lock_namespace(
    output_path: str,
    store_kind: str,
    dataset_path: str,
) -> str:
    normalized_store = os.path.normcase(str(Path(output_path).expanduser().resolve()))
    dataset_identity = dataset_path if store_kind == "ome_zarr" else "<array>"
    identity = f"{normalized_store}\x00{store_kind}\x00{dataset_identity}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"workflow-zarr-write:{digest}"


def _zarr_chunk_lock_name(namespace: str, coordinate: tuple[int, ...]) -> str:
    suffix = ",".join(str(int(index)) for index in coordinate)
    return f"{namespace}:{suffix}"


def _block_region_bounds(
    origin: tuple[int, ...],
    block_shape: tuple[int, ...],
    array_shape: tuple[int, ...],
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    ndim = len(array_shape)
    if len(origin) != ndim or len(block_shape) != ndim:
        raise RuntimeError(
            "ZarrWriter block origin, block shape, and target shape must have matching "
            f"dimensions, got origin={origin}, block_shape={block_shape}, "
            f"target_shape={array_shape}."
        )

    starts = tuple(int(value) for value in origin)
    lengths = tuple(int(value) for value in block_shape)
    stops = tuple(start + length for start, length in zip(starts, lengths))
    for axis, (start, length, stop, size) in enumerate(
        zip(starts, lengths, stops, array_shape)
    ):
        if start < 0 or length < 0 or stop > int(size):
            raise RuntimeError(
                "ZarrWriter block region is outside the destination array: "
                f"axis={axis}, start={start}, length={length}, "
                f"stop={stop}, array_size={size}."
            )
    return starts, stops


def _validate_storage_chunks(
    storage_chunks: tuple[int, ...],
    ndim: int,
) -> tuple[int, ...]:
    chunks = tuple(int(value) for value in storage_chunks)
    if len(chunks) != int(ndim) or any(value <= 0 for value in chunks):
        raise RuntimeError(
            "ZarrWriter destination storage chunks must contain one positive size per "
            f"array axis, got chunks={chunks}, ndim={ndim}."
        )
    return chunks


def _is_storage_chunk_aligned(
    starts: tuple[int, ...],
    stops: tuple[int, ...],
    array_shape: tuple[int, ...],
    storage_chunks: tuple[int, ...],
) -> bool:
    return all(
        start % chunk_size == 0
        and (stop % chunk_size == 0 or stop == int(axis_size))
        for start, stop, axis_size, chunk_size in zip(
            starts,
            stops,
            array_shape,
            storage_chunks,
        )
    )


def _partial_storage_chunk_coordinates(
    starts: tuple[int, ...],
    stops: tuple[int, ...],
    array_shape: tuple[int, ...],
    storage_chunks: tuple[int, ...],
) -> tuple[tuple[int, ...], ...]:
    if any(stop <= start for start, stop in zip(starts, stops)):
        return ()

    intersected_axes = tuple(
        range(start // chunk_size, ((stop - 1) // chunk_size) + 1)
        for start, stop, chunk_size in zip(starts, stops, storage_chunks)
    )
    partial: list[tuple[int, ...]] = []
    for coordinate in product(*intersected_axes):
        fully_covered = True
        for axis, chunk_index in enumerate(coordinate):
            chunk_start = int(chunk_index) * storage_chunks[axis]
            chunk_stop = min(
                chunk_start + storage_chunks[axis],
                int(array_shape[axis]),
            )
            if starts[axis] > chunk_start or stops[axis] < chunk_stop:
                fully_covered = False
                break
        if not fully_covered:
            partial.append(tuple(int(index) for index in coordinate))
    return tuple(sorted(partial))


def _make_distributed_lock(name: str):
    from dask.distributed import Lock

    return Lock(name)


def _write_with_storage_chunk_locks(
    *,
    target,
    region: tuple[slice, ...],
    array: np.ndarray,
    lock_names: tuple[str, ...],
) -> None:
    acquired_locks = []
    primary_error: BaseException | None = None
    try:
        for lock_name in lock_names:
            lock = _make_distributed_lock(lock_name)
            acquired = lock.acquire()
            if acquired is False:
                raise RuntimeError(f"Failed to acquire Zarr storage-chunk lock {lock_name!r}.")
            acquired_locks.append(lock)
        target[region] = array
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        first_release_error: BaseException | None = None
        for lock in reversed(acquired_locks):
            try:
                released = lock.release()
                if released is False:
                    raise RuntimeError(
                        f"Failed to release Zarr storage-chunk lock {lock.name!r}."
                    )
            except BaseException as exc:
                if first_release_error is None:
                    first_release_error = exc
                logger.exception("Failed to release a Zarr storage-chunk lock.")
        if first_release_error is not None and primary_error is None:
            raise first_release_error


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


def _validate_existing_store(
    *,
    output_path: str,
    store_kind: str,
    dataset_path: str,
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
    dtype: np.dtype,
) -> None:
    """Open a resumed target without mutating it and verify its array contract."""
    import zarr

    path = Path(output_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Cannot resume ZarrWriter because the output store does not exist: {path}"
        )

    if store_kind == "array":
        try:
            target = zarr.open(str(path), mode="r+")
        except Exception as exc:
            raise ValueError(
                f"Cannot resume ZarrWriter because {path} is not a readable Zarr array."
            ) from exc
        if not all(hasattr(target, field) for field in ("shape", "chunks", "dtype")):
            raise ValueError(
                f"Cannot resume ZarrWriter: expected an array store at {path}, "
                f"but found {type(target).__name__}."
            )
    elif store_kind == "ome_zarr":
        try:
            group = zarr.open_group(str(path), mode="r+")
        except Exception as exc:
            raise ValueError(
                f"Cannot resume ZarrWriter because {path} is not a readable Zarr group."
            ) from exc
        try:
            target = group[dataset_path]
        except (KeyError, TypeError) as exc:
            raise ValueError(
                f"Cannot resume ZarrWriter because dataset path {dataset_path!r} "
                f"does not exist in {path}."
            ) from exc
        if not all(hasattr(target, field) for field in ("shape", "chunks", "dtype")):
            raise ValueError(
                f"Cannot resume ZarrWriter: {dataset_path!r} in {path} is not a Zarr array."
            )
    else:
        raise ValueError(f"store_kind must be 'array' or 'ome_zarr', got {store_kind!r}.")

    expected_shape = tuple(int(x) for x in shape)
    expected_chunks = tuple(int(x) for x in chunks)
    expected_dtype = np.dtype(dtype)
    actual_shape = tuple(int(x) for x in target.shape)
    actual_chunks = tuple(int(x) for x in target.chunks)
    actual_dtype = np.dtype(target.dtype)
    mismatches = []
    if actual_shape != expected_shape:
        mismatches.append(f"shape={actual_shape}, expected={expected_shape}")
    if actual_chunks != expected_chunks:
        mismatches.append(f"chunks={actual_chunks}, expected={expected_chunks}")
    if actual_dtype != expected_dtype:
        mismatches.append(f"dtype={actual_dtype}, expected={expected_dtype}")
    if mismatches:
        raise ValueError(
            f"Cannot resume ZarrWriter because the existing target at {path} is incompatible: "
            + "; ".join(mismatches)
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
    is_resuming: bool = False,
) -> None:
    import zarr

    path = Path(output_path)
    if is_resuming:
        _validate_existing_store(
            output_path=output_path,
            store_kind=store_kind,
            dataset_path=dataset_path,
            shape=shape,
            chunks=chunks,
            dtype=dtype,
        )
        return

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

    array_shape = tuple(int(value) for value in target.shape)
    storage_chunks = _validate_storage_chunks(
        tuple(int(value) for value in target.chunks),
        len(array_shape),
    )
    starts, stops = _block_region_bounds(
        tuple(int(value) for value in origin),
        tuple(int(value) for value in array.shape),
        array_shape,
    )
    region = tuple(slice(start, stop) for start, stop in zip(starts, stops))

    if _is_storage_chunk_aligned(starts, stops, array_shape, storage_chunks):
        target[region] = array
    else:
        partial_coordinates = _partial_storage_chunk_coordinates(
            starts,
            stops,
            array_shape,
            storage_chunks,
        )
        namespace = _zarr_lock_namespace(
            str(output_path),
            str(store_kind),
            str(dataset_path),
        )
        lock_names = tuple(
            _zarr_chunk_lock_name(namespace, coordinate)
            for coordinate in partial_coordinates
        )
        _write_with_storage_chunk_locks(
            target=target,
            region=region,
            array=array,
            lock_names=lock_names,
        )
    token_shape = ctx.output_chunk_shape or ((1,) * int(array.ndim))
    return np.ones(tuple(int(x) for x in token_shape), dtype=TOKEN_DTYPE)


@register_node("ZarrWriter")
class ZarrWriter(BaseMapBlocksNode):
    """Generic Zarr/OME-Zarr side-effect writer backed by map_blocks."""

    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "Zarr Writer"
    EXECUTION_RESOURCE = "cpu"
    EXECUTION_WORKERS = 2
    OUTPUT_NODE = True
    OUTPUT_PATH_INPUT = "output_path"

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
                "output_path": ("STRING", {"default": "", "multiline": False}),
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

    @staticmethod
    def validate_output_path(value: str) -> str:
        return _normalize_output_path(value)

    def preprocess(
        self,
        dask_arr=None,
        params: dict | None = None,
        runtime: dict | None = None,
    ) -> dict[str, Any] | None:
        if dask_arr is None:
            raise ValueError("ZarrWriter expects an input Dask Array, got None.")
        params = params or {}
        runtime = runtime or {}
        output_path = _normalize_output_path(params.get("output_path", ""))
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
        input_shape = tuple(int(x) for x in dask_arr.shape)
        input_chunks = tuple(
            tuple(int(size) for size in axis_chunks)
            for axis_chunks in dask_arr.chunks
        )

        logger.debug(
            "[ZarrWriter] input_shape=%s input_chunks=%s "
            "destination_storage_chunks=%s",
            input_shape,
            input_chunks,
            chunks,
        )
        _prepare_store(
            output_path=output_path,
            store_kind=store_kind,
            dataset_path=dataset_path,
            shape=input_shape,
            chunks=chunks,
            dtype=np.dtype(dask_arr.dtype),
            axes=axes,
            voxel_size=voxel_size,
            compressor_name=str(params.get("compressor_name") or "default"),
            overwrite=bool(params.get("overwrite", True)),
            write_metadata=bool(params.get("write_metadata", True)),
            is_resuming=bool(runtime.get("is_resuming", False)),
        )
        return {
            "output_path": output_path,
            "store_kind": store_kind,
            "dataset_path": dataset_path,
        }
