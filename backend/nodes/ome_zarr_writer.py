from __future__ import annotations

import logging
import shutil
import uuid
from pathlib import Path

import dask
import dask.array as da
import numcodecs
import numpy as np
import zarr

from core.registry import register_node


logger = logging.getLogger("WorkFlow.OMEZarrWriter")


def _chunk_origins_from_chunks(chunks):
    origins_per_dim = []
    for dim_chunks in chunks:
        origins = []
        offset = 0
        for chunk_size in dim_chunks:
            origins.append(offset)
            offset += chunk_size
        origins_per_dim.append(origins)
    return origins_per_dim


def _validate_regular_chunks_for_region_writes(chunks):
    if not chunks:
        return
    for axis, axis_chunks in enumerate(chunks):
        if len(axis_chunks) <= 1:
            continue
        nominal = axis_chunks[0]
        for i, size in enumerate(axis_chunks):
            if size != nominal and i != len(axis_chunks) - 1:
                raise ValueError(
                    "OMEZarrWriter requires regular chunks except possibly the final boundary chunk. "
                    f"Axis {axis} has irregular internal chunk {i}: expected {nominal}, got {size}. "
                    "Please rechunk explicitly before writing."
                )


def _normalize_axes_for_ngff(axes, ndim):
    if not axes:
        defaults = {
            2: ["y", "x"],
            3: ["z", "y", "x"],
            4: ["c", "z", "y", "x"],
            5: ["t", "c", "z", "y", "x"],
        }
        axes = defaults.get(ndim, [f"dim_{i}" for i in range(ndim)])
    return [axis if isinstance(axis, dict) else {"name": str(axis).lower()} for axis in axes]


def _prepare_compressor(compressor_name: str):
    if compressor_name == "zstd":
        return numcodecs.Zstd(level=3)
    if compressor_name == "blosc":
        return numcodecs.Blosc(cname="zstd", clevel=3, shuffle=numcodecs.Blosc.SHUFFLE)
    if compressor_name == "lz4":
        return numcodecs.LZ4(acceleration=1)
    if compressor_name == "none":
        return None
    return numcodecs.Zstd(level=3)


def _normalize_output_path(output_path: str) -> str:
    raw = str(output_path or "").strip().strip('"').strip("'")
    if not raw:
        raise ValueError("OMEZarrWriter output_path cannot be empty.")
    if "\x00" in raw:
        raise ValueError("OMEZarrWriter output_path contains a null byte.")
    path = Path(raw).expanduser()
    if path.suffix.lower() != ".zarr":
        path = path.with_suffix(path.suffix + ".zarr") if path.suffix else Path(f"{path}.zarr")
    return str(path.resolve())


def _make_temp_output_path(final_path: str) -> str:
    final = Path(final_path)
    final_name = final.name.rstrip("/") or "output.zarr"
    parent = final.parent
    for _ in range(20):
        candidate = parent / f".{final_name}.tmp-{uuid.uuid4().hex}.zarr"
        if not candidate.exists():
            return str(candidate)
    raise RuntimeError(f"Failed to allocate a unique temp Zarr path near {final_path}.")


def _make_backup_path(final_path: str) -> str:
    final = Path(final_path)
    for _ in range(20):
        candidate = final.parent / f".{final.name}.backup-{uuid.uuid4().hex}.zarr"
        if not candidate.exists():
            return str(candidate)
    raise RuntimeError(f"Failed to allocate a backup path near {final_path}.")


def _remove_path_best_effort(path: str | None) -> bool:
    if not path:
        return True
    target = Path(path)
    if not target.exists():
        return True
    try:
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        else:
            target.unlink()
        return True
    except Exception as exc:
        logger.warning("[ZarrWriter] Failed to remove path %s: %s", path, exc)
        return False


def _init_zarr_store(abs_path, shape, chunks, dtype, compressor):
    path = Path(abs_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        shutil.rmtree(path)
    group = zarr.open_group(str(path), mode="w")
    group.create_dataset(
        "0",
        shape=tuple(int(x) for x in shape),
        chunks=tuple(int(x) for x in chunks),
        dtype=np.dtype(dtype),
        compressor=compressor,
        overwrite=True,
    )
    logger.info("[ZarrWriter] Store initialized: %s", path)
    return str(path)


def _finalize_store(abs_path, ndim, metadata):
    group = zarr.open(abs_path, mode="r+")
    axes = _normalize_axes_for_ngff(metadata.get("axes") if metadata else None, ndim)
    voxel_size = [1.0] * ndim
    if metadata and metadata.get("voxel_size") and len(metadata["voxel_size"]) == ndim:
        voxel_size = metadata["voxel_size"]

    group.attrs["multiscales"] = [{
        "version": "0.4",
        "name": "processed",
        "datasets": [{
            "path": "0",
            "coordinateTransformations": [{"type": "scale", "scale": voxel_size}],
        }],
        "axes": axes,
        "type": "gaussian",
    }]
    return abs_path


def _replace_final_with_temp(temp_path: str, final_path: str, overwrite: bool = True) -> str:
    temp = Path(temp_path)
    final = Path(final_path)
    if not temp.exists():
        raise FileNotFoundError(f"Temporary Zarr output does not exist: {temp}")

    final.parent.mkdir(parents=True, exist_ok=True)
    backup_path = None
    if final.exists():
        if not overwrite:
            raise FileExistsError(f"Output path already exists: {final}")
        backup_path = Path(_make_backup_path(str(final)))
        final.rename(backup_path)

    try:
        temp.rename(final)
    except Exception as exc:
        if backup_path and backup_path.exists() and not final.exists():
            backup_path.rename(final)
        raise RuntimeError(
            f"Failed to move finalized temp store into place. final={final} temp={temp}: {exc}"
        ) from exc

    if backup_path and backup_path.exists():
        _remove_path_best_effort(str(backup_path))
    return str(final)


def _write_token_block(block, init_token, temp_path, dataset_path, origin):
    target = zarr.open(temp_path, mode="r+")[dataset_path]
    region = tuple(slice(int(start), int(start) + int(length)) for start, length in zip(origin, block.shape))
    target[region] = block
    return np.ones((1,), dtype=np.uint8)


def _build_write_token_array(dask_arr, init_token, temp_path, dataset_path, origins_per_dim):
    delayed_blocks = np.asarray(dask_arr.to_delayed(), dtype=object)
    token_arrays = []
    for block_index in np.ndindex(*delayed_blocks.shape):
        origin = tuple(int(origins_per_dim[axis][block_index[axis]]) for axis in range(len(block_index)))
        token = dask.delayed(_write_token_block)(
            delayed_blocks[block_index],
            init_token,
            temp_path,
            dataset_path,
            origin,
        )
        token_arrays.append(da.from_delayed(token, shape=(1,), dtype=np.uint8))
    if not token_arrays:
        return da.from_array(np.zeros((0,), dtype=np.uint8), chunks=(1,))
    return da.concatenate(token_arrays, axis=0)


@register_node("OMEZarrWriter")
class OMEZarrWriter:
    """
    OME-Zarr sink node.

    GraphBuilding creates a lazy init/write token graph only. The temp store is
    created when the executor computes the returned uint8 token Dask Array.
    postprocess finalizes metadata and atomically moves temp output to final
    output only after all write tokens succeed. cleanup removes temp output on
    failure/cancellation.
    """

    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "OME-Zarr Writer (Save)"
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "output_path": ("STRING", {"default": "output.zarr", "multiline": False}),
                "compressor_name": (["default", "zstd", "blosc", "lz4", "none"],),
            },
            "optional": {
                "metadata": ("DICT",),
                "overwrite": ("BOOLEAN", {"default": True}),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint8]",)
    RETURN_NAMES = ("write_tokens",)
    FUNCTION = "save_zarr"

    def save_zarr(
        self,
        dask_arr,
        output_path="output.zarr",
        compressor_name="default",
        metadata=None,
        overwrite=True,
        **kwargs,
    ):
        final_path = _normalize_output_path(output_path)
        temp_path = _make_temp_output_path(final_path)
        compressor = _prepare_compressor(compressor_name or "default")
        nominal_chunks = tuple(int(c) for c in dask_arr.chunksize)
        origins_per_dim = _chunk_origins_from_chunks(dask_arr.chunks)
        metadata = metadata if isinstance(metadata, dict) else None

        _validate_regular_chunks_for_region_writes(dask_arr.chunks)

        init_token = dask.delayed(_init_zarr_store)(
            temp_path,
            tuple(dask_arr.shape),
            nominal_chunks,
            str(np.dtype(dask_arr.dtype)),
            compressor,
        )
        tokens = _build_write_token_array(dask_arr, init_token, temp_path, "0", origins_per_dim)

        self._writer_state = {
            "temp_path": temp_path,
            "final_path": final_path,
            "ndim": int(dask_arr.ndim),
            "metadata": metadata,
            "overwrite": bool(overwrite),
        }
        self._preprocess_state = self._writer_state

        logger.info(
            "[ZarrWriter] lazy plan final=%s temp=%s shape=%s chunks=%s numblocks=%s",
            final_path,
            temp_path,
            tuple(dask_arr.shape),
            dask_arr.chunks,
            tuple(dask_arr.numblocks),
        )
        return (tokens,)

    def postprocess(self, outputs=None, state=None, runtime=None, **kwargs):
        writer_state = state or getattr(self, "_writer_state", None)
        if not writer_state:
            raise RuntimeError("OMEZarrWriter has no writer state to finalize.")

        temp_path = writer_state["temp_path"]
        final_path = writer_state["final_path"]
        _finalize_store(temp_path, writer_state["ndim"], writer_state.get("metadata"))
        return _replace_final_with_temp(temp_path, final_path, overwrite=writer_state.get("overwrite", True))

    def cleanup(self):
        writer_state = getattr(self, "_writer_state", None)
        if writer_state:
            _remove_path_best_effort(writer_state.get("temp_path"))
