from __future__ import annotations

import os
import uuid
from pathlib import Path

import dask
import dask.array as da
import numpy as np
import pandas as pd
import scipy.ndimage as ndimage

from core.registry import register_node


COLUMNS = [
    "chunk_id", "local_id",
    "centroid_z", "centroid_y", "centroid_x",
    "voxel_count", "touches_boundary", "boundary_overlap_chunks",
    "bbox_z0", "bbox_y0", "bbox_x0",
    "bbox_z1", "bbox_y1", "bbox_x1",
]


def _default_format():
    import importlib.util

    return "parquet" if importlib.util.find_spec("pyarrow") else "csv"


def _normalize_output_dir(output_dir: str) -> str:
    raw = str(output_dir or "").strip().strip('"').strip("'")
    if not raw:
        raise ValueError("CellposeInstancePartitionWriter output_dir cannot be empty.")
    if "\x00" in raw:
        raise ValueError("CellposeInstancePartitionWriter output_dir contains a null byte.")
    return str(Path(raw).expanduser().resolve())


def _write_partition(df, path: Path, file_format: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        if file_format == "parquet":
            df.to_parquet(tmp, index=False)
        else:
            df.to_csv(tmp, index=False)
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _extract_instances(block, origin, chunk_id):
    """Extract per-instance rows from a 3D ZYX mask block."""
    if block.ndim != 3:
        raise ValueError(
            "CellposeInstancePartitionWriter supports 3D ZYX mask blocks only. "
            f"Received ndim={block.ndim}, shape={block.shape}."
        )
    if block.size == 0:
        return pd.DataFrame(columns=COLUMNS)

    ids = np.unique(block)
    ids = ids[ids > 0]
    if len(ids) == 0:
        return pd.DataFrame(columns=COLUMNS)

    slices = ndimage.find_objects(block)
    shape = block.shape
    cur = [int(x) for x in chunk_id.split("_")]
    rows = []

    for inst_id in ids:
        idx = int(inst_id)
        if idx - 1 >= len(slices) or slices[idx - 1] is None:
            continue
        sl = slices[idx - 1]
        mask = block[sl] == idx
        voxels = int(mask.sum())
        if voxels == 0:
            continue

        com = ndimage.center_of_mass(mask)
        cz = float(origin[0] + sl[0].start + com[0])
        cy = float(origin[1] + sl[1].start + com[1])
        cx = float(origin[2] + sl[2].start + com[2])

        touches = (
            sl[0].start == 0 or sl[0].stop == shape[0]
            or sl[1].start == 0 or sl[1].stop == shape[1]
            or sl[2].start == 0 or sl[2].stop == shape[2]
        )

        neighbors = []
        if touches:
            for axis in range(3):
                if sl[axis].start == 0 and cur[axis] > 0:
                    n = list(cur)
                    n[axis] -= 1
                    neighbors.append("_".join(str(x) for x in n))
                if sl[axis].stop == shape[axis]:
                    n = list(cur)
                    n[axis] += 1
                    neighbors.append("_".join(str(x) for x in n))

        rows.append({
            "chunk_id": chunk_id,
            "local_id": idx,
            "centroid_z": cz,
            "centroid_y": cy,
            "centroid_x": cx,
            "voxel_count": voxels,
            "touches_boundary": bool(touches),
            "boundary_overlap_chunks": ",".join(neighbors),
            "bbox_z0": int(origin[0] + sl[0].start),
            "bbox_y0": int(origin[1] + sl[1].start),
            "bbox_x0": int(origin[2] + sl[2].start),
            "bbox_z1": int(origin[0] + sl[0].stop - 1),
            "bbox_y1": int(origin[1] + sl[1].stop - 1),
            "bbox_x1": int(origin[2] + sl[2].stop - 1),
        })

    return pd.DataFrame(rows, columns=COLUMNS) if rows else pd.DataFrame(columns=COLUMNS)


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


def _write_partition_block(mask_block, output_dir, file_format, write_empty_partitions, origin, chunk_id):
    df = _extract_instances(mask_block, origin, chunk_id)
    if len(df) > 0 or write_empty_partitions:
        ext = "parquet" if file_format == "parquet" else "csv"
        _write_partition(df, Path(output_dir) / f"part_{chunk_id}.{ext}", file_format)
    return np.ones((1,), dtype=np.uint8)


def _build_partition_tokens(dask_arr, output_dir, file_format, write_empty_partitions):
    delayed_blocks = np.asarray(dask_arr.to_delayed(), dtype=object)
    origins_per_dim = _chunk_origins_from_chunks(dask_arr.chunks)
    token_arrays = []
    for block_index in np.ndindex(*delayed_blocks.shape):
        origin = tuple(int(origins_per_dim[axis][block_index[axis]]) for axis in range(3))
        chunk_id = "_".join(str(int(x)) for x in block_index)
        token = dask.delayed(_write_partition_block)(
            delayed_blocks[block_index],
            output_dir,
            file_format,
            bool(write_empty_partitions),
            origin,
            chunk_id,
        )
        token_arrays.append(da.from_delayed(token, shape=(1,), dtype=np.uint8))
    if not token_arrays:
        return da.from_array(np.zeros((0,), dtype=np.uint8), chunks=(1,))
    return da.concatenate(token_arrays, axis=0)


@register_node("CellposeInstancePartitionWriter")
class CellposeInstancePartitionWriter:
    """
    OUTPUT_NODE sink that writes one instance-table partition per 3D ZYX mask block.

    GraphBuilding validates configuration and returns a tiny uint8 token Dask
    Array. Partition files are written only when the executor computes the token
    collection. Existing completed partition files are replaced atomically.
    """

    CATEGORY = "WorkFlow/Instance"
    DISPLAY_NAME = "Cellpose Instance Partition Writer"
    OUTPUT_NODE = True
    FUNCTION = "write_partitions"
    RETURN_TYPES = ("DASK_ARRAY[uint8]",)
    RETURN_NAMES = ("write_tokens",)

    @classmethod
    def INPUT_TYPES(cls):
        fmt = _default_format()
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "output_dir": ("STRING", {"default": "instance_partitions", "multiline": False}),
            },
            "optional": {
                "file_format": (["parquet", "csv"], {"default": fmt}),
                "write_empty_partitions": ("BOOLEAN", {"default": True}),
            },
        }

    def write_partitions(
        self,
        dask_arr,
        output_dir="instance_partitions",
        file_format=None,
        write_empty_partitions=True,
        **kwargs,
    ):
        if int(dask_arr.ndim) != 3:
            raise ValueError(
                "CellposeInstancePartitionWriter supports 3D ZYX mask arrays only. "
                f"Received ndim={dask_arr.ndim}, shape={dask_arr.shape}."
            )
        output_dir = _normalize_output_dir(output_dir)
        file_format = file_format or _default_format()
        if file_format not in {"parquet", "csv"}:
            raise ValueError(f"Unsupported partition file_format {file_format!r}.")
        self._partition_state = {"output_dir": output_dir}
        return (_build_partition_tokens(dask_arr, output_dir, file_format, write_empty_partitions),)

    def cleanup(self):
        state = getattr(self, "_partition_state", None) or {}
        output_dir = state.get("output_dir")
        if not output_dir:
            return
        path = Path(output_dir)
        if not path.exists():
            return
        for tmp in path.glob(".part_*.tmp-*"):
            try:
                tmp.unlink()
            except Exception:
                pass
