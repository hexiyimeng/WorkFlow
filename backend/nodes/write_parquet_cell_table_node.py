from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from core.registry import register_node
from nodes.base import BaseMapBlocksNode


TOKEN_DTYPE = np.dtype("uint8")
SPATIAL_AXES = ("Z", "Y", "X")
LOCAL_ID_BITS = 24
TILE_Z_BITS = 10
TILE_Y_BITS = 15
TILE_X_BITS = 15
CELL_LOCAL_ID_WIDTH = 6


def _normalize_path(value: str, *, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a literal string.")
    raw = value.strip()
    if not raw:
        raise ValueError(f"{name} cannot be empty.")
    if "\x00" in raw:
        raise ValueError(f"{name} contains a null byte.")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path.")
    return str(path.resolve())


def _block_id(block_index: tuple[int, ...]) -> str:
    return "scalar" if not block_index else "_".join(str(int(i)) for i in block_index)


def _spatial_axis_indices(axes: tuple[str, ...]) -> dict[str, int | None]:
    return {axis: axes.index(axis) if axis in axes else None for axis in SPATIAL_AXES}


def _spatial_values_from_tuple(values: tuple[int, ...], axes: tuple[str, ...], default: int = 0) -> dict[str, int]:
    indices = _spatial_axis_indices(axes)
    result = {}
    for axis in SPATIAL_AXES:
        index = indices[axis]
        result[axis] = int(values[index]) if index is not None and index < len(values) else int(default)
    return result


def _block_axis_widths(numblocks: tuple[int, ...], axes: tuple[str, ...]) -> dict[str, int]:
    spatial_numblocks = _spatial_values_from_tuple(numblocks, axes, default=1)
    return {
        axis: max(2, len(str(max(0, int(spatial_numblocks[axis]) - 1))))
        for axis in SPATIAL_AXES
    }


def _spatial_block_prefix(
    block_index: tuple[int, ...],
    numblocks: tuple[int, ...],
    axes: tuple[str, ...],
) -> str:
    spatial_block = _spatial_values_from_tuple(block_index, axes)
    widths = _block_axis_widths(numblocks, axes)
    return "".join(
        f"{spatial_block[axis]:0{widths[axis]}d}"
        for axis in SPATIAL_AXES
    )


def _non_spatial_block_prefix(block_index: tuple[int, ...], axes: tuple[str, ...]) -> str:
    parts = [
        f"{axis.lower()}{int(block_index[index]):02d}"
        for index, axis in enumerate(axes)
        if axis not in SPATIAL_AXES
    ]
    return "_".join(parts)


def _flatten_block_index(block_index: tuple[int, ...], numblocks: tuple[int, ...]) -> int:
    if not block_index:
        return 0
    flat = 0
    stride = 1
    for index, axis_blocks in zip(reversed(block_index), reversed(numblocks)):
        flat += int(index) * stride
        stride *= int(axis_blocks)
    return int(flat)


def _local_id_config(total_blocks: int) -> tuple[int, int]:
    block_bits = max(0, int(total_blocks - 1).bit_length())
    if block_bits >= LOCAL_ID_BITS:
        raise ValueError(
            f"WriteParquetCellTable cannot encode {total_blocks} blocks into the "
            f"{LOCAL_ID_BITS}-bit local id budget."
        )
    return block_bits, LOCAL_ID_BITS - block_bits


def _encode_local_id(block_flat_id: int, ordinal: int, *, ordinal_bits: int) -> int:
    max_ordinal = (1 << ordinal_bits) - 1
    if ordinal < 1 or ordinal > max_ordinal:
        raise ValueError(
            f"Block has local cell ordinal {ordinal}, but only {max_ordinal} ordinals fit in "
            f"{ordinal_bits} bits. Rechunk the mask or use smaller blocks."
        )
    return (int(block_flat_id) << ordinal_bits) | int(ordinal)


def _encode_spatial_key(tile_z: int, tile_y: int, tile_x: int) -> int:
    max_z = (1 << TILE_Z_BITS) - 1
    max_y = (1 << TILE_Y_BITS) - 1
    max_x = (1 << TILE_X_BITS) - 1
    if not (0 <= tile_z <= max_z and 0 <= tile_y <= max_y and 0 <= tile_x <= max_x):
        raise ValueError(
            "Tile coordinate exceeds uint64 global id budget: "
            f"tile_z={tile_z} max={max_z}, tile_y={tile_y} max={max_y}, tile_x={tile_x} max={max_x}."
        )
    return (int(tile_z) << (TILE_Y_BITS + TILE_X_BITS)) | (int(tile_y) << TILE_X_BITS) | int(tile_x)


def _encode_global_cell_id(spatial_key: int, local_id: int) -> int:
    return (int(spatial_key) << LOCAL_ID_BITS) | int(local_id)


def _parquet_schema(label_dtype: np.dtype):
    import pyarrow as pa

    label_type = pa.uint64() if np.dtype(label_dtype) == np.dtype("uint64") else pa.uint32()
    return pa.schema([
        pa.field("global_cell_id", pa.uint64()),
        pa.field("cell_id_str", pa.string()),
        pa.field("spatial_key", pa.uint64()),
        pa.field("source_block_id", pa.uint64()),
        pa.field("block_z", pa.uint32()),
        pa.field("block_y", pa.uint32()),
        pa.field("block_x", pa.uint32()),
        pa.field("tile_z", pa.uint32()),
        pa.field("tile_y", pa.uint32()),
        pa.field("tile_x", pa.uint32()),
        pa.field("local_ordinal", pa.uint32()),
        pa.field("label", label_type),
        pa.field("centroid_z", pa.float32()),
        pa.field("centroid_y", pa.float32()),
        pa.field("centroid_x", pa.float32()),
        pa.field("bbox_z_min", pa.uint32()),
        pa.field("bbox_z_max", pa.uint32()),
        pa.field("bbox_y_min", pa.uint32()),
        pa.field("bbox_y_max", pa.uint32()),
        pa.field("bbox_x_min", pa.uint32()),
        pa.field("bbox_x_max", pa.uint32()),
        pa.field("area_or_volume", pa.uint64()),
        pa.field("touches_block_boundary", pa.bool_()),
    ])


def _metadata_is_complete(parquet_path: Path, metadata_path: Path, write_metadata: bool) -> bool:
    if not parquet_path.exists():
        return False
    if not write_metadata:
        return True
    if not metadata_path.exists():
        return False
    try:
        data = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return bool(data.get("fragment_path") and data.get("schema") and data.get("block_index") is not None)


def _block_output_paths(
    output_dir: str,
    axes: tuple[str, ...],
    block_index: tuple[int, ...],
    numblocks: tuple[int, ...],
) -> tuple[Path, Path]:
    spatial_block = _spatial_values_from_tuple(block_index, axes)
    spatial_prefix = _spatial_block_prefix(block_index, numblocks, axes)
    non_spatial_prefix = _non_spatial_block_prefix(block_index, axes)
    directory = (
        Path(output_dir)
        / "fragments"
        / f"block_z={spatial_block['Z']:04d}"
        / f"block_y={spatial_block['Y']:04d}"
        / f"block_x={spatial_block['X']:04d}"
    )
    file_stem = f"{spatial_prefix}cell"
    if non_spatial_prefix:
        file_stem = f"{non_spatial_prefix}_{file_stem}"
    return directory / f"{file_stem}.parquet", directory / f"{file_stem}.json"


def _extract_rows_from_mask(
    mask_block: np.ndarray,
    *,
    axes: tuple[str, ...],
    origin: tuple[int, ...],
    block_index: tuple[int, ...],
    numblocks: tuple[int, ...],
    tile_sizes: Mapping[str, int],
    ordinal_bits: int,
) -> list[dict[str, Any]]:
    if mask_block.size == 0:
        return []

    nonzero_coords = np.nonzero(mask_block > 0)
    if len(nonzero_coords) == 0 or len(nonzero_coords[0]) == 0:
        return []

    labels = mask_block[nonzero_coords]
    spatial_indices = _spatial_axis_indices(axes)
    batch_indices = [idx for idx, axis in enumerate(axes) if axis not in SPATIAL_AXES]
    key_arrays = [nonzero_coords[idx] for idx in batch_indices] + [labels]
    order = np.lexsort(tuple(reversed(key_arrays))) if key_arrays else np.arange(labels.size)
    sorted_labels = labels[order]
    sorted_coords = [axis_coords[order] for axis_coords in nonzero_coords]

    group_starts = [0]
    for position in range(1, int(sorted_labels.size)):
        changed = sorted_labels[position] != sorted_labels[position - 1]
        if not changed:
            for batch_axis in batch_indices:
                if sorted_coords[batch_axis][position] != sorted_coords[batch_axis][position - 1]:
                    changed = True
                    break
        if changed:
            group_starts.append(position)
    group_starts.append(int(sorted_labels.size))

    source_block_id = _flatten_block_index(block_index, numblocks)
    spatial_block = _spatial_values_from_tuple(block_index, axes)
    spatial_prefix = _spatial_block_prefix(block_index, numblocks, axes)
    non_spatial_prefix = _non_spatial_block_prefix(block_index, axes)
    cell_prefix = f"{non_spatial_prefix}_{spatial_prefix}" if non_spatial_prefix else spatial_prefix
    rows: list[dict[str, Any]] = []

    for ordinal, (start, stop) in enumerate(zip(group_starts[:-1], group_starts[1:]), start=1):
        label = int(sorted_labels[start])
        local_id = _encode_local_id(source_block_id, ordinal, ordinal_bits=ordinal_bits)

        spatial_stats: dict[str, dict[str, int | float]] = {}
        touches = False
        for axis_name in SPATIAL_AXES:
            axis_index = spatial_indices[axis_name]
            if axis_index is None:
                spatial_stats[axis_name] = {"min": 0, "max": 0, "centroid": 0.0}
                continue
            local_values = sorted_coords[axis_index][start:stop]
            local_min = int(local_values.min())
            local_max = int(local_values.max())
            if local_min == 0 or local_max == int(mask_block.shape[axis_index]) - 1:
                touches = True
            axis_origin = int(origin[axis_index])
            spatial_stats[axis_name] = {
                "min": axis_origin + local_min,
                "max": axis_origin + local_max,
                "centroid": float(axis_origin + float(local_values.mean())),
            }

        tile_z = int(spatial_stats["Z"]["centroid"]) // int(tile_sizes["Z"])
        tile_y = int(spatial_stats["Y"]["centroid"]) // int(tile_sizes["Y"])
        tile_x = int(spatial_stats["X"]["centroid"]) // int(tile_sizes["X"])
        spatial_key = _encode_spatial_key(tile_z, tile_y, tile_x)

        row = {
            "global_cell_id": _encode_global_cell_id(spatial_key, local_id),
            "cell_id_str": f"{cell_prefix}{label:0{CELL_LOCAL_ID_WIDTH}d}",
            "spatial_key": spatial_key,
            "source_block_id": source_block_id,
            "block_z": spatial_block["Z"],
            "block_y": spatial_block["Y"],
            "block_x": spatial_block["X"],
            "tile_z": tile_z,
            "tile_y": tile_y,
            "tile_x": tile_x,
            "local_ordinal": ordinal,
            "label": label,
            "centroid_z": float(spatial_stats["Z"]["centroid"]),
            "centroid_y": float(spatial_stats["Y"]["centroid"]),
            "centroid_x": float(spatial_stats["X"]["centroid"]),
            "bbox_z_min": int(spatial_stats["Z"]["min"]),
            "bbox_z_max": int(spatial_stats["Z"]["max"]),
            "bbox_y_min": int(spatial_stats["Y"]["min"]),
            "bbox_y_max": int(spatial_stats["Y"]["max"]),
            "bbox_x_min": int(spatial_stats["X"]["min"]),
            "bbox_x_max": int(spatial_stats["X"]["max"]),
            "area_or_volume": int(stop - start),
            "touches_block_boundary": bool(touches),
        }
        rows.append(row)
    return rows


def _write_json_atomic(path: Path, data: Mapping[str, Any]) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def write_cell_table_block(mask: np.ndarray, ctx=None) -> np.ndarray:
    if ctx is None:
        raise RuntimeError("WriteParquetCellTable block requires a BlockContext.")
    resources = ctx.resources or {}
    axes = tuple(resources.get("axes") or ())
    if len(axes) != mask.ndim:
        raise ValueError(
            f"WriteParquetCellTable axes {axes!r} length does not match mask ndim={mask.ndim}."
        )
    origin = ctx.chunk_origins[0] if ctx.chunk_origins else None
    block_index = ctx.block_locations[0] if ctx.block_locations else None
    if origin is None or block_index is None:
        raise RuntimeError("WriteParquetCellTable requires block location metadata.")

    mask_info = ctx.block_info.get(0) if isinstance(ctx.block_info, dict) else None
    numblocks = tuple(mask_info.get("num-chunks") or ()) if isinstance(mask_info, dict) else ()
    if not numblocks:
        numblocks = tuple(int(x) for x in resources.get("numblocks", ()))

    output_dir = resources.get("output_dir")
    parquet_path, metadata_path = _block_output_paths(
        output_dir,
        axes,
        tuple(int(x) for x in block_index),
        tuple(int(x) for x in numblocks),
    )
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    write_metadata = bool(resources.get("write_block_metadata", True))
    if _metadata_is_complete(parquet_path, metadata_path, write_metadata):
        return np.ones(ctx.output_chunk_shape or ((1,) * int(mask.ndim)), dtype=TOKEN_DTYPE)
    if parquet_path.exists() and not bool(resources.get("overwrite", True)):
        raise FileExistsError(f"Parquet fragment exists without complete metadata: {parquet_path}")

    rows = _extract_rows_from_mask(
        mask,
        axes=axes,
        origin=tuple(int(x) for x in origin),
        block_index=tuple(int(x) for x in block_index),
        numblocks=tuple(int(x) for x in numblocks),
        tile_sizes=resources.get("tile_sizes"),
        ordinal_bits=int(resources.get("ordinal_bits")),
    )
    if bool(resources.get("sort_by_spatial_key", True)):
        rows = sorted(rows, key=lambda row: (int(row["spatial_key"]), int(row["global_cell_id"])))

    import pyarrow as pa

    schema = _parquet_schema(np.dtype(mask.dtype))
    columns = {
        field.name: [row.get(field.name) for row in rows]
        for field in schema
    }
    table = pa.Table.from_arrays(
        [
            pa.array(columns[field.name], type=field.type)
            for field in schema
        ],
        schema=schema,
    )
    tmp_path = parquet_path.with_name(f".{parquet_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    import pyarrow.parquet as pq

    pq.write_table(
        table,
        tmp_path,
        compression=resources.get("compression", "zstd"),
        row_group_size=int(resources.get("row_group_size", 100000)),
        write_statistics=True,
    )
    tmp_path.replace(parquet_path)

    if write_metadata:
        spatial_keys = [int(row["spatial_key"]) for row in rows]
        metadata = {
            "fragment_path": str(parquet_path),
            "row_count": len(rows),
            "block_id": _block_id(tuple(int(x) for x in block_index)),
            "block_index": [int(x) for x in block_index],
            "block_global_bbox": [
                {
                    "axis": axis,
                    "start": int(start),
                    "stop": int(start) + int(length),
                }
                for axis, start, length in zip(axes, origin, mask.shape)
            ],
            "spatial_key_min": min(spatial_keys) if spatial_keys else None,
            "spatial_key_max": max(spatial_keys) if spatial_keys else None,
            "schema": [field.name for field in schema],
            "compression": resources.get("compression", "zstd"),
            "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "id_encoding": {
                "spatial_key_bits": {"z": TILE_Z_BITS, "y": TILE_Y_BITS, "x": TILE_X_BITS},
                "local_id_bits": LOCAL_ID_BITS,
                "global_cell_id": "spatial_key << local_id_bits | local_id",
                "cell_id_str": (
                    "zero-padded block indices in ZYX order followed by the "
                    f"{CELL_LOCAL_ID_WIDTH}-digit block-local mask label"
                ),
            },
        }
        _write_json_atomic(metadata_path, metadata)

    return np.ones(ctx.output_chunk_shape or ((1,) * int(mask.ndim)), dtype=TOKEN_DTYPE)


@register_node("WriteParquetCellTable")
class WriteParquetCellTable(BaseMapBlocksNode):
    """Write one Parquet cell-table fragment for each mask block."""

    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "Write Parquet Cell Table"
    EXECUTION_WORKERS = 0
    OUTPUT_NODE = True
    OUTPUT_PATH_INPUT = "output_dir"
    CHUNK_POLICY = {"mode": "rechunk_to_primary"}

    MAP_INPUTS = ["mask"]
    PRIMARY_INPUT = "mask"
    PROCESS_BLOCK = write_cell_table_block
    ARRAY_AXES_BY_NDIM = {
        "mask": {
            2: ("Y", "X"),
            3: ("Z", "Y", "X"),
            4: ("T", "Z", "Y", "X"),
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
                "mask": ("DASK_ARRAY[any]",),
                "output_dir": ("STRING", {"default": "", "multiline": False}),
            },
            "optional": {
                "axes": ("STRING", {"default": "", "multiline": False}),
                "tile_z": ("INT", {"default": 16, "min": 1, "max": 1048576}),
                "tile_y": ("INT", {"default": 512, "min": 1, "max": 1048576}),
                "tile_x": ("INT", {"default": 512, "min": 1, "max": 1048576}),
                "compression": (["zstd", "snappy"], {"default": "zstd"}),
                "row_group_size": ("INT", {"default": 100000, "min": 1, "max": 10000000}),
                "overwrite": ("BOOLEAN", {"default": True}),
                "write_block_metadata": ("BOOLEAN", {"default": True}),
                "sort_by_spatial_key": ("BOOLEAN", {"default": True}),
            },
        }

    @staticmethod
    def validate_output_path(value: str) -> str:
        return _normalize_path(value, name="output_dir")

    def preprocess(
        self,
        dask_arr=None,
        array_inputs: dict | None = None,
        params: dict | None = None,
        runtime: dict | None = None,
    ) -> dict[str, Any] | None:
        array_inputs = array_inputs or {}
        mask = array_inputs["mask"] if "mask" in array_inputs else dask_arr
        if mask is None:
            raise ValueError("WriteParquetCellTable expects a mask Dask Array, got None.")
        mask_dtype = np.dtype(mask.dtype)
        if mask_dtype not in {np.dtype("uint32"), np.dtype("uint64")}:
            raise ValueError(f"WriteParquetCellTable requires a uint32 or uint64 mask, got {mask_dtype}.")

        params = params or {}
        runtime = runtime or {}
        output_dir = _normalize_path(params.get("output_dir", ""), name="output_dir")
        output_path = Path(output_dir)
        if bool(runtime.get("is_resuming", False)):
            if not output_path.exists():
                raise FileNotFoundError(
                    "Cannot resume WriteParquetCellTable because the output directory "
                    f"does not exist: {output_path}"
                )
            if not output_path.is_dir():
                raise NotADirectoryError(
                    "Cannot resume WriteParquetCellTable because the output target "
                    f"is not a directory: {output_path}"
                )
        else:
            if output_path.exists() and bool(params.get("overwrite", True)):
                shutil.rmtree(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

        total_blocks = int(np.prod(tuple(int(x) for x in mask.numblocks), dtype=np.int64)) if mask.numblocks else 1
        _, ordinal_bits = _local_id_config(total_blocks)
        compression = str(params.get("compression") or "zstd").strip().lower()
        if compression not in {"zstd", "snappy"}:
            raise ValueError(f"compression must be 'zstd' or 'snappy', got {compression!r}.")
        axes = tuple(
            str(axis).upper()
            for axis in ((getattr(self, "_axes_by_name", {}) or {}).get("mask") or ())
        )
        if len(axes) != int(mask.ndim):
            raise ValueError(
                f"WriteParquetCellTable axes {axes!r} length does not match mask ndim={mask.ndim}."
            )
        if "Y" not in axes or "X" not in axes:
            raise ValueError(f"WriteParquetCellTable requires Y and X axes, got {axes!r}.")

        return {
            "axes": axes,
            "output_dir": output_dir,
            "tile_sizes": {
                "Z": int(params.get("tile_z", 16)),
                "Y": int(params.get("tile_y", 512)),
                "X": int(params.get("tile_x", 512)),
            },
            "compression": compression,
            "row_group_size": int(params.get("row_group_size", 100000)),
            "overwrite": bool(params.get("overwrite", True)),
            "write_block_metadata": bool(params.get("write_block_metadata", True)),
            "sort_by_spatial_key": bool(params.get("sort_by_spatial_key", True)),
            "numblocks": tuple(int(x) for x in mask.numblocks),
            "ordinal_bits": ordinal_bits,
        }
