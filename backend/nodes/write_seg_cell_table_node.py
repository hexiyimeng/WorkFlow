"""Overlap-aware cell table writer for globally-consistent label masks.

Consumes the merged uint64 mask of process_segmentation (or any mask whose
label values are already globally unique).  Each block reads its core chunk
plus a halo of neighbor data, so a cross-boundary cell is measured whole
whenever it fits inside core+2*halo.  A cell is recorded by the block whose
core contains its bbox center, which keeps the table free of duplicates
without any global pass.  Cells extending past the halo may be recorded by
several blocks as partial rows sharing one global_cell_id; deduplicate by
global_cell_id and union the bboxes (rows flag this via
touches_tile_boundary).
"""

from __future__ import annotations

import itertools
import json
import logging
import shutil
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from core.execution_paths import normalize_execution_path
from core.label_stitching import flatten_block_index
from core.registry import register_node
from nodes.base import BaseMapOverlapNode
from nodes.cellpose_segmentation_node import SEG_SPATIAL_AXES


logger = logging.getLogger("WorkFlow.WriteSegCellTable")

SEG_TABLE_METADATA_FILENAME = "_workflow_seg_cell_table.json"


def _seg_cell_table_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("global_cell_id", pa.uint64()),
        pa.field("cell_id_str", pa.string()),
        pa.field("source_block_id", pa.uint64()),
        pa.field("block_z", pa.uint32()),
        pa.field("block_y", pa.uint32()),
        pa.field("block_x", pa.uint32()),
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
        pa.field("touches_tile_boundary", pa.bool_()),
    ])


def _write_seg_cell_table_fragment(
    output_dir: str,
    block_index: tuple[int, ...],
    rows: list[dict[str, Any]],
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    fragment_dir = Path(output_dir) / "fragments"
    fragment_dir.mkdir(parents=True, exist_ok=True)
    name = "block_" + "_".join(str(int(index)) for index in block_index) + ".parquet"
    path = fragment_dir / name
    schema = _seg_cell_table_schema()
    table = pa.Table.from_arrays(
        [
            pa.array([row.get(field.name) for row in rows], type=field.type)
            for field in schema
        ],
        schema=schema,
    )
    tmp = path.with_name(f".{path.name}.tmp")
    if tmp.exists():
        tmp.unlink()
    pq.write_table(table, tmp, compression="zstd", write_statistics=True)
    tmp.replace(path)


def _write_seg_table_dataset_metadata(
    output_path: Path,
    *,
    axes: tuple[str, ...],
    depth: Mapping[str, int],
) -> None:
    payload = {
        "format": "workflow-seg-cell-table",
        "schemaVersion": 1,
        "axes": [str(axis) for axis in axes],
        "halo_depth": {str(k): int(v) for k, v in depth.items()},
        "canonical_id": "mask label values are already globally unique",
        "dedupe": (
            "one row per cell in the block whose core contains its bbox "
            "center; cells extending past the halo may appear in several "
            "fragments with the same global_cell_id and partial "
            "touches_tile_boundary=true bboxes - deduplicate by "
            "global_cell_id and union the bboxes"
        ),
    }
    tmp = output_path / f".{SEG_TABLE_METADATA_FILENAME}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(output_path / SEG_TABLE_METADATA_FILENAME)


def _collect_seg_cell_rows(
    view: np.ndarray,
    labels: np.ndarray,
    inverse: np.ndarray,
    *,
    spatial_names: tuple[str, ...],
    view_origin: tuple[int, ...],
    core_slices: tuple[slice, ...],
    core_lo: tuple[int, ...],
    core_hi: tuple[int, ...],
    block_index: tuple[int, ...],
    spatial_axes: tuple[int, ...],
    flat_block_id: int,
) -> list[dict[str, Any]]:
    """One row per label whose bbox center falls inside this block's core.

    Label values are the final global ids of the merged mask, so no
    namespacing or matching is needed here.
    """

    from scipy.ndimage import find_objects

    ndim = int(view.ndim)
    rows: list[dict[str, Any]] = []
    for position, slices in enumerate(find_objects(inverse, len(labels)), start=1):
        if slices is None:
            continue
        region = inverse[slices] == position
        area = int(np.count_nonzero(region))
        if area == 0:
            continue
        touches = False
        mins: list[int] = []
        maxs: list[int] = []
        centroids: list[float] = []
        for k in range(ndim):
            lo = int(slices[k].start) + int(view_origin[k])
            hi = int(slices[k].stop) + int(view_origin[k])
            mins.append(lo)
            maxs.append(hi - 1)
            if slices[k].start == 0 and core_slices[k].start > 0:
                touches = True
            if slices[k].stop == int(view.shape[k]) and core_slices[k].stop < int(view.shape[k]):
                touches = True
            reduce_axes = tuple(j for j in range(ndim) if j != k)
            if reduce_axes:
                counts = region.sum(axis=reduce_axes, dtype=np.int64)
            else:
                counts = region.astype(np.int64, copy=False)
            positions = np.arange(lo, hi, dtype=np.float64)
            centroids.append(float(np.dot(positions, counts) / area))
        center = [(mins[k] + maxs[k]) / 2.0 for k in range(ndim)]
        if not all(
            int(core_lo[k]) <= center[k] < int(core_hi[k]) for k in range(ndim)
        ):
            continue
        global_id = int(labels[position - 1])
        row: dict[str, Any] = {
            "global_cell_id": global_id,
            "cell_id_str": f"g{global_id:016x}",
            "source_block_id": int(flat_block_id),
            "area_or_volume": int(area),
            "touches_tile_boundary": bool(touches),
        }
        for name in SEG_SPATIAL_AXES:
            lower = name.lower()
            if name in spatial_names:
                k = spatial_names.index(name)
                row[f"block_{lower}"] = int(block_index[spatial_axes[k]])
                row[f"centroid_{lower}"] = float(centroids[k])
                row[f"bbox_{lower}_min"] = int(mins[k])
                row[f"bbox_{lower}_max"] = int(maxs[k])
            else:
                row[f"block_{lower}"] = 0
                row[f"centroid_{lower}"] = 0.0
                row[f"bbox_{lower}_min"] = 0
                row[f"bbox_{lower}_max"] = 0
        rows.append(row)
    return rows


def write_seg_cell_table_block(mask: np.ndarray, ctx=None) -> np.ndarray:
    if ctx is None:
        raise RuntimeError("write_seg_cell_table block requires a BlockContext.")
    resources = ctx.resources or {}
    axes = tuple(str(axis).upper() for axis in (resources.get("axes") or ()))
    if len(axes) != int(mask.ndim):
        raise ValueError(
            f"write_seg_cell_table axes {axes!r} length does not match mask ndim={mask.ndim}."
        )
    depth_by_name = {
        str(axis).upper(): int(depth)
        for axis, depth in (resources.get("halo_depth") or {}).items()
    }
    chunk_starts = resources.get("chunk_starts") or ()
    output_dir = str(resources.get("output_dir") or "")
    if not output_dir:
        raise RuntimeError("write_seg_cell_table requires output_dir in resources.")

    block_index = tuple(int(value) for value in (ctx.block_locations[0] or ()))
    mask_info = ctx.block_info.get(0) if isinstance(ctx.block_info, dict) else None
    numblocks = (
        tuple(int(value) for value in mask_info.get("num-chunks"))
        if isinstance(mask_info, dict) and mask_info.get("num-chunks")
        else ()
    )
    if len(block_index) != mask.ndim or len(numblocks) != mask.ndim:
        raise RuntimeError("write_seg_cell_table requires block location metadata.")
    if len(chunk_starts) != mask.ndim:
        raise RuntimeError("write_seg_cell_table requires chunk_starts in resources.")

    spatial_axes = tuple(
        index for index, axis in enumerate(axes) if axis in SEG_SPATIAL_AXES
    )
    if not spatial_axes:
        raise ValueError(f"write_seg_cell_table requires spatial axes, got {axes!r}.")
    batch_axes = tuple(
        index for index in range(mask.ndim) if index not in spatial_axes
    )
    flat_block_id = flatten_block_index(block_index, numblocks)

    batch_shape = tuple(int(mask.shape[index]) for index in batch_axes)
    batch_coords = (
        itertools.product(*(range(size) for size in batch_shape))
        if batch_axes
        else [()]
    )
    spatial_names = tuple(axes[index] for index in spatial_axes)

    rows: list[dict[str, Any]] = []
    for batch_coord in batch_coords:
        selection = [slice(None)] * mask.ndim
        for index, value in zip(batch_axes, batch_coord):
            selection[index] = int(value)
        sub = np.asarray(mask[tuple(selection)])
        nsp = len(spatial_axes)
        view_origin: list[int] = []
        core_slices: list[slice] = []
        core_lo: list[int] = []
        core_hi: list[int] = []
        for k in range(nsp):
            axis_index = spatial_axes[k]
            depth = depth_by_name.get(axes[axis_index], 0)
            left = depth if block_index[axis_index] > 0 else 0
            right = depth if block_index[axis_index] < numblocks[axis_index] - 1 else 0
            start = int(chunk_starts[axis_index][block_index[axis_index]])
            view_origin.append(start - left)
            core_slices.append(slice(left, int(sub.shape[k]) - right))
            core_lo.append(start)
            core_hi.append(start + int(sub.shape[k]) - left - right)

        labels, inverse = np.unique(sub, return_inverse=True)
        inverse = inverse.reshape(sub.shape)
        nonzero = labels != 0
        rows.extend(
            _collect_seg_cell_rows(
                sub,
                labels[nonzero],
                _dense_label_image(sub, inverse, nonzero),
                spatial_names=spatial_names,
                view_origin=tuple(view_origin),
                core_slices=tuple(core_slices),
                core_lo=tuple(core_lo),
                core_hi=tuple(core_hi),
                block_index=block_index,
                spatial_axes=spatial_axes,
                flat_block_id=flat_block_id,
            )
        )

    _write_seg_cell_table_fragment(output_dir, block_index, rows)
    return np.ones((1,) * int(mask.ndim), dtype=np.uint8)


def _dense_label_image(
    view: np.ndarray,
    inverse: np.ndarray,
    nonzero: np.ndarray,
) -> np.ndarray:
    """Map raw label values to 1..N positions into the nonzero label list."""

    dense = np.zeros(view.shape, dtype=np.int64)
    foreground = view != 0
    # positions within labels[]; shift so 1..N index into the nonzero labels
    shift = int(np.count_nonzero(~nonzero))
    dense[foreground] = inverse.reshape(view.shape)[foreground] - shift + 1
    return dense


@register_node("write_seg_cell_table")
class WriteSegCellTable(BaseMapOverlapNode):
    """Write per-block Parquet fragments of cell bounding boxes for a mask.

    Intended for the merged output of process_segmentation, whose label
    values are already globally unique.  The overlap halo lets each block
    measure cross-boundary cells whole; ownership by bbox center keeps the
    table duplicate-free.
    """

    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "Write Segmentation Cell Table"
    required_worker_profile = "CPU"
    OUTPUT_NODE = True
    OUTPUT_PATH_INPUT = "output_dir"

    MAP_INPUTS = ["mask"]
    PRIMARY_INPUT = "mask"
    PROCESS_BLOCK = write_seg_cell_table_block
    ARRAY_AXES_BY_NDIM = {
        "mask": {
            2: ("Y", "X"),
            3: ("Z", "Y", "X"),
            4: ("T", "Z", "Y", "X"),
        }
    }

    MAP_OVERLAP_SPEC = {
        "depth": {},
        "boundary": "none",
        "trim": False,
        "align_arrays": True,
        "allow_rechunk": True,
    }
    ALLOW_UNTRIMMED_OVERLAP_OUTPUT = True

    @staticmethod
    def _requested_depths(params: Mapping[str, Any] | None) -> dict[str, int]:
        params = params or {}
        return {
            "Z": max(0, int(params.get("overlap_z", 30))),
            "Y": max(0, int(params.get("overlap_y", 30))),
            "X": max(0, int(params.get("overlap_x", 30))),
        }

    def _mask_axes(self) -> tuple[str, ...]:
        return tuple(
            str(axis).upper()
            for axis in ((self._axes_by_name or {}).get(self.PRIMARY_INPUT) or ())
        )

    def _normalized_chunks(self, primary, axes, depths) -> tuple[tuple[int, ...], ...]:
        """Chunk layout dask's overlap will actually use (rechunk rule)."""

        from dask.array.overlap import ensure_minimum_chunksize

        normalized = []
        for index, axis in enumerate(axes):
            depth = int(depths.get(axis, 0))
            axis_chunks = tuple(int(chunk) for chunk in primary.chunks[index])
            if depth > 0:
                axis_chunks = ensure_minimum_chunksize(depth, axis_chunks)
            normalized.append(tuple(int(chunk) for chunk in axis_chunks))
        return tuple(normalized)

    def infer_overlap_spec(
        self,
        *,
        array_inputs,
        ordered_names,
        primary_name,
        params,
        runtime,
    ):
        del ordered_names, runtime
        axes = self._mask_axes()
        depths = self._requested_depths(params)
        primary = array_inputs[primary_name]
        depth = {}
        for index, axis in enumerate(axes):
            requested = int(depths.get(axis, 0))
            if requested > 0:
                requested = min(requested, int(primary.shape[index]))
            depth[index] = requested
        return {**self.MAP_OVERLAP_SPEC, "depth": depth}

    def infer_output_spec(self, array_inputs, params, primary_name):
        primary = array_inputs[primary_name]
        axes = self._mask_axes()
        depths = self._requested_depths(params)
        depth_by_name = {
            axis: min(int(depths.get(axis, 0)), int(primary.shape[index]))
            for index, axis in enumerate(axes)
        }
        normalized = self._normalized_chunks(primary, axes, depth_by_name)
        return {
            "dtype": "uint8",
            "chunks": tuple((1,) * len(axis_chunks) for axis_chunks in normalized),
            "enforce_ndim": True,
        }

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
            raise ValueError("write_seg_cell_table expects a mask Dask Array, got None.")
        if np.dtype(mask.dtype) not in {np.dtype(np.uint32), np.dtype(np.uint64)}:
            raise ValueError(
                f"write_seg_cell_table requires a uint32 or uint64 mask, got {mask.dtype}."
            )
        params = params or {}
        runtime = runtime or {}
        axes = self._mask_axes()
        if len(axes) != int(mask.ndim):
            raise ValueError(
                f"write_seg_cell_table axes {axes!r} length does not match mask ndim={mask.ndim}."
            )
        if "Y" not in axes or "X" not in axes:
            raise ValueError(f"write_seg_cell_table requires Y and X axes, got {axes!r}.")
        depths = self._requested_depths(params)
        depths = {
            axis: min(int(depths.get(axis, 0)), int(mask.shape[index]))
            for index, axis in enumerate(axes)
            if axis in SEG_SPATIAL_AXES
        }
        normalized = self._normalized_chunks(mask, axes, depths)
        chunk_starts = tuple(
            tuple(int(value) for value in np.cumsum((0, *axis_chunks[:-1]), dtype=np.int64))
            for axis_chunks in normalized
        )

        output_dir = normalize_execution_path(
            params.get("output_dir", ""), name="output_dir"
        )
        output_path = Path(output_dir)
        if bool(runtime.get("is_resuming", False)):
            if not output_path.is_dir():
                raise FileNotFoundError(
                    "Cannot resume write_seg_cell_table because the output "
                    f"directory does not exist: {output_path}"
                )
        else:
            if output_path.exists():
                if not bool(params.get("overwrite", True)):
                    raise FileExistsError(
                        "write_seg_cell_table output directory already exists "
                        f"and overwrite is disabled: {output_path}"
                    )
                shutil.rmtree(output_path)
            (output_path / "fragments").mkdir(parents=True, exist_ok=True)
            _write_seg_table_dataset_metadata(
                output_path,
                axes=axes,
                depth=depths,
            )

        return {
            "axes": axes,
            "halo_depth": {axis: int(depth) for axis, depth in depths.items()},
            "chunk_starts": chunk_starts,
            "output_dir": output_dir,
        }

    @staticmethod
    def validate_output_path(value: str) -> str:
        return normalize_execution_path(value, name="output_dir")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mask": ("DASK_ARRAY[any]",),
                "output_dir": ("STRING", {"default": "", "multiline": False}),
            },
            "optional": {
                "overlap_z": ("INT", {"default": 30, "min": 0, "max": 4096}),
                "overlap_y": ("INT", {"default": 30, "min": 0, "max": 16384}),
                "overlap_x": ("INT", {"default": 30, "min": 0, "max": 16384}),
                "overwrite": ("BOOLEAN", {"default": True}),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint8]",)
    RETURN_NAMES = ("write_tokens",)
