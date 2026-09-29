"""Local overlap-merge node for untrimmed Cellpose masks.

Consumes the halo-preserving output of cellpose_segmentation.  Because every
seam region exists twice in that array (once in each neighboring block's own
labeling), each block can compare the duplicated face/edge/corner slabs
inside its own overlap view, match equivalent labels with a mutual-best
overlap vote, and rewrite every block-local label to the component-minimum
global id.  The merge is fully block-local and needs no global pass: a cell
smaller than a block spans at most two blocks per axis, so all of its
portions are pairwise compared directly and receive the same id on both
sides of every boundary it crosses.  Each block then trims back to its
original core chunk, so the merged mask has the exact shape and chunking of
the source image.

Cell bounding-box recording lives in the separate write_seg_cell_table
writer node, which consumes this node's merged mask.
"""

from __future__ import annotations

import itertools
import logging
from typing import Any, Mapping

import numpy as np

from core.label_stitching import LOCAL_LABEL_BITS, flatten_block_index
from core.registry import register_node
from nodes.base import BaseMapOverlapNode
from nodes.cellpose_segmentation_node import (
    SEG_SPATIAL_AXES,
    get_seg_halo_metadata,
)


logger = logging.getLogger("WorkFlow.ProcessSegmentation")


def _resolve_seg_halo_depths(
    mask_array: Any,
    axes: tuple[str, ...],
    params: Mapping[str, Any] | None,
) -> dict[str, int]:
    """Halo depth per spatial axis name, from explicit params or upstream metadata."""

    params = params or {}
    metadata = get_seg_halo_metadata(mask_array) or {}
    metadata_axes = tuple(str(axis).upper() for axis in (metadata.get("axes") or ()))
    metadata_depth = (
        metadata.get("depth") if metadata_axes == tuple(axes) else None
    )
    depths: dict[str, int] = {}
    for axis in SEG_SPATIAL_AXES:
        if axis not in axes:
            continue
        raw = params.get(f"overlap_{axis.lower()}", -1)
        raw = -1 if raw is None else int(raw)
        if raw < 0:
            if not isinstance(metadata_depth, Mapping) or axis not in metadata_depth:
                raise ValueError(
                    f"process_segmentation cannot determine the {axis} overlap "
                    "depth: connect the mask output of cellpose_segmentation "
                    f"or set overlap_{axis.lower()} explicitly."
                )
            raw = int(metadata_depth[axis])
        if raw < 0:
            raise ValueError(f"overlap_{axis.lower()} must be >= 0, got {raw}.")
        depths[axis] = raw
    return depths


class _LabelUnionFind:
    def __init__(self) -> None:
        self.parent: dict[int, int] = {}

    def find(self, value: int) -> int:
        value = int(value)
        parent = self.parent.setdefault(value, value)
        while parent != self.parent[parent]:
            self.parent[parent] = self.parent[self.parent[parent]]
            parent = self.parent[parent]
        while value != parent:
            next_value = self.parent[value]
            self.parent[value] = parent
            value = next_value
        return parent

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        canonical = min(left_root, right_root)
        alias = max(left_root, right_root)
        self.parent[alias] = canonical


def _encode_block_labels(block: np.ndarray, flat_block_id: int) -> np.ndarray:
    """Namespace block-local labels as (flat_block_id << 32) | label."""

    encoded = np.zeros(block.shape, dtype=np.uint64)
    foreground = block != 0
    encoded[foreground] = (
        np.uint64(int(flat_block_id)) << np.uint64(LOCAL_LABEL_BITS)
    ) | block[foreground].astype(np.uint64)
    return encoded


def _strict_unique_best(
    counts: dict[tuple[int, int], int],
    *,
    reverse: bool,
) -> dict[int, int]:
    grouped: dict[int, list[tuple[int, int]]] = {}
    for (left, right), count in counts.items():
        key, value = (right, left) if reverse else (left, right)
        grouped.setdefault(key, []).append((value, int(count)))
    result: dict[int, int] = {}
    for key, candidates in grouped.items():
        candidates.sort(key=lambda item: (-item[1], item[0]))
        if len(candidates) > 1 and candidates[0][1] == candidates[1][1]:
            continue
        result[key] = candidates[0][0]
    return result


def _mutual_best_label_pairs(
    own: np.ndarray,
    other: np.ndarray,
    min_contact_voxels: int,
) -> tuple[tuple[int, int], ...]:
    """Accept only mutual best-overlap pairs between two labelings of one slab.

    The two arrays describe the same spatial voxels with labels from two
    different blocks.  A one-to-one mutual best vote merges cross-boundary
    cells without loss or duplication even when the two segmentations
    disagree: ambiguous one-to-many splits simply stay unmerged instead of
    collapsing distinct objects.
    """

    left_values = np.asarray(own, dtype=np.uint64).reshape(-1)
    right_values = np.asarray(other, dtype=np.uint64).reshape(-1)
    foreground = (left_values != 0) & (right_values != 0)
    if not np.any(foreground):
        return ()
    pairs = np.stack((left_values[foreground], right_values[foreground]), axis=1)
    unique_pairs, pair_counts = np.unique(pairs, axis=0, return_counts=True)
    counts = {
        (int(pair[0]), int(pair[1])): int(count)
        for pair, count in zip(unique_pairs, pair_counts)
        if int(count) >= int(min_contact_voxels)
    }
    if not counts:
        return ()
    left_best = _strict_unique_best(counts, reverse=False)
    right_best = _strict_unique_best(counts, reverse=True)
    return tuple(
        sorted(
            (left, right)
            for left, right in left_best.items()
            if right_best.get(right) == left
        )
    )


def _canonical_encode_labels(
    core: np.ndarray,
    flat_block_id: int,
    union_find: _LabelUnionFind,
) -> np.ndarray:
    """Rewrite core-region labels to the component-minimum global id."""

    output = np.zeros(core.shape, dtype=np.uint64)
    foreground = core != 0
    if not np.any(foreground):
        return output
    labels = np.unique(core[foreground])
    mapped = np.fromiter(
        (
            union_find.find((int(flat_block_id) << LOCAL_LABEL_BITS) | int(label))
            for label in labels
        ),
        dtype=np.uint64,
        count=int(labels.size),
    )
    positions = np.searchsorted(labels, core[foreground])
    output[foreground] = mapped[positions]
    return output


def process_segmentation_block(
    mask: np.ndarray,
    min_contact_voxels: int = 1,
    ctx=None,
) -> np.ndarray:
    """Merge one untrimmed mask tile against its neighbor copies and trim.

    The block view contains this block's own halo-expanded tile plus, for
    every face/edge/corner direction, a copy of the seam region labeled by
    the adjacent block.  Mutual-best matches between the two labelings of
    each seam union equivalent objects; every core label is then rewritten
    to the component-minimum id, which is identical on both sides of every
    boundary a cross-boundary cell touches.
    """

    if ctx is None:
        raise RuntimeError("process_segmentation block requires a BlockContext.")
    resources = ctx.resources or {}
    axes = tuple(str(axis).upper() for axis in (resources.get("axes") or ()))
    if len(axes) != int(mask.ndim):
        raise ValueError(
            f"process_segmentation axes {axes!r} length does not match mask ndim={mask.ndim}."
        )
    depth_by_name = {
        str(axis).upper(): int(depth)
        for axis, depth in (resources.get("seg_depth") or {}).items()
    }
    numblocks = tuple(int(value) for value in (resources.get("numblocks") or ()))
    block_index = tuple(int(value) for value in (ctx.block_locations[0] or ()))
    core_shape = tuple(int(value) for value in (ctx.output_chunk_shape or ()))
    if len(block_index) != mask.ndim or len(numblocks) != mask.ndim:
        raise RuntimeError("process_segmentation requires block location metadata.")
    if len(core_shape) != mask.ndim:
        raise RuntimeError("process_segmentation requires the declared output chunk shape.")
    min_contact_voxels = max(1, int(min_contact_voxels))

    spatial_axes = tuple(
        index for index, axis in enumerate(axes) if axis in SEG_SPATIAL_AXES
    )
    if not spatial_axes:
        raise ValueError(f"process_segmentation requires spatial axes, got {axes!r}.")
    batch_axes = tuple(
        index for index in range(mask.ndim) if index not in spatial_axes
    )

    # Per spatial axis: (halo depth, untrimmed tile size, own-tile offset)
    # inside this block's overlap view.
    geometry: dict[int, tuple[int, int, int]] = {}
    for index in spatial_axes:
        depth = depth_by_name.get(axes[index], 0)
        core = int(core_shape[index])
        tile = core + 2 * depth
        left = depth if block_index[index] > 0 else 0
        right = depth if block_index[index] < numblocks[index] - 1 else 0
        if int(mask.shape[index]) != left + tile + right:
            raise ValueError(
                f"process_segmentation halo mismatch on axis {axes[index]}: "
                f"block shape {int(mask.shape[index])} != {left}+{tile}+{right}. "
                "The input must be the untrimmed output of cellpose_segmentation "
                "with matching overlap depths."
            )
        geometry[index] = (depth, tile, left)

    own_flat = flatten_block_index(block_index, numblocks)
    output = np.zeros(core_shape, dtype=np.uint64)
    union_find = _LabelUnionFind()

    batch_shape = tuple(int(mask.shape[index]) for index in batch_axes)
    batch_coords = (
        itertools.product(*(range(size) for size in batch_shape))
        if batch_axes
        else [()]
    )

    for batch_coord in batch_coords:
        view_selection = [slice(None)] * mask.ndim
        out_selection = [slice(None)] * output.ndim
        for index, value in zip(batch_axes, batch_coord):
            view_selection[index] = int(value)
            out_selection[index] = int(value)
        sub = mask[tuple(view_selection)]
        nsp = len(spatial_axes)
        depths = [geometry[index][0] for index in spatial_axes]
        tiles = [geometry[index][1] for index in spatial_axes]
        lefts = [geometry[index][2] for index in spatial_axes]
        own_slices = tuple(
            slice(lefts[k], lefts[k] + tiles[k]) for k in range(nsp)
        )
        core_slices = tuple(
            slice(lefts[k] + depths[k], lefts[k] + depths[k] + core_shape[spatial_axes[k]])
            for k in range(nsp)
        )

        # Compare every duplicated seam: 26 neighbor directions in 3D
        # (faces, edges, corners), 8 in 2D.  Duplicated regions only exist
        # inside the core, so each comparison pairs this block's labeling of
        # a core seam slab with the adjacent block's labeling of it.
        for choice in itertools.product((-1, 0, 1), repeat=nsp):
            if all(direction == 0 for direction in choice):
                continue
            own_slab: list[slice] = []
            copy_slab: list[slice] = []
            neighbor = list(block_index)
            valid = True
            for k, direction in enumerate(choice):
                if direction == 0:
                    own_slab.append(own_slices[k])
                    copy_slab.append(own_slices[k])
                    continue
                depth = depths[k]
                if depth <= 0:
                    valid = False
                    break
                axis_index = spatial_axes[k]
                neighbor_index = block_index[axis_index] + direction
                if neighbor_index < 0 or neighbor_index >= numblocks[axis_index]:
                    valid = False
                    break
                neighbor[axis_index] = neighbor_index
                left = lefts[k]
                tile = tiles[k]
                if direction < 0:
                    own_slab.append(slice(left + depth, left + 2 * depth))
                    copy_slab.append(slice(0, depth))
                else:
                    own_slab.append(slice(left + tile - 2 * depth, left + tile - depth))
                    copy_slab.append(slice(left + tile, left + tile + depth))
            if not valid:
                continue
            own_face = np.asarray(sub[tuple(own_slab)])
            copy_face = np.asarray(sub[tuple(copy_slab)])
            if own_face.size == 0 or copy_face.size == 0:
                continue
            neighbor_flat = flatten_block_index(tuple(neighbor), numblocks)
            for left_id, right_id in _mutual_best_label_pairs(
                _encode_block_labels(own_face, own_flat),
                _encode_block_labels(copy_face, neighbor_flat),
                min_contact_voxels,
            ):
                union_find.union(left_id, right_id)

        core = np.asarray(sub[core_slices])
        output[tuple(out_selection)] = _canonical_encode_labels(
            core, own_flat, union_find
        )

    return output


@register_node("process_segmentation")
class ProcessSegmentation(BaseMapOverlapNode):
    """Merge untrimmed cellpose_segmentation masks into globally-consistent ids.

    The merge is block-local: every seam region exists twice in the
    untrimmed input (once per adjacent block's labeling), so each block
    matches its own labels against the neighbor copies, canonicalizes ids to
    the component minimum, and trims back to the original core chunk.
    """

    CATEGORY = "WorkFlow/Segmentation"
    DISPLAY_NAME = "Process Segmentation (Overlap Merge)"
    required_worker_profile = "CPU"

    MAP_INPUTS = ["mask"]
    PRIMARY_INPUT = "mask"
    PROCESS_BLOCK = process_segmentation_block
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
        # Never rechunk: the merge logic assumes one chunk per untrimmed
        # cellpose_segmentation tile.
        "allow_rechunk": False,
    }
    ALLOW_UNTRIMMED_OVERLAP_OUTPUT = True

    def _mask_axes(self) -> tuple[str, ...]:
        return tuple(
            str(axis).upper()
            for axis in ((self._axes_by_name or {}).get(self.PRIMARY_INPUT) or ())
        )

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
        depths = _resolve_seg_halo_depths(array_inputs[primary_name], axes, params)
        depth = {
            index: int(depths.get(axis, 0)) for index, axis in enumerate(axes)
        }
        return {**self.MAP_OVERLAP_SPEC, "depth": depth}

    def infer_output_spec(self, array_inputs, params, primary_name):
        primary = array_inputs[primary_name]
        axes = self._mask_axes()
        depths = _resolve_seg_halo_depths(primary, axes, params)
        chunks = []
        for index, axis in enumerate(axes):
            depth = int(depths.get(axis, 0))
            axis_chunks = tuple(int(chunk) - 2 * depth for chunk in primary.chunks[index])
            if any(chunk < 1 for chunk in axis_chunks):
                raise ValueError(
                    f"process_segmentation overlap depth {depth} on axis {axis} "
                    "leaves a non-positive core chunk; the input must be the "
                    "untrimmed output of cellpose_segmentation."
                )
            chunks.append(axis_chunks)
        return {
            "dtype": "uint64",
            "chunks": tuple(chunks),
            "enforce_ndim": True,
        }

    def preprocess(
        self,
        dask_arr=None,
        array_inputs: dict | None = None,
        params: dict | None = None,
        runtime: dict | None = None,
    ) -> dict[str, Any] | None:
        del runtime
        array_inputs = array_inputs or {}
        mask = array_inputs["mask"] if "mask" in array_inputs else dask_arr
        if mask is None:
            raise ValueError("process_segmentation expects a mask Dask Array, got None.")
        if np.dtype(mask.dtype) != np.dtype(np.uint32):
            raise ValueError(
                "process_segmentation requires the untrimmed uint32 mask "
                f"produced by cellpose_segmentation, got {mask.dtype}."
            )
        params = params or {}
        axes = self._mask_axes()
        if len(axes) != int(mask.ndim):
            raise ValueError(
                f"process_segmentation axes {axes!r} length does not match mask ndim={mask.ndim}."
            )
        if "Y" not in axes or "X" not in axes:
            raise ValueError(f"process_segmentation requires Y and X axes, got {axes!r}.")
        depths = _resolve_seg_halo_depths(mask, axes, params)

        numblocks = tuple(int(value) for value in mask.numblocks)
        for index, axis in enumerate(axes):
            if axis not in SEG_SPATIAL_AXES:
                continue
            depth = int(depths.get(axis, 0))
            core_total = int(mask.shape[index]) - 2 * depth * int(numblocks[index])
            if core_total < 1:
                raise ValueError(
                    f"process_segmentation axis {axis} has no positive core "
                    f"extent ({int(mask.shape[index])} - 2*{depth}*{numblocks[index]})."
                )
            min_chunk = min(int(chunk) for chunk in mask.chunks[index])
            if min_chunk - 2 * depth < 1:
                raise ValueError(
                    f"process_segmentation overlap depth {depth} on axis {axis} "
                    f"exceeds the halo in the smallest input tile ({min_chunk})."
                )

        return {
            "axes": axes,
            "seg_depth": {axis: int(depth) for axis, depth in depths.items()},
            "numblocks": numblocks,
            "min_contact_voxels": max(1, int(params.get("min_contact_voxels", 1))),
        }

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mask": ("DASK_ARRAY[any]",),
            },
            "optional": {
                "overlap_z": ("INT", {"default": -1, "min": -1, "max": 4096}),
                "overlap_y": ("INT", {"default": -1, "min": -1, "max": 16384}),
                "overlap_x": ("INT", {"default": -1, "min": -1, "max": 16384}),
                "min_contact_voxels": ("INT", {"default": 1, "min": 1, "max": 1000000}),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint64]",)
    RETURN_NAMES = ("mask",)
