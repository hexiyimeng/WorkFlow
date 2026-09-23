from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any, Mapping

import numpy as np


LABEL_STITCH_METADATA_ATTR = "_workflow_label_stitching"
LABEL_STITCH_PLAN_FILENAME = ".workflow-label-stitch.npz"
LABEL_STITCH_PLAN_VERSION = 1
LOCAL_LABEL_BITS = 32
LOCAL_LABEL_MASK = (1 << LOCAL_LABEL_BITS) - 1
MAX_SOURCE_BLOCK_ID = (1 << LOCAL_LABEL_BITS) - 1
MAX_LOCAL_ORDINAL = LOCAL_LABEL_MASK


@dataclass(frozen=True)
class LabelStitchPlan:
    namespace: str
    aliases: np.ndarray
    canonical: np.ndarray
    boundary_count: int
    accepted_pair_count: int

    def mapping(self) -> dict[int, int]:
        return {
            int(alias): int(root)
            for alias, root in zip(self.aliases, self.canonical)
        }


_PLAN_REGISTRY: dict[tuple[str, str], LabelStitchPlan] = {}
_PLAN_REGISTRY_LOCK = threading.RLock()


def attach_label_stitch_metadata(array: Any, metadata: Mapping[str, Any]) -> Any:
    setattr(array, LABEL_STITCH_METADATA_ATTR, dict(metadata))
    return array


def get_label_stitch_metadata(array: Any) -> dict[str, Any] | None:
    value = getattr(array, LABEL_STITCH_METADATA_ATTR, None)
    return dict(value) if isinstance(value, Mapping) else None


def register_label_stitch_plan(
    execution_id: str,
    namespace: str,
    plan: LabelStitchPlan,
) -> None:
    with _PLAN_REGISTRY_LOCK:
        _PLAN_REGISTRY[(str(execution_id), str(namespace))] = plan


def get_registered_label_stitch_plan(
    execution_id: str,
    namespace: str,
) -> LabelStitchPlan | None:
    with _PLAN_REGISTRY_LOCK:
        return _PLAN_REGISTRY.get((str(execution_id), str(namespace)))


def clear_registered_label_stitch_plans(execution_id: str) -> None:
    """Release transient Driver copies after one execution lifecycle ends."""

    execution_id = str(execution_id)
    with _PLAN_REGISTRY_LOCK:
        for key in tuple(_PLAN_REGISTRY):
            if key[0] == execution_id:
                _PLAN_REGISTRY.pop(key, None)


def flatten_block_index(
    block_index: tuple[int, ...],
    numblocks: tuple[int, ...],
) -> int:
    if len(block_index) != len(numblocks):
        raise ValueError(
            "block_index and numblocks must have the same rank, got "
            f"{block_index!r} and {numblocks!r}."
        )
    flat = 0
    stride = 1
    for index, axis_blocks in zip(reversed(block_index), reversed(numblocks)):
        index = int(index)
        axis_blocks = int(axis_blocks)
        if axis_blocks <= 0 or index < 0 or index >= axis_blocks:
            raise ValueError(
                f"Invalid block coordinate {block_index!r} for grid {numblocks!r}."
            )
        flat += index * stride
        stride *= axis_blocks
    if flat > MAX_SOURCE_BLOCK_ID:
        raise ValueError(
            f"Source block id {flat} exceeds the {LOCAL_LABEL_BITS}-bit budget."
        )
    return int(flat)


def encode_provisional_labels(
    mask: np.ndarray,
    *,
    axes: tuple[str, ...],
    block_info: Mapping[Any, Any] | None,
) -> np.ndarray:
    """Encode block-local labels into collision-free uint64 provisional IDs.

    The low 32 bits are a dense ordinal within one Dask output block.  The high
    32 bits are the flattened output-block coordinate.  Background remains 0.
    Non-spatial axes are handled independently, so equal Cellpose labels in two
    time/batch planes cannot collide.
    """

    info = None
    if isinstance(block_info, Mapping):
        info = block_info.get(None)
        if not isinstance(info, Mapping):
            info = block_info.get(0)
    if not isinstance(info, Mapping):
        raise RuntimeError("Global Cellpose label encoding requires Dask block_info.")

    block_index = tuple(int(value) for value in info.get("chunk-location", ()))
    numblocks = tuple(int(value) for value in info.get("num-chunks", ()))
    source_block_id = flatten_block_index(block_index, numblocks)
    if len(axes) != int(mask.ndim):
        raise ValueError(
            f"Label axes {axes!r} length does not match mask ndim={mask.ndim}."
        )

    result = np.zeros(mask.shape, dtype=np.uint64)
    batch_axes = tuple(
        index for index, axis in enumerate(axes) if str(axis).upper() not in {"Z", "Y", "X"}
    )
    batch_shape = tuple(int(mask.shape[index]) for index in batch_axes)
    batch_coordinates = np.ndindex(batch_shape) if batch_shape else ((),)
    ordinal = 0

    for batch_coordinate in batch_coordinates:
        selection: list[int | slice] = [slice(None)] * int(mask.ndim)
        for axis_index, coordinate in zip(batch_axes, batch_coordinate):
            selection[axis_index] = int(coordinate)
        view = np.asarray(mask[tuple(selection)])
        labels = np.unique(view)
        labels = labels[labels != 0]
        if not labels.size:
            continue
        if ordinal + int(labels.size) > MAX_LOCAL_ORDINAL:
            raise ValueError(
                "A Cellpose output block contains more labels than the 32-bit "
                "local ordinal budget permits."
            )
        local_ordinals = np.arange(
            ordinal + 1,
            ordinal + 1 + int(labels.size),
            dtype=np.uint64,
        )
        encoded = (np.uint64(source_block_id) << np.uint64(LOCAL_LABEL_BITS)) | local_ordinals
        positions = np.searchsorted(labels, view)
        foreground = view != 0
        encoded_view = np.zeros(view.shape, dtype=np.uint64)
        encoded_view[foreground] = encoded[positions[foreground]]
        result[tuple(selection)] = encoded_view
        ordinal += int(labels.size)

    return result


class _UnionFind:
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


def _chunk_starts(chunks: tuple[tuple[int, ...], ...]) -> tuple[tuple[int, ...], ...]:
    return tuple(
        tuple(int(value) for value in np.cumsum((0, *axis_chunks[:-1]), dtype=np.int64))
        for axis_chunks in chunks
    )


def _chunk_region(
    coordinate: tuple[int, ...],
    chunks: tuple[tuple[int, ...], ...],
    starts: tuple[tuple[int, ...], ...],
) -> tuple[slice, ...]:
    return tuple(
        slice(starts[axis][index], starts[axis][index] + int(chunks[axis][index]))
        for axis, index in enumerate(coordinate)
    )


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


def _mutual_best_face_pairs(
    left: np.ndarray,
    right: np.ndarray,
    *,
    min_contact_voxels: int,
) -> tuple[tuple[int, int], ...]:
    left_values = np.asarray(left, dtype=np.uint64).reshape(-1)
    right_values = np.asarray(right, dtype=np.uint64).reshape(-1)
    foreground = (left_values != 0) & (right_values != 0) & (left_values != right_values)
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


def discover_label_stitch_plan(
    target: Any,
    *,
    namespace: str,
    chunks: tuple[tuple[int, ...], ...],
    axes: tuple[str, ...],
    min_contact_voxels: int = 1,
) -> LabelStitchPlan:
    if len(chunks) != int(target.ndim) or len(axes) != int(target.ndim):
        raise ValueError("Label stitching chunks, axes, and target rank must match.")
    if int(min_contact_voxels) < 1:
        raise ValueError("min_contact_voxels must be at least 1.")

    numblocks = tuple(len(axis_chunks) for axis_chunks in chunks)
    starts = _chunk_starts(chunks)
    spatial_indices = tuple(
        index for index, axis in enumerate(axes) if str(axis).upper() in {"Z", "Y", "X"}
    )
    union_find = _UnionFind()
    boundary_count = 0
    accepted_pair_count = 0

    for coordinate in product(*(range(count) for count in numblocks)):
        coordinate = tuple(int(value) for value in coordinate)
        region = _chunk_region(coordinate, chunks, starts)
        for axis in spatial_indices:
            if coordinate[axis] + 1 >= numblocks[axis]:
                continue
            neighbor = list(coordinate)
            neighbor[axis] += 1
            neighbor = tuple(neighbor)
            neighbor_region = _chunk_region(neighbor, chunks, starts)

            left_selection = list(region)
            left_selection[axis] = slice(region[axis].stop - 1, region[axis].stop)
            right_selection = list(neighbor_region)
            right_selection[axis] = slice(
                neighbor_region[axis].start,
                neighbor_region[axis].start + 1,
            )
            left_face = np.asarray(target[tuple(left_selection)], dtype=np.uint64)
            right_face = np.asarray(target[tuple(right_selection)], dtype=np.uint64)
            boundary_count += 1
            pairs = _mutual_best_face_pairs(
                left_face,
                right_face,
                min_contact_voxels=int(min_contact_voxels),
            )
            accepted_pair_count += len(pairs)
            for left, right in pairs:
                union_find.union(left, right)

    mapping = {
        label: union_find.find(label)
        for label in tuple(union_find.parent)
    }
    aliases = np.asarray(
        sorted(label for label, root in mapping.items() if label != root),
        dtype=np.uint64,
    )
    canonical = np.asarray([mapping[int(label)] for label in aliases], dtype=np.uint64)
    return LabelStitchPlan(
        namespace=str(namespace),
        aliases=aliases,
        canonical=canonical,
        boundary_count=int(boundary_count),
        accepted_pair_count=int(accepted_pair_count),
    )


def label_stitch_plan_path(output_path: str | Path) -> Path:
    path = Path(output_path)
    return path / LABEL_STITCH_PLAN_FILENAME


def save_label_stitch_plan(
    path: Path,
    plan: LabelStitchPlan,
    *,
    shape: tuple[int, ...],
    chunks: tuple[tuple[int, ...], ...],
    axes: tuple[str, ...],
    min_contact_voxels: int,
) -> None:
    metadata = {
        "version": LABEL_STITCH_PLAN_VERSION,
        "namespace": plan.namespace,
        "shape": [int(value) for value in shape],
        "chunks": [[int(value) for value in axis] for axis in chunks],
        "axes": [str(value) for value in axes],
        "min_contact_voxels": int(min_contact_voxels),
        "boundary_count": int(plan.boundary_count),
        "accepted_pair_count": int(plan.accepted_pair_count),
    }
    tmp = path.with_name(f".{path.name}.tmp")
    if tmp.exists():
        tmp.unlink()
    with tmp.open("wb") as handle:
        np.savez_compressed(
            handle,
            metadata=np.asarray(json.dumps(metadata, sort_keys=True)),
            aliases=plan.aliases,
            canonical=plan.canonical,
        )
    tmp.replace(path)


def load_label_stitch_plan(
    path: Path,
    *,
    namespace: str,
    shape: tuple[int, ...],
    chunks: tuple[tuple[int, ...], ...],
    axes: tuple[str, ...],
    min_contact_voxels: int,
) -> LabelStitchPlan | None:
    if not path.is_file():
        return None
    try:
        with np.load(path, allow_pickle=False) as payload:
            metadata = json.loads(str(payload["metadata"].item()))
            aliases = np.asarray(payload["aliases"], dtype=np.uint64)
            canonical = np.asarray(payload["canonical"], dtype=np.uint64)
    except Exception:
        return None
    expected = {
        "version": LABEL_STITCH_PLAN_VERSION,
        "namespace": str(namespace),
        "shape": [int(value) for value in shape],
        "chunks": [[int(value) for value in axis] for axis in chunks],
        "axes": [str(value) for value in axes],
        "min_contact_voxels": int(min_contact_voxels),
    }
    if any(metadata.get(key) != value for key, value in expected.items()):
        return None
    if aliases.shape != canonical.shape or aliases.ndim != 1:
        return None
    return LabelStitchPlan(
        namespace=str(namespace),
        aliases=aliases,
        canonical=canonical,
        boundary_count=int(metadata.get("boundary_count", 0)),
        accepted_pair_count=int(metadata.get("accepted_pair_count", 0)),
    )


def apply_label_stitch_plan(
    target: Any,
    *,
    plan: LabelStitchPlan,
    chunks: tuple[tuple[int, ...], ...],
) -> int:
    if not plan.aliases.size:
        return 0
    numblocks = tuple(len(axis_chunks) for axis_chunks in chunks)
    starts = _chunk_starts(chunks)
    by_block: dict[int, list[tuple[int, int]]] = {}
    for alias, canonical in zip(plan.aliases, plan.canonical):
        source_block_id = int(alias) >> LOCAL_LABEL_BITS
        by_block.setdefault(source_block_id, []).append((int(alias), int(canonical)))

    rewritten = 0
    total_blocks = int(np.prod(numblocks, dtype=np.int64))
    for source_block_id, replacements in sorted(by_block.items()):
        if source_block_id < 0 or source_block_id >= total_blocks:
            raise ValueError(
                f"Stitch alias encodes source block {source_block_id}, but grid "
                f"{numblocks!r} contains {total_blocks} blocks."
            )
        coordinate = tuple(
            int(value) for value in np.unravel_index(source_block_id, numblocks)
        )
        region = _chunk_region(coordinate, chunks, starts)
        block = np.asarray(target[region], dtype=np.uint64)
        keys = np.asarray([item[0] for item in replacements], dtype=np.uint64)
        values = np.asarray([item[1] for item in replacements], dtype=np.uint64)
        order = np.argsort(keys)
        keys = keys[order]
        values = values[order]
        flat = block.reshape(-1)
        positions = np.searchsorted(keys, flat)
        matches = positions < len(keys)
        if np.any(matches):
            candidate_indices = np.flatnonzero(matches)
            candidate_positions = positions[matches]
            exact = keys[candidate_positions] == flat[candidate_indices]
            if np.any(exact):
                selected = candidate_indices[exact]
                flat[selected] = values[candidate_positions[exact]]
                target[region] = block
                rewritten += 1
    return rewritten


__all__ = [
    "LABEL_STITCH_METADATA_ATTR",
    "LabelStitchPlan",
    "apply_label_stitch_plan",
    "attach_label_stitch_metadata",
    "clear_registered_label_stitch_plans",
    "discover_label_stitch_plan",
    "encode_provisional_labels",
    "get_label_stitch_metadata",
    "get_registered_label_stitch_plan",
    "label_stitch_plan_path",
    "load_label_stitch_plan",
    "register_label_stitch_plan",
    "save_label_stitch_plan",
]
