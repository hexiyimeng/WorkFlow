from __future__ import annotations

import numpy as np
import zarr

from core.label_stitching import (
    apply_label_stitch_plan,
    clear_registered_label_stitch_plans,
    discover_label_stitch_plan,
    encode_provisional_labels,
    get_registered_label_stitch_plan,
    load_label_stitch_plan,
    register_label_stitch_plan,
    save_label_stitch_plan,
)


def test_provisional_ids_are_unique_across_blocks_and_batch_planes() -> None:
    first = np.asarray(
        [
            [[1, 1], [0, 2]],
            [[1, 0], [0, 0]],
        ],
        dtype=np.uint32,
    )
    second = np.asarray(
        [
            [[1, 1], [0, 2]],
            [[1, 0], [0, 0]],
        ],
        dtype=np.uint32,
    )
    first_encoded = encode_provisional_labels(
        first,
        axes=("T", "Y", "X"),
        block_info={None: {"chunk-location": (0, 0, 0), "num-chunks": (1, 1, 2)}},
    )
    second_encoded = encode_provisional_labels(
        second,
        axes=("T", "Y", "X"),
        block_info={None: {"chunk-location": (0, 0, 1), "num-chunks": (1, 1, 2)}},
    )

    first_ids = set(int(value) for value in np.unique(first_encoded) if value)
    second_ids = set(int(value) for value in np.unique(second_encoded) if value)
    assert len(first_ids) == 3
    assert len(second_ids) == 3
    assert first_ids.isdisjoint(second_ids)


def test_adjacent_local_labels_are_stitched_to_one_canonical_id(tmp_path) -> None:
    left_id = np.uint64(5)
    right_id = np.uint64((1 << 32) | 7)
    path = tmp_path / "labels.zarr"
    target = zarr.open(
        str(path),
        mode="w",
        shape=(3, 4),
        chunks=(3, 2),
        dtype="uint64",
    )
    target[:] = np.asarray(
        [
            [0, left_id, right_id, 0],
            [0, left_id, right_id, 0],
            [0, 0, 0, 0],
        ],
        dtype=np.uint64,
    )
    chunks = ((3,), (2, 2))
    plan = discover_label_stitch_plan(
        target,
        namespace="execution:cellpose",
        chunks=chunks,
        axes=("Y", "X"),
    )

    assert plan.mapping() == {int(right_id): int(left_id)}
    assert apply_label_stitch_plan(target, plan=plan, chunks=chunks) == 1
    assert set(int(value) for value in np.unique(target[:])) == {0, int(left_id)}


def test_stitch_plan_is_persisted_before_idempotent_rewrite(tmp_path) -> None:
    path = tmp_path / "plan.npz"
    target = zarr.open(
        str(tmp_path / "labels.zarr"),
        mode="w",
        shape=(2, 4),
        chunks=(2, 2),
        dtype="uint64",
    )
    right_id = np.uint64((1 << 32) | 1)
    target[:] = np.asarray([[0, 1, right_id, 0], [0, 1, right_id, 0]], dtype=np.uint64)
    chunks = ((2,), (2, 2))
    plan = discover_label_stitch_plan(
        target,
        namespace="e:c",
        chunks=chunks,
        axes=("Y", "X"),
    )
    save_label_stitch_plan(
        path,
        plan,
        shape=(2, 4),
        chunks=chunks,
        axes=("Y", "X"),
        min_contact_voxels=1,
    )
    loaded = load_label_stitch_plan(
        path,
        namespace="e:c",
        shape=(2, 4),
        chunks=chunks,
        axes=("Y", "X"),
        min_contact_voxels=1,
    )

    assert loaded is not None
    assert loaded.mapping() == plan.mapping()
    assert apply_label_stitch_plan(target, plan=loaded, chunks=chunks) == 1
    assert apply_label_stitch_plan(target, plan=loaded, chunks=chunks) == 0


def test_ambiguous_equal_contact_does_not_overmerge() -> None:
    from core.label_stitching import _mutual_best_face_pairs

    left = np.asarray([1, 1], dtype=np.uint64)
    right = np.asarray([2, 3], dtype=np.uint64)

    assert _mutual_best_face_pairs(left, right, min_contact_voxels=1) == ()


def test_transient_plan_registry_is_cleared_per_execution() -> None:
    plan = discover_label_stitch_plan(
        np.asarray([[1, 2]], dtype=np.uint64),
        namespace="cellpose:cellpose:v1",
        chunks=((1,), (1, 1)),
        axes=("Y", "X"),
    )
    register_label_stitch_plan("execution-a", plan.namespace, plan)
    register_label_stitch_plan("execution-b", plan.namespace, plan)

    clear_registered_label_stitch_plans("execution-a")

    assert get_registered_label_stitch_plan("execution-a", plan.namespace) is None
    assert get_registered_label_stitch_plan("execution-b", plan.namespace) is plan
    clear_registered_label_stitch_plans("execution-b")
