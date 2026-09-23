from __future__ import annotations

import numpy as np

import nodes.write_parquet_cell_table_node as parquet_writer
import nodes.zarr_writer_node as zarr_writer
from core.label_stitching import LabelStitchPlan


def _extract(mask: np.ndarray, axes: tuple[str, ...]) -> list[dict]:
    return parquet_writer._extract_rows_from_mask(
        mask,
        axes=axes,
        origin=tuple(range(10, 10 + mask.ndim)),
        block_index=tuple(range(mask.ndim)),
        numblocks=(10,) * mask.ndim,
        tile_sizes={"Z": 64, "Y": 256, "X": 256},
    )


def test_cell_table_extraction_is_bounded_by_label_regions(monkeypatch) -> None:
    mask = np.zeros((3, 4, 5), dtype=np.uint32)
    mask[1, 1, 1:3] = 1
    mask[2, 3, 4] = 2

    def reject_full_foreground_coordinates(*_args, **_kwargs):
        raise AssertionError("full foreground coordinate materialization is forbidden")

    monkeypatch.setattr(np, "nonzero", reject_full_foreground_coordinates)
    rows = _extract(mask, ("Z", "Y", "X"))

    assert [row["label"] for row in rows] == [1, 2]
    assert [row["area_or_volume"] for row in rows] == [2, 1]
    assert rows[0]["centroid_x"] == 13.5
    assert rows[0]["bbox_x_min"] == 13
    assert rows[0]["bbox_x_max"] == 14
    assert rows[0]["touches_block_boundary"] is False
    assert rows[1]["touches_block_boundary"] is True


def test_cell_table_extraction_preserves_batch_label_groups() -> None:
    mask = np.zeros((2, 2, 3, 4), dtype=np.uint32)
    mask[0, 0, 1, 1] = 1
    mask[1, 1, 2, 3] = 1

    rows = _extract(mask, ("C", "Z", "Y", "X"))

    assert [row["label"] for row in rows] == [1, 1]
    assert [row["local_ordinal"] for row in rows] == [1, 2]
    assert [row["area_or_volume"] for row in rows] == [1, 1]
    assert rows[0]["global_cell_id"] != rows[1]["global_cell_id"]


def test_global_cell_id_supports_more_than_1023_cells_per_block() -> None:
    first_overflow = parquet_writer._encode_global_cell_id(14_719, 1_024)
    next_block = parquet_writer._encode_global_cell_id(14_720, 1)

    assert first_overflow == (14_719 << 32) | 1_024
    assert first_overflow != next_block
    assert 0 <= first_overflow <= np.iinfo(np.uint64).max


def test_cell_table_extraction_supports_1024_cells_in_one_block() -> None:
    mask = np.arange(1, 1_025, dtype=np.uint32).reshape(32, 32)

    rows = _extract(mask, ("Y", "X"))

    assert len(rows) == 1_024
    assert rows[-1]["local_ordinal"] == 1_024
    assert rows[-1]["global_cell_id"] == (
        rows[-1]["source_block_id"] << 32
    ) | 1_024


def test_global_cell_id_is_unique_across_blocks_and_ordinals() -> None:
    ids = {
        parquet_writer._encode_global_cell_id(block_id, ordinal)
        for block_id in (0, 1, 14_719, parquet_writer.MAX_SOURCE_BLOCK_ID)
        for ordinal in (1, 1_024, parquet_writer.MAX_LOCAL_ORDINAL)
    }

    assert len(ids) == 12


def test_cell_table_preserves_provisional_uint64_cellpose_ids() -> None:
    global_id = np.uint64((7 << 32) | 3)
    mask = np.zeros((2, 3), dtype=np.uint64)
    mask[:, 1] = global_id

    rows = parquet_writer._extract_rows_from_mask(
        mask,
        axes=("Y", "X"),
        origin=(0, 0),
        block_index=(0, 7),
        numblocks=(1, 10),
        tile_sizes={"Z": 64, "Y": 256, "X": 256},
    )

    assert len(rows) == 1
    assert rows[0]["global_cell_id"] == int(global_id)
    assert rows[0]["label"] == int(global_id)
    assert rows[0]["local_ordinal"] == 3


def test_parquet_finalizer_merges_cross_block_rows(tmp_path) -> None:
    axes = ("Y", "X")
    numblocks = (1, 2)
    left_id = 5
    right_id = (1 << 32) | 7
    tile_sizes = {"Z": 16, "Y": 512, "X": 512}

    def row(global_id, source_block, block_x, x0, x1, area, centroid_x):
        return {
            "global_cell_id": global_id,
            "cell_id_str": f"g{global_id:016x}",
            "spatial_key": 0,
            "source_block_id": source_block,
            "block_z": 0,
            "block_y": 0,
            "block_x": block_x,
            "tile_z": 0,
            "tile_y": 0,
            "tile_x": 0,
            "local_ordinal": global_id & ((1 << 32) - 1),
            "label": global_id,
            "centroid_z": 0.0,
            "centroid_y": 1.0,
            "centroid_x": centroid_x,
            "bbox_z_min": 0,
            "bbox_z_max": 0,
            "bbox_y_min": 0,
            "bbox_y_max": 2,
            "bbox_x_min": x0,
            "bbox_x_max": x1,
            "area_or_volume": area,
            "touches_block_boundary": True,
        }

    left_path, _ = parquet_writer._block_output_paths(
        str(tmp_path), axes, (0, 0), numblocks
    )
    right_path, _ = parquet_writer._block_output_paths(
        str(tmp_path), axes, (0, 1), numblocks
    )
    left_path.parent.mkdir(parents=True, exist_ok=True)
    right_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_writer._write_parquet_rows_atomic(
        left_path,
        [row(left_id, 0, 0, 0, 1, 4, 0.5)],
        compression="zstd",
        row_group_size=100,
    )
    parquet_writer._write_parquet_rows_atomic(
        right_path,
        [row(right_id, 1, 1, 2, 3, 6, 2.5)],
        compression="zstd",
        row_group_size=100,
    )
    plan = LabelStitchPlan(
        namespace="e:c",
        aliases=np.asarray([right_id], dtype=np.uint64),
        canonical=np.asarray([left_id], dtype=np.uint64),
        boundary_count=1,
        accepted_pair_count=1,
    )

    summary = parquet_writer._finalize_stitched_parquet(
        output_dir=tmp_path,
        axes=axes,
        numblocks=numblocks,
        tile_sizes=tile_sizes,
        compression="zstd",
        row_group_size=100,
        plan=plan,
    )

    import pyarrow.parquet as pq

    left_rows = pq.ParquetFile(left_path).read().to_pylist()
    right_rows = pq.ParquetFile(right_path).read().to_pylist()
    assert summary["merged_row_count"] == 1
    assert len(left_rows) == 1
    assert right_rows == []
    assert left_rows[0]["global_cell_id"] == left_id
    assert left_rows[0]["area_or_volume"] == 10
    assert left_rows[0]["bbox_x_min"] == 0
    assert left_rows[0]["bbox_x_max"] == 3
    assert left_rows[0]["centroid_x"] == 1.7000000476837158


def test_interrupted_parquet_stitch_transaction_restores_original_fragments(tmp_path) -> None:
    axes = ("Y", "X")
    numblocks = (1, 2)
    left_id = 5
    right_id = (1 << 32) | 7
    plan = LabelStitchPlan(
        namespace="cellpose:cellpose:v1",
        aliases=np.asarray([right_id], dtype=np.uint64),
        canonical=np.asarray([left_id], dtype=np.uint64),
        boundary_count=1,
        accepted_pair_count=1,
    )

    left_path, left_metadata = parquet_writer._block_output_paths(
        str(tmp_path), axes, (0, 0), numblocks
    )
    right_path, right_metadata = parquet_writer._block_output_paths(
        str(tmp_path), axes, (0, 1), numblocks
    )
    for path, value in ((left_path, b"left"), (right_path, b"right")):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    left_metadata.write_text('{"side":"left"}', encoding="utf-8")
    right_metadata.write_text('{"side":"right"}', encoding="utf-8")

    paths = parquet_writer._stitch_fragment_paths(
        output_dir=tmp_path,
        axes=axes,
        numblocks=numblocks,
        plan=plan,
    )
    transaction = parquet_writer._prepare_parquet_stitch_transaction(
        tmp_path,
        paths,
        namespace=plan.namespace,
    )
    assert transaction.is_dir()

    left_path.write_bytes(b"partially rewritten")
    right_path.write_bytes(b"")
    left_metadata.write_text('{"partial":true}', encoding="utf-8")
    parquet_writer._restore_parquet_stitch_transaction(tmp_path)

    assert left_path.read_bytes() == b"left"
    assert right_path.read_bytes() == b"right"
    assert left_metadata.read_text(encoding="utf-8") == '{"side":"left"}'
    assert right_metadata.read_text(encoding="utf-8") == '{"side":"right"}'
    assert not transaction.exists()


def test_legacy_cell_table_output_cannot_be_resumed(tmp_path) -> None:
    with np.testing.assert_raises_regex(
        ValueError,
        "legacy global_cell_id encoding",
    ):
        parquet_writer._validate_dataset_metadata(tmp_path)


def test_current_cell_table_dataset_metadata_can_be_resumed(tmp_path) -> None:
    parquet_writer._write_dataset_metadata(tmp_path)

    parquet_writer._validate_dataset_metadata(tmp_path)


def test_writer_entry_points_reclaim_memory_even_after_failure(monkeypatch) -> None:
    parquet_reclaims: list[bool] = []
    zarr_reclaims: list[bool] = []
    monkeypatch.setattr(
        parquet_writer,
        "trim_process_allocator",
        lambda: parquet_reclaims.append(True),
    )
    monkeypatch.setattr(
        zarr_writer,
        "trim_process_allocator",
        lambda: zarr_reclaims.append(True),
    )

    with np.testing.assert_raises(RuntimeError):
        parquet_writer.write_cell_table_block(np.zeros((1,), dtype=np.uint32))
    with np.testing.assert_raises(RuntimeError):
        zarr_writer.write_zarr_block(np.zeros((1,), dtype=np.uint32))

    assert parquet_reclaims == [True]
    assert zarr_reclaims == [True]
