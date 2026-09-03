from __future__ import annotations

import numpy as np

import nodes.write_parquet_cell_table_node as parquet_writer
import nodes.zarr_writer_node as zarr_writer


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
