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
        ordinal_bits=16,
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
