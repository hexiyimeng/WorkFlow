from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest
import zarr

from nodes.zarr_writer_node import (
    ZarrWriter,
    _prepare_store,
    _write_zarr_block_impl,
)
from core.label_stitching import label_stitch_plan_path


def _prepare(path, *, shape=(4, 4), overwrite=False) -> None:
    _prepare_store(
        output_path=str(path),
        store_kind="array",
        dataset_path="0",
        shape=shape,
        chunks=(2, 2),
        dtype=np.dtype("uint16"),
        axes=("Y", "X"),
        voxel_size=(1.0, 1.0),
        compressor_name="none",
        overwrite=overwrite,
        write_metadata=True,
    )


def test_zarr_writer_defaults_to_preserving_existing_store() -> None:
    overwrite_config = ZarrWriter.INPUT_TYPES()["optional"]["overwrite"]

    assert overwrite_config[1]["default"] is False


def test_zarr_writer_does_not_expose_manual_axes_override() -> None:
    assert "axes" not in ZarrWriter.INPUT_TYPES()["optional"]
    assert ZarrWriter.ARRAY_AXES_BY_NDIM["array"][3] == ("Z", "Y", "X")


def test_existing_store_is_preserved_and_new_block_uses_its_position(tmp_path) -> None:
    output_path = tmp_path / "result.zarr"
    _prepare(output_path)
    target = zarr.open(str(output_path), mode="r+")
    target[0:2, 0:2] = np.uint16(7)

    # A later execution prepares the same compatible destination.  Preparation
    # must not clear chunks written by the interrupted execution.
    _prepare(output_path)

    context = SimpleNamespace(
        resources={
            "output_path": str(output_path),
            "store_kind": "array",
            "dataset_path": "0",
        },
        chunk_origins=((2, 2),),
        output_chunk_shape=(1, 1),
    )
    _write_zarr_block_impl(np.full((2, 2), 9, dtype=np.uint16), context)

    result = np.asarray(zarr.open(str(output_path), mode="r"))
    np.testing.assert_array_equal(result[0:2, 0:2], np.full((2, 2), 7))
    np.testing.assert_array_equal(result[2:4, 2:4], np.full((2, 2), 9))


def test_append_rejects_incompatible_store_without_clearing_it(tmp_path) -> None:
    output_path = tmp_path / "result.zarr"
    _prepare(output_path)
    target = zarr.open(str(output_path), mode="r+")
    target[0:2, 0:2] = np.uint16(11)

    with pytest.raises(ValueError, match="existing target.*incompatible"):
        _prepare(output_path, shape=(6, 4))

    preserved = zarr.open(str(output_path), mode="r")
    np.testing.assert_array_equal(preserved[0:2, 0:2], np.full((2, 2), 11))


def test_existing_ome_zarr_dataset_is_reused(tmp_path) -> None:
    output_path = tmp_path / "result.zarr"
    kwargs = {
        "output_path": str(output_path),
        "store_kind": "ome_zarr",
        "dataset_path": "images/0",
        "shape": (4, 4),
        "chunks": (2, 2),
        "dtype": np.dtype("uint16"),
        "axes": ("Y", "X"),
        "voxel_size": (1.0, 1.0),
        "compressor_name": "none",
        "overwrite": False,
        "write_metadata": True,
    }
    _prepare_store(**kwargs)
    target = zarr.open_group(str(output_path), mode="r+")["images/0"]
    target[0:2, 0:2] = np.uint16(17)

    _prepare_store(**kwargs)

    preserved = zarr.open_group(str(output_path), mode="r")["images/0"]
    np.testing.assert_array_equal(preserved[0:2, 0:2], np.full((2, 2), 17))


def test_explicit_overwrite_still_recreates_the_store(tmp_path) -> None:
    output_path = tmp_path / "result.zarr"
    _prepare(output_path)
    target = zarr.open(str(output_path), mode="r+")
    target[:] = np.uint16(13)

    _prepare(output_path, overwrite=True)

    recreated = np.asarray(zarr.open(str(output_path), mode="r"))
    np.testing.assert_array_equal(recreated, np.zeros((4, 4), dtype=np.uint16))


def test_new_run_invalidates_stitch_plan_but_resume_preserves_it(tmp_path):
    output_path = tmp_path / "result.zarr"
    _prepare(output_path)
    plan_path = label_stitch_plan_path(output_path)
    plan_path.write_bytes(b"saved-stitch-plan")
    target = zarr.open(str(output_path), mode="r+")
    target[:] = 5
    target.attrs["workflow_label_stitching"] = {"status": "complete"}
    _prepare_store(
        output_path=str(output_path), store_kind="array", dataset_path="0",
        shape=(4, 4), chunks=(2, 2), dtype=np.dtype("uint16"),
        axes=("Y", "X"), voxel_size=(1.0, 1.0), compressor_name="none",
        overwrite=True, write_metadata=True, is_resuming=True,
    )
    assert plan_path.read_bytes() == b"saved-stitch-plan"
    assert target.attrs["workflow_label_stitching"]["status"] == "complete"
    _prepare(output_path)
    assert not plan_path.exists()
    target = zarr.open(str(output_path), mode="r")
    assert "workflow_label_stitching" not in target.attrs
    np.testing.assert_array_equal(target[:], np.full((4, 4), 5))
