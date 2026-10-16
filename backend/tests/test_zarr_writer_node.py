from __future__ import annotations

import json

import dask.array as da
import numpy as np
import pytest
import zarr

from nodes.ome_zarr_reader import OMEZarrReader
from nodes.zarr_writer_node import ZarrWriter


def _make_writer(axes: tuple[str, ...]) -> ZarrWriter:
    writer = ZarrWriter()
    writer._axes_by_name = {"array": tuple(axes)}
    return writer


def _make_dask_array() -> da.Array:
    source = np.arange(2 * 8 * 8, dtype=np.uint32).reshape(2, 8, 8)
    return da.from_array(source, chunks=(1, 4, 4))


def _params(tmp_path, name: str = "out.zarr", **overrides) -> dict:
    params = {"output_path": str(tmp_path / name)}
    params.update(overrides)
    return params


def test_v3_array_store_roundtrip(tmp_path) -> None:
    writer = _make_writer(("Z", "Y", "X"))
    arr = _make_dask_array()
    writer.preprocess(dask_arr=arr, params=_params(tmp_path, store_kind="array", zarr_format="3"))

    store = tmp_path / "out.zarr"
    assert (store / "zarr.json").is_file()
    metadata = json.loads((store / "zarr.json").read_text())
    assert metadata["zarr_format"] == 3
    codec_names = [codec["name"] for codec in metadata["codecs"]]
    assert "zstd" in codec_names

    target = zarr.open_array(str(store), mode="r+")
    expected = np.arange(2 * 8 * 8, dtype=np.uint32).reshape(2, 8, 8)
    target[:] = expected
    assert np.array_equal(np.asarray(target[:]), expected)
    assert target.metadata.zarr_format == 3
    assert dict(target.attrs)["workflow_writer"]["zarr_format"] == 3


def test_v3_ome_zarr_store_has_ngff05_metadata(tmp_path) -> None:
    writer = _make_writer(("Z", "Y", "X"))
    arr = _make_dask_array()
    writer.preprocess(
        dask_arr=arr,
        params=_params(
            tmp_path,
            store_kind="ome_zarr",
            zarr_format="3",
            voxel_size="1.5,2,2",
        ),
    )

    store = tmp_path / "out.zarr"
    metadata = json.loads((store / "zarr.json").read_text())
    assert metadata["zarr_format"] == 3
    attributes = metadata["attributes"]
    assert attributes["ome"]["version"] == "0.5"
    multiscales = attributes["ome"]["multiscales"]
    assert len(multiscales) == 1
    assert multiscales[0]["datasets"][0]["path"] == "0"
    assert multiscales[0]["datasets"][0]["coordinateTransformations"] == [
        {"type": "scale", "scale": [1.5, 2.0, 2.0]}
    ]
    assert multiscales[0]["axes"] == [
        {"name": "z", "type": "space"},
        {"name": "y", "type": "space"},
        {"name": "x", "type": "space"},
    ]

    group = zarr.open_group(str(store), mode="r+")
    expected = np.arange(2 * 8 * 8, dtype=np.uint32).reshape(2, 8, 8)
    group["0"][:] = expected
    assert np.array_equal(np.asarray(group["0"][:]), expected)


def test_v2_ome_zarr_store_has_ngff04_metadata(tmp_path) -> None:
    writer = _make_writer(("T", "C", "Z", "Y", "X"))
    source = np.zeros((1, 1, 2, 8, 8), dtype=np.uint16)
    arr = da.from_array(source, chunks=(1, 1, 1, 4, 4))
    writer.preprocess(
        dask_arr=arr,
        params=_params(tmp_path, store_kind="ome_zarr", zarr_format="2"),
    )

    store = tmp_path / "out.zarr"
    assert (store / ".zgroup").is_file()
    assert (store / "0" / ".zarray").is_file()
    multiscales = json.loads((store / ".zattrs").read_text())["multiscales"]
    assert multiscales[0]["version"] == "0.4"
    assert multiscales[0]["axes"] == [
        {"name": "t", "type": "time"},
        {"name": "c", "type": "channel"},
        {"name": "z", "type": "space"},
        {"name": "y", "type": "space"},
        {"name": "x", "type": "space"},
    ]

    group = zarr.open_group(str(store), mode="r")
    assert group.metadata.zarr_format == 2


def test_reader_loads_v3_ome_zarr_store(tmp_path) -> None:
    writer = _make_writer(("Z", "Y", "X"))
    arr = _make_dask_array()
    writer.preprocess(
        dask_arr=arr,
        params=_params(tmp_path, store_kind="ome_zarr", zarr_format="3"),
    )
    expected = np.arange(2 * 8 * 8, dtype=np.uint32).reshape(2, 8, 8)
    zarr.open_group(str(tmp_path / "out.zarr"), mode="r+")["0"][:] = expected

    result = OMEZarrReader().load_zarr(
        str(tmp_path / "out.zarr"),
        axes="Z,Y,X",
        chunk_mode="native",
        _runtime={"node_id": "reader", "execution_id": "v3-read-test"},
    )[0]

    assert result.shape == (2, 8, 8)
    assert result.dtype == np.uint32
    assert np.array_equal(result.compute(scheduler="synchronous"), expected)


@pytest.mark.parametrize("compressor_name", ["default", "blosc", "lz4", "none"])
def test_v3_compressor_options_roundtrip(tmp_path, compressor_name) -> None:
    writer = _make_writer(("Z", "Y", "X"))
    arr = _make_dask_array()
    writer.preprocess(
        dask_arr=arr,
        params=_params(
            tmp_path,
            name=f"{compressor_name}.zarr",
            store_kind="array",
            zarr_format="3",
            compressor_name=compressor_name,
        ),
    )

    store = tmp_path / f"{compressor_name}.zarr"
    target = zarr.open_array(str(store), mode="r+")
    expected = np.arange(2 * 8 * 8, dtype=np.uint32).reshape(2, 8, 8)
    target[:] = expected
    assert np.array_equal(np.asarray(target[:]), expected)


def test_resume_rejects_format_mismatch(tmp_path) -> None:
    writer = _make_writer(("Z", "Y", "X"))
    arr = _make_dask_array()
    writer.preprocess(
        dask_arr=arr,
        params=_params(tmp_path, store_kind="array", zarr_format="3"),
    )

    with pytest.raises(ValueError, match="zarr_format=3 but"):
        writer.preprocess(
            dask_arr=arr,
            params=_params(tmp_path, store_kind="array", zarr_format="2"),
            runtime={"is_resuming": True},
        )


def test_resume_accepts_matching_format(tmp_path) -> None:
    writer = _make_writer(("Z", "Y", "X"))
    arr = _make_dask_array()
    writer.preprocess(
        dask_arr=arr,
        params=_params(tmp_path, store_kind="ome_zarr", zarr_format="3"),
    )
    resources = writer.preprocess(
        dask_arr=arr,
        params=_params(tmp_path, store_kind="ome_zarr", zarr_format="3"),
        runtime={"is_resuming": True},
    )
    assert resources["zarr_format"] == 3


def test_invalid_zarr_format_rejected(tmp_path) -> None:
    writer = _make_writer(("Z", "Y", "X"))
    with pytest.raises(ValueError, match="zarr_format must be"):
        writer.preprocess(
            dask_arr=_make_dask_array(),
            params=_params(tmp_path, zarr_format="4"),
        )
