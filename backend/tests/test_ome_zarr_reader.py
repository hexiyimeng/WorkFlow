from __future__ import annotations

import numpy as np
import pytest
import zarr

import nodes.ome_zarr_reader as reader_module
from nodes.ome_zarr_reader import OMEZarrReader, _resolve_group_array


class _FakeArray:
    shape = (1,)
    dtype = "uint8"


class _FakeGroup(dict):
    attrs: dict[str, object] = {}


def test_ambiguous_group_requires_explicit_array_path_with_candidates() -> None:
    group = _FakeGroup(s0=_FakeArray(), s1=_FakeArray(), s2=_FakeArray())

    with pytest.raises(ValueError) as error:
        _resolve_group_array(group, None, 0, 0)

    message = str(error.value)
    assert "multiple candidate arrays" in message
    assert "'s0'" in message
    assert "'s1'" in message
    assert "array_path" in message


def test_explicit_array_path_selects_requested_resolution() -> None:
    selected = _FakeArray()
    group = _FakeGroup(s0=selected, s1=_FakeArray())

    array, path = _resolve_group_array(group, "s0", 0, 0)

    assert array is selected
    assert path == "s0"


def test_reader_has_explicit_cpu_reader_materialization_layer(tmp_path) -> None:
    path = tmp_path / "image.zarr"
    source = zarr.open_array(
        str(path),
        mode="w",
        shape=(4, 8, 8),
        chunks=(2, 4, 4),
        dtype="uint16",
    )
    source[:] = 7

    result = OMEZarrReader().load_zarr(
        str(path),
        axes="Z,Y,X",
        chunk_mode="native",
        _runtime={
            "node_id": "reader",
            "execution_id": "12345678-1234-1234-1234-123456789abc",
        },
    )[0]

    read_layers = [
        layer
        for name, layer in result.dask.layers.items()
        if str(name).startswith("OMEZarrReader-read-")
    ]
    assert len(read_layers) == 1
    assert read_layers[0].annotations == {
        "brainflow_node_id": "reader",
        "required_worker_profile": "cpu-reader",
        "resources": {"cpu-reader": 1.0},
    }


def test_reader_materializes_only_requested_chunks(monkeypatch, tmp_path) -> None:
    path = tmp_path / "image.zarr"
    source = zarr.open_array(
        str(path),
        mode="w",
        shape=(4, 8, 8),
        chunks=(2, 4, 4),
        dtype="uint16",
    )
    source[:] = np.arange(source.size, dtype=np.uint16).reshape(source.shape)
    materialized_shapes: list[tuple[int, ...]] = []
    original = reader_module._materialize_zarr_block

    def recording_materializer(block):
        materialized_shapes.append(tuple(int(size) for size in block.shape))
        return original(block)

    monkeypatch.setattr(
        reader_module,
        "_materialize_zarr_block",
        recording_materializer,
    )
    result = OMEZarrReader().load_zarr(
        str(path),
        axes="Z,Y,X",
        chunk_mode="native",
        _runtime={"node_id": "reader", "execution_id": "streaming-test"},
    )[0]

    first_chunk = result.blocks[0, 0, 0].compute(scheduler="synchronous")

    assert first_chunk.shape == (2, 4, 4)
    assert materialized_shapes == [(2, 4, 4)]
    assert result.numblocks == (2, 2, 2)
