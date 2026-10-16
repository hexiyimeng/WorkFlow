from __future__ import annotations

import numpy as np
import pytest
import dask.array as da

from nodes.cellpose_segmentation_node import (
    CellposeSegmentation,
    SEG_SCRIPT_MODEL_NAME,
    _is_block_dim,
    attach_seg_halo_metadata,
    cellpose_segmentation_block,
)
from nodes.process_segmentation_node import (
    ProcessSegmentation,
    _LabelUnionFind,
    _mutual_best_label_pairs,
    _resolve_seg_halo_depths,
    process_segmentation_block,
)


class _FakeBlockContext:
    """Minimal BlockContext stand-in for process_segmentation_block."""

    def __init__(self, block_info, resources):
        self.resources = resources
        self.block_info = block_info
        entry = block_info[0]
        self.block_locations = (tuple(int(v) for v in entry["chunk-location"]),)
        self.array_locations = (
            tuple((int(a), int(b)) for a, b in entry["array-location"]),
        )
        self.chunk_origins = (
            tuple(int(a) for a, _ in self.array_locations[0]),
        )
        core_shape = tuple(
            int(v) for v in resources["core_shapes"][self.block_locations[0]]
        )
        self.output_chunk_shape = core_shape
        # The real framework reports the output entry in the trimmed output
        # array's coordinate frame (i.e. image coordinates).
        core_start = resources["core_starts"][self.block_locations[0]]
        self.output_block_info = {
            "array-location": [
                [start, start + size]
                for start, size in zip(core_start, core_shape)
            ],
            "chunk-shape": list(core_shape),
        }


def _simulate_pair(image, chunks, depth, seg_fn, min_contact=1):
    """Run node1 geometry (untrimmed tiles) + node2 merge over real dask views.

    seg_fn(tile) -> uint32 labels simulates Cellpose on each halo-expanded
    tile.  Returns the merged uint64 array in image geometry.
    """

    image = np.asarray(image)
    axes = tuple("ZYX"[-image.ndim:]) if image.ndim == 3 else ("Y", "X")
    x = da.from_array(image, chunks=chunks)

    def node1_fn(block, block_info=None):
        return seg_fn(np.asarray(block))

    expanded = tuple(
        tuple(int(c) + 2 * depth for c in axis_chunks) for axis_chunks in x.chunks
    )
    tiled = da.map_overlap(
        node1_fn,
        x,
        depth=depth,
        boundary="reflect",
        trim=False,
        dtype=np.uint32,
        chunks=expanded,
        meta=np.array((), dtype=np.uint32),
    )

    views: dict[tuple[int, ...], np.ndarray] = {}
    infos: dict[tuple[int, ...], dict] = {}

    def capture(block, block_info=None):
        loc = tuple(int(v) for v in block_info[0]["chunk-location"])
        views[loc] = np.asarray(block)
        infos[loc] = block_info
        return block

    passthrough = da.map_overlap(
        capture,
        tiled,
        depth=depth,
        boundary="none",
        trim=False,
        dtype=np.uint32,
        chunks=expanded,
        allow_rechunk=False,
        meta=np.array((), dtype=np.uint32),
    )
    passthrough.compute()

    core_chunks = x.chunks
    core_shapes = {}
    core_starts = {}
    starts_per_axis = []
    for axis_chunks in core_chunks:
        starts = np.cumsum((0, *axis_chunks[:-1]))
        starts_per_axis.append(tuple(int(v) for v in starts))
    for loc in views:
        core_shapes[loc] = tuple(
            int(core_chunks[axis][loc[axis]]) for axis in range(image.ndim)
        )
        core_starts[loc] = tuple(
            starts_per_axis[axis][loc[axis]] for axis in range(image.ndim)
        )

    numblocks = tuple(int(n) for n in tiled.numblocks)
    resources = {
        "axes": axes,
        "seg_depth": {axis: depth for axis in axes},
        "numblocks": numblocks,
        "min_contact_voxels": min_contact,
        "core_shapes": core_shapes,
        "core_starts": core_starts,
    }

    merged = np.zeros(image.shape, dtype=np.uint64)
    for loc, view in views.items():
        ctx = _FakeBlockContext(infos[loc], resources)
        out = process_segmentation_block(view, min_contact, ctx=ctx)
        assert out.shape == core_shapes[loc]
        dest = tuple(
            slice(starts_per_axis[axis][loc[axis]],
                  starts_per_axis[axis][loc[axis]] + core_shapes[loc][axis])
            for axis in range(image.ndim)
        )
        merged[dest] = out
    return merged


class _FakeWriterContext:
    """Minimal BlockContext stand-in for write_seg_cell_table_block."""

    def __init__(self, block_info, resources):
        self.resources = resources
        self.block_info = block_info
        entry = block_info[0]
        self.block_locations = (tuple(int(v) for v in entry["chunk-location"]),)


def _simulate_writer(merged, chunks, depth, output_dir):
    """Run the write_seg_cell_table block over real dask overlap views."""

    from nodes.write_seg_cell_table_node import write_seg_cell_table_block

    merged = np.asarray(merged)
    axes = ("Y", "X") if merged.ndim == 2 else ("Z", "Y", "X")
    x = da.from_array(merged, chunks=chunks)
    views, infos = {}, {}

    def capture(block, block_info=None):
        loc = tuple(int(v) for v in block_info[0]["chunk-location"])
        views[loc] = np.asarray(block)
        infos[loc] = block_info
        return np.ones((1,) * block.ndim, dtype=np.uint8)

    token_chunks = tuple((1,) * len(axis_chunks) for axis_chunks in x.chunks)
    da.map_overlap(
        capture,
        x,
        depth=depth,
        boundary="none",
        trim=False,
        dtype=np.uint8,
        chunks=token_chunks,
        allow_rechunk=False,
        meta=np.array((), dtype=np.uint8),
    ).compute()

    chunk_starts = tuple(
        tuple(int(v) for v in np.cumsum((0, *axis_chunks[:-1])))
        for axis_chunks in x.chunks
    )
    resources = {
        "axes": axes,
        "halo_depth": {axis: depth for axis in axes},
        "chunk_starts": chunk_starts,
        "output_dir": output_dir,
    }
    for loc, view in views.items():
        ctx = _FakeWriterContext(infos[loc], resources)
        write_seg_cell_table_block(view, ctx=ctx)


def _read_rows(table_dir):
    import pyarrow.parquet as pq

    rows = []
    for fragment in sorted((table_dir / "fragments").glob("*.parquet")):
        rows.extend(pq.read_table(fragment).to_pylist())
    return rows


def _blob_labels(block):
    from scipy import ndimage

    labeled, _ = ndimage.label(block > 0)
    return labeled.astype(np.uint32)


def _ellipse_mask(shape, center, radii):
    grids = np.ogrid[tuple(slice(0, s) for s in shape)]
    value = np.zeros(shape, dtype=bool)
    for g, c, r in zip(grids, center, radii):
        value = value + ((g - c) / float(r)) ** 2
    return value <= 1.0


def _assert_bijective(merged, global_labels):
    """Every global object maps to exactly one merged id and vice versa."""

    merged = np.asarray(merged)
    global_labels = np.asarray(global_labels)
    assert merged.shape == global_labels.shape
    # no loss: foreground coverage identical
    assert np.array_equal(merged != 0, global_labels != 0)
    forward: dict[int, set[int]] = {}
    backward: dict[int, set[int]] = {}
    for gid in np.unique(merged[merged != 0]):
        linked = set(np.unique(global_labels[merged == gid]).tolist()) - {0}
        forward[int(gid)] = {int(v) for v in linked}
        assert len(linked) == 1, f"merged id {gid} spans global objects {linked}"
    for glabel in np.unique(global_labels[global_labels != 0]):
        linked = set(np.unique(merged[global_labels == glabel]).tolist()) - {0}
        backward[int(glabel)] = {int(v) for v in linked}
        assert len(linked) == 1, f"global object {glabel} split into {linked}"


def test_cross_boundary_cell_same_id_2d():
    image = np.zeros((20, 24), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (10, 12), (4, 6))] = 200
    merged = _simulate_pair(
        image, chunks=(10, 12), depth=3, seg_fn=_blob_labels
    )

    from scipy import ndimage

    global_labels, _ = ndimage.label(image > 0)
    _assert_bijective(merged, global_labels)
    ids = np.unique(merged[merged != 0])
    assert ids.size == 1
    # the single cell appears on both sides of both seams with one id
    gid = int(ids[0])
    assert gid in np.unique(merged[9:11, :])
    assert gid in np.unique(merged[:, 11:13])


def test_large_cell_smaller_than_block_merges():
    # cell diameter 14 > overlap 3 on every axis, but smaller than a chunk
    image = np.zeros((24, 24), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (12, 12), (7, 7))] = 200
    merged = _simulate_pair(
        image, chunks=(16, 16), depth=3, seg_fn=_blob_labels
    )
    from scipy import ndimage

    global_labels, _ = ndimage.label(image > 0)
    _assert_bijective(merged, global_labels)


def test_corner_cell_four_blocks_one_id():
    image = np.zeros((20, 20), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (10, 10), (2, 2))] = 200
    merged = _simulate_pair(
        image, chunks=(10, 10), depth=3, seg_fn=_blob_labels
    )
    ids = np.unique(merged[merged != 0])
    assert ids.size == 1
    gid = int(ids[0])
    for loc in [(9, 9), (9, 10), (10, 9), (10, 10)]:
        assert merged[loc] == gid


def test_inconsistent_overlap_keeps_both_sides():
    # The right tile splits the seam cell into two halves; the left tile
    # keeps it whole.  Mutual-best merging must not zero out or duplicate
    # voxels: every voxel labeled in a block's own core survives, and the
    # whole-cell label merges with its best half.
    def seg_fn(block):
        return _blob_labels(block)

    image = np.zeros((10, 24), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (5, 12), (3, 5))] = 200

    chunks = (10, 12)
    depth = 3
    x = da.from_array(image, chunks=chunks)
    expanded = tuple(tuple(int(c) + 2 * depth for c in a) for a in x.chunks)

    def split_right(block, block_info=None):
        labels = _blob_labels(block)
        if block_info[0]["chunk-location"][1] == 1:
            # artificial disagreement: split every object at the local midline
            mid = block.shape[1] // 2
            right = labels[:, mid:]
            right = np.where(right > 0, right + 1000, 0).astype(np.uint32)
            labels = labels.copy()
            labels[:, mid:] = right
        return labels

    tiled = da.map_overlap(
        split_right, x, depth=depth, boundary="reflect", trim=False,
        dtype=np.uint32, chunks=expanded, meta=np.array((), dtype=np.uint32),
    )
    views, infos = {}, {}

    def capture(block, block_info=None):
        loc = tuple(int(v) for v in block_info[0]["chunk-location"])
        views[loc] = np.asarray(block)
        infos[loc] = block_info
        return block

    da.map_overlap(
        capture, tiled, depth=depth, boundary="none", trim=False,
        dtype=np.uint32, chunks=expanded, allow_rechunk=False,
        meta=np.array((), dtype=np.uint32),
    ).compute()

    core_shapes = {
        loc: tuple(int(x.chunks[a][loc[a]]) for a in range(2)) for loc in views
    }
    starts = [0, 12]
    core_starts = {
        loc: (starts[loc[0]], starts[loc[1]]) for loc in views
    }
    resources = {
        "axes": ("Y", "X"),
        "seg_depth": {"Y": depth, "X": depth},
        "numblocks": tiled.numblocks,
        "min_contact_voxels": 1,
        "core_shapes": core_shapes,
        "core_starts": core_starts,
    }
    merged = np.zeros(image.shape, dtype=np.uint64)
    for loc, view in views.items():
        ctx = _FakeBlockContext(infos[loc], resources)
        out = process_segmentation_block(view, 1, ctx=ctx)
        merged[starts[loc[0]]:starts[loc[0]] + out.shape[0],
               starts[loc[1]]:starts[loc[1]] + out.shape[1]] = out

    # no voxel lost or invented anywhere
    assert np.array_equal(merged != 0, image > 0)
    # the left half of the seam cell carries one id; the right tile's two
    # halves each keep a valid id, and the best half shares the left id
    left_ids = set(np.unique(merged[:, :12]).tolist()) - {0}
    right_ids = set(np.unique(merged[:, 12:]).tolist()) - {0}
    assert len(left_ids) == 1
    assert len(right_ids) >= 1
    assert left_ids & right_ids  # at least the mutual-best half merged


def test_3d_cross_boundary_cell():
    image = np.zeros((12, 14, 16), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (6, 7, 8), (3, 4, 5))] = 200
    merged = _simulate_pair(
        image, chunks=(6, 14, 8), depth=2, seg_fn=_blob_labels
    )
    from scipy import ndimage

    global_labels, _ = ndimage.label(image > 0)
    _assert_bijective(merged, global_labels)
    assert np.unique(merged[merged != 0]).size == 1


def test_halo_mismatch_raises():
    image = np.zeros((10, 10), dtype=np.uint8)
    x = da.from_array(image, chunks=(5, 5))
    depth = 2
    expanded = tuple(tuple(int(c) + 2 * depth for c in a) for a in x.chunks)
    tiled = da.map_overlap(
        _blob_labels, x, depth=depth, boundary="reflect", trim=False,
        dtype=np.uint32, chunks=expanded, meta=np.array((), dtype=np.uint32),
    )
    views, infos = {}, {}

    def capture(block, block_info=None):
        loc = tuple(int(v) for v in block_info[0]["chunk-location"])
        views[loc] = np.asarray(block)
        infos[loc] = block_info
        return block

    da.map_overlap(
        capture, tiled, depth=depth, boundary="none", trim=False,
        dtype=np.uint32, chunks=expanded, allow_rechunk=False,
        meta=np.array((), dtype=np.uint32),
    ).compute()
    resources = {
        "axes": ("Y", "X"),
        "seg_depth": {"Y": depth + 1, "X": depth + 1},  # wrong depth
        "numblocks": tiled.numblocks,
        "min_contact_voxels": 1,
        "core_shapes": {loc: (5, 5) for loc in views},
        "core_starts": {loc: (5 * loc[0], 5 * loc[1]) for loc in views},
    }
    ctx = _FakeBlockContext(infos[(0, 0)], resources)
    with pytest.raises(ValueError, match="halo mismatch"):
        process_segmentation_block(views[(0, 0)], 1, ctx=ctx)


def test_table_rows_record_full_bbox(tmp_path):
    image = np.zeros((20, 24), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (10, 12), (4, 6))] = 200
    table_dir = tmp_path / "seg_table"
    table_dir.mkdir()
    merged = _simulate_pair(image, chunks=(10, 12), depth=3, seg_fn=_blob_labels)
    _simulate_writer(merged, chunks=(10, 12), depth=3, output_dir=str(table_dir))

    rows = _read_rows(table_dir)
    # the cell sits on the block corner and exceeds the halo reach, so
    # several blocks record partial rows under one canonical id
    gid = int(np.unique(merged[merged != 0])[0])
    assert {row["global_cell_id"] for row in rows} == {gid}
    ys, xs = np.nonzero(image)
    # the union of the partial bboxes recovers the true bbox
    assert min(row["bbox_y_min"] for row in rows) == int(ys.min())
    assert max(row["bbox_y_max"] for row in rows) == int(ys.max())
    assert min(row["bbox_x_min"] for row in rows) == int(xs.min())
    assert max(row["bbox_x_max"] for row in rows) == int(xs.max())
    assert any(row["touches_tile_boundary"] for row in rows)


def test_table_rows_contained_cell_exact(tmp_path):
    image = np.zeros((20, 24), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (4, 5), (2, 3))] = 200
    table_dir = tmp_path / "seg_table"
    table_dir.mkdir()
    merged = _simulate_pair(image, chunks=(10, 12), depth=3, seg_fn=_blob_labels)
    _simulate_writer(merged, chunks=(10, 12), depth=3, output_dir=str(table_dir))

    rows = _read_rows(table_dir)
    assert len(rows) == 1
    row = rows[0]
    ys, xs = np.nonzero(image)
    assert row["bbox_y_min"] == int(ys.min())
    assert row["bbox_y_max"] == int(ys.max())
    assert row["bbox_x_min"] == int(xs.min())
    assert row["bbox_x_max"] == int(xs.max())
    assert row["area_or_volume"] == int((image > 0).sum())
    assert not row["touches_tile_boundary"]
    assert row["global_cell_id"] == int(np.unique(merged[merged != 0])[0])


def test_table_writer_cross_boundary_full_bbox(tmp_path):
    # a seam-crossing cell that fits inside core+2*halo is measured whole
    image = np.zeros((20, 24), dtype=np.uint8)
    image[_ellipse_mask(image.shape, (10, 12), (2, 2))] = 200
    table_dir = tmp_path / "seg_table"
    table_dir.mkdir()
    merged = _simulate_pair(image, chunks=(10, 12), depth=3, seg_fn=_blob_labels)
    _simulate_writer(merged, chunks=(10, 12), depth=3, output_dir=str(table_dir))

    rows = _read_rows(table_dir)
    assert len(rows) == 1
    row = rows[0]
    ys, xs = np.nonzero(image)
    assert (row["bbox_y_min"], row["bbox_y_max"]) == (int(ys.min()), int(ys.max()))
    assert (row["bbox_x_min"], row["bbox_x_max"]) == (int(xs.min()), int(xs.max()))
    assert row["area_or_volume"] == int((image > 0).sum())
    assert row["global_cell_id"] == int(np.unique(merged[merged != 0])[0])


def test_depth_resolution_from_metadata():
    arr = da.zeros((10, 20, 30), chunks=(5, 10, 15), dtype=np.uint32)
    attach_seg_halo_metadata(
        arr, {"version": 1, "axes": ("Z", "Y", "X"), "depth": {"Z": 2, "Y": 4, "X": 5}}
    )
    depths = _resolve_seg_halo_depths(arr, ("Z", "Y", "X"), {})
    assert depths == {"Z": 2, "Y": 4, "X": 5}
    # explicit params win over metadata
    depths = _resolve_seg_halo_depths(arr, ("Z", "Y", "X"), {"overlap_y": 7})
    assert depths == {"Z": 2, "Y": 7, "X": 5}
    # no metadata and no param -> error
    with pytest.raises(ValueError, match="overlap"):
        _resolve_seg_halo_depths(
            da.zeros((4, 4), chunks=2, dtype=np.uint32), ("Y", "X"), {}
        )


def test_mutual_best_pairs_rejects_ambiguous_split():
    uf_pairs = _mutual_best_label_pairs(
        np.array([[1, 1, 1, 1]], dtype=np.uint64),
        np.array([[2, 2, 3, 3]], dtype=np.uint64),
        1,
    )
    # 1's best is ambiguous (2 vs 3 tie) -> no pair accepted
    assert uf_pairs == ()
    pairs = _mutual_best_label_pairs(
        np.array([[1, 1, 1, 1, 1]], dtype=np.uint64),
        np.array([[2, 2, 2, 3, 3]], dtype=np.uint64),
        1,
    )
    assert pairs == ((1, 2),)
    uf = _LabelUnionFind()
    uf.union(5, 3)
    uf.union(7, 5)
    assert uf.find(7) == 3


def test_node_registration_and_types():
    from core.registry import NODE_CLASS_MAPPINGS
    from nodes.write_seg_cell_table_node import WriteSegCellTable

    assert NODE_CLASS_MAPPINGS["cellpose_segmentation"] is CellposeSegmentation
    assert NODE_CLASS_MAPPINGS["process_segmentation"] is ProcessSegmentation
    assert NODE_CLASS_MAPPINGS["write_seg_cell_table"] is WriteSegCellTable
    assert CellposeSegmentation.RETURN_TYPES == ("DASK_ARRAY[uint32]",)
    assert CellposeSegmentation.PROCESS_BLOCK is cellpose_segmentation_block
    assert ProcessSegmentation.RETURN_TYPES == ("DASK_ARRAY[uint64]",)
    assert ProcessSegmentation.RETURN_NAMES == ("mask",)
    assert WriteSegCellTable.RETURN_TYPES == ("DASK_ARRAY[uint8]",)
    assert WriteSegCellTable.OUTPUT_NODE is True


class _FakeGateContext:
    """Minimal BlockContext stand-in for cellpose_segmentation_block."""

    def __init__(self, axes, device="cpu"):
        self.resources = {"axes": axes}
        self.block_locations = (tuple(0 for _ in axes),)
        self.device = device
        self.primary_input_name = "image"


def _patched_cellpose_block(monkeypatch):
    import nodes.cellpose_segmentation_node as seg_node

    calls = []

    def fake_cellpose_block(image, **kwargs):
        calls.append(kwargs)
        return "delegated"

    monkeypatch.setattr(seg_node, "cellpose_block", fake_cellpose_block)
    return calls


def test_brightness_gate_skips_dim_block(monkeypatch):
    calls = _patched_cellpose_block(monkeypatch)
    image = np.zeros((2, 10, 10), dtype=np.uint16)
    ctx = _FakeGateContext(("C", "Y", "X"))
    out = cellpose_segmentation_block(
        image, primary_channel=0, brightness_threshold=100.0, ctx=ctx,
    )
    assert out.shape == (10, 10)
    assert out.dtype == np.uint32
    assert not out.any()
    assert not calls


def test_brightness_gate_passes_bright_block(monkeypatch):
    calls = _patched_cellpose_block(monkeypatch)
    image = np.full((2, 10, 10), 500, dtype=np.uint16)
    ctx = _FakeGateContext(("C", "Y", "X"))
    out = cellpose_segmentation_block(
        image,
        primary_channel=0,
        brightness_threshold=100.0,
        ctx=ctx,
        model_name="cpsam",
        overlap_z=30,  # node-level params must not reach cellpose_block
    )
    assert out == "delegated"
    assert len(calls) == 1
    assert calls[0]["model_name"] == "cpsam"
    assert "overlap_z" not in calls[0]
    assert "brightness_threshold" not in calls[0]


def test_brightness_gate_uses_primary_channel_only(monkeypatch):
    calls = _patched_cellpose_block(monkeypatch)
    image = np.zeros((2, 10, 10), dtype=np.uint16)
    image[1] = 1000  # bright secondary channel, dark primary
    ctx = _FakeGateContext(("C", "Y", "X"))
    out = cellpose_segmentation_block(
        image, primary_channel=0, brightness_threshold=100.0, ctx=ctx,
    )
    assert not calls
    assert not out.any()
    out = cellpose_segmentation_block(
        image, primary_channel=1, brightness_threshold=100.0, ctx=ctx,
    )
    assert out == "delegated"
    assert len(calls) == 1


def test_brightness_gate_disabled_at_zero(monkeypatch):
    calls = _patched_cellpose_block(monkeypatch)
    image = np.zeros((10, 10), dtype=np.uint16)
    ctx = _FakeGateContext(("Y", "X"))
    out = cellpose_segmentation_block(image, brightness_threshold=0.0, ctx=ctx)
    assert out == "delegated"
    assert len(calls) == 1


def test_brightness_gate_validates_primary_channel():
    image = np.zeros((2, 10, 10), dtype=np.uint16)
    ctx = _FakeGateContext(("C", "Y", "X"))
    with pytest.raises(ValueError, match="primary_channel"):
        cellpose_segmentation_block(
            image, primary_channel=5, brightness_threshold=100.0, ctx=ctx,
        )


def test_is_block_dim_matches_nearest_rank_percentile():
    import math

    rng = np.random.default_rng(0)
    view = rng.integers(0, 1000, size=(7, 65, 33)).astype(np.float32)
    flat = view.reshape(-1)
    k = max(1, math.ceil(0.99 * flat.size))
    p99 = np.partition(flat, k - 1)[k - 1]
    for threshold in (0.0, 1.0, 100.0, 500.0, 900.0, 990.0, 999.0, 1000.0):
        assert _is_block_dim(view, threshold, "cpu") == bool(p99 < threshold)
    # constant blocks: all voxels below -> dim, all voxels above -> not dim
    assert _is_block_dim(np.zeros((4, 4), dtype=np.uint16), 1.0, "cpu")
    assert not _is_block_dim(np.full((4, 4), 5, dtype=np.uint16), 1.0, "cpu")


def test_seg_models_scanned_from_node_directory():
    from core.model_registry import resolve_model_path
    from nodes.cellpose_segmentation_node import list_seg_models

    seg_models = list_seg_models()
    assert "199082" in seg_models
    assert "cpdino-vitb" in seg_models

    model_input = CellposeSegmentation.INPUT_TYPES()["required"]["model_name"]
    for name in seg_models:
        assert name in model_input[0]
    assert model_input[1]["default"] == "199082"
    # no duplicates from builtin-name overlap
    assert len(model_input[0]) == len(set(model_input[0]))

    for name in seg_models:
        resolved = resolve_model_path("cellpose", name)
        assert resolved is not None
        assert resolved.endswith(f"cellpose_segmentation/{name}")


def test_duplicate_terminal_output_paths_rejected():
    import nodes.write_seg_cell_table_node  # noqa: F401  (register)
    import nodes.zarr_writer_node  # noqa: F401  (register)
    from services.executor import validate_graph_types

    def graph_with(table_dir, zarr_path):
        return {
            "table": {
                "type": "write_seg_cell_table",
                "inputs": {"output_dir": table_dir},
            },
            "zarr": {
                "type": "ZarrWriter",
                "inputs": {"output_path": zarr_path},
            },
        }

    with pytest.raises(ValueError, match="Output path conflict"):
        validate_graph_types(graph_with("/data/seg.zarr", "/data/seg.zarr"))
    with pytest.raises(ValueError, match="Output path conflict"):
        validate_graph_types(graph_with("/data/seg.zarr", "/data/seg.zarr/mask.zarr"))
    validate_graph_types(graph_with("/data/table", "/data/seg.zarr"))
