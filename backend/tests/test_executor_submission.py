from __future__ import annotations

import dask
import dask.array as da

from services.executor import _compute_with_resource_boundaries


class _OptimizingClient:
    def __init__(self) -> None:
        self.compute_kwargs: dict[str, object] | None = None
        self.optimized_task_count: int | None = None
        self.optimized_worker_profiles: set[str] = set()

    def compute(self, collections, **kwargs):
        self.compute_kwargs = kwargs
        assert dask.config.get("optimization.annotations.fuse") is False
        assert dask.config.get("optimization.fuse.active") is False

        collection = collections[0]
        optimized = collection.__dask_optimize__(
            collection.__dask_graph__(),
            collection.__dask_keys__(),
        )
        self.optimized_task_count = len(optimized)
        self.optimized_worker_profiles = {
            annotations["worker_profile"]
            for layer in optimized.layers.values()
            if (annotations := layer.annotations)
            and "worker_profile" in annotations
        }
        return [object()]


def test_resource_boundaries_keep_window_graph_culling_enabled() -> None:
    source = da.zeros((1024, 1024), chunks=(64, 64))
    with dask.annotate(
        brainflow_node_id="reader",
        worker_profile="cpu-reader",
        resources={"cpu-reader": 1},
    ):
        reader = source + 1
    with dask.annotate(
        brainflow_node_id="cellpose",
        worker_profile="gpu-cellpose",
        resources={"gpu-cellpose": 1},
    ):
        output = reader.map_blocks(lambda block: block, dtype=reader.dtype)

    window = output[:64, :64]
    unculled_task_count = len(window.__dask_graph__())
    client = _OptimizingClient()

    futures = _compute_with_resource_boundaries(
        client,
        [window],
        preserve_resource_boundaries=True,
    )

    assert len(futures) == 1
    assert client.compute_kwargs == {}
    assert client.optimized_task_count is not None
    assert client.optimized_task_count < unculled_task_count / 10
    assert {"cpu-reader", "gpu-cellpose"}.issubset(
        client.optimized_worker_profiles
    )


class _RecordingClient:
    def __init__(self) -> None:
        self.compute_kwargs: dict[str, object] | None = None

    def compute(self, collections, **kwargs):
        del collections
        self.compute_kwargs = kwargs
        return []


def test_standard_submission_does_not_override_global_optimization() -> None:
    client = _RecordingClient()

    _compute_with_resource_boundaries(
        client,
        [],
        preserve_resource_boundaries=False,
    )

    assert client.compute_kwargs == {}
