from __future__ import annotations

import dask
import dask.array as da
from dask._expr import HLGExpr
from distributed import Client, Scheduler, SpecCluster, Worker, get_worker

from core.worker_profiles import (
    dask_annotation_kwargs,
    worker_logical_resources,
)
from core.node_invocation import NodeRuntime
from nodes.base.block_map import (
    BlockContextFactory,
    BlockwiseInputPlan,
    ProcessBlockBinder,
)
from services.executor import (
    _compute_with_resource_boundaries,
    _normalize_futures,
    _wait_for_futures_fail_fast,
)


class _ReaderNode:
    required_worker_profile = "cpu-reader"


class _GpuNode:
    required_worker_profile = "gpu-cellpose"


class _BlockNode:
    DASK_API = "map_blocks"


def test_block_callable_does_not_capture_complete_upstream_plan() -> None:
    upstream_graph_owner = object()
    plan = BlockwiseInputPlan(
        ordered_names=["dask_arr"],
        primary_name="dask_arr",
        array_inputs={"dask_arr": upstream_graph_owner},
        params={},
        ordered_arrays=[upstream_graph_owner],
        axes_by_name={"dask_arr": ("z", "y", "x")},
    )

    wrapped = ProcessBlockBinder().build_wrapped_function(
        _BlockNode(),
        input_plan=plan,
        params={},
        runtime=NodeRuntime(
            node_id="test",
            execution_id="execution",
            worker_profile="cpu-general",
        ),
        preprocess_state={},
        context_factory=BlockContextFactory(),
    )

    # The old closure captured ``input_plan`` and therefore every upstream
    # Dask Array/graph reachable through it.  Each map task then recursively
    # serialized the complete workflow graph.
    assert "input_plan" not in wrapped.__code__.co_freevars
    assert all(
        cell.cell_contents is not upstream_graph_owner
        for cell in (wrapped.__closure__ or ())
    )


def _assert_profile_and_add(block, *, profile: str, amount: int):
    resources = worker_logical_resources(get_worker())
    if resources.get(profile) != 1:
        raise RuntimeError(
            f"Task for {profile!r} ran with Worker resources {resources!r}."
        )
    return block + amount


class _OptimizingClient:
    def __init__(self) -> None:
        self.get_kwargs: dict[str, object] | None = None
        self.submitted_task_count: int | None = None
        self.submitted_worker_profiles: set[str] = set()
        self.submitted_resources: set[str] = set()

    def get(self, graph, keys, **kwargs):
        self.get_kwargs = kwargs
        # Emulate Scheduler-side materialization after the client's temporary
        # optimization config has ended. HLGExpr created by Client.get has no
        # optimizer, so annotations must remain frozen in this graph.
        expression = HLGExpr(graph)
        materialized = expression.__dask_graph__()
        annotations_by_type = expression.__dask_annotations__()
        self.submitted_task_count = len(materialized)
        self.submitted_worker_profiles = {
            str(profile)
            for profile in annotations_by_type.get(
                "required_worker_profile", {}
            ).values()
            if profile is not None
        }
        self.submitted_resources = {
            str(resource_name)
            for resources in annotations_by_type.get("resources", {}).values()
            for resource_name in dict(resources or {})
        }
        return [[object() for _ in collection_keys] for collection_keys in keys]


def test_resource_boundaries_keep_window_graph_culling_enabled() -> None:
    source = da.zeros((1024, 1024), chunks=(64, 64))
    with dask.annotate(**dask_annotation_kwargs(_ReaderNode, "reader")):
        reader = source + 1
    with dask.annotate(**dask_annotation_kwargs(_GpuNode, "cellpose")):
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
    assert client.get_kwargs == {"sync": False}
    assert client.submitted_task_count is not None
    assert client.submitted_task_count < unculled_task_count / 10
    assert {"cpu-reader", "gpu-cellpose"}.issubset(
        client.submitted_worker_profiles
    )
    assert {"cpu-reader", "gpu-cellpose"}.issubset(
        client.submitted_resources
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


def test_nested_block_futures_are_flattened_for_wait_and_cleanup() -> None:
    first = object()
    second = object()
    third = object()

    assert _normalize_futures([[first, second], [[third]]]) == [
        first,
        second,
        third,
    ]


class _CompletedFutures:
    def __init__(self, done, not_done) -> None:
        self.done = done
        self.not_done = not_done


class _FakeFuture:
    def __init__(self, exception: BaseException | None = None) -> None:
        self._exception = exception
        self.inspected = False

    def exception(self):
        self.inspected = True
        return self._exception


def test_window_future_wait_fails_before_pending_sibling_finishes(
    monkeypatch,
) -> None:
    failure = RuntimeError("writer worker exhausted its memory budget")
    failed = _FakeFuture(failure)
    still_pending = _FakeFuture()
    wait_calls = []

    def fake_wait(futures, *, return_when):
        wait_calls.append((list(futures), return_when))
        return _CompletedFutures({failed}, {still_pending})

    monkeypatch.setattr("services.executor.dist_wait", fake_wait)

    try:
        _wait_for_futures_fail_fast([failed, still_pending])
    except RuntimeError as exc:
        assert exc is failure
    else:
        raise AssertionError("the first failed Future was not propagated")

    assert wait_calls == [([failed, still_pending], "FIRST_COMPLETED")]
    assert failed.inspected is True
    assert still_pending.inspected is False


def test_live_scheduler_routes_frozen_layers_to_matching_profiles() -> None:
    cluster = SpecCluster(
        scheduler={
            "cls": Scheduler,
            "options": {"dashboard_address": None},
        },
        workers={
            "reader": {
                "cls": Worker,
                "options": {
                    "nthreads": 1,
                    "resources": {"CPU": 1, "cpu-reader": 1},
                },
            },
            "gpu": {
                "cls": Worker,
                "options": {
                    "nthreads": 1,
                    "resources": {
                        "CPU": 1,
                        "GPU": 1,
                        "gpu-cellpose": 1,
                    },
                },
            },
        },
        asynchronous=False,
    )
    client = Client(cluster)
    try:
        source = da.ones((128, 128), chunks=(64, 64))
        with dask.annotate(**dask_annotation_kwargs(_ReaderNode, "reader")):
            reader = source.map_blocks(
                _assert_profile_and_add,
                profile="cpu-reader",
                amount=1,
                dtype=source.dtype,
            )
        with dask.annotate(**dask_annotation_kwargs(_GpuNode, "cellpose")):
            output = reader.map_blocks(
                _assert_profile_and_add,
                profile="gpu-cellpose",
                amount=2,
                dtype=reader.dtype,
            )

        futures = _normalize_futures(
            _compute_with_resource_boundaries(
                client,
                [output[:64, :64]],
                preserve_resource_boundaries=True,
            )
        )
        result = client.gather(futures)

        assert len(result) == 1
        assert float(result[0].sum()) == 64 * 64 * 4
    finally:
        client.close()
        cluster.close()
