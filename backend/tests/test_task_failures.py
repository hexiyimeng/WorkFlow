from __future__ import annotations

from collections import deque

import pytest

from core.task_failures import (
    FAILURE_EVENT_TOPIC,
    TaskFailureTracker,
    failure_signature,
    is_infrastructure_error,
    node_failure_threshold,
)
from core.worker_profiles import dask_annotation_kwargs, task_retries


def test_failure_signature_ignores_numbers_and_paths() -> None:
    first = RuntimeError("CUDA error at /tmp/a/out_123.bin block 42")
    second = RuntimeError("CUDA error at /data/b/out_987.bin block 7")
    assert failure_signature(first) == failure_signature(second)
    assert failure_signature(first) != failure_signature(ValueError("shape mismatch"))


def test_infrastructure_error_detection() -> None:
    assert is_infrastructure_error(RuntimeError("CUDA error: ECC error encountered"))
    assert is_infrastructure_error(OSError("Input/output error (errno 5)"))
    assert is_infrastructure_error(TimeoutError("connection timed out"))
    assert not is_infrastructure_error(ValueError("axes length mismatch"))
    assert not is_infrastructure_error(MemoryError("cannot allocate array"))


def test_threshold_env(monkeypatch) -> None:
    monkeypatch.delenv("WorkFlow_NODE_FAILURE_THRESHOLD", raising=False)
    assert node_failure_threshold() == 2
    monkeypatch.setenv("WorkFlow_NODE_FAILURE_THRESHOLD", "5")
    assert node_failure_threshold() == 5
    monkeypatch.setenv("WorkFlow_NODE_FAILURE_THRESHOLD", "bogus")
    assert node_failure_threshold() == 2


def test_task_retries_env(monkeypatch) -> None:
    monkeypatch.delenv("WorkFlow_TASK_RETRIES", raising=False)
    assert task_retries() == 3
    monkeypatch.setenv("WorkFlow_TASK_RETRIES", "0")
    assert task_retries() == 0


def test_dask_annotation_includes_retries(monkeypatch) -> None:
    monkeypatch.setenv("WorkFlow_TASK_RETRIES", "4")

    class _Node:
        required_worker_profile = "GPU"

    annotations = dask_annotation_kwargs(_Node, "node-1")
    assert annotations["retries"] == 4
    assert annotations["resources"] == {"GPU": 1.0}


def _observe(tracker, host, sig="sig", infra=False, summary="boom"):
    return tracker.observe(host=host, signature=sig, infra=infra, summary=summary)


def test_transient_then_same_host_marked_bad() -> None:
    tracker = TaskFailureTracker(threshold=2)
    assert _observe(tracker, "cn001").category == TaskFailureTracker.TRANSIENT
    assert tracker.bad_nodes == set()
    verdict = _observe(tracker, "cn001")
    assert verdict.category == TaskFailureTracker.NODE
    assert tracker.bad_nodes == {"cn001"}


def test_deterministic_when_signature_spans_hosts() -> None:
    tracker = TaskFailureTracker(threshold=3)
    assert _observe(tracker, "cn001", infra=False).category == TaskFailureTracker.TRANSIENT
    verdict = _observe(tracker, "cn002", infra=False)
    assert verdict.category == TaskFailureTracker.DETERMINISTIC


def test_systemic_when_infra_signature_spans_hosts() -> None:
    tracker = TaskFailureTracker(threshold=3)
    # infra 首现即标记坏节点（first-strike）
    assert _observe(tracker, "cn001", infra=True).category == TaskFailureTracker.NODE
    verdict = _observe(tracker, "cn002", infra=True)
    assert verdict.category == TaskFailureTracker.SYSTEMIC


def test_known_bad_node_reports_node_again() -> None:
    tracker = TaskFailureTracker(threshold=1)
    assert _observe(tracker, "cn009").category == TaskFailureTracker.NODE
    verdict = _observe(tracker, "cn009", sig="other")
    assert verdict.category == TaskFailureTracker.NODE
    assert tracker.bad_nodes == {"cn009"}


def test_drain_events_cursor_and_fatal() -> None:
    from services.slurm_execution_service import _drain_workflow_failure_events

    tracker = TaskFailureTracker(threshold=5)
    cursor = {}
    events = deque()
    events.append((0.0, {"host": "cn001", "signature": "s1", "infra": False, "summary": "x"}))
    result = _drain_workflow_failure_events(events, tracker, cursor)
    assert result["fatal"] is None and result["notices"] == []
    # 已处理的事件不会重复消费
    assert _drain_workflow_failure_events(events, tracker, cursor) == {
        "fatal": None, "notices": [], "new_bad_nodes": set(),
    }
    events.append((1.0, {"host": "cn002", "signature": "s1", "infra": False, "summary": "x"}))
    result = _drain_workflow_failure_events(events, tracker, cursor)
    assert result["fatal"] is not None
    assert result["fatal"].category == TaskFailureTracker.DETERMINISTIC


def test_drain_events_node_notice() -> None:
    from services.slurm_execution_service import _drain_workflow_failure_events

    tracker = TaskFailureTracker(threshold=1)
    events = deque([(0.0, {"host": "cn007", "signature": "s9", "infra": True, "summary": "io"})])
    result = _drain_workflow_failure_events(events, tracker, {})
    assert result["fatal"] is None
    assert result["notices"] and "cn007" in result["notices"][0]
    assert "cn007" in tracker.bad_nodes


def test_elastic_job_merges_bad_nodes() -> None:
    from core.resource_planner import SlurmJobRequirement
    from services.slurm_execution_service import _elastic_job_with_bad_nodes

    template = SlurmJobRequirement(
        allocation_id="GPU-1", profile="GPU", node="", partition="gpu",
        workers=1, processes=1, cpu=8, memory_gib=32, gpu=1,
        logical_resources={"GPU": 1}, excluded_nodes=("cn001",),
    )
    job = _elastic_job_with_bad_nodes(template, "GPU-elastic-2", {"cn009", "cn005"})
    assert job.allocation_id == "GPU-elastic-2"
    assert job.excluded_nodes == ("cn001", "cn005", "cn009")
    assert template.excluded_nodes == ("cn001",)


def test_report_reaches_scheduler_event_log() -> None:
    from distributed import Client

    def failing_task():
        from core.task_failures import report_block_failure
        try:
            raise RuntimeError("CUDA error: ECC error encountered")
        except RuntimeError as exc:
            report_block_failure("Flaky", "node-1", exc)
            raise

    with Client(n_workers=1, processes=True, threads_per_worker=1,
                dashboard_address=None) as client:
        future = client.submit(failing_task, retries=0)
        with pytest.raises(RuntimeError):
            future.result()
        events = client.run_on_scheduler(
            lambda dask_scheduler: list(
                dask_scheduler.get_events(FAILURE_EVENT_TOPIC)
            )
        )
        assert events, "worker failure report did not reach the scheduler log"
        payload = events[0][1]
        assert payload["infra"] is True
        assert payload["signature"]


def test_dask_retries_recovers_transient_failure() -> None:
    from distributed import Client

    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("transient failure")
        return 42

    with Client(n_workers=1, processes=False, threads_per_worker=1,
                dashboard_address=None) as client:
        # retries=0 时必然失败；retries=1 时第二次尝试成功，即证明重试生效。
        with pytest.raises(RuntimeError):
            client.submit(flaky, retries=0, pure=False).result()
        assert client.submit(flaky, retries=1, pure=False).result() == 42


def test_collect_failure_verdicts_via_real_cluster_sync() -> None:
    """Regression: the monitor's drain must be awaitable for cluster.sync."""

    from distributed import Client
    from services.slurm_execution_service import _collect_failure_verdicts

    with Client(n_workers=1, processes=False, threads_per_worker=1,
                dashboard_address=None) as client:
        tracker = TaskFailureTracker(threshold=1)
        cursor: dict = {}
        client.run_on_scheduler(
            lambda dask_scheduler: dask_scheduler.log_event(
                FAILURE_EVENT_TOPIC,
                {"host": "cn007", "signature": "s1", "infra": True, "summary": "io"},
            )
        )
        drained = client.cluster.sync(
            _collect_failure_verdicts, client.cluster, tracker, cursor
        )
        assert drained["fatal"] is None
        assert drained["notices"] and "cn007" in drained["notices"][0]
        assert "cn007" in tracker.bad_nodes
        # 游标已推进，再次调用不重复消费
        drained_again = client.cluster.sync(
            _collect_failure_verdicts, client.cluster, tracker, cursor
        )
        assert drained_again == {"fatal": None, "notices": [], "new_bad_nodes": set()}


def test_bad_node_persistence_roundtrip_and_ttl(tmp_path) -> None:
    import time as time_module
    from core.task_failures import load_bad_nodes, record_bad_node

    path = tmp_path / "bad-nodes.json"
    assert load_bad_nodes(path) == set()
    record_bad_node(path, "gn004", "CUDA error")
    record_bad_node(path, "cn007", "io error")
    assert load_bad_nodes(path) == {"gn004", "cn007"}

    import json as json_module
    data = json_module.loads(path.read_text(encoding="utf-8"))
    data["gn004"]["marked_at"] = time_module.time() - 49 * 3600
    path.write_text(json_module.dumps(data), encoding="utf-8")
    assert load_bad_nodes(path) == {"cn007"}

    path.write_text("{broken", encoding="utf-8")
    assert load_bad_nodes(path) == set()


def test_drain_reports_only_new_bad_nodes() -> None:
    from services.slurm_execution_service import _drain_workflow_failure_events

    tracker = TaskFailureTracker(threshold=1, bad_nodes={"cn001"})
    events = deque([(0.0, {"host": "cn001", "signature": "s1", "infra": False, "summary": "x"})])
    result = _drain_workflow_failure_events(events, tracker, {})
    assert result["new_bad_nodes"] == set()
    events.append((1.0, {"host": "cn002", "signature": "s2", "infra": False, "summary": "y"}))
    result = _drain_workflow_failure_events(events, tracker, {"index": 1})
    assert result["new_bad_nodes"] == {"cn002"}


def test_tracker_summary_for() -> None:
    tracker = TaskFailureTracker(threshold=2)
    tracker.observe(host="cn001", signature="s1", infra=False, summary="first boom")
    tracker.observe(host="cn002", signature="s2", infra=False, summary="second boom")
    assert tracker.summary_for("cn001") == "first boom"
    assert tracker.summary_for("cn002") == "second boom"
    assert tracker.summary_for("cn999") == ""


def test_monitor_scheduler_callbacks_are_coroutine_functions() -> None:
    import inspect
    from services import slurm_execution_service as svc

    for name in (
        "_collect_failure_verdicts",
        "_workers_registered",
        "_bad_host_victims",
        "_swap_adaptive_baselines",
    ):
        assert inspect.iscoroutinefunction(getattr(svc, name)), name


def test_workers_registered_via_real_cluster_sync() -> None:
    from distributed import Client
    from services.slurm_execution_service import _workers_registered

    with Client(n_workers=1, processes=False, threads_per_worker=1,
                dashboard_address=None) as client:
        assert client.cluster.sync(_workers_registered, client.cluster, set())
        assert not client.cluster.sync(
            _workers_registered, client.cluster, {"no-such-worker"})


def test_infra_first_strike_marks_node() -> None:
    tracker = TaskFailureTracker(threshold=2)
    verdict = _observe(tracker, "gn004", infra=True)
    assert verdict.category == TaskFailureTracker.NODE
    assert "gn004" in tracker.bad_nodes


def test_non_infra_first_occurrence_stays_transient() -> None:
    tracker = TaskFailureTracker(threshold=2)
    assert _observe(tracker, "cn001", infra=False).category == TaskFailureTracker.TRANSIENT
    assert tracker.bad_nodes == set()


def test_bad_node_ttl_tier_escalation(tmp_path) -> None:
    import time as time_module
    from core.task_failures import (
        BAD_NODE_TTL_TIERS,
        load_bad_nodes,
        record_bad_node,
    )

    path = tmp_path / "bad-nodes.json"
    record_bad_node(path, "gn004", "boom")
    assert load_bad_nodes(path, now=time_module.time() + 47 * 3600) == {"gn004"}
    assert load_bad_nodes(path, now=time_module.time() + 49 * 3600) == set()

    record_bad_node(path, "gn004", "boom again")
    # 第二次标记后 tier 升到 7 天：49h 后仍然有效
    assert load_bad_nodes(path, now=time_module.time() + 4 * 24 * 3600) == {"gn004"}
    assert load_bad_nodes(
        path, now=time_module.time() + 8 * 24 * 3600) == set()

    record_bad_node(path, "gn004", "third")
    # 第三次标记后 tier 升到 30 天
    assert load_bad_nodes(path, now=time_module.time() + 29 * 24 * 3600) == {"gn004"}
    assert load_bad_nodes(
        path, now=time_module.time() + 31 * 24 * 3600) == set()
    assert BAD_NODE_TTL_TIERS[2] == 30 * 24 * 3600


def test_window_soft_failure_completes_healthy_windows() -> None:
    import asyncio
    import dask.array as da
    import numpy as np
    from distributed import Client
    from core.window_execution import WindowGenerator
    from services.executor import _execute_pending_windows

    def maybe_fail(x, block_info=None):
        if block_info[0]["chunk-location"][0] == 3:
            raise RuntimeError("boom")
        return x

    class FakeStore:
        def __init__(self):
            self.done = []
        def mark_completed(self, bitmap, coords):
            self.done.append(coords)
            bitmap[coords] = 1
            return True

    async def _noop(**kwargs):
        return None

    arr = da.map_blocks(maybe_fail, da.zeros((4,), chunks=1), dtype=float)
    store = FakeStore()
    bitmap = np.zeros((2,), dtype=np.uint8)

    with Client(n_workers=1, processes=False, threads_per_worker=2,
                dashboard_address=None) as client:
        with pytest.raises(RuntimeError, match="partially failed"):
            asyncio.run(_execute_pending_windows(
                client=client,
                root_arrays=[arr],
                window_generator=WindowGenerator((4,), (2,)),
                pending_indices=np.arange(2),
                completed_windows_bitmap=bitmap,
                completed_count=0,
                window_store=store,
                max_in_flight_windows=2,
                execution_roots=[],
                tracked_futures=[],
                progress_callback=_noop,
                window_progress_callback=_noop,
                preserve_resource_boundaries=False,
            ))
    assert store.done == [(0,)]
    assert list(bitmap) == [1, 0]
