from types import SimpleNamespace

import dask
import asyncio
import pytest
from distributed import Client, LocalCluster, wait

from core.execution_diagnostics import exception_diagnostics, scheduler_failure_context


def fail_algorithm():
    raise ValueError("Test algorithm rejected this image block")


def test_failed_sink_reports_upstream_algorithm():
    from services.executor import _collect_failed_task_context

    with LocalCluster(n_workers=1, threads_per_worker=1, processes=False,
                      dashboard_address=None) as cluster, Client(cluster) as client:
        with dask.annotate(brainflow_node_id="algorithm-node"):
            algorithm = dask.delayed(fail_algorithm)()
        with dask.annotate(brainflow_node_id="writer-node"):
            sink = dask.delayed(str)(algorithm)
        future = client.compute(sink, optimize_graph=False)
        wait(future)
        context = asyncio.run(_collect_failed_task_context(client, [future]))
        assert context["taskId"] == "algorithm-node"
        assert "fail_algorithm" in context["taskKey"]
        assert "fail_algorithm" in context["traceback"]
        failure = future.exception()
        failure._workflow_dask_context = context
        failure._workflow_window = 3
        future.release()
        diagnostics = exception_diagnostics(failure, "Dask task execution")
        assert diagnostics["taskId"] == "algorithm-node"
        assert diagnostics["stage"] == "Dask task execution (Window 3)"


def test_failure_context_is_optional_when_scheduler_is_unavailable():
    from services.executor import _collect_failed_task_context

    future = SimpleNamespace(key="missing-task", status="error")
    assert asyncio.run(_collect_failed_task_context(None, [future])) == {}


def test_missing_or_nonfailed_tasks_are_not_misattributed():
    scheduler = SimpleNamespace(tasks={"pending": SimpleNamespace(state="waiting")})
    assert scheduler_failure_context(["missing", "pending"], scheduler) == {}


def test_exception_details_include_stage_type_and_cause():
    try:
        raise FileNotFoundError("Input path does not exist")
    except FileNotFoundError as exc:
        details = exception_diagnostics(exc, "Graph building")
    assert details["stage"] == "Graph building"
    assert details["errorType"] == "FileNotFoundError"
    assert "Input path does not exist" in details["traceback"]


def test_graph_building_reports_original_node_and_blocked_dependents(monkeypatch):
    from services import executor

    class BrokenReader:
        def execute(self):
            raise FileNotFoundError("Microscopy input is missing")

    class Writer:
        def execute(self, image):
            return image

    monkeypatch.setitem(executor.NODE_CLASS_MAPPINGS, "BrokenReader", BrokenReader)
    monkeypatch.setitem(executor.NODE_CLASS_MAPPINGS, "TestWriter", Writer)
    events = []

    async def report(node_id, progress, message, run_state):
        events.append((node_id, message, run_state))

    graph = {
        "reader": {"type": "BrokenReader", "inputs": {}},
        "writer": {"type": "TestWriter", "inputs": {"image": ["reader", 0]}},
    }
    with pytest.raises(FileNotFoundError) as failure:
        asyncio.run(executor._build_lazy_execution_roots(
            graph, ["writer"], execution_id="test", progress_callback=report,
        ))
    assert exception_diagnostics(failure.value, "Graph building")["taskId"] == "reader"
    assert ("reader", "FileNotFoundError: Microscopy input is missing", "failed") in events
    assert ("writer", "Blocked by failed upstream node [reader]", "failed") in events
