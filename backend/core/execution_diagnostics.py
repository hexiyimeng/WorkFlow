"""Small, serializable failure context for the execution console."""

import traceback


def exception_diagnostics(exc: BaseException, stage: str) -> dict:
    context = {
        "stage": stage,
        "errorType": type(exc).__name__,
        "traceback": "".join(traceback.format_exception(exc))[-16000:],
    }
    node_id = getattr(exc, "_workflow_node_id", None)
    if node_id:
        context["taskId"] = str(node_id)
    window = getattr(exc, "_workflow_window", None)
    if window is not None:
        context["stage"] = f"{stage} (Window {window})"
    context.update(getattr(exc, "_workflow_dask_context", {}))
    return context


def scheduler_failure_context(keys, dask_scheduler=None) -> dict:
    """Follow a failed sink to the task that actually raised the exception.

    A Writer Future may fail because an upstream Reader or algorithm failed.
    Only report a node from its task annotations; never assume it is the sink.
    """
    for key in keys:
        task = dask_scheduler.tasks.get(key)
        if task is None or task.state != "erred":
            continue
        origin = task.exception_blame or task
        context = {"taskKey": str(origin.key)}
        node_id = (origin.annotations or {}).get("brainflow_node_id")
        if node_id:
            context["taskId"] = str(node_id)
        if origin.traceback_text:
            context["traceback"] = origin.traceback_text[-16000:]
        return context
    return {}
