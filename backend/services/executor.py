import asyncio
import inspect
import logging
import operator
import uuid

import dask
import dask.array as da
import numpy as np
from dask.base import is_dask_collection
from distributed import wait as dist_wait

from core.invocation_builder import (
    build_node_invocation,
    get_node_input_defs,
    prepare_node_inputs,
)
from core.registry import NODE_CLASS_MAPPINGS, validate_node_port_types
from core.state_manager import state_manager, ExecutionStatus
from core.type_system import can_connect_types, is_dask_array_type
from core.window_execution import (
    ExecutionConfig,
    WindowCheckpointStore,
    WindowGenerator,
    compute_plan_fingerprint,
    compute_workflow_fingerprint,
    parse_execution_config,
)
from core.worker_cache import force_clear_worker_cache
from services.dask_service import dask_service
from services.memory_monitor import get_memory_monitor

logger = logging.getLogger("BrainFlow.Executor")
logging.getLogger("distributed.core").setLevel(logging.CRITICAL)
logging.getLogger("distributed.utils").setLevel(logging.CRITICAL)


# =============================================================================
# 1. Graph validation
# =============================================================================
def validate_graph_structure(graph: dict):
    if not isinstance(graph, dict):
        raise ValueError("graph must be a dict")
    for node_id, node_data in graph.items():
        if not isinstance(node_data, dict):
            raise ValueError(f"Node {node_id} data must be a dict")
        if "type" not in node_data:
            raise ValueError(f"Node {node_id} is missing 'type'")
        type_name = node_data["type"]
        if type_name not in NODE_CLASS_MAPPINGS:
            raise ValueError(f"Node type '{type_name}' is not registered")
        for input_name, val in node_data.get("inputs", {}).items():
            if isinstance(val, list) and len(val) == 2:
                src_id, src_idx = val
                if src_id not in graph:
                    raise ValueError(
                        f"Node {node_id} input '{input_name}' references unknown node '{src_id}'"
                    )
                try:
                    idx = int(src_idx)
                    if idx < 0:
                        raise ValueError(
                            f"Node {node_id} input '{input_name}' has negative output index {idx}"
                        )
                except (ValueError, TypeError):
                    raise ValueError(
                        f"Node {node_id} input '{input_name}' has invalid output index {src_idx!r}"
                    )


def validate_graph_acyclic(graph: dict):
    visited = set()
    recursion_stack = set()

    def dfs(node_id):
        visited.add(node_id)
        recursion_stack.add(node_id)
        node_data = graph.get(node_id)
        if not node_data:
            return
        for val in node_data.get("inputs", {}).values():
            if isinstance(val, list) and len(val) == 2:
                dep_id = val[0]
                if dep_id not in graph:
                    continue
                if dep_id in recursion_stack:
                    raise ValueError(f"Cycle detected: '{node_id}' -> '{dep_id}'")
                if dep_id not in visited:
                    dfs(dep_id)
        recursion_stack.remove(node_id)

    for node_id in graph:
        if node_id not in visited:
            dfs(node_id)


def _get_node_input_defs(node_cls) -> dict:
    return get_node_input_defs(node_cls)


def _get_declared_input_type(node_cls, input_name: str):
    input_defs = _get_node_input_defs(node_cls)
    config = (
        input_defs.get("required", {}).get(input_name)
        or input_defs.get("optional", {}).get(input_name)
    )
    if not config:
        return None
    declared = config[0] if isinstance(config, (tuple, list)) and len(config) > 0 else config
    if isinstance(declared, list):
        return None
    return declared


def _resolve_source_return_types(node_cls, node_inputs: dict, graph: dict | None = None):
    resolver = getattr(node_cls, "RESOLVE_RETURN_TYPES", None)
    if resolver is not None:
        try:
            input_types = _resolve_connected_input_types(node_inputs or {}, graph)
            try:
                resolved = resolver(node_inputs or {}, input_types=input_types)
            except TypeError:
                resolved = resolver(node_inputs or {})
            if resolved:
                return tuple(resolved)
        except Exception as e:
            logger.warning(f"Failed to resolve dynamic RETURN_TYPES for {node_cls}: {e}")
    return tuple(getattr(node_cls, "RETURN_TYPES", ()))


def _resolve_connected_input_types(node_inputs: dict, graph: dict | None = None) -> dict:
    if not graph:
        return {}
    input_types = {}
    for input_name, input_value in (node_inputs or {}).items():
        if not (isinstance(input_value, list) and len(input_value) == 2):
            continue
        source_id, source_output_index = input_value
        source_data = graph.get(source_id)
        if source_data is None:
            continue
        source_cls = NODE_CLASS_MAPPINGS.get(source_data.get("type"))
        if source_cls is None:
            continue
        try:
            source_output_index = int(source_output_index)
        except Exception:
            continue
        source_types = _resolve_source_return_types(source_cls, source_data.get("inputs", {}), graph)
        if 0 <= source_output_index < len(source_types):
            input_types[input_name] = str(source_types[source_output_index])
    return input_types


def validate_graph_types(graph: dict):
    for target_id, target_data in graph.items():
        target_type_name = target_data.get("type")
        target_cls = NODE_CLASS_MAPPINGS.get(target_type_name)
        if target_cls is None:
            continue
        try:
            validate_node_port_types(
                _get_node_input_defs(target_cls),
                getattr(target_cls, "RETURN_TYPES", ()),
            )
        except ValueError as exc:
            raise ValueError(
                f"Node type '{target_type_name}' has an unsupported port declaration: {exc}"
            ) from exc

        for input_name, input_value in target_data.get("inputs", {}).items():
            if not (isinstance(input_value, list) and len(input_value) == 2):
                continue

            source_id, source_output_index = input_value
            source_data = graph.get(source_id)
            if source_data is None:
                continue

            source_type_name = source_data.get("type")
            source_cls = NODE_CLASS_MAPPINGS.get(source_type_name)
            if source_cls is None:
                continue

            target_declared_type = _get_declared_input_type(target_cls, input_name)
            if target_declared_type is None:
                continue

            try:
                source_output_index = int(source_output_index)
            except Exception:
                raise ValueError(
                    f"Connection type mismatch: invalid source output index "
                    f"{source_output_index!r} on {source_type_name}({source_id})."
                )

            source_return_types = _resolve_source_return_types(
                source_cls,
                source_data.get("inputs", {}),
                graph,
            )
            if source_output_index < 0 or source_output_index >= len(source_return_types):
                raise ValueError(
                    f"Connection type mismatch: {source_type_name}({source_id}) "
                    f"has no output index {source_output_index}."
                )

            source_declared_type = source_return_types[source_output_index]
            ok, reason = can_connect_types(str(source_declared_type), str(target_declared_type))
            if ok:
                continue

            source_names = tuple(getattr(source_cls, "RETURN_NAMES", ()))
            source_output_name = (
                source_names[source_output_index]
                if source_output_index < len(source_names)
                else f"output_{source_output_index}"
            )
            raise ValueError(
                "Connection type mismatch:\n"
                f"{source_type_name}({source_id}).{source_output_name} outputs {source_declared_type},\n"
                f"but {target_type_name}({target_id}).{input_name} requires {target_declared_type}.\n"
                f"Reason: {reason}.\n"
                "Please insert DaskTypeCast between Dask arrays or use compatible node ports."
            )


def find_execution_roots(graph: dict) -> list[str]:
    """Return nodes that are output-capable and terminal in the workflow graph."""
    referenced_as_source = set()
    for node_data in graph.values():
        for value in node_data.get("inputs", {}).values():
            if isinstance(value, list) and len(value) == 2:
                referenced_as_source.add(value[0])

    return [
        node_id
        for node_id, node_data in graph.items()
        if (
            getattr(NODE_CLASS_MAPPINGS.get(node_data["type"]), "OUTPUT_NODE", False)
            and node_id not in referenced_as_source
        )
    ]


# =============================================================================
# 2. Dask collection helpers
# =============================================================================
def _is_dask_collection(obj) -> bool:
    try:
        return is_dask_collection(obj)
    except Exception:
        return hasattr(obj, "__dask_graph__")


def _is_delayed(obj) -> bool:
    try:
        from dask.delayed import Delayed
        return isinstance(obj, Delayed)
    except Exception:
        return False


def _iter_output_items(result):
    if result is None:
        return
    if isinstance(result, tuple):
        for item in result:
            yield item
        return
    if isinstance(result, list):
        for item in result:
            yield item
        return
    yield result


def _extract_compute_collection(item):
    # Plain dask collection (delayed, array, dataframe, bag, etc.)
    if _is_dask_collection(item):
        return item

    return None


def _cancel_sink_futures(client, sink_futures) -> None:
    if not client or not sink_futures:
        return
    client.cancel(sink_futures, force=True)
    try:
        dist_wait(sink_futures, timeout=2)
    except Exception as exc:
        logger.debug(f"[Cleanup] Future cancellation drain skipped/failed: {exc}")


def _normalize_futures(futures) -> list:
    if isinstance(futures, (list, tuple)):
        return list(futures)
    return [futures]


def _release_futures(futures: list) -> None:
    for future in futures:
        release = getattr(future, "release", None)
        if callable(release):
            try:
                release()
            except Exception as exc:
                logger.debug("[Cleanup] Future release failed: %s", exc)


def _remove_futures(tracked_futures: list, completed_futures: list) -> None:
    for future in completed_futures:
        try:
            tracked_futures.remove(future)
        except ValueError:
            pass


async def _cancel_and_await_tasks(tasks: dict) -> None:
    """Cancel unfinished graph-building tasks and wait until they have stopped."""
    pending = [task for task in tasks.values() if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


def _collect_output_sinks(results: dict, execution_roots: list[str]) -> list[dict]:
    output_sinks = []
    for node_id in execution_roots:
        node_result = results.get(node_id)
        found_for_node = False
        for item in _iter_output_items(node_result):
            collection = _extract_compute_collection(item)
            if collection is None:
                continue
            is_delayed = _is_delayed(collection)
            if not is_delayed:
                logger.warning(
                    "Execution root %s returned a non-delayed Dask collection. Full Graph "
                    "Execution may materialize a large result in Dask memory.",
                    node_id,
                )
            output_sinks.append({
                "collection": collection,
                "is_delayed": is_delayed,
            })
            found_for_node = True
        if not found_for_node:
            logger.info(
                "Execution root %s returned no Dask collection. No Dask compute will be "
                "submitted for this root.",
                node_id,
            )
    return output_sinks


def _inspect_window_roots(
    results: dict,
    execution_roots: list[str],
) -> tuple[list[da.Array], tuple[int, ...] | None, str | None]:
    root_arrays: list[da.Array] = []
    root_shapes: list[tuple[int, ...]] = []

    for node_id in execution_roots:
        items = list(_iter_output_items(results.get(node_id)))
        if not items:
            return [], None, "An execution root returned no result."
        if any(not isinstance(item, da.Array) for item in items):
            return (
                [],
                None,
                "Window Execution V1 requires every execution-root result to be a Dask Array.",
            )

        for item in items:
            try:
                shape = tuple(int(operator.index(size)) for size in item.shape)
            except (TypeError, ValueError, OverflowError):
                return (
                    [],
                    None,
                    "Window Execution requires final Dask Arrays with known integer shapes.",
                )
            if not shape:
                return (
                    [],
                    None,
                    "Window Execution V1 does not support scalar (0-dimensional) roots.",
                )
            if any(size < 0 for size in shape):
                return [], None, "Window Execution requires non-negative output dimensions."
            root_arrays.append(item)
            root_shapes.append(shape)

    if not root_arrays:
        return [], None, "No Dask Array execution-root results were found."

    output_shape = root_shapes[0]
    if any(shape != output_shape for shape in root_shapes[1:]):
        return (
            [],
            None,
            "Execution roots have incompatible Dask Array shapes.",
        )
    return root_arrays, output_shape, None


def _declared_window_root_reason(
    graph: dict,
    execution_roots: list[str],
) -> str | None:
    """Reject clearly non-array roots without invoking side-effecting execute methods."""
    for node_id in execution_roots:
        node_data = graph[node_id]
        node_cls = NODE_CLASS_MAPPINGS[node_data["type"]]
        return_types = _resolve_source_return_types(
            node_cls,
            node_data.get("inputs", {}),
            graph,
        )
        if not return_types or any(
            not is_dask_array_type(return_type)
            for return_type in return_types
        ):
            return (
                "Window Execution V1 requires every declared execution-root "
                "output type to be a Dask Array."
            )

    reachable: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in reachable:
            return
        reachable.add(node_id)
        for value in graph[node_id].get("inputs", {}).values():
            if isinstance(value, list) and len(value) == 2:
                visit(value[0])

    for root_id in execution_roots:
        visit(root_id)

    for node_id in sorted(reachable):
        node_data = graph[node_id]
        node_cls = NODE_CLASS_MAPPINGS[node_data["type"]]
        if not getattr(node_cls, "PREFLIGHT_SAFE", False):
            return (
                "Window Execution is unavailable because node type "
                f"{node_data['type']!r} is not declared safe for lazy preflight. "
                "Full Graph Execution remains available."
            )
    return None


async def _build_lazy_execution_roots(
    graph: dict,
    execution_roots: list[str],
    *,
    execution_id: str | None,
    progress_callback=None,
    is_preflight: bool = False,
    is_resuming: bool = False,
    tasks: dict | None = None,
    results: dict | None = None,
    node_instances: dict | None = None,
) -> tuple[dict, dict, dict]:
    """Recursively build execution-root results without submitting Dask compute."""
    tasks = tasks if tasks is not None else {}
    results = results if results is not None else {}
    node_instances = node_instances if node_instances is not None else {}
    loop = asyncio.get_running_loop()

    async def report(
        node_id: str,
        progress: int | None,
        message: str,
        run_state: str,
    ) -> None:
        if progress_callback is not None:
            await progress_callback(node_id, progress, message, run_state)

    async def compute_node(node_id: str):
        node_cls = None
        class_name = None
        func_args = None

        try:
            await report(node_id, None, "Initializing...", "ready")
            node_data = graph[node_id]
            class_name = node_data["type"]

            pending_inputs = {}
            final_inputs = {}
            for input_name, value in node_data.get("inputs", {}).items():
                if isinstance(value, list) and len(value) == 2:
                    pending_inputs[input_name] = (value[0], value[1])
                else:
                    final_inputs[input_name] = value

            dependency_ids = list(dict.fromkeys(
                source_id for source_id, _ in pending_inputs.values()
            ))
            if dependency_ids:
                await asyncio.gather(*(schedule_node(dep_id) for dep_id in dependency_ids))

            for argument_name, (dependency_id, raw_slot_index) in pending_inputs.items():
                try:
                    slot_index = int(raw_slot_index)
                except (ValueError, TypeError) as exc:
                    raise ValueError(
                        f"Node '{node_id}': invalid output slot index {raw_slot_index!r} "
                        f"(source '{dependency_id}'). Must be a non-negative integer."
                    ) from exc
                if slot_index < 0:
                    raise ValueError(
                        f"Node '{node_id}': negative output slot index {slot_index} "
                        f"(source '{dependency_id}'). Must be a non-negative integer."
                    )

                source_result = results[dependency_id]
                if isinstance(source_result, tuple):
                    if slot_index >= len(source_result):
                        raise ValueError(
                            f"Node '{node_id}': output slot {slot_index} out of range "
                            f"(source '{dependency_id}' has {len(source_result)} slots)."
                        )
                    value = source_result[slot_index]
                else:
                    if slot_index != 0:
                        raise ValueError(
                            f"Node '{node_id}': output slot {slot_index} out of range "
                            f"(source '{dependency_id}' has 1 slot)."
                        )
                    value = source_result
                final_inputs[argument_name] = value

            node_cls = NODE_CLASS_MAPPINGS.get(class_name)
            if node_cls is None:
                raise ValueError(f"Node class '{class_name}' not found.")

            func_args = prepare_node_inputs(node_cls, final_inputs, node_id)
            invocation = build_node_invocation(
                node_cls,
                func_args,
                node_id=node_id,
                execution_id=execution_id,
                is_preflight=is_preflight,
                is_resuming=is_resuming,
            )
            func_args["_node_id"] = node_id
            func_args["_execution_id"] = execution_id
            func_args["_runtime"] = invocation.runtime
            func_args["_invocation"] = invocation

            instance = node_cls()
            node_instances[node_id] = instance
            method_name = getattr(node_cls, "FUNCTION", "execute")
            method = getattr(instance, method_name)

            signature = inspect.signature(method)
            accepts_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
            if not accepts_kwargs:
                func_args = {
                    key: value
                    for key, value in func_args.items()
                    if key in signature.parameters
                }

            with dask.annotate(brainflow_node_id=node_id):
                if asyncio.iscoroutinefunction(method):
                    output = await method(**func_args)
                else:
                    output = await loop.run_in_executor(
                        None,
                        lambda: method(**func_args),
                    )

            output_items = list(output if isinstance(output, tuple) else (output,))
            if any(_is_dask_collection(item) for item in output_items):
                await report(node_id, None, "Ready", "ready")
            else:
                await report(node_id, 100, "Done", "done")

            results[node_id] = output if isinstance(output, tuple) else (output,)
            return results[node_id]

        except Exception as exc:
            error_context = {
                "node_id": node_id,
                "node_type": class_name,
                "node_category": getattr(node_cls, "CATEGORY", "Unknown") if node_cls else "Unknown",
                "display_name": getattr(node_cls, "DISPLAY_NAME", class_name) if node_cls else class_name,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:500],
            }
            if func_args:
                error_context["inputs"] = {
                    key: str(value)[:100]
                    if isinstance(value, (str, int, float))
                    else f"<{type(value).__name__}>"
                    for key, value in func_args.items()
                    if not key.startswith("_")
                }
            logger.error(
                "Node %s (%s) failed: %s: %s",
                node_id,
                error_context["node_type"],
                error_context["error_type"],
                error_context["error_message"],
                extra=error_context,
            )
            await report(
                node_id,
                None,
                f"Error: {error_context['error_type']}",
                "failed",
            )
            raise

    async def schedule_node(node_id: str):
        if node_id in results:
            return results[node_id]
        if node_id in tasks:
            return await tasks[node_id]
        task = asyncio.create_task(compute_node(node_id))
        tasks[node_id] = task
        return await task

    await asyncio.gather(*(schedule_node(node_id) for node_id in execution_roots))
    return results, node_instances, tasks


async def preflight_graph(graph: dict) -> dict:
    """Validate and inspect lazy terminal metadata without compute or an execution slot."""
    validate_graph_structure(graph)
    validate_graph_acyclic(graph)
    validate_graph_types(graph)
    execution_roots = find_execution_roots(graph)
    if not execution_roots:
        raise ValueError(
            "No terminal output node found. A workflow execution root must declare "
            "OUTPUT_NODE=True and have no outgoing graph connections."
        )

    declared_reason = _declared_window_root_reason(graph, execution_roots)
    if declared_reason is not None:
        return {
            "windowable": False,
            "output_shape": None,
            "ndim": None,
            "reason": declared_reason,
        }

    tasks: dict = {}
    results: dict = {}
    node_instances: dict = {}
    try:
        await _build_lazy_execution_roots(
            graph,
            execution_roots,
            execution_id=None,
            is_preflight=True,
            tasks=tasks,
            results=results,
            node_instances=node_instances,
        )
        _, output_shape, reason = _inspect_window_roots(results, execution_roots)
        if reason is not None:
            return {
                "windowable": False,
                "output_shape": None,
                "ndim": None,
                "reason": reason,
            }
        return {
            "windowable": True,
            "output_shape": list(output_shape),
            "ndim": len(output_shape),
        }
    finally:
        await _cancel_and_await_tasks(tasks)
        tasks.clear()
        results.clear()
        node_instances.clear()


# =============================================================================
# 3. Core executor
# =============================================================================
async def execute_graph(
    graph: dict,
    execution_id: str = None,
    execution_config: ExecutionConfig | dict | None = None,
    *,
    checkpoint_store: WindowCheckpointStore | None = None,
):
    """Execute true terminal roots in Full Graph or sequential Window mode."""
    tasks: dict = {}
    sink_futures: list = []
    results: dict = {}
    node_instances: dict = {}
    client = None
    should_cancel_dask_objects = False

    window_store: WindowCheckpointStore | None = None
    workflow_fingerprint: str | None = None
    plan_fingerprint: str | None = None
    completed_windows_bitmap: np.ndarray | None = None
    window_generator: WindowGenerator | None = None
    selected_config: ExecutionConfig | None = None

    if not execution_id:
        execution_id = uuid.uuid4().hex

    mem_monitor = get_memory_monitor()
    mem_monitor.snapshots.clear()
    state_manager.create_execution(execution_id)

    try:
        selected_config = parse_execution_config(execution_config)
        validate_graph_structure(graph)
        validate_graph_acyclic(graph)
        validate_graph_types(graph)
        # Full Graph keeps the existing eager client lifecycle.  Preflight never
        # reaches this executor, while Window mode creates a client only when it
        # actually has a Window to submit.
        if selected_config.mode == "full_graph":
            client = dask_service.ensure_client()
        mem_monitor.log_snapshot("execution_start", client=client)

        await state_manager.broadcast(execution_id, {
            "type": "log",
            "message": "Engine Started...",
            "executionId": execution_id,
        })
        state_manager.add_log("Engine Started...", "info", execution_id=execution_id)

        execution_roots = find_execution_roots(graph)
        execution_root_set = set(execution_roots)
        if not execution_roots:
            state_manager.set_execution_status(execution_id, ExecutionStatus.FAILED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "failed",
                "message": (
                    "No terminal output node found. A workflow execution root must "
                    "declare OUTPUT_NODE=True and have no outgoing graph connections."
                ),
            })
            state_manager.add_log(
                "No terminal output node found. Cannot execute.",
                "error",
                execution_id=execution_id,
            )
            return execution_id

        loop = asyncio.get_running_loop()

        async def progress_callback(
            node_id: str,
            progress: int | None = None,
            message: str = "",
            run_state: str = "ready",
            *,
            broadcast_update: bool = True,
        ):
            node_data = graph.get(node_id)
            node_cls = (
                NODE_CLASS_MAPPINGS.get(node_data.get("type"))
                if node_data
                else None
            )
            progress_type = "state_only"
            device = None
            if node_cls:
                declared_progress = getattr(node_cls, "PROGRESS_TYPE", None)
                if declared_progress:
                    progress_type = (
                        declared_progress.value
                        if hasattr(declared_progress, "value")
                        else str(declared_progress)
                    )
                device = getattr(node_cls, "DEVICE_HINT", None)
            progress_role = "output" if node_id in execution_root_set else "state"

            state_manager.update_node_status(
                node_id,
                message,
                execution_id=execution_id,
                run_state=run_state,
                device=device,
                progress=progress,
                progress_type=progress_type,
                progress_role=progress_role,
            )
            broadcast_message = {
                "type": "progress",
                "taskId": node_id,
                "executionId": execution_id,
                "progressType": progress_type,
                "progress": progress,
                "message": message,
                "runState": run_state,
                "progressRole": progress_role,
            }
            if device:
                broadcast_message["device"] = device
            if broadcast_update:
                await state_manager.broadcast(execution_id, broadcast_message)

        async def window_progress_callback(
            *,
            current_window: int,
            completed_windows: int,
            total_windows: int,
            window_status: str,
            message: str,
        ) -> None:
            progress = (
                100.0
                if total_windows == 0
                else min(
                    100.0,
                    max(0.0, completed_windows * 100.0 / total_windows),
                )
            )
            payload = state_manager.update_window_progress(
                execution_id,
                current_window=current_window,
                completed_windows=completed_windows,
                total_windows=total_windows,
                progress=progress,
                window_status=window_status,
                message=message,
            )
            if payload is not None:
                await state_manager.broadcast(execution_id, payload)

        is_resuming = False
        expected_output_shape: tuple[int, ...] | None = None

        if selected_config.mode == "window":
            declared_reason = _declared_window_root_reason(graph, execution_roots)
            if declared_reason is not None:
                raise ValueError(declared_reason)
            # This metadata pass skips preprocess, so opening the Run dialog and
            # selecting a plan cannot initialize or overwrite writer outputs.
            metadata_tasks: dict = {}
            metadata_results: dict = {}
            metadata_instances: dict = {}
            try:
                await _build_lazy_execution_roots(
                    graph,
                    execution_roots,
                    execution_id=None,
                    is_preflight=True,
                    tasks=metadata_tasks,
                    results=metadata_results,
                    node_instances=metadata_instances,
                )
                _, expected_output_shape, window_reason = _inspect_window_roots(
                    metadata_results,
                    execution_roots,
                )
                if window_reason is not None:
                    raise ValueError(window_reason)

                window_generator = WindowGenerator(
                    expected_output_shape,
                    selected_config.window_shape,
                )
                workflow_fingerprint = compute_workflow_fingerprint(
                    graph,
                    execution_roots,
                )
                plan_fingerprint = compute_plan_fingerprint(
                    expected_output_shape,
                    selected_config.window_shape,
                )
                window_store = checkpoint_store or WindowCheckpointStore()
                completed_windows_bitmap = window_store.load(
                    workflow_fingerprint,
                    plan_fingerprint,
                    expected_shape=window_generator.axis_counts,
                )
                if completed_windows_bitmap is not None:
                    is_resuming = True
            finally:
                await _cancel_and_await_tasks(metadata_tasks)
                metadata_tasks.clear()
                metadata_results.clear()
                metadata_instances.clear()

        await state_manager.broadcast(
            execution_id,
            {"type": "log", "message": "GraphBuilding..."},
        )
        state_manager.add_log("GraphBuilding...", "info", execution_id=execution_id)
        await _build_lazy_execution_roots(
            graph,
            execution_roots,
            execution_id=execution_id,
            progress_callback=progress_callback,
            is_resuming=is_resuming,
            tasks=tasks,
            results=results,
            node_instances=node_instances,
        )

        if selected_config.mode == "full_graph":
            output_sinks = _collect_output_sinks(results, execution_roots)
            if output_sinks:
                client = dask_service.ensure_client()
                for node_id in execution_roots:
                    await progress_callback(node_id, None, "Submitted", "submitted")

                collections = [sink["collection"] for sink in output_sinks]
                futures = _normalize_futures(client.compute(collections))
                sink_futures.extend(futures)
                await state_manager.broadcast(execution_id, {
                    "type": "log",
                    "message": f"Submitted {len(output_sinks)} sink(s) - Computing...",
                })
                state_manager.add_log(
                    f"Submitted {len(output_sinks)} sink(s) - Computing...",
                    "info",
                    execution_id=execution_id,
                )
                for node_id in execution_roots:
                    await progress_callback(node_id, None, "Running", "running")

                await loop.run_in_executor(None, lambda: dist_wait(futures))
                for sink, future in zip(output_sinks, futures):
                    if sink["is_delayed"]:
                        await loop.run_in_executor(None, future.result)
                    else:
                        # Never pull a complete large collection to the driver.
                        exception = await loop.run_in_executor(
                            None,
                            future.exception,
                        )
                        if exception is not None:
                            raise exception
        else:
            root_arrays, actual_output_shape, window_reason = _inspect_window_roots(
                results,
                execution_roots,
            )
            if window_reason is not None:
                raise ValueError(window_reason)
            if actual_output_shape != expected_output_shape:
                raise ValueError(
                    "Execution-root shape changed between preflight and execution "
                    f"({expected_output_shape} != {actual_output_shape})."
                )
            if window_generator is None or window_store is None:
                raise RuntimeError("Window Execution was not initialized.")
            if workflow_fingerprint is None or plan_fingerprint is None:
                raise RuntimeError("Window recovery identity was not initialized.")

            if completed_windows_bitmap is None:
                completed_windows_bitmap = window_store.create(
                    workflow_fingerprint,
                    plan_fingerprint,
                    window_generator.axis_counts,
                )
            else:
                completed_count = int(np.count_nonzero(completed_windows_bitmap))
                resume_message = (
                    "Resuming Window Execution at finalization."
                    if completed_count == window_generator.total_windows
                    else (
                        "Resuming Window Execution with "
                        f"{completed_count} / {window_generator.total_windows} "
                        "Window(s) completed."
                    )
                )
                await state_manager.broadcast(
                    execution_id,
                    {"type": "log", "message": resume_message},
                )
                state_manager.add_log(
                    resume_message,
                    "info",
                    execution_id=execution_id,
                )

            total_windows = window_generator.total_windows
            flat_bitmap = completed_windows_bitmap.reshape(-1, order="C")
            pending_indices = np.flatnonzero(flat_bitmap == 0)
            all_windows_completed = (
                total_windows == 0
                or bool(np.all(completed_windows_bitmap == 1))
            )
            if not all_windows_completed:
                client = dask_service.ensure_client()
                for node_id in execution_roots:
                    await progress_callback(node_id, None, "Submitted", "submitted")
                window_summary = (
                    f"Window Execution: {total_windows} Window(s), "
                    f"{len(pending_indices)} pending."
                )
                await state_manager.broadcast(
                    execution_id,
                    {"type": "log", "message": window_summary},
                )
                state_manager.add_log(
                    window_summary,
                    "info",
                    execution_id=execution_id,
                )
                progress_stride = max(1, (len(pending_indices) + 999) // 1000)

                for pending_position, raw_index in enumerate(pending_indices):
                    window = window_generator.window_at(int(raw_index))
                    completed_count = int(
                        np.count_nonzero(completed_windows_bitmap)
                    )
                    message = f"Window {window.index + 1} / {total_windows}"
                    await window_progress_callback(
                        current_window=window.index + 1,
                        completed_windows=completed_count,
                        total_windows=total_windows,
                        window_status="running",
                        message=message,
                    )
                    progress_percent = min(
                        99,
                        int(completed_count * 100 / total_windows),
                    )
                    should_broadcast_progress = (
                        pending_position == 0
                        or pending_position + 1 == len(pending_indices)
                        or pending_position % progress_stride == 0
                    )
                    for node_id in execution_roots:
                        await progress_callback(
                            node_id,
                            progress_percent,
                            message,
                            "running",
                            broadcast_update=should_broadcast_progress,
                        )

                    window_collections = [
                        root_array[window.slices]
                        for root_array in root_arrays
                    ]
                    current_futures: list = []
                    wait_completed = False
                    try:
                        current_futures = _normalize_futures(
                            client.compute(window_collections)
                        )
                        sink_futures.extend(current_futures)
                        await loop.run_in_executor(
                            None,
                            lambda futures=current_futures: dist_wait(futures),
                        )
                        wait_completed = True
                        future_exceptions: list[BaseException] = []
                        for future in current_futures:
                            exception = await loop.run_in_executor(
                                None,
                                future.exception,
                            )
                            if exception is not None:
                                future_exceptions.append(exception)
                        if future_exceptions:
                            raise future_exceptions[0]

                        # Commit only after every terminal Future succeeded.
                        window_store.mark_completed(
                            workflow_fingerprint,
                            plan_fingerprint,
                            completed_windows_bitmap,
                            window.coordinates,
                        )
                        completed_count = int(
                            np.count_nonzero(completed_windows_bitmap)
                        )
                        await window_progress_callback(
                            current_window=window.index + 1,
                            completed_windows=completed_count,
                            total_windows=total_windows,
                            window_status="running",
                            message=(
                                f"Completed Window {window.index + 1} / "
                                f"{total_windows}"
                            ),
                        )
                    finally:
                        if wait_completed:
                            _release_futures(current_futures)
                            _remove_futures(sink_futures, current_futures)
            else:
                for node_id in execution_roots:
                    await progress_callback(
                        node_id,
                        99,
                        "Finalizing",
                        "running",
                    )

            await window_progress_callback(
                current_window=(total_windows if total_windows else 0),
                completed_windows=int(
                    np.count_nonzero(completed_windows_bitmap)
                ),
                total_windows=total_windows,
                window_status="finalizing",
                message="Finalizing Window Execution",
            )

        # postprocess remains a whole-workflow lifecycle hook.
        for node_id in execution_roots:
            instance = node_instances.get(node_id)
            postprocess = getattr(instance, "postprocess", None)
            if callable(postprocess):
                post_value = postprocess(
                    outputs=results.get(node_id),
                    state=getattr(instance, "_preprocess_state", None),
                    runtime={"execution_id": execution_id, "node_id": node_id},
                )
                if inspect.isawaitable(post_value):
                    post_value = await post_value
                if post_value is not None:
                    results[node_id] = post_value

        for node_id in execution_roots:
            await progress_callback(node_id, 100, "Done", "done")

        # No await is allowed between checkpoint deletion and the terminal state
        # transition.  A cancellation during the progress update above keeps the
        # checkpoint; once it is deleted the execution is synchronously terminal.
        if selected_config.mode == "window":
            window_store.delete(workflow_fingerprint, plan_fingerprint)
        state_manager.set_execution_status(execution_id, ExecutionStatus.SUCCEEDED)
        await state_manager.broadcast(execution_id, {
            "type": "execution_finished",
            "executionId": execution_id,
            "status": "succeeded",
            "message": "Workflow Finished Successfully",
        })
        state_manager.add_log(
            "Workflow Finished Successfully",
            "success",
            execution_id=execution_id,
        )
        await state_manager.broadcast(execution_id, {
            "type": "done",
            "executionId": execution_id,
            "status": "succeeded",
            "message": "Workflow Finished",
        })

    except asyncio.CancelledError:
        should_cancel_dask_objects = True
        logger.warning("Execution Cancelled.")
        session = state_manager.get_execution(execution_id)
        if session and session.status == ExecutionStatus.RUNNING:
            state_manager.set_execution_status(
                execution_id,
                ExecutionStatus.CANCELLING,
            )
        state_manager.set_execution_status(execution_id, ExecutionStatus.CANCELLED)
        await state_manager.broadcast(execution_id, {
            "type": "execution_finished",
            "executionId": execution_id,
            "status": "cancelled",
            "message": "Execution Cancelled",
        })
        state_manager.add_log(
            "Execution Cancelled",
            "warning",
            execution_id=execution_id,
        )
    except Exception as exc:
        should_cancel_dask_objects = True
        logger.error(
            "Execution failed: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        session = state_manager.get_execution(execution_id)
        if session and session.status == ExecutionStatus.CANCELLING:
            state_manager.set_execution_status(
                execution_id,
                ExecutionStatus.CANCELLED,
            )
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "cancelled",
                "message": (
                    f"Execution Cancelled (error during shutdown: "
                    f"{type(exc).__name__})"
                ),
            })
            state_manager.add_log(
                f"Cancellation error: {type(exc).__name__}",
                "warning",
                execution_id=execution_id,
            )
        else:
            state_manager.set_execution_status(execution_id, ExecutionStatus.FAILED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "failed",
                "message": str(exc),
            })
            state_manager.add_log(
                f"Global Error: {str(exc)}",
                "error",
                execution_id=execution_id,
            )
    finally:
        mem_monitor.log_snapshot("execution_end_before_cleanup", client=client)
        await _cancel_and_await_tasks(tasks)

        if client and sink_futures and should_cancel_dask_objects:
            try:
                _cancel_sink_futures(client, sink_futures)
                logger.info(
                    "[Cleanup] Force cancelled %s sink futures",
                    len(sink_futures),
                )
            except Exception as exc:
                logger.debug("[Cleanup] Cancel failed: %s", exc)

        if should_cancel_dask_objects:
            for node_id, instance in node_instances.items():
                cleanup = getattr(instance, "cleanup", None)
                if callable(cleanup):
                    try:
                        cleanup()
                        logger.info("[Cleanup] Called cleanup() on %s", node_id)
                    except Exception as exc:
                        logger.warning(
                            "[Cleanup] cleanup() failed for %s: %s",
                            node_id,
                            exc,
                        )

        if client:
            try:
                stats = client.run(force_clear_worker_cache)
                logger.info("[Cleanup] Worker cache cleared: %s", stats)
            except Exception as exc:
                logger.debug("[Cleanup] Worker cache clear failed: %s", exc)

        sink_futures.clear()
        tasks.clear()
        results.clear()
        state_manager.cleanup_old_executions()

        mem_monitor.log_snapshot("execution_end_after_cleanup", client=client)
        mem_monitor.log_delta(
            "execution_start",
            "execution_end_before_cleanup",
        )
        mem_monitor.log_delta(
            "execution_end_before_cleanup",
            "execution_end_after_cleanup",
        )

        start_snapshot = mem_monitor.snapshots.get("execution_start")
        end_before = mem_monitor.snapshots.get("execution_end_before_cleanup")
        end_after = mem_monitor.snapshots.get("execution_end_after_cleanup")
        if start_snapshot and end_before and end_after:
            start_mb = start_snapshot.get("process_mb", 0)
            before_mb = end_before.get("process_mb", 0)
            after_mb = end_after.get("process_mb", 0)
            if start_mb and before_mb and after_mb:
                released = before_mb - after_mb
                delta = after_mb - start_mb
                if delta > 3000:
                    logger.warning(
                        "[Memory] +%.0fMB retained after execution "
                        "(cleanup released %.0fMB).",
                        delta,
                        released,
                    )
                else:
                    logger.info(
                        "[Memory] Execution: +%.0fMB total, "
                        "cleanup released %.0fMB. OK.",
                        delta,
                        released,
                    )

    return execution_id
