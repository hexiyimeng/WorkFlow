import asyncio
import inspect
import logging
import traceback
import uuid

import dask
from dask.base import is_dask_collection
from distributed import wait as dist_wait

from core.invocation_builder import (
    build_node_invocation,
    get_node_input_defs,
    prepare_node_inputs,
)
from core.registry import NODE_CLASS_MAPPINGS, validate_node_port_types
from core.state_manager import state_manager, ExecutionStatus
from core.type_system import can_connect_types
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


# =============================================================================
# 3. Core executor
# =============================================================================
async def execute_graph(graph: dict, execution_id: str = None):
    """
    Two-phase executor:

    Phase 1 (GraphBuilding): recursively executes dependencies of execution
    roots, building a lazy Dask graph. An execution root is a node whose class
    declares OUTPUT_NODE=True and whose graph out-degree is zero.

    Phase 2 (Compute): discovers Dask collections from execution-root return values.
    If any are found, submits them in a single client.compute([...]) call.
    After futures complete (or if no collections were found), calls postprocess()
    on each execution-root instance once.
    """
    tasks = {}
    sink_futures = []
    results = {}
    node_instances = {}
    client = None
    should_cancel_dask_objects = False

    if not execution_id:
        execution_id = uuid.uuid4().hex

    mem_monitor = get_memory_monitor()
    mem_monitor.snapshots.clear()

    state_manager.create_execution(execution_id)

    try:
        client = dask_service.ensure_client()
        validate_graph_structure(graph)
        validate_graph_acyclic(graph)
        validate_graph_types(graph)

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
                    "No terminal output node found. A workflow execution root must declare "
                    "OUTPUT_NODE=True and have no outgoing graph connections."
                ),
            })
            state_manager.add_log("No terminal output node found. Cannot execute.", "error", execution_id=execution_id)
            return execution_id

        loop = asyncio.get_running_loop()

        async def progress_callback(
            node_id: str,
            progress: int | None = None,
            message: str = "",
            run_state: str = "ready",
        ):
            node_data = graph.get(node_id)
            NodeCls = NODE_CLASS_MAPPINGS.get(node_data.get("type")) if node_data else None
            pt_value = "state_only"
            device = None
            if NodeCls:
                pt = getattr(NodeCls, "PROGRESS_TYPE", None)
                if pt:
                    pt_value = pt.value if hasattr(pt, "value") else str(pt)
                device = getattr(NodeCls, "DEVICE_HINT", None)
            progress_role = "output" if node_id in execution_root_set else "state"

            state_manager.update_node_status(
                node_id, message,
                execution_id=execution_id,
                run_state=run_state,
                device=device,
                progress=progress,
                progress_type=pt_value,
                progress_role=progress_role,
            )

            broadcast_msg = {
                "type": "progress",
                "taskId": node_id,
                "executionId": execution_id,
                "progressType": pt_value,
                "progress": progress,
                "message": message,
                "runState": run_state,
                "progressRole": progress_role,
            }
            if device:
                broadcast_msg["device"] = device
            await state_manager.broadcast(execution_id, broadcast_msg)

        async def _compute_node(node_id: str):
            NodeCls = None
            class_name = None
            func_args = None

            try:
                await progress_callback(node_id, None, "Initializing...", "ready")
                node_data = graph.get(node_id)
                class_name = node_data["type"]

                pending_inputs = {}
                final_inputs = {}

                for k, v in node_data.get("inputs", {}).items():
                    if isinstance(v, list) and len(v) == 2:
                        pending_inputs[k] = (v[0], v[1])
                    else:
                        final_inputs[k] = v

                dep_ids = list(set([x[0] for x in pending_inputs.values()]))
                if dep_ids:
                    await asyncio.gather(*(schedule_node(dep_id) for dep_id in dep_ids))

                for arg_name, raw_slot_idx in pending_inputs.items():
                    dep_id, raw_idx = raw_slot_idx
                    try:
                        slot_idx = int(raw_idx)
                    except (ValueError, TypeError):
                        raise ValueError(
                            f"Node '{node_id}': invalid output slot index {raw_idx!r} "
                            f"(source '{dep_id}'). Must be a non-negative integer."
                        )
                    if slot_idx < 0:
                        raise ValueError(
                            f"Node '{node_id}': negative output slot index {slot_idx} "
                            f"(source '{dep_id}'). Must be a non-negative integer."
                        )

                    src_result = results[dep_id]
                    val = None
                    if isinstance(src_result, tuple):
                        if slot_idx >= len(src_result):
                            raise ValueError(
                                f"Node '{node_id}': output slot {slot_idx} out of range "
                                f"(source '{dep_id}' has {len(src_result)} slots)."
                            )
                        val = src_result[slot_idx]
                    else:
                        if slot_idx != 0:
                            raise ValueError(
                                f"Node '{node_id}': output slot {slot_idx} out of range "
                                f"(source '{dep_id}' has 1 slot)."
                            )
                        val = src_result
                    final_inputs[arg_name] = val

                NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                if NodeCls is None:
                    raise ValueError(f"Node class '{class_name}' not found.")

                func_args = prepare_node_inputs(NodeCls, final_inputs, node_id)
                invocation = build_node_invocation(
                    NodeCls,
                    func_args,
                    node_id=node_id,
                    execution_id=execution_id,
                )
                func_args["_node_id"] = node_id
                func_args["_execution_id"] = execution_id
                func_args["_runtime"] = invocation.runtime
                func_args["_invocation"] = invocation

                # Instantiate FIRST, then get method from the instance
                instance = NodeCls()
                node_instances[node_id] = instance

                method_name = getattr(NodeCls, "FUNCTION", "execute")
                method = getattr(instance, method_name)

                # Filter _node_id / _execution_id for methods that don't accept **kwargs
                sig = inspect.signature(method)
                accepts_kwargs = any(
                    p.kind == inspect.Parameter.VAR_KEYWORD
                    for p in sig.parameters.values()
                )
                if not accepts_kwargs:
                    filtered = {k: v for k, v in func_args.items() if k in sig.parameters}
                    func_args = filtered

                with dask.annotate(brainflow_node_id=node_id):
                    if asyncio.iscoroutinefunction(method):
                        output = await method(**func_args)
                    else:
                        output = await loop.run_in_executor(None, lambda: method(**func_args))

                output_list = list(output if isinstance(output, tuple) else (output,))

                is_lazy = any(_is_dask_collection(item) for item in output_list)

                if is_lazy:
                    await progress_callback(node_id, None, "Ready", "ready")
                else:
                    await progress_callback(node_id, 100, "Done", "done")

                results[node_id] = output if isinstance(output, tuple) else (output,)
                return results[node_id]

            except Exception as e:
                error_context = {
                    "node_id": node_id,
                    "node_type": class_name,
                    "node_category": getattr(NodeCls, "CATEGORY", "Unknown") if NodeCls else "Unknown",
                    "display_name": getattr(NodeCls, "DISPLAY_NAME", class_name) if NodeCls else class_name,
                    "error_type": type(e).__name__,
                    "error_message": str(e)[:500],
                }
                if func_args:
                    inputs_summary = {
                        k: str(v)[:100] if isinstance(v, (str, int, float)) else f"<{type(v).__name__}>"
                        for k, v in func_args.items() if k != "_node_id"
                    }
                    error_context["inputs"] = inputs_summary

                logger.error(
                    f"Node {node_id} ({error_context['node_type']}) Failed: "
                    f"{error_context['error_type']}: {error_context['error_message']}",
                    extra=error_context,
                )
                traceback.print_exc()
                await progress_callback(node_id, None, f"Error: {error_context['error_type']}", "failed")
                raise e

        async def schedule_node(node_id: str):
            if node_id in results:
                return results[node_id]
            if node_id in tasks:
                return await tasks[node_id]
            task = asyncio.create_task(_compute_node(node_id))
            tasks[node_id] = task
            return await task

        # =========================================================================
        # Phase 1: GraphBuilding
        # =========================================================================
        await state_manager.broadcast(execution_id, {"type": "log", "message": "GraphBuilding..."})
        state_manager.add_log("GraphBuilding...", "info", execution_id=execution_id)
        await asyncio.gather(*(schedule_node(nid) for nid in execution_roots))

        # =========================================================================
        # Phase 2: collect execution-root collections and compute
        # =========================================================================
        output_sinks = []
        for nid in execution_roots:
            node_result = results.get(nid)
            found_for_node = False
            for item in _iter_output_items(node_result):
                collection = _extract_compute_collection(item)
                if collection is not None:
                    if not _is_delayed(collection):
                        logger.warning(
                            "Output node %s returned a Dask Array. Executor will compute it, "
                            "which may materialize a large result in Dask memory. "
                            "Writers should prefer delayed side-effect tasks or small token "
                            "sink collections.",
                            nid,
                        )
                    output_sinks.append({
                        "collection": collection,
                        "is_delayed": _is_delayed(collection),
                    })
                    found_for_node = True
            if not found_for_node:
                logger.info(
                    "Output node %s returned no Dask collection. "
                    "Executor will not submit Dask compute for this output. "
                    "This is valid if the node completed its side effects during execute().",
                    nid,
                )

        # Dask client is only required when there are collections to compute
        if output_sinks:
            client = dask_service.ensure_client()

            # Send submitted state for all execution roots.
            for nid in execution_roots:
                await progress_callback(nid, None, "Submitted", "submitted")

            collections = [s["collection"] for s in output_sinks]
            futures = client.compute(collections)
            if not isinstance(futures, (list, tuple)):
                futures = [futures]
            sink_futures.extend(futures)

            await state_manager.broadcast(execution_id, {
                "type": "log",
                "message": f"Submitted {len(output_sinks)} sink(s) — Computing...",
            })
            state_manager.add_log(
                f"Submitted {len(output_sinks)} sink(s) — Computing...",
                "info",
                execution_id=execution_id,
            )

            # Send running state for all execution roots.
            for nid in execution_roots:
                await progress_callback(nid, None, "Running", "running")

            # Wait for all futures; this does NOT pull large results back to driver
            await loop.run_in_executor(None, lambda: dist_wait(futures))

            # Check exceptions and optionally collect delayed results
            for sink, future in zip(output_sinks, futures):
                if sink["is_delayed"]:
                    # dask.delayed: safe to call result() — returns small values
                    # None return is valid success; exception propagates
                    # delayed functions may return None (valid success); result() propagates exceptions
                    _ = await loop.run_in_executor(None, future.result)
                    # value may be None (valid) — do not confuse with failure
                else:
                    # Dask array or other large collection:
                    # DO NOT call future.result() — that materializes the full array on the driver
                    exc = await loop.run_in_executor(None, future.exception)
                    if exc is not None:
                        raise exc

            # All futures succeeded; postprocess each execution root once.
        for nid in execution_roots:
            instance = node_instances.get(nid)
            postprocess = getattr(instance, "postprocess", None)
            if callable(postprocess):
                post_value = postprocess(
                    outputs=results.get(nid),
                    state=getattr(instance, "_preprocess_state", None),
                    runtime={"execution_id": execution_id, "node_id": nid},
                )
                if inspect.isawaitable(post_value):
                    post_value = await post_value
                if post_value is not None:
                    results[nid] = post_value

        # Send done state for all execution roots.
        for nid in execution_roots:
            await progress_callback(nid, 100, "Done", "done")

        # Success
        state_manager.set_execution_status(execution_id, ExecutionStatus.SUCCEEDED)
        await state_manager.broadcast(execution_id, {
            "type": "execution_finished",
            "executionId": execution_id,
            "status": "succeeded",
            "message": "Workflow Finished Successfully",
        })
        state_manager.add_log("Workflow Finished Successfully", "success", execution_id=execution_id)
        # Legacy compatibility
        await state_manager.broadcast(execution_id, {
            "type": "done",
            "executionId": execution_id,
            "status": "succeeded",
            "message": "Workflow Finished",
        })

    except asyncio.CancelledError:
        should_cancel_dask_objects = True
        logger.warning("Execution Cancelled.")
        state_manager.set_execution_status(execution_id, ExecutionStatus.CANCELLED)
        await state_manager.broadcast(execution_id, {
            "type": "execution_finished",
            "executionId": execution_id,
            "status": "cancelled",
            "message": "Execution Cancelled",
        })
        state_manager.add_log("Execution Cancelled", "warning", execution_id=execution_id)
    except Exception as e:
        should_cancel_dask_objects = True
        traceback.print_exc()

        session = state_manager.get_execution(execution_id)
        if session and session.status == ExecutionStatus.CANCELLING:
            logger.warning(f"Exception during CANCELLING, finalizing as CANCELLED: {e}")
            state_manager.set_execution_status(execution_id, ExecutionStatus.CANCELLED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "cancelled",
                "message": f"Execution Cancelled (error during shutdown: {type(e).__name__})",
            })
            state_manager.add_log(f"Cancellation error: {type(e).__name__}", "warning", execution_id=execution_id)
        else:
            state_manager.set_execution_status(execution_id, ExecutionStatus.FAILED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "failed",
                "message": str(e),
            })
            state_manager.add_log(f"Global Error: {str(e)}", "error", execution_id=execution_id)
    finally:
        mem_monitor.log_snapshot("execution_end_before_cleanup", client=client)

        # Cancel asyncio tasks
        for t in tasks.values():
            if not t.done():
                t.cancel()

        if client:
            if sink_futures and should_cancel_dask_objects:
                try:
                    _cancel_sink_futures(client, sink_futures)
                    logger.info(f"[Cleanup] Force cancelled {len(sink_futures)} sink futures")
                except Exception as exc:
                    logger.debug(f"[Cleanup] Cancel failed: {exc}")

        # Node cleanup runs after Dask sink futures are cancelled/drained so
        # writer-owned temporary resources are not deleted while workers may
        # still be using them.
        if should_cancel_dask_objects:
            for nid, instance in node_instances.items():
                cleanup_fn = getattr(instance, "cleanup", None)
                if callable(cleanup_fn):
                    try:
                        cleanup_fn()
                        logger.info(f"[Cleanup] Called cleanup() on {nid}")
                    except Exception as exc:
                        logger.warning(f"[Cleanup] cleanup() failed for {nid}: {exc}")

        if client:
            try:
                stats = client.run(force_clear_worker_cache)
                logger.info(f"[Cleanup] Worker cache cleared: {stats}")
            except Exception as exc:
                logger.debug(f"[Cleanup] Worker cache clear failed: {exc}")

        sink_futures.clear()
        tasks.clear()
        results.clear()

        state_manager.cleanup_old_executions()

        mem_monitor.log_snapshot("execution_end_after_cleanup", client=client)
        mem_monitor.log_delta("execution_start", "execution_end_before_cleanup")

        mem_monitor.log_delta("execution_end_before_cleanup", "execution_end_after_cleanup")

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
                        f"[Memory] +{delta:.0f}MB retained after execution "
                        f"(cleanup released {released:.0f}MB)."
                    )
                else:
                    logger.info(
                        f"[Memory] Execution: +{delta:.0f}MB total, "
                        f"cleanup released {released:.0f}MB. OK."
                    )

    return execution_id
