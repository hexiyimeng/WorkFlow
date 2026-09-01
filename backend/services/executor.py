import asyncio
import contextvars
import inspect
import logging
import operator
import uuid
from dataclasses import dataclass
from typing import Awaitable, Callable

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
from core.config import config
from core.worker_profiles import dask_annotation_kwargs
from core.resource_planner import parse_required_worker_resources
from core.platform import rewrite_dashboard_url
from core.registry import NODE_CLASS_MAPPINGS, validate_node_port_types
from core.state_manager import state_manager, ExecutionStatus
from core.type_system import can_connect_types, is_dask_array_type
from core.window_execution import (
    ActiveExecutionLock,
    ExecutionConfig,
    ExecutionLayout,
    RecoveryManifest,
    RecoveryOutput,
    WindowCheckpointStore,
    WindowGenerator,
    Window,
    compute_plan_fingerprint,
    compute_workflow_fingerprint,
    load_execution_config_snapshot,
    parse_execution_config,
    require_window_recovery_location,
    write_execution_config_snapshot,
    write_graph_snapshot,
    write_recovery_manifest,
)
from core.workflow_resources import (
    WorkflowResourcePlan,
    build_workflow_resource_plan,
    ensure_executable_resource_plan,
    validate_workflow_resource_plan,
)
from core.worker_cache import force_clear_worker_cache
from services.dask_service import dask_service, get_fresh_scheduler_info
from services.memory_monitor import get_memory_monitor
from services.recovery_service import (
    discover_terminal_outputs,
    inspect_recovery_directory,
)

logger = logging.getLogger("BrainFlow.Executor")
# Keep connection failures visible.  Suppressing ``distributed.core`` at
# CRITICAL hid the reason why a Scheduler disappeared while large graphs were
# being submitted, leaving only the downstream FuturesCancelledError.
logging.getLogger("distributed.core").setLevel(logging.WARNING)
logging.getLogger("distributed.utils").setLevel(logging.WARNING)


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


async def _cancel_sink_futures_with_timeout(
    client,
    sink_futures,
    *,
    timeout_seconds: float = 5.0,
) -> None:
    """Bound synchronous distributed cancellation during Driver teardown.

    A cross-node Scheduler or Worker network failure must not keep the service
    event loop inside ``Client.cancel`` forever; the outer Slurm controller
    still has to issue ``scancel`` and prove that the allocation is terminal.
    The underlying call may finish later in its disposable thread, but the
    execution cleanup barrier remains the authoritative remote-process fence.
    """

    await asyncio.wait_for(
        asyncio.to_thread(_cancel_sink_futures, client, sink_futures),
        timeout=timeout_seconds,
    )


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


async def _clear_worker_caches_with_timeout(
    client,
    *,
    timeout_seconds: float = 15.0,
) -> dict[str, dict]:
    """Clear caches through ordinary pinned tasks, not ``Client.run`` RPCs.

    ``Client.run`` broadcasts code onto Worker control/event-loop threads.  A
    timed-out synchronous broadcast continues running in its background thread
    and can emit communication failures minutes after an execution has already
    finished.  Normal Dask tasks are schedulable, observable, and cancellable;
    pinning one task to every currently registered Worker preserves the intended
    process-local cache cleanup without blocking Worker control threads.
    """
    # Dask defaults scheduler_info() to at most five Workers.  Large GPU
    # topologies must enumerate the complete pool or model caches survive on
    # every omitted process.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    scheduler_info = get_fresh_scheduler_info(client, timeout=timeout_seconds)
    if loop.time() >= deadline:
        raise TimeoutError(
            "Timed out while enumerating Workers for cache cleanup."
        )
    worker_addresses = tuple(sorted(dict(scheduler_info.get("workers", {}))))
    if not worker_addresses:
        return {}

    futures = []
    try:
        for index, worker_address in enumerate(worker_addresses):
            if loop.time() >= deadline:
                raise TimeoutError(
                    "Timed out while submitting Worker cache-clear tasks."
                )
            futures.append(
                client.submit(
                    force_clear_worker_cache,
                    key=f"workflow-cache-clear-{uuid.uuid4().hex}-{index}",
                    workers=[worker_address],
                    allow_other_workers=False,
                    pure=False,
                )
            )

        while not all(future.done() for future in futures):
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(
                    "Timed out waiting for Worker cache-clear tasks."
                )
            await asyncio.sleep(min(0.05, remaining))

        # done() makes these local state reads non-blocking while preserving
        # any Worker exception.  This avoids an uncancellable synchronous
        # client.gather() thread entirely.
        values = [future.result(timeout=0) for future in futures]
        return dict(zip(worker_addresses, values))
    finally:
        # The caller resets the local cluster on any exception.  release() is
        # asynchronous/non-blocking and avoids replacing one stuck RPC with an
        # equally unbounded synchronous client.cancel() call.
        _release_futures(futures)


def _remove_futures(tracked_futures: list, completed_futures: list) -> None:
    for future in completed_futures:
        try:
            tracked_futures.remove(future)
        except ValueError:
            pass


def _compute_with_resource_boundaries(
    client,
    collections,
    *,
    preserve_resource_boundaries: bool,
):
    """Submit collections while retaining both culling and resource routing.

    Window execution slices a collection whose HighLevelGraph describes the
    complete dataset.  Disabling optimization to preserve Worker Profile
    annotations also disabled Dask's culling pass, so every Window submitted
    that complete graph to the Scheduler.  Large datasets consequently grew
    the Driver by gigabytes before any Worker received useful work and could
    make the Scheduler miss its communication timeout.

    Dask already treats layers with different annotations as separate when
    annotation fusion is disabled.  Keep normal collection optimization (and
    therefore culling), while disabling only the fusion passes that could move
    work across a Worker Profile boundary.
    """
    # ``Client.compute`` adds finalization tasks for Dask collections.  Those
    # tasks are framework work rather than node work, so they may run on any
    # available Worker. The annotation applies only to layers created by
    # ``compute``; existing node layers keep their Worker Profile annotations.
    with dask.annotate(
        brainflow_node_id="__framework_finalize__",
        worker_profile="framework",
    ):
        if preserve_resource_boundaries:
            with dask.config.set(
                {
                    "optimization.annotations.fuse": False,
                    "optimization.fuse.active": False,
                }
            ):
                return client.compute(collections)
        return client.compute(collections)


def _available_resources_dict(summary) -> dict[str, int | float | None]:
    if summary is None:
        return {
            "cpuWorkers": None,
            "gpuWorkers": None,
            "cpuSlots": None,
            "gpuSlots": None,
        }
    return {
        "cpuWorkers": len(summary.cpu_workers),
        "gpuWorkers": len(summary.gpu_workers),
        "cpuSlots": summary.total_cpu_slots,
        "gpuSlots": summary.total_gpu_slots,
    }


def _resolve_max_in_flight_windows(
    requested: int | None,
    *,
    resource_plan: WorkflowResourcePlan,
    cluster_summary,
) -> int:
    del resource_plan, cluster_summary
    if requested is not None:
        resolved = requested
    else:
        resolved = 1

    configured_cap = config.MAX_IN_FLIGHT_WINDOWS
    if configured_cap is not None:
        resolved = min(resolved, int(configured_cap))
    return max(1, int(resolved))


def _resource_plan_for_execution_mode(
    plan: WorkflowResourcePlan,
    execution_config: ExecutionConfig | None,
) -> WorkflowResourcePlan:
    """Compatibility wrapper for the backend-neutral plan normalizer."""
    del execution_config
    return ensure_executable_resource_plan(plan)


def _requires_resource_boundary_preservation(
    plan: WorkflowResourcePlan,
) -> bool:
    """Return whether optimization could fuse differently constrained layers."""

    return len({node.worker_profile for node in plan.nodes}) > 1


@dataclass
class InFlightWindow:
    window: Window
    futures: list
    waiter: asyncio.Task


async def _log_memory_snapshot_with_timeout(
    memory_monitor,
    name: str,
    client,
    *,
    timeout_seconds: float = 15.0,
) -> bool:
    """Publish a memory snapshot only if its background collection is timely.

    Cancelling ``asyncio.to_thread`` cannot stop the worker thread.  Keeping
    collection side-effect free prevents a timed-out request from logging or
    mutating snapshot state minutes later when that thread eventually returns.
    """
    if not getattr(memory_monitor, "enabled", True):
        return False
    try:
        snapshot = await asyncio.wait_for(
            asyncio.to_thread(memory_monitor.collect_snapshot, client),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "[Memory] Timed out collecting Worker diagnostics for %s.",
            name,
        )
        return False
    except Exception as exc:
        logger.warning(
            "[Memory] Failed collecting Worker diagnostics for %s: %s",
            name,
            exc,
        )
        return False

    memory_monitor.record_snapshot(name, snapshot)
    return True


async def _wait_for_window_futures(futures: list) -> None:
    """Wait for, then inspect, every terminal Future for one Window."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: dist_wait(futures),
    )
    future_exceptions: list[BaseException] = []
    for future in futures:
        try:
            exception = await loop.run_in_executor(None, future.exception)
        except BaseException as exc:
            future_exceptions.append(exc)
        else:
            if exception is not None:
                future_exceptions.append(exception)
    if future_exceptions:
        raise future_exceptions[0]


async def _cancel_in_flight_windows(
    client,
    in_flight: dict[asyncio.Task, InFlightWindow],
    tracked_futures: list,
) -> None:
    """Cancel and drain all uncommitted Window work without changing the bitmap."""
    entries = list(in_flight.values())
    futures = [future for entry in entries for future in entry.futures]
    waiters = [entry.waiter for entry in entries]

    if futures:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(client.cancel, futures, force=True),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "[Cleanup] Timed out requesting cancellation for in-flight Windows."
            )
        except Exception as exc:
            logger.debug("[Cleanup] In-flight Window cancellation failed: %s", exc)

    for waiter in waiters:
        if not waiter.done():
            waiter.cancel()
    if waiters:
        await asyncio.gather(*waiters, return_exceptions=True)

    if futures:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: dist_wait(futures, timeout=2),
            )
        except Exception as exc:
            logger.debug("[Cleanup] In-flight Window drain failed: %s", exc)
        _release_futures(futures)
        _remove_futures(tracked_futures, futures)
    in_flight.clear()


async def _execute_pending_windows(
    *,
    client,
    root_arrays: list[da.Array],
    window_generator: WindowGenerator,
    pending_indices: np.ndarray,
    completed_windows_bitmap: np.ndarray,
    completed_count: int,
    window_store: WindowCheckpointStore,
    max_in_flight_windows: int,
    execution_roots: list[str],
    tracked_futures: list,
    progress_callback,
    window_progress_callback,
    preserve_resource_boundaries: bool,
) -> int:
    """Execute zero-valued Window positions with bounded, out-of-order completion."""
    total_windows = window_generator.total_windows
    pending_iterator = iter(pending_indices)
    in_flight: dict[asyncio.Task, InFlightWindow] = {}
    submitted_count = 0
    progress_stride = max(1, (len(pending_indices) + 999) // 1000)

    async def submit_next_window() -> bool:
        nonlocal submitted_count
        try:
            raw_index = next(pending_iterator)
        except StopIteration:
            return False

        window = window_generator.window_at(int(raw_index))
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
            submitted_count == 0
            or submitted_count + 1 == len(pending_indices)
            or submitted_count % progress_stride == 0
        )
        for node_id in execution_roots:
            await progress_callback(
                node_id,
                progress_percent,
                message,
                "running",
                broadcast_update=should_broadcast_progress,
            )

        # Slicing is framework-managed work that may run on either Worker pool.
        # Existing node layers retain their own resource annotations.
        with dask.annotate(
            brainflow_node_id="__window__",
            worker_profile="framework",
        ):
            window_collections = [
                root_array[window.slices]
                for root_array in root_arrays
            ]
        futures = _normalize_futures(
            _compute_with_resource_boundaries(
                client,
                window_collections,
                preserve_resource_boundaries=preserve_resource_boundaries,
            )
        )
        tracked_futures.extend(futures)
        waiter = asyncio.create_task(_wait_for_window_futures(futures))
        in_flight[waiter] = InFlightWindow(
            window=window,
            futures=futures,
            waiter=waiter,
        )
        submitted_count += 1
        return True

    try:
        while (
            len(in_flight) < max_in_flight_windows
            and await submit_next_window()
        ):
            pass

        while in_flight:
            done, _ = await asyncio.wait(
                tuple(in_flight),
                return_when=asyncio.FIRST_COMPLETED,
            )
            first_error: BaseException | None = None
            for waiter in done:
                entry = in_flight.pop(waiter)
                try:
                    waiter.result()
                    # The Driver commits only after all terminal Futures for
                    # this specific Window have succeeded.
                    committed = window_store.mark_completed(
                        completed_windows_bitmap,
                        entry.window.coordinates,
                    )
                    if committed:
                        completed_count += 1
                    await window_progress_callback(
                        current_window=entry.window.index + 1,
                        completed_windows=completed_count,
                        total_windows=total_windows,
                        window_status="running",
                        message=(
                            f"Completed Window {entry.window.index + 1} / "
                            f"{total_windows}"
                        ),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
                finally:
                    _release_futures(entry.futures)
                    _remove_futures(tracked_futures, entry.futures)

            if first_error is not None:
                raise first_error

            while (
                len(in_flight) < max_in_flight_windows
                and await submit_next_window()
            ):
                pass
    except BaseException:
        await _cancel_in_flight_windows(
            client,
            in_flight,
            tracked_futures,
        )
        raise
    return completed_count


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

            with dask.annotate(**dask_annotation_kwargs(node_cls, node_id)):
                if asyncio.iscoroutinefunction(method):
                    output = await method(**func_args)
                else:
                    annotation_context = contextvars.copy_context()
                    output = await loop.run_in_executor(
                        None,
                        lambda: annotation_context.run(method, **func_args),
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


async def preflight_graph(
    graph: dict,
    execution_config: ExecutionConfig | dict | None = None,
) -> dict:
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

    selected_config = (
        None
        if execution_config is None
        else parse_execution_config(execution_config)
    )
    resource_plan = _resource_plan_for_execution_mode(
        build_workflow_resource_plan(graph, execution_roots),
        selected_config,
    )
    resource_summary = None
    resources_satisfied: bool | None = None
    resource_error: str | None = None
    active_client = dask_service.get_client()
    if active_client is not None:
        try:
            resource_summary = await asyncio.to_thread(
                dask_service.get_cluster_resource_summary,
                active_client,
            )
            validate_workflow_resource_plan(resource_plan, resource_summary)
            resources_satisfied = True
        except Exception as exc:
            # The formal execution path provisions the exact DAG-derived
            # topology. A smaller cluster left by an earlier workflow is not a
            # permanent resource failure and must not disable Run in the UI.
            resources_satisfied = None
            logger.debug(
                "[Preflight] Active cluster will be resized for this DAG: %s",
                exc,
            )

    outputs = discover_terminal_outputs(graph, execution_roots)
    if (
        selected_config is not None
        and selected_config.mode == "window"
        and selected_config.recovery_location is not None
    ):
        # Read-only layout resolution validates the saved recovery location,
        # including separation from every terminal output path.  It does not
        # create the directory or any recovery files.
        ExecutionLayout.resolve(selected_config, outputs)
    output_entries = [
        output.to_dict(include_path_input=True)
        for output in outputs
    ]
    def response(
        *,
        windowable: bool,
        output_shape: tuple[int, ...] | None,
        reason: str | None = None,
    ) -> dict:
        window_shape: tuple[int, ...] | None = None
        window_grid_shape: tuple[int, ...] | None = None
        total_windows: int | None = None
        if (
            windowable
            and output_shape is not None
            and selected_config is not None
            and selected_config.mode == "window"
            and selected_config.window_shape is not None
        ):
            generator = WindowGenerator(
                output_shape,
                selected_config.window_shape,
            )
            window_shape = generator.window_shape
            window_grid_shape = generator.axis_counts
            total_windows = generator.total_windows

        result = {
            "windowable": windowable,
            # Keep the former snake_case field for older clients while the
            # canonical API uses camelCase.
            "output_shape": (
                list(output_shape) if output_shape is not None else None
            ),
            "outputShape": (
                list(output_shape) if output_shape is not None else None
            ),
            "ndim": len(output_shape) if output_shape is not None else None,
            "windowShape": (
                list(window_shape) if window_shape is not None else None
            ),
            "windowGridShape": (
                list(window_grid_shape)
                if window_grid_shape is not None
                else None
            ),
            "totalWindows": total_windows,
            "outputs": output_entries,
            "requiredResources": resource_plan.to_preflight_dict(),
            "availableResources": _available_resources_dict(resource_summary),
            "resourcesSatisfied": resources_satisfied,
            "resourceError": resource_error,
        }
        if reason is not None:
            result["reason"] = reason
        return result

    declared_reason = _declared_window_root_reason(graph, execution_roots)
    if declared_reason is not None:
        return response(
            windowable=False,
            output_shape=None,
            reason=declared_reason,
        )

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
            return response(
                windowable=False,
                output_shape=None,
                reason=reason,
            )
        return response(
            windowable=True,
            output_shape=output_shape,
        )
    finally:
        await _cancel_and_await_tasks(tasks)
        tasks.clear()
        results.clear()
        node_instances.clear()


def _recovery_control_paths(layout: ExecutionLayout) -> tuple:
    return (
        layout.manifest_path,
        layout.graph_path,
        layout.execution_config_path,
        layout.checkpoint_path,
    )


def _unexpected_recovery_entries(layout: ExecutionLayout) -> tuple:
    """Return entries that do not belong to the current recovery format."""

    if not layout.control_directory.exists():
        return ()
    allowed_names = {
        *(path.name for path in _recovery_control_paths(layout)),
        layout.lock_path.name,
        layout.lock_mutation_guard_path.name,
    }
    return tuple(
        child
        for child in layout.control_directory.iterdir()
        if child.name not in allowed_names
    )


def _output_contract(
    outputs: tuple[RecoveryOutput, ...],
) -> dict[str, tuple[str, str]]:
    return {
        output.node_id: (output.node_type, output.path)
        for output in outputs
    }


def _validate_resume_contract(
    *,
    manifest: RecoveryManifest,
    saved_config: ExecutionConfig,
    outputs: tuple[RecoveryOutput, ...],
    output_shape: tuple[int, ...],
    workflow_fingerprint: str,
    plan_fingerprint: str,
) -> None:
    if saved_config.mode != "window" or saved_config.window_shape is None:
        raise ValueError(
            "Recovery execution_config.json must describe Window execution "
            "with a windowShape."
        )
    if manifest.workflow_fingerprint != workflow_fingerprint:
        raise ValueError(
            "Recovery workflow fingerprint does not match immutable graph.json."
        )
    if manifest.plan_fingerprint != plan_fingerprint:
        raise ValueError(
            "Recovery plan fingerprint does not match the saved Window plan."
        )
    if manifest.window_plan.output_shape != output_shape:
        raise ValueError(
            "Recovery output shape does not match the lazy graph metadata "
            f"({manifest.window_plan.output_shape} != {output_shape})."
        )
    if manifest.window_plan.window_shape != saved_config.window_shape:
        raise ValueError(
            "Recovery execution_config.json windowShape does not match manifest.json."
        )
    if _output_contract(manifest.outputs) != _output_contract(outputs):
        raise ValueError(
            "Recovery manifest outputs do not match the terminal OUTPUT nodes "
            "in immutable graph.json."
        )


# =============================================================================
# 3. Core executor
# =============================================================================
async def execute_graph(
    graph: dict,
    execution_id: str = None,
    execution_config: ExecutionConfig | dict | None = None,
    *,
    checkpoint_store: WindowCheckpointStore | None = None,
    release_active_execution: bool = True,
    external_cleanup_barrier: Callable[[], Awaitable[None]] | None = None,
    worker_profiles: object = None,
    worker_pools: object = None,
):
    """Execute true terminal roots in Full Graph or bounded Window mode."""
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
    completed_count = 0
    window_generator: WindowGenerator | None = None
    selected_config: ExecutionConfig | None = None
    requested_config: ExecutionConfig | None = None
    recovery_layout: ExecutionLayout | None = None
    recovery_lock: ActiveExecutionLock | None = None
    recovery_manifest: RecoveryManifest | None = None
    terminal_outputs: tuple[RecoveryOutput, ...] = ()
    resource_plan: WorkflowResourcePlan | None = None
    cluster_summary = None
    external_cleanup_completed = False

    async def run_external_cleanup_barrier() -> None:
        nonlocal external_cleanup_completed
        if external_cleanup_barrier is None or external_cleanup_completed:
            return
        await external_cleanup_barrier()
        external_cleanup_completed = True

    if not execution_id:
        execution_id = uuid.uuid4().hex

    mem_monitor = get_memory_monitor()
    mem_monitor.snapshots.clear()
    state_manager.create_execution(execution_id)

    async def log_memory_snapshot(name: str) -> None:
        await _log_memory_snapshot_with_timeout(
            mem_monitor,
            name,
            client,
        )

    def persist_recovery_status(status: str) -> None:
        nonlocal recovery_manifest
        if (
            recovery_layout is None
            or recovery_manifest is None
            or recovery_lock is None
            or not recovery_lock.acquired
        ):
            return
        recovery_manifest = recovery_manifest.with_status(
            status,
            execution_id=execution_id,
        )
        write_recovery_manifest(recovery_layout, recovery_manifest)

    try:
        selected_config = parse_execution_config(execution_config)
        require_window_recovery_location(selected_config)
        requested_config = selected_config

        # Recovery actions are rooted in the immutable recovery snapshot, not
        # in a potentially edited graph sent by the browser. Resolve and own
        # the selected directory before reading its control files. A custom
        # location therefore supports graph=None for both Resume and Restart.
        if (
            selected_config.mode == "window"
            and selected_config.resume_action in {"resume", "restart"}
        ):
            if selected_config.recovery_location.mode == "custom":
                recovery_layout = ExecutionLayout.resolve(
                    selected_config,
                    (),
                )
            else:
                # output_sidecar needs the submitted graph only to locate the
                # canonical directory.  It is discarded immediately after the
                # immutable graph snapshot is loaded.
                validate_graph_structure(graph)
                validate_graph_acyclic(graph)
                validate_graph_types(graph)
                request_roots = find_execution_roots(graph)
                request_outputs = discover_terminal_outputs(
                    graph,
                    request_roots,
                )
                recovery_layout = ExecutionLayout.resolve(
                    selected_config,
                    request_outputs,
                )
            if checkpoint_store is not None:
                if (
                    checkpoint_store.layout.control_directory
                    != recovery_layout.control_directory
                ):
                    raise ValueError(
                        "Injected Window checkpoint store does not match the "
                        "explicit recoveryLocation."
                    )
                window_store = checkpoint_store
            else:
                window_store = WindowCheckpointStore(recovery_layout)

            if not recovery_layout.control_directory.exists():
                raise FileNotFoundError(
                    "Recovery directory does not exist: "
                    f"{recovery_layout.control_directory}"
                )
            if not recovery_layout.control_directory.is_dir():
                raise NotADirectoryError(
                    "Recovery path is not a directory: "
                    f"{recovery_layout.control_directory}"
                )
            recovery_lock = ActiveExecutionLock(
                recovery_layout,
                execution_id,
            ).acquire()
            # Load and validate the recovery record once while ownership keeps
            # its immutable files and bitmap stable for this execution.
            inspection = inspect_recovery_directory(
                recovery_layout.control_directory,
                require_unlocked=False,
            )
            recovery_manifest = inspection.manifest
            completed_windows_bitmap = inspection.completed_windows_bitmap
            completed_count = inspection.completed_windows
            saved_graph = inspection.graph
            saved_config = inspection.execution_config
            if saved_config.mode != "window" or saved_config.window_shape is None:
                raise ValueError(
                    "Recovery execution_config.json does not contain a valid "
                    "Window plan."
                )
            if (
                selected_config.window_shape is not None
                and selected_config.window_shape != saved_config.window_shape
            ):
                raise ValueError(
                    "Requested windowShape does not match the saved recovery plan."
                )
            graph = saved_graph
            selected_config = ExecutionConfig(
                mode="window",
                window_shape=saved_config.window_shape,
                max_in_flight_windows=saved_config.max_in_flight_windows,
                recovery_location=requested_config.recovery_location,
                resume_action=requested_config.resume_action,
            )

        validate_graph_structure(graph)
        validate_graph_acyclic(graph)
        validate_graph_types(graph)
        execution_roots = find_execution_roots(graph)
        execution_root_set = set(execution_roots)
        if not execution_roots:
            raise ValueError(
                "No terminal output node found. A workflow execution root must "
                "declare OUTPUT_NODE=True and have no outgoing graph connections."
            )

        # This is the authoritative validation path. For Resume and Restart,
        # graph is now the immutable snapshot from the recovery directory.
        resource_plan = _resource_plan_for_execution_mode(
            build_workflow_resource_plan(graph, execution_roots),
            selected_config,
        )
        logger.info(
            "[Dask] Graph Worker Profile requirements: %s contributors=%s",
            resource_plan.required_worker_profiles,
            tuple(
                (node.node_id, node.node_type, node.worker_profile)
                for node in resource_plan.nodes
            ),
        )
        # Profile requirements are not Worker counts. Slurm execution has
        # already provisioned the browser-configured Pools; a local backend
        # builds the same Pool topology directly from the supplied Profiles.
        if dask_service.uses_external_workers():
            client = dask_service.require_active_client()
        else:
            if worker_profiles is None or worker_pools is None:
                raise ValueError(
                    "Workflow execution requires Worker Profiles and Worker Pools. "
                    "Configure Worker Resources in the frontend and retry."
                )
            profiles, pools = parse_required_worker_resources(
                worker_profiles,
                worker_pools,
                tuple(resource_plan.required_worker_profiles),
            )
            client = await asyncio.to_thread(
                dask_service.ensure_profile_client,
                profiles=profiles,
                pools=pools,
                required_profiles=resource_plan.required_worker_profiles,
            )
        cluster_summary = await asyncio.to_thread(
            dask_service.get_cluster_resource_summary,
            client,
        )
        validate_workflow_resource_plan(resource_plan, cluster_summary)
        await log_memory_snapshot("execution_start")

        dashboard_url = str(
            rewrite_dashboard_url(
                getattr(client, "dashboard_link", None),
                config.DASHBOARD_HOST,
            )
            or ""
        )
        cluster_message = (
            "[System] Dask Cluster Ready: "
            f"CPU Workers={len(cluster_summary.cpu_workers)}, "
            f"GPU Workers={len(cluster_summary.gpu_workers)}, "
            f"Dashboard={dashboard_url or 'unavailable'}"
        )
        await state_manager.broadcast(execution_id, {
            "type": "cluster_ready",
            "executionId": execution_id,
            "dashboardUrl": dashboard_url,
            "cpuWorkers": len(cluster_summary.cpu_workers),
            "gpuWorkers": len(cluster_summary.gpu_workers),
            "message": cluster_message,
        })
        state_manager.add_log(
            cluster_message,
            "success",
            execution_id=execution_id,
        )
        logger.info("%s | execution_id=%s", cluster_message, execution_id)

        await state_manager.broadcast(execution_id, {
            "type": "log",
            "message": "Engine Started...",
            "executionId": execution_id,
        })
        state_manager.add_log("Engine Started...", "info", execution_id=execution_id)

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
            terminal_outputs = discover_terminal_outputs(
                graph,
                execution_roots,
            )
            preflight = await preflight_graph(graph, selected_config)
            if not preflight.get("windowable"):
                raise ValueError(
                    preflight.get("reason")
                    or "Window execution is unavailable for this workflow."
                )
            expected_output_shape = tuple(preflight["outputShape"])
            if selected_config.window_shape is None:
                raise ValueError("Window execution requires a resolved windowShape.")
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

            if selected_config.resume_action in {"resume", "restart"}:
                if (
                    recovery_layout is None
                    or recovery_manifest is None
                    or window_store is None
                ):
                    raise RuntimeError("Window recovery was not initialized.")
                _validate_resume_contract(
                    manifest=recovery_manifest,
                    saved_config=load_execution_config_snapshot(recovery_layout),
                    outputs=terminal_outputs,
                    output_shape=expected_output_shape,
                    workflow_fingerprint=workflow_fingerprint,
                    plan_fingerprint=plan_fingerprint,
                )
                if (
                    requested_config is not None
                    and requested_config.recovery_location is not None
                    and requested_config.recovery_location.mode == "output_sidecar"
                ):
                    saved_sidecar_layout = ExecutionLayout.resolve(
                        requested_config,
                        terminal_outputs,
                    )
                    if (
                        saved_sidecar_layout.control_directory
                        != recovery_layout.control_directory
                    ):
                        raise ValueError(
                            "Selected sidecar does not match the immutable recovery graph."
                        )

                canonical_layout = ExecutionLayout.resolve(
                    selected_config,
                    terminal_outputs,
                )
                if (
                    canonical_layout.control_directory
                    != recovery_layout.control_directory
                ):
                    raise ValueError(
                        "Selected recovery directory does not match the immutable "
                        "recovery graph."
                    )

                if selected_config.resume_action == "resume":
                    if completed_windows_bitmap is None:
                        raise ValueError(
                            f"Recovery checkpoint is missing: {recovery_layout.checkpoint_path}"
                        )
                    is_resuming = True
                else:
                    # Restart retains the immutable graph/config snapshots and
                    # resets only mutable execution state.
                    completed_windows_bitmap = window_store.create(
                        window_generator.axis_counts,
                        overwrite=True,
                    )
                    completed_count = 0
            else:
                if selected_config.resume_action != "new":
                    raise RuntimeError(
                        "Unexpected Window recovery action during initialization."
                    )
                canonical_layout = ExecutionLayout.resolve(
                    selected_config,
                    terminal_outputs,
                )
                if checkpoint_store is not None:
                    if (
                        checkpoint_store.layout.control_directory
                        != canonical_layout.control_directory
                    ):
                        raise ValueError(
                            "Injected Window checkpoint store does not match "
                            "the explicit recoveryLocation."
                        )
                    window_store = checkpoint_store
                else:
                    window_store = WindowCheckpointStore(canonical_layout)
                recovery_layout = canonical_layout

                recovery_lock = ActiveExecutionLock(
                    recovery_layout,
                    execution_id,
                ).acquire()
                existing_control_paths = tuple(
                    path
                    for path in _recovery_control_paths(recovery_layout)
                    if path.exists()
                )
                unexpected_entries = _unexpected_recovery_entries(
                    recovery_layout
                )
                if existing_control_paths or unexpected_entries:
                    raise FileExistsError(
                        "Recovery directory is not empty. To run the current "
                        "edited workflow, choose another recovery location or "
                        "delete the old record in Recovery; Resume and Restart "
                        f"use its saved workflow: "
                        f"{recovery_layout.control_directory}"
                    )
                write_graph_snapshot(recovery_layout, graph)
                write_execution_config_snapshot(
                    recovery_layout,
                    selected_config,
                )
                completed_windows_bitmap = window_store.create(
                    window_generator.axis_counts,
                    overwrite=False,
                )
                completed_count = 0

            if selected_config.resume_action != "resume":
                if completed_windows_bitmap is None:
                    raise ValueError(
                        "Window completion bitmap was not initialized."
                    )
                recovery_manifest = RecoveryManifest.create(
                    execution_id=execution_id,
                    workflow_fingerprint=workflow_fingerprint,
                    plan_fingerprint=plan_fingerprint,
                    output_shape=expected_output_shape,
                    window_shape=selected_config.window_shape,
                    outputs=terminal_outputs,
                    status="prepared",
                )
                write_recovery_manifest(
                    recovery_layout,
                    recovery_manifest,
                )

            recovery_message = (
                "Window recovery directory: "
                f"{recovery_layout.control_directory}"
            )
            await state_manager.broadcast(
                execution_id,
                {"type": "log", "message": recovery_message},
            )
            state_manager.add_log(
                recovery_message,
                "info",
                execution_id=execution_id,
            )
        await state_manager.broadcast(
            execution_id,
            {"type": "log", "message": "GraphBuilding..."},
        )
        state_manager.add_log("GraphBuilding...", "info", execution_id=execution_id)
        logger.info("[Execution %s] Building lazy Dask graph...", execution_id)
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
        logger.info("[Execution %s] Lazy Dask graph built.", execution_id)

        if selected_config.mode == "full_graph":
            output_sinks = _collect_output_sinks(results, execution_roots)
            if output_sinks:
                for node_id in execution_roots:
                    await progress_callback(node_id, None, "Submitted", "submitted")

                collections = [sink["collection"] for sink in output_sinks]
                futures = _normalize_futures(
                    _compute_with_resource_boundaries(
                        client,
                        collections,
                        preserve_resource_boundaries=(
                            _requires_resource_boundary_preservation(resource_plan)
                        ),
                    )
                )
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
                logger.info(
                    "[Execution %s] Submitted %s terminal sink(s) to Dask.",
                    execution_id,
                    len(output_sinks),
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
                raise RuntimeError(
                    "Window completion checkpoint was not initialized before "
                    "the execution graph was built."
                )
            if recovery_layout is None or recovery_manifest is None:
                raise RuntimeError("Window recovery manifest was not initialized.")

            recovery_manifest = recovery_manifest.with_status(
                "running",
                execution_id=execution_id,
            )
            write_recovery_manifest(recovery_layout, recovery_manifest)

            if is_resuming:
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
            all_windows_completed = completed_count == total_windows
            if not all_windows_completed:
                for node_id in execution_roots:
                    await progress_callback(node_id, None, "Submitted", "submitted")
                max_in_flight_windows = _resolve_max_in_flight_windows(
                    selected_config.max_in_flight_windows,
                    resource_plan=resource_plan,
                    cluster_summary=cluster_summary,
                )
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
                if max_in_flight_windows > 1:
                    concurrency_message = (
                        "Window concurrency: up to "
                        f"{max_in_flight_windows} Window(s) in flight."
                    )
                    await state_manager.broadcast(
                        execution_id,
                        {"type": "log", "message": concurrency_message},
                    )
                    state_manager.add_log(
                        concurrency_message,
                        "info",
                        execution_id=execution_id,
                    )
                completed_count = await _execute_pending_windows(
                    client=client,
                    root_arrays=root_arrays,
                    window_generator=window_generator,
                    pending_indices=pending_indices,
                    completed_windows_bitmap=completed_windows_bitmap,
                    completed_count=completed_count,
                    window_store=window_store,
                    max_in_flight_windows=max_in_flight_windows,
                    execution_roots=execution_roots,
                    tracked_futures=sink_futures,
                    progress_callback=progress_callback,
                    window_progress_callback=window_progress_callback,
                    preserve_resource_boundaries=(
                        _requires_resource_boundary_preservation(resource_plan)
                    ),
                )
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
                completed_windows=completed_count,
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

        # A service-node Driver must prove its remote Worker allocation gone
        # and close the Scheduler while it still owns active.lock. Only then
        # may a successful recovery record become externally available.
        await run_external_cleanup_barrier()

        # Recovery history remains durable after success. No await is allowed
        # between committing the terminal manifest, releasing ownership, and the
        # synchronous successful state transition.
        if selected_config.mode == "window":
            if recovery_layout is None or recovery_manifest is None:
                raise RuntimeError("Window recovery manifest was not initialized.")
            if recovery_lock is None or not recovery_lock.acquired:
                raise RuntimeError("Window recovery lock was not held at success.")
            recovery_manifest = recovery_manifest.with_status(
                "succeeded",
                execution_id=execution_id,
            )
            write_recovery_manifest(recovery_layout, recovery_manifest)
            recovery_lock.release()
        state_manager.set_execution_status(
            execution_id,
            ExecutionStatus.SUCCEEDED,
            release_active=False,
        )
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
        session = state_manager.get_execution(execution_id)
        user_requested_stop = bool(
            session and session.status == ExecutionStatus.CANCELLING
        )
        terminal_status = (
            ExecutionStatus.CANCELLED
            if user_requested_stop
            else ExecutionStatus.INTERRUPTED
        )
        terminal_message = (
            "Execution Cancelled"
            if user_requested_stop
            else "Execution Interrupted by backend shutdown"
        )
        logger.warning("%s.", terminal_message)
        try:
            persist_recovery_status(terminal_status)
        except Exception as status_exc:
            logger.error(
                "Failed to persist %s recovery status: %s",
                terminal_status,
                status_exc,
                exc_info=True,
            )
        state_manager.set_execution_status(
            execution_id,
            terminal_status,
            release_active=False,
        )
        await state_manager.broadcast(execution_id, {
            "type": "execution_finished",
            "executionId": execution_id,
            "status": terminal_status,
            "message": terminal_message,
        })
        state_manager.add_log(
            terminal_message,
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
            try:
                persist_recovery_status("cancelled")
            except Exception as status_exc:
                logger.error(
                    "Failed to persist cancelled recovery status: %s",
                    status_exc,
                    exc_info=True,
                )
            state_manager.set_execution_status(
                execution_id,
                ExecutionStatus.CANCELLED,
                release_active=False,
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
            try:
                persist_recovery_status("failed")
            except Exception as status_exc:
                logger.error(
                    "Failed to persist failed recovery status: %s",
                    status_exc,
                    exc_info=True,
                )
            state_manager.set_execution_status(
                execution_id,
                ExecutionStatus.FAILED,
                release_active=False,
            )
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
        await log_memory_snapshot("execution_end_before_cleanup")
        await _cancel_and_await_tasks(tasks)

        if client and sink_futures and should_cancel_dask_objects:
            try:
                await _cancel_sink_futures_with_timeout(
                    client,
                    sink_futures,
                    timeout_seconds=5.0,
                )
                logger.info(
                    "[Cleanup] Force cancelled %s sink futures",
                    len(sink_futures),
                )
            except (Exception, asyncio.TimeoutError) as exc:
                logger.debug("[Cleanup] Cancel failed: %s", exc)

        # Successful Full Graph Futures and cancelled failure Futures both
        # release their Driver references here.
        _release_futures(sink_futures)

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

        externally_managed_workers = external_cleanup_barrier is not None
        # On failure/cancellation the normal success barrier was not reached.
        # Run it after local Futures/node cleanup but before active.lock.
        await run_external_cleanup_barrier()
        if externally_managed_workers:
            # The service-node Slurm Driver owns the Scheduler Client, while
            # the Worker allocation is a separately tracked Slurm job.  Its
            # outer lifecycle must first cancel/confirm that allocation and
            # only then close the Scheduler; doing either operation here would
            # release the execution lease before remote writers are gone.
            logger.info(
                "[Cleanup] External Slurm Worker allocation cleanup is "
                "delegated to the service-node execution controller."
            )
        elif client and should_cancel_dask_objects:
            # A failed/cancelled task may have died while owning a no-expiry
            # Zarr storage-chunk lease. Rebuild the local Profile cluster before
            # another execution instead of either deadlocking on that stale
            # lease or weakening it into an unsafe expiring lock.
            try:
                graceful = await asyncio.to_thread(dask_service.stop_cluster)
                log_method = logger.info if graceful else logger.warning
                log_method(
                    "[Cleanup] Dask cluster stopped after unsuccessful execution%s",
                    "" if graceful else " using emergency Worker cleanup",
                )
            except Exception as exc:
                logger.error(
                    "[Cleanup] Dask cluster reset failed after unsuccessful execution: %s",
                    exc,
                    exc_info=True,
                )
            client = None
        elif client:
            try:
                stats = await _clear_worker_caches_with_timeout(
                    client,
                    timeout_seconds=15.0,
                )
                logger.info("[Cleanup] Worker cache cleared: %s", stats)
            except Exception as exc:
                # Reusing a partially cleared mixed cluster can retain large
                # Cellpose/CUDA allocations on just the omitted or unreachable
                # Workers and make the next otherwise-valid execution OOM.
                # Computation has already reached its terminal state, so reset
                # only the disposable local Dask runtime here.
                logger.warning(
                    "[Cleanup] Worker cache clear failed; resetting the Dask "
                    "cluster before the next execution: %s",
                    exc,
                )
                try:
                    await asyncio.to_thread(dask_service.stop_cluster)
                except Exception as reset_exc:
                    logger.error(
                        "[Cleanup] Dask cluster reset after cache failure failed: %s",
                        reset_exc,
                        exc_info=True,
                    )
                client = None

        # Failed, interrupted, and cancelled executions keep ownership until
        # outstanding Dask work and node cleanup finish, then expose recovery.
        if recovery_lock is not None and recovery_lock.acquired:
            try:
                recovery_lock.release()
            except Exception as exc:
                logger.error(
                    "[Cleanup] Recovery lock release failed: %s",
                    exc,
                    exc_info=True,
                )

        sink_futures.clear()
        tasks.clear()
        results.clear()
        # Keep the single-active-execution lease until all Driver/Worker
        # cleanup has completed. A following DAG may require a different
        # Worker topology and must not replace this execution's cluster early.
        if release_active_execution:
            state_manager.clear_active_execution(execution_id)
            state_manager.cleanup_old_executions()

        await log_memory_snapshot("execution_end_after_cleanup")
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
