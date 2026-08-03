"""Framework services for persistent Window recovery directories."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from core.registry import NODE_CLASS_MAPPINGS
from core.window_execution import (
    CheckpointSummary,
    ExecutionConfig,
    ExecutionLayout,
    RecoveryManifest,
    RecoveryOutput,
    WindowCheckpointStore,
    cleanup_stale_active_lock,
    compute_plan_fingerprint,
    compute_workflow_fingerprint,
    execution_config_to_dict,
    load_execution_config_snapshot,
    load_graph_snapshot,
    load_recovery_manifest,
)


def _absolute_path(value: Any, *, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty absolute path.")
    if "\x00" in value:
        raise ValueError(f"{name} contains a null byte.")
    path = Path(value.strip()).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path.")
    return path.resolve()


def _resolve_literal_output_path(
    node_id: str,
    node_cls: type,
    path_input: str,
    value: Any,
) -> str:
    if not isinstance(value, str):
        raise ValueError(
            f"Terminal OUTPUT node {node_id!r} input {path_input!r} must be a "
            "literal string, not a graph connection or another value type."
        )
    resolved = _absolute_path(
        value,
        name=f"Terminal OUTPUT node {node_id!r} input {path_input!r}",
    )

    # Writers may keep format-specific validation locally. The framework still
    # enforces that a validator cannot infer or append part of the final path.
    validator = getattr(node_cls, "validate_output_path", None)
    if callable(validator):
        validated = _absolute_path(
            validator(value),
            name=f"Terminal OUTPUT node {node_id!r} input {path_input!r}",
        )
        if validated != resolved:
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} modified its configured output "
                "path. The graph must contain the complete final path."
            )
    return str(resolved)


def discover_terminal_outputs(
    graph: Mapping[str, Mapping[str, Any]],
    execution_roots: Sequence[str] | None = None,
) -> tuple[RecoveryOutput, ...]:
    """Discover persistent terminal outputs through ``OUTPUT_PATH_INPUT`` only."""

    if not isinstance(graph, Mapping):
        raise ValueError("graph must be an object.")
    if execution_roots is None:
        # Import lazily so the executor may also consume this service without an
        # import cycle.
        from services.executor import find_execution_roots

        execution_roots = find_execution_roots(dict(graph))

    outputs: list[RecoveryOutput] = []
    for node_id in execution_roots:
        node_data = graph.get(node_id)
        if not isinstance(node_data, Mapping):
            raise ValueError(f"Terminal OUTPUT node {node_id!r} is missing from the graph.")
        node_type = node_data.get("type")
        node_cls = NODE_CLASS_MAPPINGS.get(node_type)
        if node_cls is None:
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} has unregistered type {node_type!r}."
            )

        path_input = getattr(node_cls, "OUTPUT_PATH_INPUT", None)
        if not isinstance(path_input, str) or not path_input.strip():
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} ({node_type}) must declare "
                "OUTPUT_PATH_INPUT for Window recovery."
            )
        path_input = path_input.strip()
        inputs = node_data.get("inputs")
        if not isinstance(inputs, Mapping) or path_input not in inputs:
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} is missing declared output path "
                f"input {path_input!r}."
            )
        path = _resolve_literal_output_path(
            node_id,
            node_cls,
            path_input,
            inputs[path_input],
        )
        display_name = getattr(node_cls, "DISPLAY_NAME", None) or str(node_type)
        outputs.append(
            RecoveryOutput(
                node_id=str(node_id),
                node_type=str(node_type),
                display_name=str(display_name),
                path_input=path_input,
                path=path,
            )
        )
    return tuple(outputs)


@dataclass(frozen=True)
class RecoveryInspection:
    layout: ExecutionLayout
    manifest: RecoveryManifest
    graph: dict[str, Any]
    execution_config: ExecutionConfig
    checkpoint_summary: CheckpointSummary

    @property
    def completed_windows(self) -> int:
        return self.checkpoint_summary.completed_windows

    @property
    def total_windows(self) -> int:
        return self.checkpoint_summary.total_windows

    @property
    def remaining_windows(self) -> int:
        return self.total_windows - self.completed_windows

    def to_summary(self) -> dict[str, Any]:
        plan = self.manifest.window_plan
        return {
            "found": True,
            "valid": True,
            "compatible": True,
            "status": self.manifest.status,
            "recoveryDirectory": str(self.layout.control_directory),
            "completedWindows": self.completed_windows,
            "totalWindows": self.total_windows,
            "remainingWindows": self.remaining_windows,
            "outputShape": list(plan.output_shape),
            "windowShape": list(plan.window_shape),
            "windowGridShape": list(plan.window_grid_shape),
            "outputs": [output.to_dict() for output in self.manifest.outputs],
        }


def _validate_saved_graph(
    graph: dict[str, Any],
) -> tuple[list[str], tuple[RecoveryOutput, ...]]:
    from services.executor import (
        find_execution_roots,
        validate_graph_acyclic,
        validate_graph_structure,
        validate_graph_types,
    )

    validate_graph_structure(graph)
    validate_graph_acyclic(graph)
    validate_graph_types(graph)
    execution_roots = find_execution_roots(graph)
    if not execution_roots:
        raise ValueError("Recovery graph has no terminal OUTPUT nodes.")
    return execution_roots, discover_terminal_outputs(graph, execution_roots)


def inspect_recovery_directory(
    directory: str | os.PathLike[str],
    *,
    require_unlocked: bool = True,
) -> RecoveryInspection:
    """Validate a complete recovery directory without starting execution."""

    control_directory = _absolute_path(
        str(directory),
        name="recoveryDirectory",
    )
    if not control_directory.exists():
        raise FileNotFoundError(
            f"Recovery directory does not exist: {control_directory}"
        )
    if not control_directory.is_dir():
        raise NotADirectoryError(
            f"Recovery path is not a directory: {control_directory}"
        )

    layout = ExecutionLayout(control_directory)
    if require_unlocked and layout.lock_path.exists():
        if not cleanup_stale_active_lock(layout):
            raise RuntimeError(
                f"Recovery directory is currently active: {control_directory}"
            )

    manifest = load_recovery_manifest(layout)
    if (
        manifest.status in {"prepared", "running"}
        and not layout.lock_path.exists()
    ):
        # A live Window execution owns active.lock. Without that owner these
        # statuses are last-known state from an interrupted backend process.
        manifest = manifest.with_status("interrupted")
    graph = load_graph_snapshot(layout)
    execution_config = load_execution_config_snapshot(layout)
    if execution_config.mode != "window":
        raise ValueError("Recovery execution_config.json must select Window execution.")

    execution_roots, discovered_outputs = _validate_saved_graph(graph)
    workflow_fingerprint = compute_workflow_fingerprint(graph, execution_roots)
    if workflow_fingerprint != manifest.workflow_fingerprint:
        raise ValueError(
            "Recovery graph workflow fingerprint does not match manifest.json."
        )

    plan = manifest.window_plan
    if execution_config.window_shape != plan.window_shape:
        raise ValueError(
            "Recovery execution configuration Window shape does not match manifest.json."
        )
    plan_fingerprint = compute_plan_fingerprint(
        plan.output_shape,
        plan.window_shape,
    )
    if plan_fingerprint != manifest.plan_fingerprint:
        raise ValueError(
            "Recovery Window plan fingerprint does not match manifest.json."
        )

    recorded_outputs = {
        output.node_id: output
        for output in manifest.outputs
    }
    if set(recorded_outputs) != {output.node_id for output in discovered_outputs}:
        raise ValueError(
            "Recovery manifest outputs do not match the saved graph terminal outputs."
        )
    for discovered in discovered_outputs:
        recorded = recorded_outputs[discovered.node_id]
        if (
            recorded.node_type != discovered.node_type
            or recorded.path != discovered.path
        ):
            raise ValueError(
                f"Recovery output {discovered.node_id!r} does not match graph.json."
            )

    checkpoint_store = WindowCheckpointStore(layout)
    checkpoint = checkpoint_store.inspect(
        expected_shape=plan.window_grid_shape,
    )
    if checkpoint is None:
        legacy = checkpoint_store.inspect_legacy(
            workflow_fingerprint=manifest.workflow_fingerprint,
            plan_fingerprint=manifest.plan_fingerprint,
            output_shape=plan.output_shape,
            window_shape=plan.window_shape,
            expected_shape=plan.window_grid_shape,
            total_windows=plan.total_windows,
        )
        if legacy is None:
            raise ValueError(
                f"Recovery checkpoint is missing: {layout.checkpoint_path}"
            )
        # Inspection stays read-only. The executor performs the conversion only
        # after it owns active.lock.
        checkpoint = CheckpointSummary(
            shape=plan.window_grid_shape,
            dtype=np.dtype(np.uint8),
            completed_windows=legacy.next_window_index,
            total_windows=legacy.total_windows,
        )
    if checkpoint.total_windows != plan.total_windows:
        raise ValueError(
            "Recovery checkpoint size does not match manifest totalWindows."
        )

    return RecoveryInspection(
        layout=layout,
        manifest=manifest,
        graph=graph,
        execution_config=execution_config,
        checkpoint_summary=checkpoint,
    )


def open_recovery_directory(
    directory: str | os.PathLike[str],
) -> dict[str, Any]:
    inspection = inspect_recovery_directory(
        directory,
        require_unlocked=True,
    )
    return {
        "graph": inspection.graph,
        "readOnly": True,
        "executionConfig": execution_config_to_dict(
            inspection.execution_config
        ),
        "recoverySummary": inspection.to_summary(),
    }


def _is_recognized_recovery_directory(path: Path) -> bool:
    if not path.is_dir():
        return False
    try:
        load_recovery_manifest(ExecutionLayout(path))
    except (OSError, ValueError):
        return False
    return True


def _available_filesystem_roots() -> tuple[Path, ...]:
    if os.name != "nt":
        return (Path("/"),)

    roots: list[Path] = []
    for codepoint in range(ord("A"), ord("Z") + 1):
        candidate = Path(f"{chr(codepoint)}:\\")
        try:
            if candidate.is_dir():
                roots.append(candidate.resolve())
        except OSError:
            continue
    return tuple(roots)


def list_directories(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """List any absolute directory accessible to the backend process."""

    if path is None or not str(path).strip():
        roots = _available_filesystem_roots()
        if os.name != "nt":
            return list_directories(roots[0])
        return {
            "path": None,
            "parent": None,
            "directories": [
                {
                    "name": root.drive or str(root),
                    "path": str(root),
                    "isRecoveryDirectory": _is_recognized_recovery_directory(root),
                }
                for root in roots
            ],
        }

    current = _absolute_path(str(path), name="path")
    if not current.exists():
        raise FileNotFoundError(f"Directory does not exist: {current}")
    if not current.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {current}")

    directories: list[dict[str, Any]] = []
    try:
        children = sorted(current.iterdir(), key=lambda item: item.name.casefold())
    except OSError as exc:
        raise PermissionError(f"Cannot list directory {current}: {exc}") from exc
    for child in children:
        try:
            resolved_child = child.resolve()
            if not resolved_child.is_dir():
                continue
        except (OSError, RuntimeError):
            continue
        directories.append({
            "name": child.name,
            "path": str(resolved_child),
            "isRecoveryDirectory": _is_recognized_recovery_directory(
                resolved_child
            ),
        })

    parent = None if current.parent == current else str(current.parent)
    return {
        "path": str(current),
        "parent": parent,
        "directories": directories,
    }
