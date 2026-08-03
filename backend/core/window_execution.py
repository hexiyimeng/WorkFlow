"""Small primitives for final-array Window execution and durable resume.

Recovery is intentionally at-least-once: a Window can finish its terminal side
effects and the process can fail before its completion is durably recorded.
Terminal side effects therefore need to be safe to retry.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import math
import operator
import os
from pathlib import Path
import re
import socket
import threading
import time
from typing import Any, BinaryIO, Iterator, Literal, Mapping, Sequence
import uuid
import zipfile

import numpy as np
import psutil

WINDOW_GENERATOR_VERSION = 1
WINDOW_TRAVERSAL_ORDER = "lexicographic"
RECOVERY_FORMAT = "workflow-window-recovery"
RECOVERY_SCHEMA_VERSION = 1
RECOVERY_STATUSES = frozenset({
    "prepared",
    "running",
    "interrupted",
    "failed",
    "cancelled",
    "succeeded",
})
MANIFEST_FILENAME = "manifest.json"
GRAPH_FILENAME = "graph.json"
EXECUTION_CONFIG_FILENAME = "execution_config.json"
CHECKPOINT_FILENAME = "completed_windows.npy"
ACTIVE_LOCK_FILENAME = "active.lock"
ACTIVE_LOCK_SCHEMA_VERSION = 2
ACTIVE_LOCK_GUARD_OFFSET = 1 << 20
ACTIVE_LOCK_MUTATION_GUARD_FILENAME = ".active.guard"
LEGACY_CHECKPOINT_FILENAME = "resume_state.json"
LEGACY_CHECKPOINT_SCHEMA_VERSION = 1

ResumeAction = Literal["new", "resume", "restart"]
RecoveryMode = Literal["output_sidecar", "custom"]


def _non_empty_string(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string.")
    return value.strip()


def _absolute_directory(value: Any, *, name: str) -> str:
    raw = _non_empty_string(value, name=name)
    path = Path(raw).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path.")
    return str(path.resolve())


@dataclass(frozen=True)
class RecoveryLocation:
    mode: RecoveryMode
    anchor_node_id: str | None = None
    directory: str | None = None

    def __post_init__(self) -> None:
        if self.mode == "output_sidecar":
            anchor_node_id = _non_empty_string(
                self.anchor_node_id,
                name="recoveryLocation.anchorNodeId",
            )
            if self.directory not in (None, ""):
                raise ValueError(
                    "output_sidecar recoveryLocation must not specify directory."
                )
            object.__setattr__(self, "anchor_node_id", anchor_node_id)
            object.__setattr__(self, "directory", None)
            return

        if self.mode == "custom":
            directory = _absolute_directory(
                self.directory,
                name="recoveryLocation.directory",
            )
            if self.anchor_node_id not in (None, ""):
                raise ValueError(
                    "custom recoveryLocation must not specify anchorNodeId."
                )
            object.__setattr__(self, "anchor_node_id", None)
            object.__setattr__(self, "directory", directory)
            return

        raise ValueError(
            "recoveryLocation.mode must be 'output_sidecar' or 'custom'."
        )

    def to_dict(self) -> dict[str, Any]:
        if self.mode == "output_sidecar":
            return {
                "mode": self.mode,
                "anchorNodeId": self.anchor_node_id,
            }
        return {
            "mode": self.mode,
            "directory": self.directory,
        }


@dataclass(frozen=True)
class ExecutionConfig:
    mode: Literal["full_graph", "window"]
    window_shape: tuple[int, ...] | None = None
    recovery_location: RecoveryLocation | None = None
    resume_action: ResumeAction = "new"
    max_in_flight_windows: int | None = None

    def __post_init__(self) -> None:
        if self.mode not in ("full_graph", "window"):
            raise ValueError("ExecutionConfig.mode must be 'full_graph' or 'window'.")
        if self.resume_action not in ("new", "resume", "restart"):
            raise ValueError(
                "ExecutionConfig.resume_action must be 'new', 'resume', or 'restart'."
            )
        if (
            self.recovery_location is not None
            and not isinstance(self.recovery_location, RecoveryLocation)
        ):
            raise ValueError(
                "ExecutionConfig.recovery_location must be a RecoveryLocation."
            )

        if self.max_in_flight_windows is not None:
            if isinstance(self.max_in_flight_windows, bool):
                raise ValueError("max_in_flight_windows must be a positive integer.")
            try:
                normalized_limit = operator.index(self.max_in_flight_windows)
            except TypeError as exc:
                raise ValueError(
                    "max_in_flight_windows must be a positive integer."
                ) from exc
            if normalized_limit <= 0:
                raise ValueError("max_in_flight_windows must be a positive integer.")
            object.__setattr__(
                self,
                "max_in_flight_windows",
                int(normalized_limit),
            )

        if self.mode == "full_graph":
            if self.window_shape is not None:
                raise ValueError("Full Graph execution must not specify window_shape.")
            if self.recovery_location is not None or self.resume_action != "new":
                raise ValueError(
                    "Full Graph execution must not specify recovery settings."
                )
            if self.max_in_flight_windows is not None:
                raise ValueError(
                    "Full Graph execution must not specify max_in_flight_windows."
                )
            return

        if self.window_shape is not None:
            normalized_shape = _shape_tuple(
                self.window_shape,
                name="window_shape",
                allow_zero=False,
            )
            object.__setattr__(self, "window_shape", normalized_shape)
        elif self.resume_action != "resume":
            raise ValueError(
                "Window Execution requires window_shape for new or restart execution."
            )

        if self.resume_action == "resume" and self.recovery_location is None:
            raise ValueError(
                "Window resume requires an explicit recoveryLocation."
            )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"mode": self.mode}
        if self.mode == "window":
            if self.window_shape is not None:
                payload["windowShape"] = list(self.window_shape)
            payload["resumeAction"] = self.resume_action
            if self.recovery_location is not None:
                payload["recoveryLocation"] = self.recovery_location.to_dict()
            if self.max_in_flight_windows is not None:
                payload["maxInFlightWindows"] = self.max_in_flight_windows
        return payload


@dataclass(frozen=True)
class Window:
    index: int
    coordinates: tuple[int, ...]
    starts: tuple[int, ...]
    stops: tuple[int, ...]

    @property
    def slices(self) -> tuple[slice, ...]:
        return tuple(
            slice(start, stop)
            for start, stop in zip(self.starts, self.stops)
        )


def _shape_tuple(
    value: Sequence[Any],
    *,
    name: str,
    allow_zero: bool,
) -> tuple[int, ...]:
    if isinstance(value, (str, bytes)):
        raise ValueError(f"{name} must be a sequence of integers.")

    result: list[int] = []
    try:
        raw_values = tuple(value)
    except TypeError as exc:
        raise ValueError(f"{name} must be a sequence of integers.") from exc

    for raw in raw_values:
        if isinstance(raw, bool):
            raise ValueError(f"{name} must contain integers, not booleans.")
        try:
            size = operator.index(raw)
        except TypeError as exc:
            raise ValueError(f"{name} must contain integers, got {raw!r}.") from exc
        if size < 0 or (size == 0 and not allow_zero):
            qualifier = "non-negative" if allow_zero else "positive"
            raise ValueError(f"{name} must contain {qualifier} integers, got {size}.")
        result.append(int(size))
    return tuple(result)


def _parse_recovery_location(value: Any) -> RecoveryLocation | None:
    if value is None:
        return None
    if isinstance(value, RecoveryLocation):
        return value
    if not isinstance(value, Mapping):
        raise ValueError("executionConfig.recoveryLocation must be an object.")

    return RecoveryLocation(
        mode=value.get("mode"),
        anchor_node_id=value.get("anchorNodeId", value.get("anchor_node_id")),
        directory=value.get("directory"),
    )


def parse_execution_config(
    payload: Mapping[str, Any] | ExecutionConfig | None,
) -> ExecutionConfig:
    """Parse frontend execution settings while preserving old Full Graph clients."""
    if payload is None:
        return ExecutionConfig(mode="full_graph")
    if isinstance(payload, ExecutionConfig):
        return payload
    if not isinstance(payload, Mapping):
        raise ValueError("executionConfig must be an object.")

    mode = payload.get("mode", "full_graph")
    if mode == "full_graph":
        if payload.get(
            "maxInFlightWindows",
            payload.get("max_in_flight_windows"),
        ) is not None:
            raise ValueError(
                "maxInFlightWindows is valid only for Window execution."
            )
        return ExecutionConfig(mode="full_graph")
    if mode != "window":
        raise ValueError("executionConfig.mode must be 'full_graph' or 'window'.")

    resume_action = payload.get(
        "resumeAction",
        payload.get("resume_action", "new"),
    )
    raw_shape = payload.get("windowShape", payload.get("window_shape"))
    window_shape = (
        None
        if raw_shape is None
        else _shape_tuple(raw_shape, name="windowShape", allow_zero=False)
    )
    recovery_location = _parse_recovery_location(
        payload.get("recoveryLocation", payload.get("recovery_location"))
    )
    max_in_flight_windows = payload.get(
        "maxInFlightWindows",
        payload.get("max_in_flight_windows"),
    )
    return ExecutionConfig(
        mode="window",
        window_shape=window_shape,
        recovery_location=recovery_location,
        resume_action=resume_action,
        max_in_flight_windows=max_in_flight_windows,
    )


def execution_config_to_dict(
    config: Mapping[str, Any] | ExecutionConfig | None,
) -> dict[str, Any]:
    return parse_execution_config(config).to_dict()


class WindowGenerator:
    """Deterministic, last-axis-fastest windows in final-array index space."""

    def __init__(
        self,
        output_shape: Sequence[Any],
        window_shape: Sequence[Any],
    ) -> None:
        self.output_shape = _shape_tuple(
            output_shape,
            name="output_shape",
            allow_zero=True,
        )
        self.window_shape = _shape_tuple(
            window_shape,
            name="window_shape",
            allow_zero=False,
        )
        if not self.output_shape:
            raise ValueError("Window Execution requires a Dask Array with at least one dimension.")
        if len(self.output_shape) != len(self.window_shape):
            raise ValueError(
                "window_shape rank must match output_shape rank "
                f"({len(self.window_shape)} != {len(self.output_shape)})."
            )

        self.axis_counts = tuple(
            (output_size + window_size - 1) // window_size
            for output_size, window_size in zip(self.output_shape, self.window_shape)
        )
        self.total_windows = math.prod(self.axis_counts)

    def window_at(self, index: int) -> Window:
        try:
            normalized_index = operator.index(index)
        except TypeError as exc:
            raise TypeError("Window index must be an integer.") from exc
        if normalized_index < 0 or normalized_index >= self.total_windows:
            raise IndexError(
                f"Window index {normalized_index} is outside [0, {self.total_windows})."
            )

        remainder = int(normalized_index)
        coordinates = [0] * len(self.axis_counts)
        for axis in range(len(self.axis_counts) - 1, -1, -1):
            count = self.axis_counts[axis]
            coordinates[axis] = remainder % count
            remainder //= count

        starts = tuple(
            coordinate * window_size
            for coordinate, window_size in zip(coordinates, self.window_shape)
        )
        stops = tuple(
            min(start + window_size, output_size)
            for start, window_size, output_size in zip(
                starts,
                self.window_shape,
                self.output_shape,
            )
        )
        return Window(
            index=int(normalized_index),
            coordinates=tuple(coordinates),
            starts=starts,
            stops=stops,
        )

    def iter_from(self, start_index: int = 0) -> Iterator[Window]:
        try:
            normalized_start = operator.index(start_index)
        except TypeError as exc:
            raise TypeError("Start index must be an integer.") from exc
        if normalized_start < 0 or normalized_start > self.total_windows:
            raise ValueError(
                f"Start index {normalized_start} is outside [0, {self.total_windows}]."
            )
        for index in range(int(normalized_start), self.total_windows):
            yield self.window_at(index)

    def __iter__(self) -> Iterator[Window]:
        return self.iter_from(0)


def generate_windows(
    output_shape: Sequence[Any],
    window_shape: Sequence[Any],
    *,
    start_index: int = 0,
) -> Iterator[Window]:
    yield from WindowGenerator(output_shape, window_shape).iter_from(start_index)


def _canonical_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Workflow fingerprint values must not contain NaN or Infinity.")
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    raise TypeError(
        "Workflow graph values must be JSON-compatible for fingerprinting, "
        f"got {type(value).__name__}."
    )


def _canonical_json(payload: Any) -> str:
    return json.dumps(
        _canonical_value(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_payload(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _semantic_graph_inputs(node_data: Mapping[str, Any]) -> Mapping[str, Any]:
    """Mirror execution-time defaults/coercion when node metadata is available."""
    raw_inputs = node_data.get("inputs") or {}
    try:
        from core.invocation_builder import (
            coerce_input_value,
            declared_type_and_meta,
            get_node_input_defs,
        )
        from core.registry import NODE_CLASS_MAPPINGS
        from core.type_system import is_dask_array_type
    except ImportError:
        return raw_inputs

    node_cls = NODE_CLASS_MAPPINGS.get(node_data.get("type"))
    if node_cls is None:
        # The standalone helper remains useful for semantic graphs whose node
        # types are not loaded in the current registry.
        return raw_inputs

    input_defs = get_node_input_defs(node_cls)
    normalized: dict[str, Any] = {}
    for section in ("required", "optional"):
        for name, config in input_defs.get(section, {}).items():
            declared, meta = declared_type_and_meta(config)
            value = raw_inputs.get(name)

            if isinstance(value, list) and len(value) == 2:
                normalized[name] = value
                continue

            is_enum = isinstance(declared, list) and bool(declared)
            missing_required = section == "required" and (
                value is None or (isinstance(value, str) and value == "")
            )
            if missing_required:
                if "default" in meta:
                    value = meta["default"]
                elif is_enum:
                    value = declared[0]
            elif section == "optional" and value is None and "default" in meta:
                value = meta["default"]

            if isinstance(declared, str) and not is_dask_array_type(declared):
                value = coerce_input_value(name, declared, value)
            normalized[name] = value

    return normalized


def compute_workflow_fingerprint(
    graph: Mapping[str, Mapping[str, Any]],
    execution_roots: Sequence[str],
) -> str:
    """Hash the semantic execution DAG without concrete node identifiers."""
    memo: dict[str, str] = {}
    visiting: set[str] = set()

    def node_fingerprint(node_id: str) -> str:
        if node_id in memo:
            return memo[node_id]
        if node_id in visiting:
            raise ValueError(f"Cycle detected while fingerprinting node {node_id!r}.")
        node_data = graph.get(node_id)
        if node_data is None:
            raise ValueError(f"Cannot fingerprint unknown node {node_id!r}.")

        visiting.add(node_id)
        semantic_inputs: list[dict[str, Any]] = []
        for input_name, value in sorted(
            _semantic_graph_inputs(node_data).items(),
            key=lambda pair: str(pair[0]),
        ):
            if isinstance(value, list) and len(value) == 2:
                source_id, output_slot = value
                semantic_inputs.append({
                    "name": str(input_name),
                    "connection": {
                        "upstream": node_fingerprint(str(source_id)),
                        "output_slot": int(output_slot),
                    },
                })
            else:
                semantic_inputs.append({
                    "name": str(input_name),
                    "literal": _canonical_value(value),
                })

        payload = {
            "node_type": str(node_data.get("type")),
            "inputs": semantic_inputs,
        }
        fingerprint = _sha256_payload(payload)
        visiting.remove(node_id)
        memo[node_id] = fingerprint
        return fingerprint

    root_fingerprints = sorted(node_fingerprint(root_id) for root_id in execution_roots)
    return _sha256_payload({
        "fingerprint_version": 1,
        "execution_roots": root_fingerprints,
    })


def compute_plan_fingerprint(
    output_shape: Sequence[Any],
    window_shape: Sequence[Any],
) -> str:
    normalized_output = _shape_tuple(
        output_shape,
        name="output_shape",
        allow_zero=True,
    )
    normalized_window = _shape_tuple(
        window_shape,
        name="window_shape",
        allow_zero=False,
    )
    if len(normalized_output) != len(normalized_window):
        raise ValueError("window_shape rank must match output_shape rank.")
    return _sha256_payload({
        "output_shape": list(normalized_output),
        "window_shape": list(normalized_window),
        "traversal_order": WINDOW_TRAVERSAL_ORDER,
        "window_generator_version": WINDOW_GENERATOR_VERSION,
    })


def default_checkpoint_root() -> Path:
    configured = os.getenv("WorkFlow_CHECKPOINT_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / ".checkpoints"


@dataclass(frozen=True)
class RecoveryOutput:
    node_id: str
    node_type: str
    display_name: str
    path: str
    path_input: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "node_id",
            _non_empty_string(self.node_id, name="output.nodeId"),
        )
        object.__setattr__(
            self,
            "node_type",
            _non_empty_string(self.node_type, name="output.nodeType"),
        )
        object.__setattr__(
            self,
            "display_name",
            _non_empty_string(self.display_name, name="output.displayName"),
        )
        raw_path = _non_empty_string(self.path, name="output.path")
        output_path = Path(raw_path).expanduser()
        if not output_path.is_absolute():
            raise ValueError("output.path must be an absolute path.")
        object.__setattr__(self, "path", str(output_path.resolve()))

        if self.path_input is not None:
            object.__setattr__(
                self,
                "path_input",
                _non_empty_string(self.path_input, name="output.pathInput"),
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | RecoveryOutput) -> RecoveryOutput:
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("Recovery output entries must be objects.")
        return cls(
            node_id=value.get("nodeId", value.get("node_id")),
            node_type=value.get("nodeType", value.get("node_type")),
            display_name=value.get("displayName", value.get("display_name")),
            path=value.get("path"),
            path_input=value.get("pathInput", value.get("path_input")),
        )

    def to_dict(self, *, include_path_input: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "nodeId": self.node_id,
            "nodeType": self.node_type,
            "displayName": self.display_name,
            "path": self.path,
        }
        if include_path_input and self.path_input is not None:
            result["pathInput"] = self.path_input
        return result


@dataclass(frozen=True)
class WindowPlan:
    output_shape: tuple[int, ...]
    window_shape: tuple[int, ...]
    window_grid_shape: tuple[int, ...]
    total_windows: int

    def __post_init__(self) -> None:
        output_shape = _shape_tuple(
            self.output_shape,
            name="windowPlan.outputShape",
            allow_zero=True,
        )
        window_shape = _shape_tuple(
            self.window_shape,
            name="windowPlan.windowShape",
            allow_zero=False,
        )
        generator = WindowGenerator(output_shape, window_shape)
        window_grid_shape = _shape_tuple(
            self.window_grid_shape,
            name="windowPlan.windowGridShape",
            allow_zero=True,
        )
        if window_grid_shape != generator.axis_counts:
            raise ValueError(
                "windowPlan.windowGridShape does not match outputShape/windowShape "
                f"({window_grid_shape} != {generator.axis_counts})."
            )
        if isinstance(self.total_windows, bool):
            raise ValueError("windowPlan.totalWindows must be a non-negative integer.")
        try:
            total_windows = operator.index(self.total_windows)
        except TypeError as exc:
            raise ValueError(
                "windowPlan.totalWindows must be a non-negative integer."
            ) from exc
        if total_windows != generator.total_windows:
            raise ValueError(
                "windowPlan.totalWindows does not match windowGridShape "
                f"({total_windows} != {generator.total_windows})."
            )
        object.__setattr__(self, "output_shape", output_shape)
        object.__setattr__(self, "window_shape", window_shape)
        object.__setattr__(self, "window_grid_shape", window_grid_shape)
        object.__setattr__(self, "total_windows", int(total_windows))

    @classmethod
    def create(
        cls,
        output_shape: Sequence[Any],
        window_shape: Sequence[Any],
    ) -> WindowPlan:
        generator = WindowGenerator(output_shape, window_shape)
        return cls(
            output_shape=generator.output_shape,
            window_shape=generator.window_shape,
            window_grid_shape=generator.axis_counts,
            total_windows=generator.total_windows,
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WindowPlan:
        if not isinstance(value, Mapping):
            raise ValueError("manifest.windowPlan must be an object.")
        return cls(
            output_shape=value.get("outputShape"),
            window_shape=value.get("windowShape"),
            window_grid_shape=value.get("windowGridShape"),
            total_windows=value.get("totalWindows"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "outputShape": list(self.output_shape),
            "windowShape": list(self.window_shape),
            "windowGridShape": list(self.window_grid_shape),
            "totalWindows": self.total_windows,
        }


def _validate_fingerprint(value: Any, *, name: str) -> str:
    fingerprint = _non_empty_string(value, name=name)
    if re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 fingerprint.")
    return fingerprint


@dataclass(frozen=True)
class RecoveryManifest:
    execution_id: str
    status: str
    workflow_fingerprint: str
    plan_fingerprint: str
    window_plan: WindowPlan
    outputs: tuple[RecoveryOutput, ...]
    format: str = RECOVERY_FORMAT
    schema_version: int = RECOVERY_SCHEMA_VERSION
    graph_file: str = GRAPH_FILENAME
    execution_config_file: str = EXECUTION_CONFIG_FILENAME
    checkpoint_file: str = CHECKPOINT_FILENAME

    def __post_init__(self) -> None:
        if self.format != RECOVERY_FORMAT:
            raise ValueError(
                f"Unsupported recovery format {self.format!r}; "
                f"expected {RECOVERY_FORMAT!r}."
            )
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version != RECOVERY_SCHEMA_VERSION
        ):
            raise ValueError(
                f"Unsupported recovery schemaVersion {self.schema_version!r}; "
                f"expected {RECOVERY_SCHEMA_VERSION}."
            )
        object.__setattr__(
            self,
            "execution_id",
            _non_empty_string(self.execution_id, name="manifest.executionId"),
        )
        if self.status not in RECOVERY_STATUSES:
            raise ValueError(
                "manifest.status must be one of "
                f"{sorted(RECOVERY_STATUSES)}, got {self.status!r}."
            )
        object.__setattr__(
            self,
            "workflow_fingerprint",
            _validate_fingerprint(
                self.workflow_fingerprint,
                name="manifest.workflowFingerprint",
            ),
        )
        object.__setattr__(
            self,
            "plan_fingerprint",
            _validate_fingerprint(
                self.plan_fingerprint,
                name="manifest.planFingerprint",
            ),
        )
        if not isinstance(self.window_plan, WindowPlan):
            raise ValueError("manifest.windowPlan must be a WindowPlan.")

        outputs = tuple(
            RecoveryOutput(
                node_id=normalized.node_id,
                node_type=normalized.node_type,
                display_name=normalized.display_name,
                path=normalized.path,
            )
            for normalized in (
                RecoveryOutput.from_mapping(output)
                for output in self.outputs
            )
        )
        if not outputs:
            raise ValueError("manifest.outputs must contain at least one output.")
        node_ids = [output.node_id for output in outputs]
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("manifest.outputs contains duplicate nodeId values.")
        object.__setattr__(self, "outputs", outputs)

        required_files = {
            "graphFile": (self.graph_file, GRAPH_FILENAME),
            "executionConfigFile": (
                self.execution_config_file,
                EXECUTION_CONFIG_FILENAME,
            ),
            "checkpointFile": (self.checkpoint_file, CHECKPOINT_FILENAME),
        }
        for field_name, (actual, expected) in required_files.items():
            if actual != expected:
                raise ValueError(
                    f"manifest.{field_name} must be {expected!r}, got {actual!r}."
                )

    @classmethod
    def create(
        cls,
        *,
        execution_id: str,
        workflow_fingerprint: str,
        plan_fingerprint: str,
        output_shape: Sequence[Any],
        window_shape: Sequence[Any],
        outputs: Sequence[Mapping[str, Any] | RecoveryOutput],
        status: str = "prepared",
    ) -> RecoveryManifest:
        return cls(
            execution_id=execution_id,
            status=status,
            workflow_fingerprint=workflow_fingerprint,
            plan_fingerprint=plan_fingerprint,
            window_plan=WindowPlan.create(output_shape, window_shape),
            outputs=tuple(RecoveryOutput.from_mapping(value) for value in outputs),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> RecoveryManifest:
        if not isinstance(value, Mapping):
            raise ValueError("Recovery manifest must be a JSON object.")
        try:
            outputs = value["outputs"]
            if isinstance(outputs, (str, bytes)):
                raise TypeError
            normalized_outputs = tuple(
                RecoveryOutput.from_mapping(output)
                for output in outputs
            )
        except (KeyError, TypeError) as exc:
            raise ValueError("manifest.outputs must be an array of output entries.") from exc

        return cls(
            format=value.get("format"),
            schema_version=value.get("schemaVersion"),
            execution_id=value.get("executionId"),
            status=value.get("status"),
            workflow_fingerprint=value.get("workflowFingerprint"),
            plan_fingerprint=value.get("planFingerprint"),
            graph_file=value.get("graphFile"),
            execution_config_file=value.get("executionConfigFile"),
            checkpoint_file=value.get("checkpointFile"),
            window_plan=WindowPlan.from_mapping(value.get("windowPlan")),
            outputs=normalized_outputs,
        )

    def with_status(
        self,
        status: str,
        *,
        execution_id: str | None = None,
    ) -> RecoveryManifest:
        updates: dict[str, Any] = {"status": status}
        if execution_id is not None:
            updates["execution_id"] = execution_id
        return replace(self, **updates)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "schemaVersion": self.schema_version,
            "executionId": self.execution_id,
            "status": self.status,
            "workflowFingerprint": self.workflow_fingerprint,
            "planFingerprint": self.plan_fingerprint,
            "graphFile": self.graph_file,
            "executionConfigFile": self.execution_config_file,
            "checkpointFile": self.checkpoint_file,
            "windowPlan": self.window_plan.to_dict(),
            "outputs": [output.to_dict() for output in self.outputs],
        }


@dataclass(frozen=True)
class ExecutionLayout:
    control_directory: Path

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "control_directory",
            Path(self.control_directory).expanduser().resolve(),
        )

    @classmethod
    def resolve(
        cls,
        config: Mapping[str, Any] | ExecutionConfig,
        outputs: Sequence[Mapping[str, Any] | RecoveryOutput],
        *,
        workflow_fingerprint: str | None = None,
        plan_fingerprint: str | None = None,
    ) -> ExecutionLayout:
        selected = parse_execution_config(config)
        if selected.mode != "window":
            raise ValueError("A recovery layout exists only for Window execution.")

        location = selected.recovery_location
        if location is None:
            # Backward-compatible clients use the existing fingerprint layout.
            # New creation still refuses any persistent file already present.
            workflow = _validate_fingerprint(
                workflow_fingerprint,
                name="workflow_fingerprint",
            )
            plan = _validate_fingerprint(
                plan_fingerprint,
                name="plan_fingerprint",
            )
            return cls(default_checkpoint_root() / workflow / plan)

        if location.mode == "custom":
            return cls(Path(location.directory))

        normalized_outputs = tuple(
            RecoveryOutput.from_mapping(output)
            for output in outputs
        )
        matches = [
            output
            for output in normalized_outputs
            if output.node_id == location.anchor_node_id
        ]
        if len(matches) != 1:
            raise ValueError(
                "recoveryLocation.anchorNodeId must identify exactly one terminal "
                f"OUTPUT node; found {len(matches)} matches for "
                f"{location.anchor_node_id!r}."
            )
        if matches[0].path_input is None:
            raise ValueError(
                f"Terminal OUTPUT node {matches[0].node_id!r} does not declare "
                "OUTPUT_PATH_INPUT."
            )
        return cls(Path(f"{matches[0].path}.workflow"))

    @property
    def manifest_path(self) -> Path:
        return self.control_directory / MANIFEST_FILENAME

    @property
    def graph_path(self) -> Path:
        return self.control_directory / GRAPH_FILENAME

    @property
    def execution_config_path(self) -> Path:
        return self.control_directory / EXECUTION_CONFIG_FILENAME

    @property
    def checkpoint_path(self) -> Path:
        return self.control_directory / CHECKPOINT_FILENAME

    @property
    def legacy_checkpoint_path(self) -> Path:
        return self.control_directory / LEGACY_CHECKPOINT_FILENAME

    @property
    def lock_path(self) -> Path:
        return self.control_directory / ACTIVE_LOCK_FILENAME

    @property
    def lock_mutation_guard_path(self) -> Path:
        return self.control_directory / ACTIVE_LOCK_MUTATION_GUARD_FILENAME


def atomic_write_json(
    path: str | os.PathLike[str],
    payload: Any,
    *,
    overwrite: bool = True,
) -> Path:
    """Durably replace one JSON file without exposing a partially written file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and target.exists():
        raise FileExistsError(f"Immutable recovery file already exists: {target}")

    normalized = _canonical_value(payload)
    # Fingerprint directories are already long on Windows. Keep the same-dir
    # temporary name compact while retaining enough entropy for concurrent writers.
    temporary_path = target.with_name(f".{uuid.uuid4().hex[:12]}.tmp")
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                normalized,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if not overwrite and target.exists():
            raise FileExistsError(f"Immutable recovery file already exists: {target}")
        os.replace(temporary_path, target)
    finally:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
    return target


def write_immutable_json(path: str | os.PathLike[str], payload: Any) -> Path:
    return atomic_write_json(path, payload, overwrite=False)


def load_json_object(path: str | os.PathLike[str], *, label: str = "JSON file") -> dict[str, Any]:
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {label} {source}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} {source} must contain a JSON object.")
    return value


def write_graph_snapshot(layout: ExecutionLayout, graph: Mapping[str, Any]) -> Path:
    return write_immutable_json(layout.graph_path, graph)


def load_graph_snapshot(layout: ExecutionLayout) -> dict[str, Any]:
    return load_json_object(layout.graph_path, label="recovery graph snapshot")


def write_execution_config_snapshot(
    layout: ExecutionLayout,
    config: Mapping[str, Any] | ExecutionConfig,
) -> Path:
    return write_immutable_json(
        layout.execution_config_path,
        execution_config_to_dict(config),
    )


def load_execution_config_snapshot(layout: ExecutionLayout) -> ExecutionConfig:
    payload = load_json_object(
        layout.execution_config_path,
        label="recovery execution configuration",
    )
    return parse_execution_config(payload)


def write_recovery_manifest(
    layout: ExecutionLayout,
    manifest: RecoveryManifest,
) -> Path:
    return atomic_write_json(layout.manifest_path, manifest.to_dict())


def load_recovery_manifest(layout: ExecutionLayout) -> RecoveryManifest:
    payload = load_json_object(layout.manifest_path, label="recovery manifest")
    return RecoveryManifest.from_mapping(payload)


class RecoveryLockError(RuntimeError):
    pass


def read_active_lock(layout: ExecutionLayout) -> dict[str, Any] | None:
    if not layout.lock_path.exists():
        return None
    return load_json_object(layout.lock_path, label="active recovery lock")


def _normalized_hostname() -> str:
    return socket.gethostname().strip().casefold()


def _is_proven_local_filesystem(path: Path) -> bool:
    """Return true only for storage that is clearly local to this host."""

    try:
        resolved = path.expanduser().resolve()
        partitions = psutil.disk_partitions(all=True)
    except (OSError, RuntimeError):
        return False

    matching = []
    for partition in partitions:
        try:
            mountpoint = Path(partition.mountpoint).expanduser().resolve()
            if resolved == mountpoint or mountpoint in resolved.parents:
                matching.append((len(str(mountpoint)), partition))
        except (OSError, RuntimeError):
            continue
    if not matching:
        return False

    partition = max(matching, key=lambda item: item[0])[1]
    options = {
        option.strip().casefold()
        for option in str(partition.opts).split(",")
        if option.strip()
    }
    if os.name == "nt":
        return "fixed" in options and "remote" not in options

    local_filesystems = {
        "apfs",
        "btrfs",
        "ext2",
        "ext3",
        "ext4",
        "hfs",
        "hfsplus",
        "jfs",
        "overlay",
        "reiserfs",
        "tmpfs",
        "ufs",
        "xfs",
        "zfs",
    }
    return str(partition.fstype).strip().casefold() in local_filesystems


def _active_lock_owner_state(
    payload: Mapping[str, Any],
    *,
    lock_path: Path,
) -> Literal["alive", "stale", "unknown"]:
    """Classify an owner without guessing about foreign/shared hosts."""

    schema_version = payload.get("schemaVersion")
    if (
        isinstance(schema_version, bool)
        or schema_version not in (None, 1, ACTIVE_LOCK_SCHEMA_VERSION)
    ):
        return "unknown"
    for field_name in ("executionId", "lockId", "createdAt"):
        field_value = payload.get(field_name)
        if not isinstance(field_value, str) or not field_value.strip():
            return "unknown"

    pid = payload.get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return "unknown"

    hostname = payload.get("hostname")
    if hostname is None:
        # Locks written by the previous format can be reclaimed only when the
        # storage itself proves that a remote host cannot own the PID.
        if not _is_proven_local_filesystem(lock_path):
            return "unknown"
    elif (
        not isinstance(hostname, str)
        or not hostname.strip()
        or hostname.strip().casefold() != _normalized_hostname()
    ):
        return "unknown"

    expected_create_time = payload.get("processCreateTime")
    if schema_version == ACTIVE_LOCK_SCHEMA_VERSION and (
        hostname is None or expected_create_time is None
    ):
        return "unknown"
    if expected_create_time is not None and (
        isinstance(expected_create_time, bool)
        or not isinstance(expected_create_time, (int, float))
        or not math.isfinite(float(expected_create_time))
        or float(expected_create_time) <= 0
    ):
        return "unknown"

    try:
        process = psutil.Process(pid)
        if process.status() == psutil.STATUS_ZOMBIE:
            return "stale"
        actual_create_time = process.create_time()
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return "stale"
    except (psutil.AccessDenied, OSError):
        return "unknown"

    if (
        expected_create_time is not None
        and abs(actual_create_time - float(expected_create_time)) > 1e-3
    ):
        # The PID has been reused by a different process.
        return "stale"
    return "alive"


def _try_lock_active_handle(handle: BinaryIO) -> bool:
    """Take a non-blocking advisory lock outside the JSON payload."""

    handle.seek(ACTIVE_LOCK_GUARD_OFFSET)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock_active_handle(handle: BinaryIO) -> None:
    handle.seek(ACTIVE_LOCK_GUARD_OFFSET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _active_lock_mutation_guard(layout: ExecutionLayout) -> Iterator[None]:
    """Serialize active-lock create, reclaim, and release operations."""

    path = layout.lock_mutation_guard_path
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    locked = False
    deadline = time.monotonic() + 5.0
    try:
        while time.monotonic() < deadline:
            if _try_lock_active_handle(handle):
                locked = True
                break
            time.sleep(0.01)
        if not locked:
            raise RecoveryLockError(
                f"Timed out while coordinating recovery lock: {layout.control_directory}"
            )
        yield
    finally:
        if locked:
            try:
                _unlock_active_handle(handle)
            except OSError:
                pass
        handle.close()


def _cleanup_stale_active_lock_unlocked(layout: ExecutionLayout) -> bool:
    path = layout.lock_path
    if not path.exists() or path.is_symlink():
        return False
    try:
        payload = load_json_object(path, label="active recovery lock")
    except ValueError:
        return False
    if _active_lock_owner_state(payload, lock_path=path) != "stale":
        return False
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    return True


def cleanup_stale_active_lock(
    layout_or_directory: ExecutionLayout | str | os.PathLike[str],
) -> bool:
    """Remove an active lock only when its local owner is provably gone."""

    layout = (
        layout_or_directory
        if isinstance(layout_or_directory, ExecutionLayout)
        else ExecutionLayout(Path(layout_or_directory))
    )
    if not layout.lock_path.exists():
        return False
    with _active_lock_mutation_guard(layout):
        return _cleanup_stale_active_lock_unlocked(layout)


class ActiveExecutionLock:
    """Exclusive ownership marker for one canonical recovery directory."""

    def __init__(
        self,
        layout_or_directory: ExecutionLayout | str | os.PathLike[str],
        execution_id: str,
    ) -> None:
        if isinstance(layout_or_directory, ExecutionLayout):
            self.layout = layout_or_directory
        else:
            self.layout = ExecutionLayout(Path(layout_or_directory))
        self.execution_id = _non_empty_string(
            execution_id,
            name="execution_id",
        )
        self._lock_id = uuid.uuid4().hex
        self._acquired = False

    @property
    def path(self) -> Path:
        return self.layout.lock_path

    @property
    def acquired(self) -> bool:
        return self._acquired

    @property
    def locked(self) -> bool:
        return self._acquired

    def acquire(self) -> ActiveExecutionLock:
        if self._acquired:
            return self
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            process_create_time = psutil.Process(os.getpid()).create_time()
        except (psutil.Error, OSError) as exc:
            raise RecoveryLockError(
                "Cannot determine the recovery lock owner's process identity."
            ) from exc
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        payload = {
            "schemaVersion": ACTIVE_LOCK_SCHEMA_VERSION,
            "executionId": self.execution_id,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "processCreateTime": process_create_time,
            "lockId": self._lock_id,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }

        with _active_lock_mutation_guard(self.layout):
            if self.path.exists() and not _cleanup_stale_active_lock_unlocked(
                self.layout
            ):
                raise RecoveryLockError(
                    f"Recovery directory is already active: {self.path.parent}"
                )
            try:
                descriptor = os.open(self.path, flags, 0o600)
            except FileExistsError as exc:
                raise RecoveryLockError(
                    f"Recovery directory is already active: {self.path.parent}"
                ) from exc
            try:
                with os.fdopen(
                    descriptor,
                    "w",
                    encoding="utf-8",
                    newline="\n",
                ) as handle:
                    json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except BaseException:
                try:
                    self.path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
        self._acquired = True
        return self

    def release(self) -> None:
        if not self._acquired:
            return
        with _active_lock_mutation_guard(self.layout):
            if not self.path.exists():
                self._acquired = False
                return
            payload = load_json_object(self.path, label="active recovery lock")
            if payload.get("lockId") != self._lock_id:
                raise RecoveryLockError(
                    "Refusing to release a recovery lock owned by another "
                    f"execution: {self.path}"
                )
            self.path.unlink()
            self._acquired = False

    def __enter__(self) -> ActiveExecutionLock:
        return self.acquire()

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


@dataclass(frozen=True)
class CheckpointSummary:
    shape: tuple[int, ...]
    dtype: np.dtype[Any]
    completed_windows: int
    total_windows: int

    @property
    def remaining_windows(self) -> int:
        return self.total_windows - self.completed_windows


@dataclass(frozen=True)
class LegacyCheckpointSummary:
    next_window_index: int
    total_windows: int
    phase: Literal["windows_running", "finalizing"]


class WindowCheckpointStore:
    """Driver-side persistence for one recovery directory's Window bitmap."""

    def __init__(
        self,
        layout_or_directory: ExecutionLayout | str | os.PathLike[str],
    ) -> None:
        if isinstance(layout_or_directory, ExecutionLayout):
            self.layout = layout_or_directory
        else:
            self.layout = ExecutionLayout(Path(layout_or_directory))
        self._lock = threading.RLock()

    @property
    def checkpoint_path(self) -> Path:
        return self.layout.checkpoint_path

    @property
    def legacy_checkpoint_path(self) -> Path:
        return self.layout.legacy_checkpoint_path

    def _remove_legacy_files_unlocked(self) -> None:
        for path in (
            self.legacy_checkpoint_path,
            self.legacy_checkpoint_path.with_name(
                f"{self.legacy_checkpoint_path.name}.tmp"
            ),
        ):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                # The NumPy checkpoint is already durable and authoritative.
                # A later writable load/restart will retry this cleanup.
                pass

    def _inspect_legacy_unlocked(
        self,
        *,
        workflow_fingerprint: str,
        plan_fingerprint: str,
        output_shape: tuple[int, ...],
        window_shape: tuple[int, ...],
        expected_shape: tuple[int, ...],
        total_windows: int,
    ) -> LegacyCheckpointSummary | None:
        if self.checkpoint_path.exists():
            # Once the NumPy checkpoint exists, JSON is never an input source.
            return None
        path = self.legacy_checkpoint_path
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"Cannot read legacy Window checkpoint {path}: {exc}"
            ) from exc
        if not isinstance(state, Mapping):
            raise ValueError(
                f"Legacy Window checkpoint {path} must contain a JSON object."
            )

        schema_version = state.get("schema_version")
        if (
            isinstance(schema_version, bool)
            or schema_version != LEGACY_CHECKPOINT_SCHEMA_VERSION
        ):
            raise ValueError(
                "Unsupported legacy Window checkpoint schema_version="
                f"{schema_version!r}."
            )
        if state.get("workflow_fingerprint") != workflow_fingerprint:
            raise ValueError(
                "Legacy Window checkpoint workflow fingerprint does not match "
                "the submitted graph."
            )
        if state.get("plan_fingerprint") != plan_fingerprint:
            raise ValueError(
                "Legacy Window checkpoint plan fingerprint does not match the "
                "submitted Window plan."
            )

        legacy_output_shape = _shape_tuple(
            state.get("output_shape"),
            name="legacy output_shape",
            allow_zero=True,
        )
        legacy_window_shape = _shape_tuple(
            state.get("window_shape"),
            name="legacy window_shape",
            allow_zero=False,
        )
        if legacy_output_shape != tuple(output_shape):
            raise ValueError(
                "Legacy Window checkpoint output_shape does not match the "
                "current plan."
            )
        if legacy_window_shape != tuple(window_shape):
            raise ValueError(
                "Legacy Window checkpoint window_shape does not match the "
                "current plan."
            )

        legacy_total = state.get("total_windows")
        if (
            isinstance(legacy_total, bool)
            or not isinstance(legacy_total, int)
            or legacy_total != total_windows
        ):
            raise ValueError(
                "Legacy Window checkpoint total_windows does not match the "
                "current plan."
            )
        normalized_expected_shape = _shape_tuple(
            expected_shape,
            name="expected_shape",
            allow_zero=True,
        )
        if math.prod(normalized_expected_shape) != total_windows:
            raise ValueError(
                "Legacy Window checkpoint total_windows does not match the "
                "Window-grid shape."
            )

        next_window_index = state.get("next_window_index")
        if (
            isinstance(next_window_index, bool)
            or not isinstance(next_window_index, int)
        ):
            raise ValueError(
                "Legacy Window checkpoint next_window_index must be an integer."
            )
        if next_window_index < 0 or next_window_index > total_windows:
            raise ValueError(
                "Legacy Window checkpoint next_window_index is out of range."
            )

        phase = state.get("phase")
        expected_phase = (
            "finalizing"
            if next_window_index == total_windows
            else "windows_running"
        )
        if phase != expected_phase:
            raise ValueError(
                "Legacy Window checkpoint phase is inconsistent with "
                "next_window_index."
            )
        return LegacyCheckpointSummary(
            next_window_index=next_window_index,
            total_windows=total_windows,
            phase=phase,
        )

    def inspect_legacy(
        self,
        *,
        workflow_fingerprint: str,
        plan_fingerprint: str,
        output_shape: tuple[int, ...],
        window_shape: tuple[int, ...],
        expected_shape: tuple[int, ...],
        total_windows: int,
    ) -> LegacyCheckpointSummary | None:
        with self._lock:
            return self._inspect_legacy_unlocked(
                workflow_fingerprint=workflow_fingerprint,
                plan_fingerprint=plan_fingerprint,
                output_shape=output_shape,
                window_shape=window_shape,
                expected_shape=expected_shape,
                total_windows=total_windows,
            )

    def migrate_legacy(
        self,
        *,
        workflow_fingerprint: str,
        plan_fingerprint: str,
        output_shape: tuple[int, ...],
        window_shape: tuple[int, ...],
        expected_shape: tuple[int, ...],
        total_windows: int,
    ) -> np.ndarray | None:
        """Convert one validated continuous prefix to a C-order bitmap."""

        with self._lock:
            summary = self._inspect_legacy_unlocked(
                workflow_fingerprint=workflow_fingerprint,
                plan_fingerprint=plan_fingerprint,
                output_shape=output_shape,
                window_shape=window_shape,
                expected_shape=expected_shape,
                total_windows=total_windows,
            )
            if summary is None:
                return None
            completed_windows = np.zeros(expected_shape, dtype=np.uint8)
            completed_windows.reshape(-1, order="C")[
                :summary.next_window_index
            ] = 1
            self._save_unlocked(completed_windows)
            # JSON is removed only after the authoritative NumPy file is durable.
            self._remove_legacy_files_unlocked()
            return completed_windows

    @staticmethod
    def _validate_completed_windows(
        completed_windows: np.ndarray,
        *,
        path: Path | None = None,
        expected_shape: Sequence[Any] | None = None,
    ) -> None:
        location = f" {path}" if path is not None else ""
        if not isinstance(completed_windows, np.ndarray):
            raise ValueError(f"Window checkpoint{location} must contain a NumPy array.")
        if completed_windows.ndim == 0:
            raise ValueError(
                f"Window checkpoint{location} must have at least one dimension."
            )
        if completed_windows.dtype != np.dtype(np.uint8):
            raise ValueError(
                f"Window checkpoint{location} must have dtype uint8, "
                f"got {completed_windows.dtype}."
            )
        if expected_shape is not None:
            normalized_shape = _shape_tuple(
                expected_shape,
                name="expected_shape",
                allow_zero=True,
            )
            if completed_windows.shape != normalized_shape:
                raise ValueError(
                    f"Window checkpoint{location} has shape {completed_windows.shape}, "
                    f"expected {normalized_shape}."
                )
        if not np.all((completed_windows == 0) | (completed_windows == 1)):
            raise ValueError(
                f"Window checkpoint{location} must contain only completion values 0 and 1."
            )

    @staticmethod
    def _close_loaded(value: Any) -> None:
        memory_map = getattr(value, "_mmap", None)
        if memory_map is not None:
            memory_map.close()
            return
        close = getattr(value, "close", None)
        if callable(close):
            close()

    def _load_readonly(self) -> np.ndarray | None:
        path = self.checkpoint_path
        if not path.exists():
            return None
        try:
            loaded = np.load(
                path,
                mmap_mode="r",
                allow_pickle=False,
            )
        except (OSError, ValueError, EOFError, zipfile.BadZipFile) as exc:
            raise ValueError(f"Cannot read Window checkpoint {path}: {exc}") from exc
        if not isinstance(loaded, np.ndarray):
            self._close_loaded(loaded)
            raise ValueError(f"Window checkpoint {path} must contain a NumPy array.")
        return loaded

    def create(
        self,
        window_grid_shape: tuple[int, ...],
        *,
        overwrite: bool = False,
    ) -> np.ndarray:
        normalized_shape = _shape_tuple(
            window_grid_shape,
            name="window_grid_shape",
            allow_zero=True,
        )
        completed_windows = np.zeros(normalized_shape, dtype=np.uint8)
        if not normalized_shape:
            raise ValueError("window_grid_shape must have at least one dimension.")
        with self._lock:
            if self.checkpoint_path.exists() and not overwrite:
                raise FileExistsError(
                    f"Window checkpoint already exists: {self.checkpoint_path}"
                )
            self._save_unlocked(completed_windows)
        return completed_windows

    def remove_legacy_checkpoint(self) -> bool:
        """Remove legacy JSON only when a valid NumPy checkpoint exists."""

        with self._lock:
            loaded = self._load_readonly()
            if loaded is None:
                return False
            try:
                self._validate_completed_windows(
                    loaded,
                    path=self.checkpoint_path,
                )
            finally:
                self._close_loaded(loaded)
            existed = self.legacy_checkpoint_path.exists()
            self._remove_legacy_files_unlocked()
            return existed and not self.legacy_checkpoint_path.exists()

    def inspect(
        self,
        *,
        expected_shape: tuple[int, ...] | None = None,
    ) -> CheckpointSummary | None:
        loaded = self._load_readonly()
        if loaded is None:
            return None
        try:
            self._validate_completed_windows(
                loaded,
                path=self.checkpoint_path,
                expected_shape=expected_shape,
            )
            completed_count = int(np.count_nonzero(loaded))
            return CheckpointSummary(
                shape=tuple(int(size) for size in loaded.shape),
                dtype=loaded.dtype,
                completed_windows=completed_count,
                total_windows=int(loaded.size),
            )
        finally:
            self._close_loaded(loaded)

    def load_writable(
        self,
        *,
        expected_shape: tuple[int, ...],
    ) -> np.ndarray | None:
        loaded = self._load_readonly()
        if loaded is None:
            return None
        try:
            self._validate_completed_windows(
                loaded,
                path=self.checkpoint_path,
                expected_shape=expected_shape,
            )
            completed_windows = np.array(
                loaded,
                dtype=np.uint8,
                copy=True,
                order="C",
            )
        finally:
            self._close_loaded(loaded)
        with self._lock:
            self._remove_legacy_files_unlocked()
        return completed_windows

    def save(
        self,
        completed_windows: np.ndarray,
    ) -> Path:
        with self._lock:
            path = self._save_unlocked(completed_windows)
            self._remove_legacy_files_unlocked()
            return path

    def _save_unlocked(
        self,
        completed_windows: np.ndarray,
    ) -> Path:
        self._validate_completed_windows(completed_windows)
        path = self.checkpoint_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_name(f"{path.name}.tmp")

        try:
            with temporary_path.open("wb") as handle:
                np.save(
                    handle,
                    completed_windows,
                    allow_pickle=False,
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, path)
        finally:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
        return path

    def mark_completed(
        self,
        completed_windows: np.ndarray,
        coordinates: tuple[int, ...],
    ) -> bool:
        with self._lock:
            self._validate_completed_windows(completed_windows)
            normalized_coordinates = _shape_tuple(
                coordinates,
                name="coordinates",
                allow_zero=True,
            )
            if len(normalized_coordinates) != completed_windows.ndim:
                raise ValueError(
                    "Window coordinates rank must match checkpoint rank "
                    f"({len(normalized_coordinates)} != {completed_windows.ndim})."
                )
            for axis, (coordinate, axis_size) in enumerate(
                zip(normalized_coordinates, completed_windows.shape)
            ):
                if coordinate >= axis_size:
                    raise ValueError(
                        f"Window coordinate {coordinate} is outside axis {axis} "
                        f"with size {axis_size}."
                    )

            if completed_windows[normalized_coordinates] == 1:
                return False

            completed_windows[normalized_coordinates] = 1
            try:
                self._save_unlocked(completed_windows)
            except BaseException:
                # The Driver may safely retry after a failed durability commit.
                completed_windows[normalized_coordinates] = 0
                raise
            return True

    def delete(self) -> None:
        checkpoint_path = self.checkpoint_path
        checkpoint_path.unlink(missing_ok=True)
        try:
            checkpoint_path.with_name(
                f"{checkpoint_path.name}.tmp"
            ).unlink(missing_ok=True)
        except OSError:
            pass
