"""Small primitives for final-array Window execution and durable resume.

V1 recovery is intentionally at-least-once: a Window can finish its terminal
side effects and the process can fail before the atomic checkpoint advances.
Terminal side effects therefore need to be safe to retry.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import operator
import os
from pathlib import Path
from typing import Any, Iterator, Literal, Mapping, Sequence


CHECKPOINT_SCHEMA_VERSION = 1
WINDOW_GENERATOR_VERSION = 1
WINDOW_TRAVERSAL_ORDER = "lexicographic"


@dataclass(frozen=True)
class ExecutionConfig:
    mode: Literal["full_graph", "window"]
    window_shape: tuple[int, ...] | None = None


@dataclass(frozen=True)
class Window:
    index: int
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


def parse_execution_config(payload: Mapping[str, Any] | ExecutionConfig | None) -> ExecutionConfig:
    """Parse the small frontend execution payload, defaulting old clients to full graph."""
    if payload is None:
        return ExecutionConfig(mode="full_graph")
    if isinstance(payload, ExecutionConfig):
        if payload.mode == "full_graph":
            return ExecutionConfig(mode="full_graph")
        if payload.mode != "window":
            raise ValueError("ExecutionConfig.mode must be 'full_graph' or 'window'.")
        if payload.window_shape is None:
            raise ValueError("Window Execution requires window_shape.")
        return ExecutionConfig(
            mode="window",
            window_shape=_shape_tuple(
                payload.window_shape,
                name="window_shape",
                allow_zero=False,
            ),
        )
    if not isinstance(payload, Mapping):
        raise ValueError("executionConfig must be an object.")

    mode = payload.get("mode", "full_graph")
    if mode == "full_graph":
        return ExecutionConfig(mode="full_graph")
    if mode != "window":
        raise ValueError("executionConfig.mode must be 'full_graph' or 'window'.")

    raw_shape = payload.get("windowShape", payload.get("window_shape"))
    if raw_shape is None:
        raise ValueError("Window Execution requires executionConfig.windowShape.")
    window_shape = _shape_tuple(raw_shape, name="windowShape", allow_zero=False)
    return ExecutionConfig(mode="window", window_shape=window_shape)


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
        return Window(index=int(normalized_index), starts=starts, stops=stops)

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


class WindowCheckpointStore:
    def __init__(self, root: str | os.PathLike[str] | None = None) -> None:
        self.root = Path(root).expanduser().resolve() if root is not None else default_checkpoint_root()

    def state_path(self, workflow_fingerprint: str, plan_fingerprint: str) -> Path:
        return self.root / workflow_fingerprint / plan_fingerprint / "resume_state.json"

    def load(
        self,
        workflow_fingerprint: str,
        plan_fingerprint: str,
    ) -> dict[str, Any] | None:
        path = self.state_path(workflow_fingerprint, plan_fingerprint)
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read Window checkpoint {path}: {exc}") from exc
        if not isinstance(state, dict):
            raise ValueError(f"Window checkpoint {path} must contain a JSON object.")
        if state.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported Window checkpoint schema_version={state.get('schema_version')!r}."
            )
        if (
            state.get("workflow_fingerprint") != workflow_fingerprint
            or state.get("plan_fingerprint") != plan_fingerprint
        ):
            return None
        return state

    def save(self, state: Mapping[str, Any]) -> Path:
        workflow_fingerprint = str(state["workflow_fingerprint"])
        plan_fingerprint = str(state["plan_fingerprint"])
        path = self.state_path(workflow_fingerprint, plan_fingerprint)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_name(f"{path.name}.tmp")

        try:
            with temporary_path.open("w", encoding="utf-8") as handle:
                json.dump(
                    dict(state),
                    handle,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, path)
        finally:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
        return path

    def delete(self, workflow_fingerprint: str, plan_fingerprint: str) -> None:
        state_path = self.state_path(
            workflow_fingerprint,
            plan_fingerprint,
        )
        # Delete the recovery identity first.  Avoid recursive removal so an
        # unrelated file can never be partially deleted if cleanup fails.
        state_path.unlink(missing_ok=True)
        try:
            state_path.with_name(f"{state_path.name}.tmp").unlink(missing_ok=True)
        except OSError:
            pass

        plan_directory = state_path.parent
        try:
            plan_directory.rmdir()
        except (FileNotFoundError, OSError):
            pass
        workflow_directory = plan_directory.parent
        try:
            workflow_directory.rmdir()
        except (FileNotFoundError, OSError):
            pass
