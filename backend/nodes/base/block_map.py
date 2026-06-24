from __future__ import annotations

from dataclasses import dataclass
import inspect
import logging
import os
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import numpy as np

from core.invocation_builder import declared_type_and_meta, get_node_input_defs, validate_dask_array_input
from core.node_invocation import NodeInvocation, NodeRuntime
from core.type_system import dtype_name_to_numpy, is_dask_array_type, parse_port_type


logger = logging.getLogger("WorkFlow.DaskArrayMap")


RUNTIME_KEYS = {"_node_id", "_execution_id", "_params", "_runtime", "_invocation"}


@dataclass(frozen=True)
class ArrayMeta:
    name: str
    shape: tuple
    chunks: tuple
    dtype: np.dtype
    axes: tuple[str, ...] | None = None
    voxel_size: tuple[float, ...] | None = None
    source: dict | None = None


@dataclass(frozen=True)
class MapBlocksOutputSpec:
    dtype: np.dtype
    chunks: Any = None
    drop_axis: tuple[int, ...] | None = None
    new_axis: tuple[int, ...] | None = None
    enforce_ndim: bool = True
    meta: Any = None
    same_as_primary: bool = False
    chunks_source: Any = None


@dataclass(frozen=True)
class OverlapSpec:
    depth: Any
    boundary: Any = "none"
    trim: bool = True
    align_arrays: bool = True
    allow_rechunk: bool = True


@dataclass(frozen=True)
class BlockContext:
    node_id: Optional[str]
    execution_id: Optional[str]
    device_hint: str
    block_info: dict
    input_names: tuple[str, ...]
    block_locations: tuple
    array_locations: tuple
    chunk_origins: tuple
    output_block_info: dict | None
    input_blocks: Mapping[str, np.ndarray]
    input_shapes: Mapping[str, tuple]
    input_dtypes: Mapping[str, np.dtype]
    primary_input_name: str
    primary_block_shape: tuple
    output_chunk_shape: tuple | None = None
    resources: Any = None

    @property
    def device(self) -> str:
        return self.device_hint

    @property
    def block_shape(self) -> tuple:
        return self.primary_block_shape

    @property
    def input_dtype(self) -> np.dtype:
        return self.input_dtypes[self.primary_input_name]

    def cached(
        self,
        namespace: str,
        key: Any,
        factory: Callable[[], Any],
        dispose: Callable[[Any], None] | None = None,
        clear_cuda: bool = False,
    ) -> Any:
        from core.worker_cache import get_or_create_worker_cached

        return get_or_create_worker_cached(
            namespace=namespace,
            key=key,
            factory=factory,
            dispose=dispose,
            clear_cuda=clear_cuda,
        )

    def model(
        self,
        provider: str,
        name: str,
        factory: Callable[[str, str], Any],
        *,
        dispose: Callable[[Any], None] | None = None,
        clear_cuda: bool = True,
        validate: Callable[[str, str], None] | None = None,
    ) -> Any:
        from core.model_registry import resolve_model_path
        from core.worker_cache import get_or_create_worker_cached

        normalized_device = self.device_hint or "cpu"
        resolved = resolve_model_path(provider, name)
        resolved_name = resolved if resolved else name
        cache_key = (provider, resolved_name, normalized_device)

        def _factory_wrapper():
            if validate is not None:
                validate(resolved_name, name)
            return factory(resolved_name, normalized_device)

        return get_or_create_worker_cached(
            namespace=f"model:{provider}",
            key=cache_key,
            factory=_factory_wrapper,
            dispose=dispose,
            clear_cuda=clear_cuda,
        )


class BlockResources:
    def __init__(self, owner: "BaseDaskArrayMapNode", ctx: BlockContext, specs: dict | None = None):
        self._owner = owner
        self._ctx = ctx
        self._specs = dict(specs or {})

    @property
    def specs(self) -> dict:
        return dict(self._specs)

    def get(self, name, default=None):
        return self._specs.get(name, default)

    def keys(self):
        return self._specs.keys()

    def items(self):
        return self._specs.items()

    def __contains__(self, name):
        return name in self._specs

    def release_all(self) -> None:
        return None


class ChunkPlanner:
    """Reusable lazy rechunk and chunk-alignment helper for Dask Array nodes."""

    @staticmethod
    def normalize_axes(axes: Any) -> tuple[str, ...] | None:
        if axes is None:
            return None
        if isinstance(axes, str):
            axes = tuple(x.strip() for x in axes.split(",") if x.strip())
        elif isinstance(axes, (list, tuple)):
            axes = tuple(str(x).strip() for x in axes if str(x).strip())
        else:
            raise ValueError(f"Unsupported axes metadata {axes!r}.")
        return axes or None

    @classmethod
    def parse_explicit_chunks(cls, chunks: Any) -> Any:
        if chunks is None:
            raise ValueError("Explicit chunk spec is missing.")
        if isinstance(chunks, str):
            raw = chunks.strip()
            if not raw:
                raise ValueError("Explicit chunk string is empty.")
            if raw.lower() == "auto":
                raise ValueError("Auto chunks are disabled. Please provide explicit chunks.")
            return tuple(cls._parse_chunk_int(part) for part in raw.split(",") if part.strip())
        if isinstance(chunks, (tuple, list)):
            return tuple(chunks)
        raise ValueError(f"Unsupported explicit chunk spec {chunks!r}.")

    @classmethod
    def parse_axis_chunks(cls, axis_chunks: Any) -> dict[Any, int]:
        if axis_chunks is None:
            raise ValueError("Axis chunk spec is missing.")
        if isinstance(axis_chunks, str):
            result: dict[Any, int] = {}
            raw = axis_chunks.strip()
            if not raw:
                raise ValueError("Axis chunk string is empty.")
            for item in raw.split(","):
                if not item.strip():
                    continue
                if ":" not in item:
                    raise ValueError(f"Invalid axis chunk item {item!r}; expected 'axis:size'.")
                axis, size = item.split(":", 1)
                axis = axis.strip()
                if not axis:
                    raise ValueError(f"Invalid axis chunk item {item!r}; axis is empty.")
                try:
                    key: Any = int(axis)
                except ValueError:
                    key = axis
                result[key] = cls._parse_chunk_int(size)
            return result
        if isinstance(axis_chunks, dict):
            return {k: cls._parse_chunk_int(v) for k, v in axis_chunks.items()}
        raise ValueError(f"Unsupported axis chunk spec {axis_chunks!r}.")

    @staticmethod
    def _parse_chunk_int(value: Any) -> int:
        try:
            parsed = int(value)
        except Exception as exc:
            raise ValueError(f"Chunk size {value!r} is not an integer.") from exc
        if parsed == 0 or parsed < -1:
            raise ValueError(f"Chunk size must be positive or -1, got {parsed}.")
        return parsed

    @classmethod
    def rechunk_array(
        cls,
        arr: Any,
        spec: Any,
        *,
        input_name: str = "dask_arr",
        all_inputs: Mapping[str, Any] | None = None,
        axes: tuple[str, ...] | None = None,
    ) -> Any:
        if spec is None:
            return arr
        if isinstance(spec, str):
            return arr.rechunk(cls.parse_explicit_chunks(spec))
        if isinstance(spec, (tuple, list)):
            return arr.rechunk(cls._expand_negative_chunks(arr, tuple(spec)))
        if not isinstance(spec, dict):
            raise ValueError(f"Unsupported chunk spec for input '{input_name}': {spec!r}.")

        if "explicit" in spec:
            chunks = cls.parse_explicit_chunks(spec["explicit"])
            if isinstance(chunks, tuple):
                chunks = cls._expand_negative_chunks(arr, chunks)
            return arr.rechunk(chunks)
        if "axis_index" in spec:
            chunks = cls.parse_axis_chunks(spec["axis_index"])
            return arr.rechunk(cls._expand_negative_axis_chunks(arr, chunks))
        if "axis_name" in spec:
            axis_chunks = cls.parse_axis_chunks(spec["axis_name"])
            return arr.rechunk(cls._axis_name_chunks_to_indices(arr, axis_chunks, axes, input_name))
        if "match" in spec:
            if all_inputs is None:
                raise ValueError(f"Chunk spec for '{input_name}' uses match but no inputs were provided.")
            reference_name = spec["match"]
            if reference_name not in all_inputs:
                raise ValueError(
                    f"Chunk spec for '{input_name}' references missing match input '{reference_name}'."
                )
            reference = all_inputs[reference_name]
            cls.validate_same_shape(input_name, arr, reference_name, reference)
            return arr.rechunk(reference.chunks)

        raise ValueError(f"Unsupported chunk spec for input '{input_name}': {spec!r}.")

    @classmethod
    def apply_input_chunks(
        cls,
        array_inputs: Dict[str, Any],
        input_chunks: Mapping[str, Any] | None,
        *,
        axes_by_name: Mapping[str, tuple[str, ...] | None] | None = None,
    ) -> Dict[str, Any]:
        planned = dict(array_inputs)
        if not input_chunks:
            return planned
        axes_by_name = axes_by_name or {}
        for name, spec in input_chunks.items():
            if name not in planned:
                raise ValueError(f"INPUT_CHUNKS references missing Dask input '{name}'.")
            planned[name] = cls.rechunk_array(
                planned[name],
                spec,
                input_name=name,
                all_inputs=planned,
                axes=axes_by_name.get(name),
            )
        return planned

    @classmethod
    def apply_policy(
        cls,
        array_inputs: Dict[str, Any],
        *,
        mapped_names: list[str],
        primary_name: str,
        policy: Mapping[str, Any] | None,
    ) -> Dict[str, Any]:
        planned = dict(array_inputs)
        mode = str((policy or {}).get("mode", "none")).lower()
        if mode in ("none", "", "relaxed"):
            return planned
        if primary_name not in planned:
            raise ValueError(f"PRIMARY_INPUT '{primary_name}' is missing from Dask inputs.")
        primary = planned[primary_name]

        if mode == "strict":
            for name in mapped_names:
                cls.validate_strict(name, planned[name], primary_name, primary)
            return planned

        if mode == "rechunk_to_primary":
            for name in mapped_names:
                if name == primary_name:
                    continue
                cls.validate_same_shape(name, planned[name], primary_name, primary)
                planned[name] = planned[name].rechunk(primary.chunks)
            return planned

        raise ValueError(f"Unsupported CHUNK_POLICY mode {mode!r}.")

    @staticmethod
    def validate_same_shape(name: str, arr: Any, expected_name: str, expected: Any) -> None:
        if tuple(arr.shape) != tuple(expected.shape):
            raise ValueError(
                f"Shape mismatch while matching chunks for input '{name}': "
                f"shape={tuple(arr.shape)} must match '{expected_name}' shape={tuple(expected.shape)}."
            )

    @staticmethod
    def validate_strict(name: str, arr: Any, expected_name: str, expected: Any) -> None:
        failures = []
        if tuple(arr.shape) != tuple(expected.shape):
            failures.append(f"shape={tuple(arr.shape)} expected={tuple(expected.shape)}")
        if tuple(arr.chunks) != tuple(expected.chunks):
            failures.append(f"chunks={arr.chunks} expected={expected.chunks}")
        if tuple(arr.numblocks) != tuple(expected.numblocks):
            failures.append(f"numblocks={tuple(arr.numblocks)} expected={tuple(expected.numblocks)}")
        if failures:
            raise ValueError(
                f"Strict chunk policy failed for input '{name}' against '{expected_name}': "
                + "; ".join(failures)
            )

    @staticmethod
    def _expand_negative_chunks(arr: Any, chunks: tuple) -> tuple:
        if len(chunks) != int(arr.ndim):
            raise ValueError(f"Explicit chunks rank {len(chunks)} does not match array ndim {arr.ndim}.")
        expanded = []
        for axis, item in enumerate(chunks):
            if isinstance(item, (tuple, list)):
                expanded.append(tuple(int(x) for x in item))
                continue
            size = ChunkPlanner._parse_chunk_int(item)
            expanded.append(int(arr.shape[axis]) if size == -1 else size)
        return tuple(expanded)

    @staticmethod
    def _expand_negative_axis_chunks(arr: Any, chunks: Mapping[Any, int]) -> dict[int, int]:
        expanded: dict[int, int] = {}
        for raw_axis, raw_size in chunks.items():
            try:
                axis = int(raw_axis)
            except Exception as exc:
                raise ValueError(f"Axis-index chunk key {raw_axis!r} is not an integer.") from exc
            if axis < 0:
                axis = int(arr.ndim) + axis
            if axis < 0 or axis >= int(arr.ndim):
                raise ValueError(f"Axis index {raw_axis!r} is out of bounds for ndim {arr.ndim}.")
            size = ChunkPlanner._parse_chunk_int(raw_size)
            expanded[axis] = int(arr.shape[axis]) if size == -1 else size
        return expanded

    @staticmethod
    def _axis_name_chunks_to_indices(
        arr: Any,
        axis_chunks: Mapping[Any, int],
        axes: tuple[str, ...] | None,
        input_name: str,
    ) -> dict[int, int]:
        return ChunkPlanner.axis_name_mapping_to_indices(
            arr=arr,
            mapping=axis_chunks,
            axes=axes,
            input_name=input_name,
            value_name="chunk",
            expand_minus_one=True,
        )

    @staticmethod
    def axis_name_mapping_to_indices(
        *,
        arr: Any,
        mapping: Mapping[Any, Any],
        axes: tuple[str, ...] | None,
        input_name: str,
        value_name: str,
        expand_minus_one: bool = False,
    ) -> dict[int, int]:
        if not axes:
            raise ValueError(
                f"Axis-name {value_name} requested for input '{input_name}', but no axes metadata was provided."
            )
        if len(axes) != int(arr.ndim):
            raise ValueError(
                f"Axes metadata for input '{input_name}' has length {len(axes)}, "
                f"but array ndim is {arr.ndim}."
            )
        axis_lookup = {axis.lower(): idx for idx, axis in enumerate(axes)}
        result: dict[int, int] = {}
        for raw_name, raw_value in mapping.items():
            axis_name = str(raw_name).strip().lower()
            if axis_name not in axis_lookup:
                raise ValueError(f"Axis name {raw_name!r} is not present in axes {axes} for input '{input_name}'.")
            idx = axis_lookup[axis_name]
            value = ChunkPlanner._parse_chunk_int(raw_value)
            if expand_minus_one and value == -1:
                value = int(arr.shape[idx])
            elif value == -1:
                raise ValueError(f"{value_name.title()} size for axis {raw_name!r} cannot be -1.")
            result[idx] = value
        return result

    @staticmethod
    def axis_index_mapping_to_indices(
        *,
        arr: Any,
        mapping: Mapping[Any, Any],
        value_name: str,
    ) -> dict[int, int]:
        result: dict[int, int] = {}
        for raw_axis, raw_value in mapping.items():
            try:
                axis = int(raw_axis)
            except Exception as exc:
                raise ValueError(f"Axis-index {value_name} key {raw_axis!r} is not an integer.") from exc
            if axis < 0:
                axis = int(arr.ndim) + axis
            if axis < 0 or axis >= int(arr.ndim):
                raise ValueError(f"Axis index {raw_axis!r} is out of bounds for ndim {arr.ndim}.")
            value = ChunkPlanner._parse_chunk_int(raw_value)
            if value == -1:
                raise ValueError(f"{value_name.title()} size for axis {raw_axis!r} cannot be -1.")
            result[axis] = value
        return result


class BaseNode:
    FUNCTION = "execute"
    OUTPUT_NODE = False
    RETURN_TYPES: tuple = ()
    RETURN_NAMES: tuple = ()

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {}, "optional": {}}

    def preprocess(self, dask_arr=None, params: dict | None = None, runtime: dict | None = None) -> dict[str, Any] | None:
        return None

    def postprocess(self, outputs=None, state=None, runtime: dict | None = None, **kwargs):
        return outputs

    def cleanup(self):
        return None

    def get_invocation(self, kwargs: Mapping[str, Any]) -> NodeInvocation:
        invocation = kwargs.get("_invocation")
        if isinstance(invocation, NodeInvocation):
            return invocation
        return self._fallback_invocation(kwargs)

    def _fallback_invocation(self, raw_inputs: Mapping[str, Any]) -> NodeInvocation:
        input_defs = self._input_definitions()
        inputs: dict[str, Any] = {}
        provided_params = raw_inputs.get("_params")
        provided_params = provided_params if isinstance(provided_params, Mapping) else {}

        for section in ("required", "optional"):
            for name, config in input_defs.get(section, {}).items():
                declared, _ = self._declared_type_and_meta(config)
                if name in RUNTIME_KEYS:
                    continue
                value = raw_inputs.get(name)
                if isinstance(declared, str) and is_dask_array_type(declared):
                    if value is not None:
                        validate_dask_array_input(name, value)
                        inputs[name] = value
                    continue
                if name in raw_inputs:
                    inputs[name] = value
                elif name in provided_params:
                    inputs[name] = provided_params[name]

        runtime = raw_inputs.get("_runtime")
        if not isinstance(runtime, NodeRuntime):
            runtime = NodeRuntime(
                node_id=raw_inputs.get("_node_id"),
                execution_id=raw_inputs.get("_execution_id"),
            )
        undeclared_inputs = {
            key: value
            for key, value in raw_inputs.items()
            if key not in RUNTIME_KEYS and not key.startswith("_")
        }
        inputs = {**undeclared_inputs, **inputs}
        return NodeInvocation(
            runtime=runtime,
            input_defs=input_defs,
            inputs=inputs,
        )

    def _input_definitions(self) -> dict:
        return get_node_input_defs(type(self))

    @classmethod
    def _declared_type_and_meta(cls, config: Any) -> tuple[Any, dict]:
        return declared_type_and_meta(config)

class BaseDaskNode(BaseNode):
    DASK_API = None

    def make_task_name(self, runtime: NodeRuntime | Mapping[str, Any] | None) -> str:
        node_id = None
        if isinstance(runtime, NodeRuntime):
            node_id = runtime.node_id
        elif isinstance(runtime, Mapping):
            node_id = runtime.get("node_id") or runtime.get("_node_id")
        node_cls_name = type(self).__name__
        return f"{node_cls_name}_{node_id}" if node_id else node_cls_name

    @staticmethod
    def is_dask_collection(value: Any) -> bool:
        try:
            from dask.base import is_dask_collection

            return is_dask_collection(value)
        except Exception:
            return hasattr(value, "__dask_graph__")

    def assert_lazy_collection(self, value: Any) -> None:
        if not self.is_dask_collection(value):
            raise TypeError(f"{type(self).__name__} must return a lazy Dask collection, got {type(value).__name__}.")


@dataclass(frozen=True)
class BlockwiseInputPlan:
    ordered_names: list[str]
    primary_name: str
    array_inputs: dict[str, Any]
    params: dict[str, Any]
    ordered_arrays: list[Any]
    axes_by_name: dict[str, tuple[str, ...] | None]


def select_inputs_by_container(
    inputs: Mapping[str, Any],
    input_defs: Mapping[str, Any],
    container_name: str,
) -> dict[str, Any]:
    selected: dict[str, Any] = {}
    for section in ("required", "optional"):
        for name, config in input_defs.get(section, {}).items():
            if name not in inputs or inputs[name] is None:
                continue
            declared, _ = declared_type_and_meta(config)
            if not isinstance(declared, str):
                continue
            if parse_port_type(declared).container == container_name:
                selected[name] = inputs[name]
    return selected


def split_dask_array_inputs(invocation: NodeInvocation) -> tuple[dict[str, Any], dict[str, Any]]:
    array_inputs = select_inputs_by_container(invocation.inputs, invocation.input_defs, "DASK_ARRAY")
    dask_input_names = set()
    for section in ("required", "optional"):
        for name, config in invocation.input_defs.get(section, {}).items():
            declared, _ = declared_type_and_meta(config)
            if isinstance(declared, str) and parse_port_type(declared).container == "DASK_ARRAY":
                dask_input_names.add(name)
    params = {
        name: value
        for name, value in invocation.inputs.items()
        if name not in dask_input_names
    }
    return array_inputs, params


# Compatibility alias for older internal imports. This helper is not specific
# to Dask blockwise; new code should use split_dask_array_inputs.
split_blockwise_inputs = split_dask_array_inputs


class BlockwiseInputPlanner:
    def build(self, node: "BaseDaskArrayMapNode", invocation: NodeInvocation) -> BlockwiseInputPlan:
        raw_array_inputs, params = split_dask_array_inputs(invocation)
        if not raw_array_inputs:
            raise ValueError(f"{type(node).__name__} requires at least one Dask Array input.")
        ordered_names = self.resolve_map_inputs(node, raw_array_inputs, invocation.input_defs)
        primary_name = self.resolve_primary_input(node, raw_array_inputs, ordered_names)
        axes_by_name = self.resolve_axes_by_name(node, ordered_names, invocation)
        array_inputs = ChunkPlanner.apply_input_chunks(
            dict(raw_array_inputs),
            getattr(type(node), "INPUT_CHUNKS", None),
            axes_by_name=axes_by_name,
        )
        array_inputs = ChunkPlanner.apply_policy(
            array_inputs,
            mapped_names=ordered_names,
            primary_name=primary_name,
            policy=getattr(type(node), "CHUNK_POLICY", None),
        )
        return BlockwiseInputPlan(
            ordered_names=ordered_names,
            primary_name=primary_name,
            array_inputs=array_inputs,
            params=params,
            ordered_arrays=[array_inputs[name] for name in ordered_names],
            axes_by_name=axes_by_name,
        )

    def resolve_map_inputs(
        self,
        node: "BaseDaskArrayMapNode",
        array_inputs: Mapping[str, Any],
        input_defs: Mapping[str, Any],
    ) -> list[str]:
        declared = getattr(type(node), "MAP_INPUTS", None)
        if declared:
            names = list(declared)
            for name in names:
                if name not in array_inputs:
                    raise ValueError(f"MAP_INPUTS references missing Dask input '{name}'.")
            return names

        names: list[str] = []
        for section in ("required", "optional"):
            for name, config in input_defs.get(section, {}).items():
                declared_type, _ = declared_type_and_meta(config)
                if isinstance(declared_type, str) and is_dask_array_type(declared_type) and name in array_inputs:
                    names.append(name)
        if not names:
            raise ValueError(f"{type(node).__name__} could not infer any mapped Dask Array inputs.")
        return names

    def resolve_primary_input(
        self,
        node: "BaseDaskArrayMapNode",
        array_inputs: Mapping[str, Any],
        ordered_names: list[str],
    ) -> str:
        primary = getattr(type(node), "PRIMARY_INPUT", None) or ordered_names[0]
        if primary not in array_inputs:
            raise ValueError(f"PRIMARY_INPUT references missing Dask input '{primary}'.")
        return primary

    def resolve_axes_by_name(
        self,
        node: "BaseDaskArrayMapNode",
        ordered_names: list[str],
        invocation: NodeInvocation,
    ) -> dict[str, tuple[str, ...] | None]:
        axes_decl = getattr(type(node), "ARRAY_AXES", None)
        axes_by_ndim_decl = getattr(type(node), "ARRAY_AXES_BY_NDIM", None)
        existing_axes = getattr(node, "_axes_by_name", {}) or {}
        result: dict[str, tuple[str, ...] | None] = {}

        for name in ordered_names:
            axes = ChunkPlanner.normalize_axes(existing_axes.get(name))
            if axes is None:
                axes_resolver = getattr(node, f"_axes_for_{name}", None)
                if callable(axes_resolver):
                    axes = ChunkPlanner.normalize_axes(axes_resolver(invocation.inputs[name]))
            arr = invocation.inputs.get(name)
            ndim = int(arr.ndim) if arr is not None and hasattr(arr, "ndim") else None
            ndim_mapping = axes_by_ndim_decl
            if isinstance(axes_by_ndim_decl, Mapping) and name in axes_by_ndim_decl:
                ndim_mapping = axes_by_ndim_decl.get(name)
            if axes is None and isinstance(ndim_mapping, Mapping) and ndim is not None:
                axes = ChunkPlanner.normalize_axes(ndim_mapping.get(ndim))
            result[name] = axes

        if isinstance(axes_decl, dict):
            for name in ordered_names:
                declared_axes = ChunkPlanner.normalize_axes(axes_decl.get(name))
                if declared_axes is not None:
                    result[name] = declared_axes
        elif axes_decl is not None:
            axes = ChunkPlanner.normalize_axes(axes_decl)
            for name in ordered_names:
                result[name] = axes

        for name in ordered_names:
            arr = invocation.inputs.get(name)
            ndim = int(arr.ndim) if arr is not None and hasattr(arr, "ndim") else None
            meta = (
                invocation.inputs.get(f"{name}_metadata")
                or invocation.inputs.get(f"{name}_meta")
                or (invocation.inputs.get("metadata") if len(ordered_names) == 1 else None)
            )
            axes = None
            if isinstance(meta, ArrayMeta):
                axes = meta.axes
            elif isinstance(meta, dict):
                axes = meta.get("axes")
            if axes is not None:
                result[name] = ChunkPlanner.normalize_axes(axes)

            explicit_axes = invocation.inputs.get(f"{name}_axes")
            if explicit_axes in (None, "") and len(ordered_names) == 1:
                explicit_axes = invocation.inputs.get("axes")
            if explicit_axes not in (None, ""):
                result[name] = ChunkPlanner.normalize_axes(explicit_axes)

            resolved_axes = result[name]
            if resolved_axes is not None and ndim is not None and len(resolved_axes) != ndim:
                raise ValueError(
                    f"Axes for input '{name}' has length {len(resolved_axes)}, "
                    f"but array ndim is {ndim}."
                )
        return result


class MapBlocksOutputSpecResolver:
    def resolve(
        self,
        node: "BaseDaskArrayMapNode",
        array_inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        primary_name: str,
    ) -> MapBlocksOutputSpec:
        override = getattr(node, "infer_output_spec", None)
        if override is not None:
            inferred = override(array_inputs, params, primary_name)
            if inferred is not None:
                return self.coerce(node, inferred, array_inputs, params, primary_name)
        if "OUTPUT_SPEC" in type(node).__dict__:
            raise ValueError(
                f"{type(node).__name__} uses deprecated OUTPUT_SPEC. "
                "Rename it to MAP_BLOCKS_OUTPUT_SPEC."
            )
        return self.coerce(
            node,
            getattr(type(node), "MAP_BLOCKS_OUTPUT_SPEC", None) or {},
            array_inputs,
            params,
            primary_name,
        )

    def coerce(
        self,
        node: "BaseDaskArrayMapNode",
        spec: MapBlocksOutputSpec | Mapping[str, Any],
        array_inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        primary_name: str,
    ) -> MapBlocksOutputSpec:
        if isinstance(spec, MapBlocksOutputSpec):
            if spec.same_as_primary:
                raise ValueError(
                    "MapBlocksOutputSpec.same_as_primary is internal and is derived from "
                    "chunks, new_axis, and drop_axis. Do not set it manually."
                )
            primary = array_inputs[primary_name]
            dtype = np.dtype(spec.dtype)
            chunks_spec = "same_as_primary" if spec.chunks is None else spec.chunks
            drop_axis = self.resolve_drop_axis(
                spec.drop_axis,
                node=node,
                primary=primary,
                primary_name=primary_name,
            )
            new_axis = self.resolve_new_axis(
                spec.new_axis,
                node=node,
                primary=primary,
                primary_name=primary_name,
            )
            chunks = self.resolve_chunks(
                chunks_spec,
                array_inputs,
                params,
                primary_name,
                drop_axis=drop_axis,
            )
            return MapBlocksOutputSpec(
                dtype=dtype,
                chunks=chunks,
                drop_axis=drop_axis,
                new_axis=new_axis,
                enforce_ndim=bool(spec.enforce_ndim),
                meta=spec.meta if spec.meta is not None else np.array((), dtype=dtype),
                same_as_primary=self.derive_same_as_primary(chunks_spec, drop_axis, new_axis),
                chunks_source=chunks_spec,
            )
        if not isinstance(spec, Mapping):
            raise ValueError(f"Unsupported output spec {spec!r}.")
        if "same_as_primary" in spec:
            raise ValueError(
                "MAP_BLOCKS_OUTPUT_SPEC.same_as_primary is internal and is derived from "
                "chunks, new_axis, and drop_axis. Do not set it manually."
            )
        primary = array_inputs[primary_name]
        dtype = self.resolve_dtype(
            node,
            spec.get("dtype", "same"),
            primary.dtype,
            params,
            array_inputs=array_inputs,
            primary_name=primary_name,
        )
        chunks_spec = spec.get("chunks", "same_as_primary")
        drop_axis = self.resolve_drop_axis(
            spec.get("drop_axis"),
            node=node,
            primary=primary,
            primary_name=primary_name,
        )
        new_axis = self.resolve_new_axis(
            spec.get("new_axis"),
            node=node,
            primary=primary,
            primary_name=primary_name,
        )
        chunks = self.resolve_chunks(
            chunks_spec,
            array_inputs,
            params,
            primary_name,
            drop_axis=drop_axis,
        )
        meta = spec.get("meta")
        if meta is None:
            meta = np.array((), dtype=dtype)
        return MapBlocksOutputSpec(
            dtype=np.dtype(dtype),
            chunks=chunks,
            drop_axis=drop_axis,
            new_axis=new_axis,
            enforce_ndim=bool(spec.get("enforce_ndim", True)),
            meta=meta,
            same_as_primary=self.derive_same_as_primary(chunks_spec, drop_axis, new_axis),
            chunks_source=chunks_spec,
        )

    @staticmethod
    def derive_same_as_primary(
        chunks_spec: Any,
        drop_axis: tuple[int, ...] | None,
        new_axis: tuple[int, ...] | None,
    ) -> bool:
        return chunks_spec in (None, "same_as_primary") and drop_axis is None and new_axis is None

    def resolve_dtype(
        self,
        node: "BaseDaskArrayMapNode",
        dtype_spec: Any,
        primary_dtype: Any,
        params: Mapping[str, Any],
        *,
        array_inputs: Mapping[str, Any] | None = None,
        primary_name: str | None = None,
    ) -> np.dtype:
        if callable(dtype_spec):
            dtype_spec = dtype_spec(
                array_inputs=array_inputs or {},
                params=params,
                primary_name=primary_name,
            )
        elif isinstance(dtype_spec, Mapping):
            if set(dtype_spec) != {"param"}:
                raise ValueError(
                    "MAP_BLOCKS_OUTPUT_SPEC.dtype mapping must contain only "
                    f"{{'param': '<parameter name>'}}, got {dtype_spec!r}."
                )
            param_name = str(dtype_spec["param"] or "").strip()
            if not param_name:
                raise ValueError("MAP_BLOCKS_OUTPUT_SPEC.dtype param name cannot be empty.")
            if param_name not in params or params[param_name] in (None, ""):
                raise ValueError(
                    f"MAP_BLOCKS_OUTPUT_SPEC.dtype references missing parameter {param_name!r}."
                )
            dtype_spec = params[param_name]

        if node.OUTPUT_DTYPE is not None and dtype_spec in (None, "same"):
            return np.dtype(node.OUTPUT_DTYPE)
        if dtype_spec in (None, "same", "any"):
            infer_dtype = getattr(node, "infer_output_dtype", None)
            if callable(infer_dtype):
                inferred = infer_dtype(primary_dtype, params)
                if inferred is not None:
                    return np.dtype(inferred)
            parsed_return = parse_port_type(node._declared_return_type())
            if parsed_return.dtype not in (None, "any", "same"):
                return np.dtype(dtype_name_to_numpy(parsed_return.dtype, input_dtype=primary_dtype))
            return np.dtype(primary_dtype)
        if isinstance(dtype_spec, str):
            resolved = dtype_name_to_numpy(dtype_spec, input_dtype=primary_dtype)
            if resolved is not None:
                return np.dtype(resolved)
        try:
            return np.dtype(dtype_spec)
        except Exception as exc:
            raise ValueError(f"Unsupported output dtype {dtype_spec!r}.") from exc

    def resolve_chunks(
        self,
        chunks_spec: Any,
        array_inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        primary_name: str,
        *,
        drop_axis: tuple[int, ...] | None = None,
    ) -> Any:
        primary = array_inputs[primary_name]
        if chunks_spec in (None, "same_as_primary"):
            return primary.chunks
        if chunks_spec == "drop_axis_from_primary":
            if not drop_axis:
                raise ValueError(
                    "MAP_BLOCKS_OUTPUT_SPEC.chunks='drop_axis_from_primary' requires "
                    "MAP_BLOCKS_OUTPUT_SPEC.drop_axis."
                )
            dropped = set(drop_axis)
            return tuple(
                axis_chunks
                for axis, axis_chunks in enumerate(primary.chunks)
                if axis not in dropped
            )
        if chunks_spec == "token_chunks_from_primary":
            return tuple((1,) * int(numblocks) for numblocks in primary.numblocks)
        if callable(chunks_spec):
            return chunks_spec(array_inputs=array_inputs, params=params, primary_name=primary_name)
        if isinstance(chunks_spec, str):
            return ChunkPlanner.parse_explicit_chunks(chunks_spec)
        if isinstance(chunks_spec, (tuple, list)):
            return tuple(chunks_spec)
        raise ValueError(f"Unsupported output chunks {chunks_spec!r}.")

    def resolve_drop_axis(
        self,
        value: Any,
        *,
        node: "BaseDaskArrayMapNode",
        primary: Any,
        primary_name: str,
    ) -> tuple[int, ...] | None:
        return self.resolve_axis_tuple(
            value,
            field_name="drop_axis",
            node=node,
            primary=primary,
            primary_name=primary_name,
        )

    def resolve_new_axis(
        self,
        value: Any,
        *,
        node: "BaseDaskArrayMapNode",
        primary: Any,
        primary_name: str,
    ) -> tuple[int, ...] | None:
        return self.resolve_axis_tuple(
            value,
            field_name="new_axis",
            node=node,
            primary=primary,
            primary_name=primary_name,
        )

    def resolve_axis_tuple(
        self,
        value: Any,
        *,
        field_name: str,
        node: "BaseDaskArrayMapNode",
        primary: Any,
        primary_name: str,
    ) -> tuple[int, ...] | None:
        if value is None:
            return None
        if isinstance(value, int):
            raw_axes = (value,)
        elif isinstance(value, str):
            raw_axes = tuple(part.strip() for part in value.split(",") if part.strip())
            if not raw_axes:
                return None
        elif isinstance(value, (tuple, list)):
            raw_axes = tuple(value)
        elif not isinstance(value, int):
            raise ValueError(f"Unsupported {field_name} value {value!r}.")

        axes = (getattr(node, "_axes_by_name", {}) or {}).get(primary_name)
        axis_lookup = {
            str(axis_name).strip().lower(): index
            for index, axis_name in enumerate(axes or ())
        }
        result: list[int] = []
        for raw_axis in raw_axes:
            if isinstance(raw_axis, int) or (
                isinstance(raw_axis, str) and raw_axis.strip().lstrip("-").isdigit()
            ):
                axis = int(raw_axis)
                if field_name == "drop_axis":
                    if axis < 0:
                        axis += int(primary.ndim)
                    if axis < 0 or axis >= int(primary.ndim):
                        raise ValueError(
                            f"MAP_BLOCKS_OUTPUT_SPEC.drop_axis={raw_axis!r} is out of bounds "
                            f"for primary input '{primary_name}' ndim={primary.ndim}."
                        )
                result.append(axis)
                continue

            if not axes:
                raise ValueError(
                    f"MAP_BLOCKS_OUTPUT_SPEC.{field_name} uses axis name {raw_axis!r}, "
                    f"but no axes metadata is available for primary input '{primary_name}'."
                )
            axis_name = str(raw_axis).strip().lower()
            if axis_name not in axis_lookup:
                raise ValueError(
                    f"MAP_BLOCKS_OUTPUT_SPEC.{field_name} uses axis name {raw_axis!r}, "
                    f"but it is not present in axes {tuple(axes)!r} for primary input '{primary_name}'."
                )
            result.append(axis_lookup[axis_name])

        if len(set(result)) != len(result):
            raise ValueError(f"MAP_BLOCKS_OUTPUT_SPEC.{field_name} contains duplicate axes: {value!r}.")
        return tuple(result) or None


class MapBlocksOutputSpecPreflightValidator:
    """Validate map output specs before Dask graph construction."""

    def validate(
        self,
        node: "BaseDaskArrayMapNode",
        output_spec: MapBlocksOutputSpec,
        *,
        primary: Any,
    ) -> None:
        input_ndim = int(primary.ndim)
        drop_axis = self.normalize_drop_axis(node, output_spec.drop_axis, input_ndim)
        new_axis, output_ndim = self.normalize_new_axis(node, output_spec.new_axis, input_ndim, len(drop_axis))
        shape_changing = bool(drop_axis or new_axis)

        if output_spec.same_as_primary:
            if shape_changing or output_ndim != input_ndim:
                raise ValueError(
                    f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.same_as_primary requires no drop_axis/new_axis "
                    f"and output ndim={input_ndim}; got output ndim={output_ndim}."
                )
            return

        if shape_changing and output_spec.chunks_source in (None, "same_as_primary"):
            raise ValueError(
                f"{type(node).__name__} shape-changing output requires explicit MAP_BLOCKS_OUTPUT_SPEC['chunks']; "
                "got same_as_primary."
            )

        self.validate_chunks(node, output_spec.chunks, output_ndim)

    def normalize_drop_axis(self, node: "BaseDaskArrayMapNode", axes: tuple[int, ...] | None, input_ndim: int) -> tuple[int, ...]:
        result = []
        for raw_axis in axes or ():
            axis = int(raw_axis)
            if axis < 0:
                axis += input_ndim
            if axis < 0 or axis >= input_ndim:
                raise ValueError(
                    f"{type(node).__name__} drop_axis={raw_axis} is out of bounds for input ndim={input_ndim}."
                )
            result.append(axis)
        if len(set(result)) != len(result):
            raise ValueError(f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.drop_axis contains duplicate axes: {axes!r}.")
        return tuple(result)

    def normalize_new_axis(
        self,
        node: "BaseDaskArrayMapNode",
        axes: tuple[int, ...] | None,
        input_ndim: int,
        drop_count: int,
    ) -> tuple[tuple[int, ...], int]:
        output_ndim = input_ndim - drop_count + len(axes or ())
        result = []
        for raw_axis in axes or ():
            axis = int(raw_axis)
            if axis < 0:
                axis += output_ndim
            if axis < 0 or axis >= output_ndim:
                raise ValueError(
                    f"{type(node).__name__} new_axis={raw_axis} is out of bounds for output ndim={output_ndim}."
                )
            result.append(axis)
        if len(set(result)) != len(result):
            raise ValueError(f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.new_axis contains duplicate axes: {axes!r}.")
        return tuple(result), output_ndim

    def validate_chunks(self, node: "BaseDaskArrayMapNode", chunks: Any, output_ndim: int) -> None:
        if chunks in (None, "auto"):
            return
        if isinstance(chunks, str):
            if chunks == "same_as_primary":
                raise ValueError(
                    f"{type(node).__name__} shape-changing output requires explicit MAP_BLOCKS_OUTPUT_SPEC['chunks']; "
                    "got same_as_primary."
                )
            parts = tuple(part.strip() for part in chunks.split(",") if part.strip())
            if len(parts) != output_ndim:
                raise ValueError(
                    f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.chunks rank={len(parts)} "
                    f"but expected output ndim={output_ndim} after drop_axis/new_axis."
                )
            for part in parts:
                self.validate_chunk_item(node, part)
            return
        if isinstance(chunks, (tuple, list)):
            if len(chunks) != output_ndim:
                raise ValueError(
                    f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.chunks rank={len(chunks)} "
                    f"but expected output ndim={output_ndim} after drop_axis/new_axis."
                )
            for item in chunks:
                self.validate_chunk_item(node, item)
            return
        raise ValueError(f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.chunks has unsupported value {chunks!r}.")

    def validate_chunk_item(self, node: "BaseDaskArrayMapNode", item: Any) -> None:
        if item == "auto":
            return
        if isinstance(item, (tuple, list)):
            if not item:
                raise ValueError(f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.chunks contains an empty chunk tuple.")
            for sub_item in item:
                self.validate_positive_int(node, sub_item)
            return
        self.validate_positive_int(node, item)

    def validate_positive_int(self, node: "BaseDaskArrayMapNode", value: Any) -> None:
        try:
            parsed = int(value)
        except Exception as exc:
            raise ValueError(f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.chunks item {value!r} is not an integer.") from exc
        if parsed <= 0:
            raise ValueError(f"{type(node).__name__} MAP_BLOCKS_OUTPUT_SPEC.chunks item {value!r} must be a positive integer.")

class BlockContextFactory:
    def build(
        self,
        node: "BaseDaskArrayMapNode",
        *,
        named_blocks: Mapping[str, np.ndarray],
        ordered_names: list[str],
        primary_name: str,
        output_chunk_shape: tuple | None,
        block_info: dict,
        runtime: dict,
    ) -> BlockContext:
        primary_block = named_blocks[primary_name]
        output_block_info = self.extract_output_block_info(block_info)
        resolved_output_chunk_shape = output_chunk_shape or self.extract_output_chunk_shape(output_block_info)
        array_locations = self.extract_array_locations(block_info, len(ordered_names))
        ctx = BlockContext(
            node_id=runtime.get("node_id"),
            execution_id=runtime.get("execution_id"),
            device_hint=runtime.get("device_hint", "cpu"),
            block_info=block_info,
            input_names=tuple(ordered_names),
            block_locations=self.extract_block_locations(block_info, len(ordered_names)),
            array_locations=array_locations,
            chunk_origins=self.derive_chunk_origins(array_locations),
            output_block_info=output_block_info,
            input_blocks=dict(named_blocks),
            input_shapes={name: tuple(block.shape) for name, block in named_blocks.items()},
            input_dtypes={name: np.dtype(block.dtype) for name, block in named_blocks.items()},
            primary_input_name=primary_name,
            primary_block_shape=tuple(primary_block.shape),
            output_chunk_shape=resolved_output_chunk_shape,
            resources=None,
        )
        object.__setattr__(ctx, "resources", BlockResources(node, ctx, runtime.get("resources") or {}))
        return ctx

    def resolve_device_hint(self) -> str:
        try:
            from distributed import get_worker

            worker = get_worker()
            assigned = getattr(worker, "assigned_gpu", None)
            if assigned:
                return assigned
        except Exception:
            pass
        allow_implicit_cuda = os.getenv("WorkFlow_ALLOW_IMPLICIT_CUDA0", "").lower() in ("1", "true", "yes")
        if allow_implicit_cuda:
            try:
                import torch

                if torch.cuda.is_available():
                    return "cuda:0"
            except Exception:
                pass
        return "cpu"

    def extract_block_locations(self, block_info: Any, input_count: int) -> tuple:
        return tuple(
            self.normalize_location(self.input_block_info_entry(block_info, index).get("chunk-location"))
            for index in range(input_count)
        )

    def extract_array_locations(self, block_info: Any, input_count: int) -> tuple:
        return tuple(
            self.normalize_array_location(self.input_block_info_entry(block_info, index).get("array-location"))
            for index in range(input_count)
        )

    def derive_chunk_origins(self, array_locations: tuple) -> tuple:
        result = []
        for array_location in array_locations:
            if array_location is None:
                result.append(None)
                continue
            starts = []
            for axis_location in array_location:
                if isinstance(axis_location, slice):
                    starts.append(int(axis_location.start or 0))
                elif isinstance(axis_location, (list, tuple)) and axis_location:
                    starts.append(int(axis_location[0]))
                else:
                    starts.append(0)
            result.append(tuple(starts))
        return tuple(result)

    def input_block_info_entry(self, block_info: Any, index: int) -> dict:
        if isinstance(block_info, (list, tuple)) and index < len(block_info) and isinstance(block_info[index], dict):
            return block_info[index]
        if isinstance(block_info, dict):
            entry = block_info.get(index)
            if isinstance(entry, dict):
                return entry
            if index == 0 and "chunk-location" in block_info:
                return block_info
        return {}

    def extract_output_block_info(self, block_info: Any) -> dict | None:
        if isinstance(block_info, dict):
            entry = block_info.get(None)
            if isinstance(entry, dict):
                return entry
        return None

    def extract_output_chunk_shape(self, output_block_info: dict | None) -> tuple | None:
        if not isinstance(output_block_info, dict):
            return None
        chunk_shape = output_block_info.get("chunk-shape")
        if chunk_shape is None:
            return None
        return tuple(int(x) for x in chunk_shape)

    def normalize_location(self, location: Any) -> tuple | None:
        if location is None:
            return None
        if isinstance(location, (list, tuple)):
            return tuple(int(x) for x in location)
        return None

    def normalize_array_location(self, location: Any) -> tuple | None:
        if location is None:
            return None
        if not isinstance(location, (list, tuple)):
            return None
        normalized = []
        for axis_location in location:
            if isinstance(axis_location, slice):
                normalized.append((int(axis_location.start or 0), int(axis_location.stop or 0)))
            elif isinstance(axis_location, (list, tuple)) and len(axis_location) >= 2:
                normalized.append((int(axis_location[0]), int(axis_location[1])))
            else:
                return None
        return tuple(normalized)


class BlockOutputValidator:
    def expected_output_chunk_shape(self, output_spec: MapBlocksOutputSpec, primary_block: np.ndarray, block_info: Any) -> tuple | None:
        entry = block_info.get(None) if isinstance(block_info, dict) else None
        if isinstance(entry, dict):
            chunk_shape = entry.get("chunk-shape")
            if chunk_shape is not None:
                return tuple(int(x) for x in chunk_shape)
        if output_spec.same_as_primary:
            return tuple(primary_block.shape)
        return None

    def expected_shape_for_validation(
        self,
        node: "BaseDaskArrayMapNode",
        *,
        block_info: Any,
        output_spec: MapBlocksOutputSpec,
        primary_block: np.ndarray,
        output_chunk_shape: tuple | None,
    ) -> tuple | None:
        return node.expected_output_shape_for_validation(
            block_info=block_info,
            output_spec=output_spec,
            primary_block=primary_block,
            output_chunk_shape=output_chunk_shape,
        )

    def validate(
        self,
        node: "BaseDaskArrayMapNode",
        result: np.ndarray,
        primary_block: np.ndarray,
        *,
        expected_dtype: np.dtype,
        expected_shape: tuple | None,
        output_spec: MapBlocksOutputSpec,
        node_id: Optional[str],
    ) -> None:
        validator = getattr(node, "validate_block_output", None)
        if validator is not None:
            validator(result, primary_block)
            return
        if not isinstance(result, np.ndarray):
            raise ValueError(f"{type(node).__name__} PROCESS_BLOCK must return np.ndarray, got {type(result).__name__}.")
        if np.dtype(result.dtype) != np.dtype(expected_dtype):
            raise TypeError(
                f"{type(node).__name__}[{node_id}] declared output dtype {expected_dtype}, "
                f"but PROCESS_BLOCK returned {result.dtype}. BaseDaskArrayMapNode does not auto-cast."
            )
        if expected_shape is not None and tuple(result.shape) != tuple(expected_shape):
            raise ValueError(
                f"{type(node).__name__}[{node_id}] PROCESS_BLOCK returned shape {tuple(result.shape)}, "
                f"but expected output block shape is {tuple(expected_shape)}."
            )
        if expected_shape is None and output_spec.same_as_primary and tuple(result.shape) != tuple(primary_block.shape):
            raise ValueError(
                f"{type(node).__name__}[{node_id}] PROCESS_BLOCK returned shape {tuple(result.shape)}, "
                f"but same_as_primary output requires {tuple(primary_block.shape)}."
            )

    def fallback_block(
        self,
        node: "BaseDaskArrayMapNode",
        primary_block: np.ndarray,
        output_spec: MapBlocksOutputSpec,
        ctx: BlockContext,
        output_dtype: np.dtype,
    ) -> np.ndarray:
        shape = ctx.output_chunk_shape
        if shape is None and output_spec.same_as_primary:
            shape = tuple(primary_block.shape)
        if shape is None:
            custom = getattr(node, "fallback_output_shape", None)
            if custom is not None:
                shape = custom(primary_block, output_spec, ctx)
        if shape is None:
            raise ValueError(
                f"{type(node).__name__} cannot build fallback block because output shape is unknown. "
                "Use same_as_primary output chunks or implement fallback_output_shape(primary_block, output_spec, ctx)."
            )
        return np.zeros(tuple(shape), dtype=output_dtype)


class BlockwiseRuntimePolicy:
    def should_skip(
        self,
        node: "BaseDaskArrayMapNode",
        primary_block: np.ndarray,
    ) -> bool:
        if node.SKIP_EMPTY_BLOCKS and primary_block.size == 0:
            return True
        if node.SKIP_ALL_ZERO_BLOCKS and np.all(primary_block == 0):
            return True
        return False


class ProcessBlockBinder:
    def build_wrapped_function(
        self,
        node: "BaseDaskArrayMapNode",
        *,
        input_plan: BlockwiseInputPlan,
        params: dict,
        runtime: NodeRuntime,
        output_spec: MapBlocksOutputSpec,
        preprocess_state: dict,
        context_factory: BlockContextFactory,
        output_validator: BlockOutputValidator,
        runtime_policy: BlockwiseRuntimePolicy,
    ):
        output_dtype = np.dtype(output_spec.dtype)

        def wrapped(*blocks, block_info=None):
            block_info = block_info or {}
            named_blocks = dict(zip(input_plan.ordered_names, blocks))
            primary_block = named_blocks[input_plan.primary_name]
            output_chunk_shape = output_validator.expected_output_chunk_shape(output_spec, primary_block, block_info)
            validation_shape = output_validator.expected_shape_for_validation(
                node,
                block_info=block_info,
                output_spec=output_spec,
                primary_block=primary_block,
                output_chunk_shape=output_chunk_shape,
            )
            runtime_dict = {
                "node_id": runtime.node_id,
                "execution_id": runtime.execution_id,
                "device_hint": context_factory.resolve_device_hint(),
                "resources": preprocess_state or {},
            }
            ctx = context_factory.build(
                node,
                named_blocks=named_blocks,
                ordered_names=input_plan.ordered_names,
                primary_name=input_plan.primary_name,
                output_chunk_shape=output_chunk_shape,
                block_info=block_info,
                runtime=runtime_dict,
            )

            try:
                if runtime_policy.should_skip(
                    node,
                    primary_block,
                ):
                    return output_validator.fallback_block(node, primary_block, output_spec, ctx, output_dtype)

                result = self.call_process_block(node, named_blocks, input_plan.primary_name, params, ctx)
                output_validator.validate(
                    node,
                    result,
                    primary_block,
                    expected_dtype=output_dtype,
                    expected_shape=validation_shape,
                    output_spec=output_spec,
                    node_id=runtime.node_id,
                )
                return result
            except Exception as exc:
                if node.FAILURE_POLICY != "zeros_like":
                    raise
                logger.error(
                    "%s error in %s[%s]: %s",
                    node.DASK_API or "array_map",
                    type(node).__name__,
                    runtime.node_id or "",
                    exc,
                )
                return output_validator.fallback_block(node, primary_block, output_spec, ctx, output_dtype)
            finally:
                resources = getattr(ctx, "resources", None)
                if resources is not None:
                    resources.release_all()

        wrapped._is_dask_array_map_wrapped = True
        wrapped.__name__ = (
            f"{node.DASK_API or 'array_map'}_{runtime.node_id}_{type(node).__name__}"
            if runtime.node_id else f"{node.DASK_API or 'array_map'}_{type(node).__name__}"
        )
        return wrapped

    def call_process_block(
        self,
        node: "BaseDaskArrayMapNode",
        named_blocks: Mapping[str, np.ndarray],
        primary_name: str,
        params: dict,
        ctx: BlockContext,
    ) -> np.ndarray:
        fn = self.get_process_block_callable(node)
        sig = inspect.signature(fn)
        parameters = list(sig.parameters.values())
        if self.is_legacy_process_signature(parameters):
            runtime = {
                "node_id": ctx.node_id,
                "execution_id": ctx.execution_id,
                "device_hint": ctx.device_hint,
                "device": ctx.device_hint,
            }
            return fn(named_blocks[primary_name], ctx.block_info, params, runtime)

        pool = {**named_blocks, **params}
        call_args = []
        call_kwargs: dict[str, Any] = {}
        accepts_kwargs = False
        primary_consumed = False
        for param in parameters:
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                continue
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                accepts_kwargs = True
                continue
            value_missing = object()
            value = value_missing
            if param.name == "ctx":
                value = ctx
            elif param.name in pool:
                value = pool[param.name]
                if param.name == primary_name:
                    primary_consumed = True
            elif not primary_consumed and param.name in {"block", "arr", "array", "dask_arr", "x"}:
                value = named_blocks[primary_name]
                primary_consumed = True

            if value is value_missing:
                if param.default is inspect.Parameter.empty:
                    raise TypeError(
                        f"{type(node).__name__} PROCESS_BLOCK requires parameter "
                        f"'{param.name}', but no matching Dask input or scalar parameter exists."
                    )
                continue
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                call_args.append(value)
            else:
                call_kwargs[param.name] = value

        if accepts_kwargs:
            for key, value in pool.items():
                call_kwargs.setdefault(key, value)
            call_kwargs.setdefault("ctx", ctx)
        return fn(*call_args, **call_kwargs)

    @staticmethod
    def get_process_block_callable(node: "BaseDaskArrayMapNode"):
        raw = inspect.getattr_static(type(node), "PROCESS_BLOCK", None)
        if isinstance(raw, staticmethod):
            raw = raw.__func__
        if raw is not None:
            return raw
        return node.process_block

    @staticmethod
    def is_legacy_process_signature(parameters) -> bool:
        names = [p.name for p in parameters[:4]]
        return names == ["block", "block_info", "params", "runtime"]


class BaseDaskArrayMapNode(BaseDaskNode):
    """Shared base for Dask Array nodes backed by map_blocks or map_overlap."""

    CATEGORY = "WorkFlow/Dask"
    DISPLAY_NAME = "Dask Array Map Node"

    PROCESS_BLOCK = None
    MAP_INPUTS: list[str] | None = None
    PRIMARY_INPUT: str | None = None
    CHUNK_POLICY = {"mode": "none"}
    INPUT_CHUNKS: dict[str, Any] = {}
    MAP_BLOCKS_OUTPUT_SPEC = {
        "dtype": "same",
        "chunks": "same_as_primary",
        "drop_axis": None,
        "new_axis": None,
        "enforce_ndim": True,
    }
    ARRAY_AXES: dict[str, Any] | tuple[str, ...] | str | None = None
    ARRAY_AXES_BY_NDIM: Mapping[int, Any] | Mapping[str, Mapping[int, Any]] | None = None

    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = False
    FAILURE_POLICY = "raise"
    OUTPUT_DTYPE = None

    INPUT_PLANNER = BlockwiseInputPlanner
    OUTPUT_SPEC_RESOLVER = MapBlocksOutputSpecResolver
    OUTPUT_SPEC_PREFLIGHT_VALIDATOR = MapBlocksOutputSpecPreflightValidator
    PROCESS_BINDER = ProcessBlockBinder
    CONTEXT_FACTORY = BlockContextFactory
    OUTPUT_VALIDATOR = BlockOutputValidator
    RUNTIME_POLICY = BlockwiseRuntimePolicy

    def process_block(
        self,
        block: np.ndarray,
        block_info: dict,
        params: dict,
        runtime: dict,
    ) -> np.ndarray:
        raise NotImplementedError(
            f"{type(self).__name__} must define PROCESS_BLOCK or override process_block()."
        )

    def execute(self, **kwargs) -> Tuple:
        invocation = self.get_invocation(kwargs)
        input_planner = self.INPUT_PLANNER()
        output_resolver = self.OUTPUT_SPEC_RESOLVER()
        context_factory = self.CONTEXT_FACTORY()
        output_validator = self.OUTPUT_VALIDATOR()
        runtime_policy = self.RUNTIME_POLICY()
        process_binder = self.PROCESS_BINDER()

        input_plan = input_planner.build(self, invocation)
        self._axes_by_name = dict(input_plan.axes_by_name)
        primary = input_plan.array_inputs[input_plan.primary_name]

        preprocess_state = self._call_preprocess(
            primary,
            array_inputs=input_plan.array_inputs,
            params=dict(input_plan.params),
            runtime=invocation.runtime,
        )
        if preprocess_state is None:
            preprocess_state = {}
        if not isinstance(preprocess_state, dict):
            raise TypeError(
                f"{type(self).__name__}.preprocess() must return dict or None, "
                f"got {type(preprocess_state).__name__}."
            )
        preprocess_state = dict(preprocess_state)
        preprocess_state.setdefault("axes_by_name", dict(input_plan.axes_by_name))
        primary_axes = input_plan.axes_by_name.get(input_plan.primary_name)
        if primary_axes is not None:
            preprocess_state.setdefault("axes", primary_axes)
        self._preprocess_state = dict(preprocess_state)

        output_spec = output_resolver.resolve(
            self,
            input_plan.array_inputs,
            input_plan.params,
            input_plan.primary_name,
        )
        self.OUTPUT_SPEC_PREFLIGHT_VALIDATOR().validate(self, output_spec, primary=primary)
        self.validate_output_spec_for_api(output_spec)
        wrapped_fn = process_binder.build_wrapped_function(
            self,
            input_plan=input_plan,
            params=dict(input_plan.params),
            runtime=invocation.runtime,
            output_spec=output_spec,
            preprocess_state=preprocess_state,
            context_factory=context_factory,
            output_validator=output_validator,
            runtime_policy=runtime_policy,
        )
        result = self.build_dask_collection(
            wrapped_fn=wrapped_fn,
            ordered_arrays=input_plan.ordered_arrays,
            array_inputs=input_plan.array_inputs,
            ordered_names=input_plan.ordered_names,
            primary_name=input_plan.primary_name,
            output_spec=output_spec,
            runtime=invocation.runtime,
            params=input_plan.params,
        )
        self.assert_lazy_collection(result)
        self._log_graph_debug(result, invocation.runtime)
        return (result,)

    def validate_output_spec_for_api(self, output_spec: MapBlocksOutputSpec) -> None:
        return None

    def build_dask_collection(
        self,
        wrapped_fn,
        ordered_arrays,
        array_inputs,
        ordered_names,
        primary_name,
        output_spec,
        runtime,
        params,
    ):
        raise NotImplementedError

    def _call_preprocess(self, primary, *, array_inputs: Mapping[str, Any], params: dict, runtime: NodeRuntime):
        preprocess = getattr(self, "preprocess")
        runtime_dict = {"node_id": runtime.node_id, "execution_id": runtime.execution_id}
        try:
            sig = inspect.signature(preprocess)
        except (TypeError, ValueError):
            return preprocess(primary, params=params, runtime=runtime_dict)
        if "array_inputs" in sig.parameters:
            return preprocess(primary, array_inputs=array_inputs, params=params, runtime=runtime_dict)
        return preprocess(primary, params=params, runtime=runtime_dict)

    def _declared_return_type(self) -> str:
        return_types = getattr(type(self), "RETURN_TYPES", ()) or ()
        if not return_types:
            return "DASK_ARRAY"
        return str(return_types[0])

    def expected_output_shape_for_validation(
        self,
        *,
        block_info: Any,
        output_spec: MapBlocksOutputSpec,
        primary_block: np.ndarray,
        output_chunk_shape: tuple | None,
    ) -> tuple | None:
        return output_chunk_shape

    def _log_graph_debug(self, result: Any, runtime: NodeRuntime) -> None:
        try:
            graph_keys = list(result.__dask_graph__().keys())
            sample_keys = [str(k) for k in graph_keys[:2]]
            logger.debug(
                "[%s] %s[%s] graph tasks: count=%s sample=%s",
                self.DASK_API or "dask",
                type(self).__name__,
                runtime.node_id,
                len(graph_keys),
                sample_keys,
            )
        except Exception:
            pass


class BaseMapBlocksNode(BaseDaskArrayMapNode):
    """Thin adapter from the shared array-map template to dask.array.map_blocks."""

    DASK_API = "map_blocks"
    DISPLAY_NAME = "Map Blocks Node"

    def build_dask_collection(
        self,
        wrapped_fn,
        ordered_arrays,
        array_inputs,
        ordered_names,
        primary_name,
        output_spec,
        runtime,
        params,
    ):
        import dask.array as da

        map_kwargs = {
            "dtype": output_spec.dtype,
            "chunks": output_spec.chunks,
            "drop_axis": output_spec.drop_axis,
            "new_axis": output_spec.new_axis,
            "enforce_ndim": output_spec.enforce_ndim,
            "meta": output_spec.meta if output_spec.meta is not None else np.array((), dtype=output_spec.dtype),
            "name": self.make_task_name(runtime),
        }
        map_kwargs = {key: value for key, value in map_kwargs.items() if value is not None}
        return da.map_blocks(wrapped_fn, *ordered_arrays, **map_kwargs)


class BaseMapOverlapNode(BaseDaskArrayMapNode):
    """
    Thin adapter from the shared array-map template to dask.array.map_overlap.

    MAP_OVERLAP_SPEC declares halo behavior. MAP_BLOCKS_OUTPUT_SPEC declares
    the output array structure and is forwarded to Dask automatically.
    """

    DASK_API = "map_overlap"
    DISPLAY_NAME = "Map Overlap Node"
    MAP_OVERLAP_SPEC = {
        "depth": 0,
        "boundary": "none",
        "trim": True,
        "align_arrays": True,
        "allow_rechunk": True,
    }
    ALLOW_UNTRIMMED_OVERLAP_OUTPUT = False

    def build_dask_collection(
        self,
        wrapped_fn,
        ordered_arrays,
        array_inputs,
        ordered_names,
        primary_name,
        output_spec,
        runtime,
        params,
    ):
        import dask.array as da

        overlap_spec = self.resolve_overlap_spec(
            array_inputs=array_inputs,
            ordered_names=ordered_names,
            primary_name=primary_name,
            params=params,
            runtime=runtime,
        )
        self.validate_overlap_spec_for_api(overlap_spec, output_spec)
        overlap_kwargs = {
            "depth": overlap_spec.depth,
            "boundary": overlap_spec.boundary,
            "trim": overlap_spec.trim,
            "align_arrays": overlap_spec.align_arrays,
            "allow_rechunk": overlap_spec.allow_rechunk,
            "dtype": output_spec.dtype,
            "meta": output_spec.meta if output_spec.meta is not None else np.array((), dtype=output_spec.dtype),
            "name": self.make_task_name(runtime),
            "drop_axis": output_spec.drop_axis,
            "new_axis": output_spec.new_axis,
            "enforce_ndim": output_spec.enforce_ndim,
        }
        # With trim=True, Dask interprets ``chunks`` as pre-trim map_blocks
        # chunks. MAP_BLOCKS_OUTPUT_SPEC describes final chunks, so forwarding
        # them would make trim_internal shrink the collection metadata twice.
        # Dask can derive the halo-expanded chunks, including drop/new axes.
        if not overlap_spec.trim:
            overlap_kwargs["chunks"] = output_spec.chunks
        overlap_kwargs = {key: value for key, value in overlap_kwargs.items() if value is not None}
        return da.map_overlap(wrapped_fn, *ordered_arrays, **overlap_kwargs)

    def resolve_overlap_spec(self, array_inputs, ordered_names, primary_name, params, runtime) -> OverlapSpec:
        override = getattr(self, "infer_overlap_spec", None)
        if override is not None:
            inferred = override(
                array_inputs=array_inputs,
                ordered_names=ordered_names,
                primary_name=primary_name,
                params=params,
                runtime=runtime,
            )
            if inferred is not None:
                return self._coerce_overlap_spec(inferred, array_inputs, ordered_names, primary_name)
        return self._coerce_overlap_spec(
            getattr(type(self), "MAP_OVERLAP_SPEC", None) or {},
            array_inputs,
            ordered_names,
            primary_name,
        )

    def _coerce_overlap_spec(
        self,
        spec: OverlapSpec | Mapping[str, Any],
        array_inputs: Mapping[str, Any],
        ordered_names: list[str],
        primary_name: str,
    ) -> OverlapSpec:
        if isinstance(spec, OverlapSpec):
            depth = self._resolve_overlap_depth(spec.depth, array_inputs, ordered_names, primary_name)
            resolved = OverlapSpec(
                depth=depth,
                boundary=spec.boundary,
                trim=spec.trim,
                align_arrays=spec.align_arrays,
                allow_rechunk=spec.allow_rechunk,
            )
            self.validate_asymmetric_depth_boundary(resolved.depth, resolved.boundary)
            return resolved
        if not isinstance(spec, Mapping):
            raise ValueError(f"Unsupported overlap spec {spec!r}.")
        if "depth_by_input" in spec:
            depth = self._resolve_depth_by_input(spec["depth_by_input"], array_inputs, ordered_names)
        else:
            depth = self._resolve_overlap_depth(spec.get("depth", 0), array_inputs, ordered_names, primary_name)
        resolved = OverlapSpec(
            depth=depth,
            boundary=spec.get("boundary", "none"),
            trim=bool(spec.get("trim", True)),
            align_arrays=bool(spec.get("align_arrays", True)),
            allow_rechunk=bool(spec.get("allow_rechunk", True)),
        )
        self.validate_asymmetric_depth_boundary(resolved.depth, resolved.boundary)
        return resolved

    def _resolve_overlap_depth(
        self,
        depth: Any,
        array_inputs: Mapping[str, Any],
        ordered_names: list[str],
        primary_name: str,
    ) -> Any:
        primary = array_inputs[primary_name]
        if self.is_non_negative_int(depth):
            return int(depth)
        if isinstance(depth, list):
            if len(depth) != len(ordered_names):
                raise ValueError(
                    f"map_overlap depth list length {len(depth)} must match the number of mapped inputs "
                    f"{len(ordered_names)}: {ordered_names}."
                )
            return [
                self.resolve_depth_for_input(item, array_inputs[name], name)
                for name, item in zip(ordered_names, depth)
            ]
        if isinstance(depth, tuple):
            return self.resolve_axis_sequence_depth(depth, primary, primary_name)
        if not isinstance(depth, Mapping):
            raise ValueError(f"{type(self).__name__} MAP_OVERLAP_SPEC.depth has unsupported value {depth!r}.")
        if not depth:
            return {}
        return self.resolve_depth_for_input(depth, primary, primary_name)

    def _resolve_depth_by_input(
        self,
        depth_by_input: Any,
        array_inputs: Mapping[str, Any],
        ordered_names: list[str],
    ) -> list[Any]:
        if not isinstance(depth_by_input, Mapping):
            raise ValueError(f"{type(self).__name__} MAP_OVERLAP_SPEC.depth_by_input must be a mapping.")
        unknown = set(depth_by_input) - set(ordered_names)
        if unknown:
            raise ValueError(f"{type(self).__name__} MAP_OVERLAP_SPEC.depth_by_input references unknown input(s): {sorted(unknown)!r}.")
        missing = [name for name in ordered_names if name not in depth_by_input]
        if missing:
            raise ValueError(f"{type(self).__name__} MAP_OVERLAP_SPEC.depth_by_input is missing mapped input(s): {missing!r}.")
        return [
            self.resolve_depth_for_input(depth_by_input[name], array_inputs[name], name)
            for name in ordered_names
        ]

    def resolve_depth_for_input(self, depth: Any, arr: Any, input_name: str) -> Any:
        if self.is_non_negative_int(depth):
            return int(depth)
        if isinstance(depth, (tuple, list)):
            return self.resolve_axis_sequence_depth(depth, arr, input_name)
        if not isinstance(depth, Mapping):
            raise ValueError(f"{type(self).__name__} overlap depth for input '{input_name}' has unsupported value {depth!r}.")
        if not depth:
            return {}
        if self.mapping_uses_axis_indices(depth):
            resolved = {}
            for raw_axis, raw_value in depth.items():
                axis = self.normalize_depth_axis(raw_axis, arr, input_name)
                resolved[axis] = self.normalize_depth_value(raw_value, input_name)
            return resolved
        axes_by_name = getattr(self, "_axes_by_name", {}) or {}
        axes = axes_by_name.get(input_name)
        if not axes:
            raise ValueError(
                "MAP_OVERLAP_SPEC.depth uses axis names, "
                f"but no axes metadata is available for input '{input_name}'."
            )
        if len(axes) != int(arr.ndim):
            raise ValueError(
                f"Axes metadata for input '{input_name}' has length {len(axes)}, "
                f"but array ndim is {arr.ndim}."
            )
        axis_lookup = {str(axis).lower(): idx for idx, axis in enumerate(axes)}
        resolved = {}
        for raw_axis, raw_value in depth.items():
            axis_name = str(raw_axis).strip().lower()
            if axis_name not in axis_lookup:
                raise ValueError(f"Axis name {raw_axis!r} is not present in axes {axes} for input '{input_name}'.")
            axis = axis_lookup[axis_name]
            resolved[axis] = self.normalize_depth_value(raw_value, input_name)
        return resolved

    def resolve_axis_sequence_depth(self, depth: tuple | list, arr: Any, input_name: str) -> tuple:
        if len(depth) != int(arr.ndim):
            raise ValueError(
                f"{type(self).__name__} overlap depth for input '{input_name}' has rank {len(depth)} "
                f"but expected ndim {arr.ndim}."
            )
        return tuple(self.normalize_depth_value(value, input_name) for value in depth)

    def normalize_depth_axis(self, raw_axis: Any, arr: Any, input_name: str) -> int:
        try:
            axis = int(raw_axis)
        except Exception as exc:
            raise ValueError(f"Overlap depth axis {raw_axis!r} for input '{input_name}' is not an integer.") from exc
        if axis < 0:
            axis += int(arr.ndim)
        if axis < 0 or axis >= int(arr.ndim):
            raise ValueError(f"Overlap depth axis {raw_axis!r} is out of bounds for input '{input_name}' ndim={arr.ndim}.")
        return axis

    def normalize_depth_value(self, value: Any, input_name: str) -> int | tuple[int, int]:
        if self.is_non_negative_int(value):
            return int(value)
        if isinstance(value, (tuple, list)):
            if len(value) != 2:
                raise ValueError(
                    f"Overlap depth value {value!r} for input '{input_name}' must be an int "
                    "or a two-item tuple/list."
                )
            left, right = value
            if not self.is_non_negative_int(left) or not self.is_non_negative_int(right):
                raise ValueError(f"Overlap depth value {value!r} for input '{input_name}' must contain non-negative integers.")
            return (int(left), int(right))
        raise ValueError(f"Overlap depth value {value!r} for input '{input_name}' must be a non-negative integer.")

    @staticmethod
    def is_non_negative_int(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0

    @staticmethod
    def mapping_uses_axis_indices(mapping: Mapping[Any, Any]) -> bool:
        return all(isinstance(key, int) or (isinstance(key, str) and key.strip().lstrip("-").isdigit()) for key in mapping)

    def validate_asymmetric_depth_boundary(self, depth: Any, boundary: Any) -> None:
        if self.depth_has_asymmetric_value(depth) and boundary != "none":
            raise ValueError(
                f"{type(self).__name__} asymmetric depth requires boundary='none'. Got boundary={boundary!r}."
            )

    def depth_has_asymmetric_value(self, depth: Any) -> bool:
        if isinstance(depth, Mapping):
            return any(self.depth_has_asymmetric_value(value) for value in depth.values())
        if isinstance(depth, (tuple, list)):
            if len(depth) == 2 and all(self.is_non_negative_int(item) for item in depth):
                return int(depth[0]) != int(depth[1])
            return any(self.depth_has_asymmetric_value(item) for item in depth)
        return False

    def validate_overlap_spec_for_api(
        self,
        overlap_spec: OverlapSpec,
        output_spec: MapBlocksOutputSpec,
    ) -> None:
        if overlap_spec.trim:
            return
        if not output_spec.same_as_primary:
            return
        if getattr(type(self), "ALLOW_UNTRIMMED_OVERLAP_OUTPUT", False) is True:
            return
        raise ValueError(
            f"{type(self).__name__} uses MAP_OVERLAP_SPEC.trim=False with a same-shape output. "
            "This is unsafe unless the block function trims or otherwise controls halo output shape. "
            "Set ALLOW_UNTRIMMED_OVERLAP_OUTPUT=True to opt in explicitly."
        )

    def expected_output_shape_for_validation(
        self,
        *,
        block_info: Any,
        output_spec: MapBlocksOutputSpec,
        primary_block: np.ndarray,
        output_chunk_shape: tuple | None,
    ) -> tuple | None:
        return None
