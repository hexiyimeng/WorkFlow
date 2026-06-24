from __future__ import annotations

from dataclasses import dataclass
import inspect
import os
from collections.abc import Iterable
from typing import Any, Callable, Mapping, Optional, Tuple

import numpy as np

from core.node_invocation import NodeInvocation, NodeRuntime
from core.type_system import dtype_name_to_numpy, parse_port_type
from nodes.base.block_map import (
    BaseDaskNode,
    BlockResources,
    ChunkPlanner,
    split_dask_array_inputs,
)


@dataclass(frozen=True)
class DaskBlockwiseSpec:
    out_ind: Any
    input_indices: Mapping[str, Any]
    dtype: Any = "same"
    adjust_chunks: Mapping[Any, Any] | None = None
    new_axes: Mapping[Any, Any] | None = None
    align_arrays: bool = True
    concatenate: bool | None = None
    meta: Any = None
    literal_args: tuple[Any, ...] = ()
    literal_kwargs: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class DaskBlockwiseInputPlan:
    ordered_names: list[str]
    primary_name: str
    array_inputs: dict[str, Any]
    params: dict[str, Any]
    axes_by_name: dict[str, tuple[str, ...] | None]


@dataclass(frozen=True)
class DaskBlockwiseContext:
    """Runtime context for dask.array.blockwise block functions.

    Dask blockwise does not provide map_blocks-style block_info metadata, so
    block_info is intentionally None unless a future Dask API adds support.
    """

    node_id: Optional[str]
    execution_id: Optional[str]
    device_hint: str
    input_blocks: Mapping[str, Any]
    input_shapes: Mapping[str, tuple]
    input_dtypes: Mapping[str, np.dtype]
    input_indices: Mapping[str, Any]
    out_ind: Any
    resources: Any = None
    block_info: dict | None = None

    @property
    def device(self) -> str:
        return self.device_hint

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


class DaskBlockwiseInputPlanner:
    def build(self, node: "BaseDaskBlockwiseNode", invocation: NodeInvocation) -> DaskBlockwiseInputPlan:
        raw_array_inputs, params = split_dask_array_inputs(invocation)
        if not raw_array_inputs:
            raise ValueError(f"{type(node).__name__} requires at least one Dask Array input.")

        ordered_names = self.resolve_map_inputs(node, raw_array_inputs)
        primary_name = self.resolve_primary_input(node, raw_array_inputs, ordered_names)
        axes_by_name = self.resolve_axes_by_name(node, ordered_names, invocation)
        array_inputs = ChunkPlanner.apply_input_chunks(
            dict(raw_array_inputs),
            getattr(type(node), "INPUT_CHUNKS", None),
            axes_by_name=axes_by_name,
        )
        return DaskBlockwiseInputPlan(
            ordered_names=ordered_names,
            primary_name=primary_name,
            array_inputs=array_inputs,
            params=params,
            axes_by_name=axes_by_name,
        )

    def resolve_map_inputs(self, node: "BaseDaskBlockwiseNode", array_inputs: Mapping[str, Any]) -> list[str]:
        declared = getattr(type(node), "MAP_INPUTS", None)
        if declared:
            names = list(declared)
            for name in names:
                if name not in array_inputs:
                    raise ValueError(f"MAP_INPUTS references missing Dask input '{name}'.")
            return names
        return list(array_inputs)

    def resolve_primary_input(
        self,
        node: "BaseDaskBlockwiseNode",
        array_inputs: Mapping[str, Any],
        ordered_names: list[str],
    ) -> str:
        primary = getattr(type(node), "PRIMARY_INPUT", None) or ordered_names[0]
        if primary not in array_inputs:
            raise ValueError(f"PRIMARY_INPUT references missing Dask input '{primary}'.")
        return primary

    def resolve_axes_by_name(
        self,
        node: "BaseDaskBlockwiseNode",
        ordered_names: list[str],
        invocation: NodeInvocation,
    ) -> dict[str, tuple[str, ...] | None]:
        axes_decl = getattr(type(node), "ARRAY_AXES", None)
        result: dict[str, tuple[str, ...] | None] = {name: None for name in ordered_names}
        if isinstance(axes_decl, dict):
            for name in ordered_names:
                result[name] = ChunkPlanner.normalize_axes(axes_decl.get(name))
        elif axes_decl is not None:
            axes = ChunkPlanner.normalize_axes(axes_decl)
            for name in ordered_names:
                result[name] = axes

        for name in ordered_names:
            meta = (
                invocation.inputs.get(f"{name}_metadata")
                or invocation.inputs.get(f"{name}_meta")
                or (invocation.inputs.get("metadata") if len(ordered_names) == 1 else None)
            )
            axes = meta.get("axes") if isinstance(meta, dict) else getattr(meta, "axes", None)
            if axes is not None:
                result[name] = ChunkPlanner.normalize_axes(axes)
        return result


class DaskBlockwiseSpecResolver:
    def resolve(
        self,
        node: "BaseDaskBlockwiseNode",
        input_plan: DaskBlockwiseInputPlan,
    ) -> DaskBlockwiseSpec:
        override = getattr(node, "infer_blockwise_spec", None)
        if override is not None:
            inferred = override(input_plan.array_inputs, input_plan.params, input_plan.primary_name)
            if inferred is not None:
                return self.coerce(node, inferred, input_plan)
        return self.coerce(node, getattr(type(node), "BLOCKWISE_SPEC", None) or {}, input_plan)

    def coerce(
        self,
        node: "BaseDaskBlockwiseNode",
        spec: DaskBlockwiseSpec | Mapping[str, Any],
        input_plan: DaskBlockwiseInputPlan,
    ) -> DaskBlockwiseSpec:
        if isinstance(spec, DaskBlockwiseSpec):
            data = {
                "out_ind": spec.out_ind,
                "input_indices": dict(spec.input_indices),
                "dtype": spec.dtype,
                "adjust_chunks": spec.adjust_chunks,
                "new_axes": spec.new_axes,
                "align_arrays": spec.align_arrays,
                "concatenate": spec.concatenate,
                "meta": spec.meta,
                "literal_args": spec.literal_args,
                "literal_kwargs": spec.literal_kwargs,
            }
        elif isinstance(spec, Mapping):
            data = dict(spec)
        else:
            raise ValueError(f"Unsupported Dask blockwise spec {spec!r}.")

        if "out_ind" not in data:
            raise ValueError(f"{type(node).__name__} BLOCKWISE_SPEC requires out_ind.")
        input_indices = data.get("input_indices")
        if not isinstance(input_indices, Mapping):
            raise ValueError(f"{type(node).__name__} BLOCKWISE_SPEC.input_indices must be a mapping.")

        dtype = self.resolve_dtype(
            node,
            data.get("dtype"),
            input_plan.array_inputs[input_plan.primary_name].dtype,
        )
        meta = data.get("meta")
        if meta is None:
            meta = np.array((), dtype=dtype)

        return DaskBlockwiseSpec(
            out_ind=data["out_ind"],
            input_indices=dict(input_indices),
            dtype=np.dtype(dtype),
            adjust_chunks=dict(data["adjust_chunks"]) if data.get("adjust_chunks") is not None else None,
            new_axes=dict(data["new_axes"]) if data.get("new_axes") is not None else None,
            align_arrays=bool(data.get("align_arrays", True)),
            concatenate=data.get("concatenate"),
            meta=meta,
            literal_args=tuple(data.get("literal_args") or ()),
            literal_kwargs=dict(data.get("literal_kwargs") or {}),
        )

    def resolve_dtype(
        self,
        node: "BaseDaskBlockwiseNode",
        dtype_spec: Any,
        primary_dtype: Any,
    ) -> np.dtype:
        if getattr(node, "OUTPUT_DTYPE", None) is not None and dtype_spec in (None, "same"):
            return np.dtype(node.OUTPUT_DTYPE)
        if dtype_spec == "same":
            return np.dtype(primary_dtype)
        if dtype_spec in (None, "any"):
            parsed_return = parse_port_type(node._declared_return_type())
            if parsed_return.dtype not in (None, "any", "same"):
                return np.dtype(dtype_name_to_numpy(parsed_return.dtype, input_dtype=primary_dtype))
            return np.dtype(primary_dtype)
        if isinstance(dtype_spec, str):
            try:
                resolved = dtype_name_to_numpy(dtype_spec, input_dtype=primary_dtype)
            except ValueError as exc:
                raise ValueError(f"Unsupported Dask blockwise dtype {dtype_spec!r}.") from exc
            if resolved is not None:
                return np.dtype(resolved)
        try:
            return np.dtype(dtype_spec)
        except Exception as exc:
            raise ValueError(f"Unsupported Dask blockwise dtype {dtype_spec!r}.") from exc


class DaskBlockwiseSpecValidator:
    def validate(
        self,
        node: "BaseDaskBlockwiseNode",
        spec: DaskBlockwiseSpec,
        input_plan: DaskBlockwiseInputPlan,
    ) -> None:
        out_ind = _index_tuple(spec.out_ind)
        if spec.out_ind is None:
            raise ValueError(f"{type(node).__name__} Dask blockwise out_ind is required.")
        if not isinstance(spec.input_indices, Mapping):
            raise ValueError(f"{type(node).__name__} Dask blockwise input_indices must be a mapping.")
        if len(set(out_ind)) != len(out_ind):
            raise ValueError(f"Dask blockwise out_ind {spec.out_ind!r} contains repeated labels.")

        extra_indices = set(spec.input_indices) - set(input_plan.ordered_names)
        if extra_indices:
            raise ValueError(
                f"Dask blockwise input_indices contains entries for non-mapped inputs: "
                f"{sorted(str(name) for name in extra_indices)}."
            )

        for name in input_plan.ordered_names:
            if name not in spec.input_indices:
                raise ValueError(f"Dask blockwise input '{name}' is missing an input_indices entry.")
            input_ind = _index_tuple(spec.input_indices[name])
            arr = input_plan.array_inputs[name]
            if len(input_ind) != int(arr.ndim):
                raise ValueError(
                    f"Dask blockwise input '{name}' index {spec.input_indices[name]!r} has rank "
                    f"{len(input_ind)}, but array ndim is {arr.ndim}."
                )

        out_index_set = set(out_ind)
        for key in (spec.adjust_chunks or {}):
            if key not in out_index_set:
                raise ValueError(f"Dask blockwise adjust_chunks key {key!r} is not present in out_ind {spec.out_ind!r}.")

        input_index_set = {
            label
            for name in input_plan.ordered_names
            for label in _index_tuple(spec.input_indices[name])
        }
        unknown_out_indices = out_index_set - input_index_set - set(spec.new_axes or {})
        if unknown_out_indices:
            raise ValueError(
                f"Dask blockwise out_ind contains labels that are not present in inputs "
                f"or new_axes: {sorted(str(label) for label in unknown_out_indices)}."
            )
        for key in (spec.new_axes or {}):
            if key not in out_index_set:
                raise ValueError(f"Dask blockwise new_axes key {key!r} is not present in out_ind {spec.out_ind!r}.")
            if key in input_index_set:
                raise ValueError(f"Dask blockwise new_axes key {key!r} already exists in an input index.")

        if not spec.align_arrays:
            self.validate_shared_numblocks(spec, input_plan)

    def validate_shared_numblocks(self, spec: DaskBlockwiseSpec, input_plan: DaskBlockwiseInputPlan) -> None:
        by_index: dict[Any, list[tuple[str, int]]] = {}
        for name in input_plan.ordered_names:
            arr = input_plan.array_inputs[name]
            for axis, label in enumerate(_index_tuple(spec.input_indices[name])):
                by_index.setdefault(label, []).append((name, int(arr.numblocks[axis])))

        for label, entries in by_index.items():
            expected = entries[0][1]
            mismatched = [(name, blocks) for name, blocks in entries if blocks != expected]
            if mismatched:
                details = ", ".join(f"{name}={blocks}" for name, blocks in entries)
                raise ValueError(
                    f"Dask blockwise align_arrays=False requires matching numblocks for shared "
                    f"index {label!r}; got {details}."
                )


class DaskBlockwiseContextFactory:
    def build(
        self,
        node: "BaseDaskBlockwiseNode",
        *,
        named_blocks: Mapping[str, Any],
        spec: DaskBlockwiseSpec,
        block_info: dict | None,
        runtime: NodeRuntime,
        resources: Mapping[str, Any],
    ) -> DaskBlockwiseContext:
        runtime_dict = {
            "node_id": runtime.node_id,
            "execution_id": runtime.execution_id,
            "device_hint": self.resolve_device_hint(),
        }
        ctx = DaskBlockwiseContext(
            node_id=runtime_dict["node_id"],
            execution_id=runtime_dict["execution_id"],
            device_hint=runtime_dict["device_hint"],
            input_blocks=dict(named_blocks),
            input_shapes={name: _block_shape(block) for name, block in named_blocks.items()},
            input_dtypes={name: _block_dtype(block) for name, block in named_blocks.items()},
            input_indices=dict(spec.input_indices),
            out_ind=spec.out_ind,
            resources=None,
            block_info=block_info,
        )
        object.__setattr__(ctx, "resources", BlockResources(node, ctx, dict(resources or {})))
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


class DaskBlockwiseOutputValidator:
    def validate(
        self,
        node: "BaseDaskBlockwiseNode",
        result: Any,
        spec: DaskBlockwiseSpec,
        ctx: DaskBlockwiseContext,
    ) -> None:
        normalized = self._normalize_result(node, result, spec)
        if getattr(node, "STRICT_DTYPE", True):
            self.validate_dtype(node, normalized, spec)
        if getattr(node, "STRICT_NDIM", True):
            self.validate_ndim(node, normalized, spec)
        custom = getattr(node, "validate_blockwise_output", None)
        if callable(custom):
            custom(result, ctx, spec)

    def _normalize_result(self, node: "BaseDaskBlockwiseNode", result: Any, spec: DaskBlockwiseSpec) -> Any:
        if isinstance(result, np.ndarray):
            return result
        if np.isscalar(result):
            return np.asarray(result)
        if _index_tuple(spec.out_ind) == () and hasattr(result, "dtype") and hasattr(result, "ndim"):
            return result
        raise TypeError(
            f"{type(node).__name__} PROCESS_BLOCK must return a NumPy array or NumPy scalar, "
            f"got {type(result).__name__}."
        )

    def validate_dtype(self, node: "BaseDaskBlockwiseNode", result: Any, spec: DaskBlockwiseSpec) -> None:
        dtype = getattr(result, "dtype", None)
        if dtype is None:
            return
        if np.dtype(dtype) != np.dtype(spec.dtype):
            raise TypeError(
                f"{type(node).__name__} declared blockwise output dtype {np.dtype(spec.dtype)}, "
                f"but PROCESS_BLOCK returned {np.dtype(dtype)}. BaseDaskBlockwiseNode does not auto-cast; "
                f"return result.astype(np.{np.dtype(spec.dtype).name}, copy=False) or update BLOCKWISE_SPEC.dtype."
            )

    def validate_ndim(self, node: "BaseDaskBlockwiseNode", result: Any, spec: DaskBlockwiseSpec) -> None:
        ndim = getattr(result, "ndim", None)
        if ndim is None:
            return
        expected_ndim = len(_index_tuple(spec.out_ind))
        if int(ndim) != expected_ndim:
            raise ValueError(
                f"{type(node).__name__} declared out_ind={spec.out_ind!r} so PROCESS_BLOCK is expected "
                f"to return ndim={expected_ndim}, but returned ndim={int(ndim)}."
            )


class DaskBlockwiseFunctionBinder:
    def build_wrapped_function(
        self,
        node: "BaseDaskBlockwiseNode",
        *,
        input_plan: DaskBlockwiseInputPlan,
        spec: DaskBlockwiseSpec,
        preprocess_state: Mapping[str, Any],
        runtime: NodeRuntime,
        context_factory: DaskBlockwiseContextFactory,
        output_validator: DaskBlockwiseOutputValidator,
    ):
        array_arg_count = len(input_plan.ordered_names)

        def wrapped(*args, **literal_kwargs):
            named_blocks = dict(zip(input_plan.ordered_names, args[:array_arg_count]))
            literal_args = args[array_arg_count:]
            ctx = context_factory.build(
                node,
                named_blocks=named_blocks,
                spec=spec,
                block_info=None,
                runtime=runtime,
                resources=preprocess_state,
            )
            try:
                result = self.call_process_block(
                    node,
                    named_blocks=named_blocks,
                    params=input_plan.params,
                    literal_args=literal_args,
                    literal_kwargs=literal_kwargs,
                    ctx=ctx,
                )
                output_validator.validate(node, result, spec, ctx)
                return result
            finally:
                resources = getattr(ctx, "resources", None)
                if resources is not None:
                    resources.release_all()

        wrapped._is_dask_blockwise_wrapped = True
        wrapped.__name__ = (
            f"dask_blockwise_{runtime.node_id}_{type(node).__name__}"
            if runtime.node_id else f"dask_blockwise_{type(node).__name__}"
        )
        return wrapped

    def call_process_block(
        self,
        node: "BaseDaskBlockwiseNode",
        *,
        named_blocks: Mapping[str, Any],
        params: Mapping[str, Any],
        literal_args: tuple[Any, ...],
        literal_kwargs: Mapping[str, Any],
        ctx: DaskBlockwiseContext,
    ) -> Any:
        fn = self.get_process_block_callable(node)
        sig = inspect.signature(fn)
        pool = {**named_blocks, **dict(params), **dict(literal_kwargs)}
        call_args = []
        call_kwargs: dict[str, Any] = {}
        accepts_kwargs = False
        literal_pos = 0

        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                call_args.extend(literal_args[literal_pos:])
                literal_pos = len(literal_args)
                continue
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                accepts_kwargs = True
                continue

            missing = object()
            value = missing
            if param.name == "ctx":
                value = ctx
            elif param.name in pool:
                value = pool[param.name]
            elif literal_pos < len(literal_args):
                value = literal_args[literal_pos]
                literal_pos += 1

            if value is missing:
                if param.default is inspect.Parameter.empty:
                    raise TypeError(
                        f"{type(node).__name__} PROCESS_BLOCK requires parameter "
                        f"'{param.name}', but no matching Dask input, scalar parameter, or literal exists."
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
    def get_process_block_callable(node: "BaseDaskBlockwiseNode"):
        raw = inspect.getattr_static(type(node), "PROCESS_BLOCK", None)
        if isinstance(raw, staticmethod):
            raw = raw.__func__
        if raw is not None:
            return raw
        return node.process_block


class BaseDaskBlockwiseNode(BaseDaskNode):
    FUNCTION = "execute"
    DASK_API = "blockwise"
    CATEGORY = "WorkFlow/Dask"
    DISPLAY_NAME = "Dask Blockwise Node"

    PROCESS_BLOCK = None
    MAP_INPUTS: list[str] | None = None
    PRIMARY_INPUT: str | None = None
    INPUT_CHUNKS: dict[str, Any] = {}
    ARRAY_AXES: dict[str, Any] | tuple[str, ...] | str | None = None
    BLOCKWISE_SPEC: Mapping[str, Any] | DaskBlockwiseSpec = {}
    OUTPUT_DTYPE = None

    INPUT_PLANNER = DaskBlockwiseInputPlanner
    SPEC_RESOLVER = DaskBlockwiseSpecResolver
    SPEC_VALIDATOR = DaskBlockwiseSpecValidator
    OUTPUT_VALIDATOR = DaskBlockwiseOutputValidator
    FUNCTION_BINDER = DaskBlockwiseFunctionBinder
    CONTEXT_FACTORY = DaskBlockwiseContextFactory
    STRICT_DTYPE = True
    STRICT_NDIM = True
    STRICT_SHAPE = False

    def process_block(self, **kwargs) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} must define PROCESS_BLOCK or override process_block()."
        )

    def validate_blockwise_output(self, result, ctx: DaskBlockwiseContext, spec: DaskBlockwiseSpec):
        return None

    def execute(self, **kwargs) -> Tuple:
        invocation = self.get_invocation(kwargs)
        input_plan = self.INPUT_PLANNER().build(self, invocation)
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
        self._preprocess_state = dict(preprocess_state)

        spec = self.SPEC_RESOLVER().resolve(self, input_plan)
        self.SPEC_VALIDATOR().validate(self, spec, input_plan)
        wrapped_fn = self.FUNCTION_BINDER().build_wrapped_function(
            self,
            input_plan=input_plan,
            spec=spec,
            preprocess_state=preprocess_state,
            runtime=invocation.runtime,
            context_factory=self.CONTEXT_FACTORY(),
            output_validator=self.OUTPUT_VALIDATOR(),
        )
        result = self.build_dask_collection(wrapped_fn, spec, input_plan, invocation.runtime)
        self.assert_lazy_collection(result)
        self._log_graph_debug(result, invocation.runtime)
        return (result,)

    def build_dask_collection(
        self,
        wrapped_fn,
        spec: DaskBlockwiseSpec,
        input_plan: DaskBlockwiseInputPlan,
        runtime: NodeRuntime,
    ):
        import dask.array as da

        blockwise_args = []
        for name in input_plan.ordered_names:
            blockwise_args.extend([input_plan.array_inputs[name], spec.input_indices[name]])
        for literal in spec.literal_args:
            blockwise_args.extend([literal, None])

        kwargs = {
            "name": self.make_task_name(runtime),
            "dtype": spec.dtype,
            "adjust_chunks": spec.adjust_chunks,
            "new_axes": spec.new_axes,
            "align_arrays": spec.align_arrays,
            "concatenate": spec.concatenate,
            "meta": spec.meta,
            **dict(spec.literal_kwargs or {}),
        }
        kwargs = {key: value for key, value in kwargs.items() if value is not None}
        return da.blockwise(wrapped_fn, spec.out_ind, *blockwise_args, **kwargs)

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

    def _log_graph_debug(self, result: Any, runtime: NodeRuntime) -> None:
        try:
            graph_keys = list(result.__dask_graph__().keys())
            sample_keys = [str(k) for k in graph_keys[:2]]
            import logging

            logging.getLogger("WorkFlow.DaskBlockwise").debug(
                "[blockwise] %s[%s] graph tasks: count=%s sample=%s",
                type(self).__name__,
                runtime.node_id,
                len(graph_keys),
                sample_keys,
            )
        except Exception:
            pass


def _index_tuple(index: Any) -> tuple:
    if index is None:
        return ()
    if isinstance(index, str):
        return tuple(index)
    if isinstance(index, tuple):
        return index
    if isinstance(index, list):
        return tuple(index)
    if isinstance(index, Iterable):
        return tuple(index)
    return (index,)


def _block_shape(block: Any) -> tuple:
    shape = getattr(block, "shape", None)
    if shape is not None:
        return tuple(shape)
    if isinstance(block, (list, tuple)):
        return tuple(_block_shape(item) for item in block)
    return ()


def _block_dtype(block: Any) -> np.dtype:
    dtype = getattr(block, "dtype", None)
    if dtype is not None:
        return np.dtype(dtype)
    if isinstance(block, (list, tuple)) and block:
        return _block_dtype(block[0])
    return np.dtype(object)
