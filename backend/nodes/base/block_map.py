from __future__ import annotations

from dataclasses import dataclass
import inspect
import logging
import os
from typing import Any, Callable, Dict, Optional, Tuple

import numpy as np

from core.registry import register_node
from core.type_system import dtype_name_to_numpy, is_dask_array_type, parse_port_type


logger = logging.getLogger("WorkFlow.BlockMap")


@dataclass(frozen=True)
class BlockContext:
    """
    Block execution context passed to every PROCESS_BLOCK call.

    Attributes:
        device_hint: WorkFlow-inferred device hint (e.g. "cpu", "cuda:0").
            This is NOT a Dask-provided CUDA device. It is a hint derived from
            worker.assigned_gpu (set by MultiGPUDevicePlugin) or CPU fallback.
            Used for worker-local model cache keys and factory(device) calls.
        device: Backward-compatible alias for device_hint.
            Prefer ctx.device_hint in new code; ctx.device is retained for
            existing nodes that read it directly.
        block_info: Raw block metadata from Dask (array-location, chunk-location).
        block_location: Per-axis block index tuple from Dask scheduler.
        block_shape: Shape of the current block in (Z,Y,X) or (Y,X) order.
        input_dtype: NumPy dtype of the input block.
        resources: Per-block BlockResources (from preprocess return value).
    """
    node_id: Optional[str]
    execution_id: Optional[str]
    device_hint: str          # PRIMARY field (WorkFlow runtime hint, not Dask-provided)
    block_info: dict
    block_location: Any
    block_shape: tuple
    input_dtype: np.dtype
    chunk_origin: tuple = None
    resources: Any = None

    @property
    def device(self) -> str:      # BACKWARD COMPAT: ctx.device == ctx.device_hint
        return self.device_hint

    def cached(
        self,
        namespace: str,
        key: Any,
        factory: Callable[[], Any],
        dispose: Callable[[Any], None] | None = None,
        clear_cuda: bool = False,
    ) -> Any:
        """
        Get or create a worker-local cached object.

        Args:
            namespace: Logical grouping (e.g. "model:cellpose").
            key: Unique identifier within the namespace.
            factory: Called on cache miss to create the object.
            dispose: Optional callback run when the object is evicted.
            clear_cuda: If True, call torch.cuda.empty_cache() after eviction.

        Returns:
            The cached (or newly created) object.
        """
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
        """
        Get or create a worker-local cached model object.

        Resolves the model path, validates if provided, then caches the model
        per (provider, resolved_path, device) so the same model is reused across
        blocks on the same worker.

        Args:
            provider: Model provider name (e.g. "cellpose").
            name: Model name or path.
            factory: Called as factory(resolved_path, device) on cache miss.
            dispose: Optional callback run when the model is evicted.
            clear_cuda: If True (default), call torch.cuda.empty_cache() on cleanup.
            validate: Optional callable(path, name) to validate the model before use.

        Returns:
            The cached model object.
        """
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
    def __init__(self, owner: "BaseBlockMapNode", ctx: BlockContext, specs: dict | None = None):
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


class BaseMapBlockNode:
    """
    Simple block-wise algorithm base class for algorithm engineers.

    Public node-author contract::

        - INPUT_TYPES / RETURN_TYPES / RETURN_NAMES
        - PROCESS_BLOCK(block, scalar_params..., ctx=None)
        - optionally preprocess(dask_arr, params, runtime) -> dict|None
        - optionally postprocess(outputs, state, runtime)  # OUTPUT_NODE only

    Parameter flow (two-phase):
      1. The executor parses INPUT_TYPES, fills defaults, and converts
         INT/FLOAT/BOOLEAN string values, then injects the result as _params
         to execute(). This is the normal path.
      2. If _params is not provided (e.g. direct unit test calls), this class
         falls back to self._extract_params() which re-derives params from
         INPUT_TYPES. This fallback exists only for test compatibility.

    ``OUTPUT_NODE = True`` is the only public marker that tells the executor to
    compute the node's returned Dask collection in Phase 2. After the collection
    computes successfully, the executor calls ``postprocess()`` on the node
    instance if defined.

    Subclass this instead of writing raw Dask graph code when you only need
    per-block NumPy logic.
    """

    CATEGORY = "WorkFlow/BlockMap"
    DISPLAY_NAME = "MapBlock Node"
    OUTPUT_NODE = False  # Set True on output/writer nodes that should trigger compute

    PROCESS_BLOCK = None

    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = False
    FAILURE_POLICY = "raise"

    # Legacy compatibility. New nodes should declare RETURN_TYPES instead.
    OUTPUT_DTYPE = None

    FUNCTION = "execute"

    def preprocess(self, dask_arr, params: dict, runtime: dict) -> dict | None:
        """
        Run during GraphBuilding before worker block execution.

        Return a small serializable dict to expose data through ctx.resources.
        Do not create heavy worker-only objects here.
        """
        return None

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

    def postprocess(self, outputs=None, state=None, runtime: dict | None = None, **kwargs):
        """
        Optional after-compute hook for OUTPUT_NODE classes.

        BaseMapBlockNode.execute() never calls this method. The executor calls
        it after the output node's returned Dask collection has completed
        successfully.
        """
        return outputs

    def execute(self, dask_arr, **kwargs) -> Tuple:
        node_id = kwargs.get("_node_id")
        execution_id = kwargs.get("_execution_id")
        # Prefer executor-provided _params (deduplicated INPUT_TYPES parsing).
        # Fall back to self._extract_params() for direct unit tests / manual calls.
        _params = kwargs.get("_params")
        if _params is None:
            params = self._extract_params(kwargs)
        else:
            params = dict(_params)

        return_type = self._declared_return_type()
        static_runtime = {
            "node_id": node_id,
            "execution_id": execution_id,
        }

        preprocess_state = self.preprocess(
            dask_arr,
            params=params,
            runtime=static_runtime,
        )
        if preprocess_state is None:
            preprocess_state = {}
        if not isinstance(preprocess_state, dict):
            raise TypeError(
                f"{type(self).__name__}.preprocess() must return dict or None, "
                f"got {type(preprocess_state).__name__}."
            )
        self._preprocess_state = dict(preprocess_state)

        output_dtype = self._resolve_output_dtype(
            dask_arr.dtype,
            params,
            return_type,
        )
        meta = np.array((), dtype=output_dtype)

        wrapped_fn = self._make_wrapped_function(
            static_runtime=static_runtime,
            params=params,
            output_dtype=output_dtype,
            return_type=return_type,
            preprocess_state=preprocess_state,
        )

        node_cls_name = type(self).__name__
        map_blocks_name = f"{node_cls_name}_{node_id}" if node_id else node_cls_name
        map_kwargs = {
            "dtype": output_dtype,
            "meta": meta,
            "name": map_blocks_name,
        }
        result = dask_arr.map_blocks(wrapped_fn, **map_kwargs)

        try:
            graph_keys = list(result.__dask_graph__().keys())
            sample_keys = [str(k) for k in graph_keys[:2]]
            logger.debug(
                "[BlockMap] %s[%s] graph tasks: count=%s sample=%s",
                node_cls_name,
                node_id,
                len(graph_keys),
                sample_keys,
            )
        except Exception:
            pass

        return (result,)

    def _declared_return_type(self) -> str:
        return_types = getattr(type(self), "RETURN_TYPES", ()) or ()
        if not return_types:
            return "DASK_ARRAY"
        return str(return_types[0])

    def _extract_params(self, raw_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract scalar/widget params from raw function arguments.

        This is a **legacy fallback**: the executor normally provides _params
        directly to execute(), which bypasses this method entirely.

        This method remains for:
          - Direct unit tests that call node.execute(dask_arr, threshold=...)
          - Manual / ad-hoc invocation outside the executor graph flow

        When called by the executor, _params is already deduplicated INPUT_TYPES
        parsing, default filling, and INT/FLOAT/BOOLEAN conversion.

        This method re-derives the same params from raw_inputs → INPUT_TYPES,
        which duplicates what the executor already did. Use _params when available.
        """
        framework_keys = {"_node_id", "_execution_id"}
        params: Dict[str, Any] = {}

        input_defs = self._input_definitions()
        for section in ("required", "optional"):
            for name, config in input_defs.get(section, {}).items():
                declared = config[0] if isinstance(config, (tuple, list)) and len(config) > 0 else config
                meta = config[1] if isinstance(config, (tuple, list)) and len(config) > 1 and isinstance(config[1], dict) else {}

                if name in framework_keys:
                    continue
                if isinstance(declared, str) and is_dask_array_type(declared):
                    continue

                value = raw_inputs.get(name)
                if value is None or (isinstance(value, str) and value == ""):
                    if "default" in meta:
                        value = meta["default"]
                    elif isinstance(declared, list) and declared:
                        value = declared[0]
                    elif section == "optional":
                        value = None

                value = self._coerce_param_value(name, declared, value)
                params[name] = value

        internal_param_keys = {"_origins_per_dim"}
        for name, value in raw_inputs.items():
            if name in framework_keys or name in params:
                continue
            if name.startswith("_") and name not in internal_param_keys:
                continue
            params[name] = value

        return params

    def _input_definitions(self) -> dict:
        input_types = getattr(type(self), "INPUT_TYPES", None)
        if input_types is None:
            return {"required": {}, "optional": {}}
        try:
            return input_types()
        except Exception as exc:
            logger.warning("Failed to read INPUT_TYPES from %s: %s", type(self).__name__, exc)
            return {"required": {}, "optional": {}}

    def _coerce_param_value(self, name: str, declared: Any, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(declared, str):
            return value
        try:
            if declared == "INT":
                return int(float(value))
            if declared == "FLOAT":
                return float(value)
            if declared == "BOOLEAN":
                if isinstance(value, str):
                    return value.lower() == "true"
                return bool(value)
        except Exception as exc:
            logger.warning("Failed to convert BlockMap param %s=%r: %s", name, value, exc)
        return value

    def _resolve_output_dtype(self, input_dtype, params: Dict[str, Any], return_type: str) -> np.dtype:
        parsed = parse_port_type(return_type)
        resolved = dtype_name_to_numpy(parsed.dtype, input_dtype=input_dtype)
        if resolved is not None:
            return np.dtype(resolved)

        if self.OUTPUT_DTYPE is not None:
            return np.dtype(self.OUTPUT_DTYPE)

        infer = getattr(self, "infer_output_dtype", None)
        if infer is not None:
            inferred = infer(input_dtype, params)
            if inferred is not None:
                return np.dtype(inferred)

        return np.dtype(input_dtype)

    def _make_wrapped_function(
        self,
        static_runtime: dict,
        params: dict,
        output_dtype: np.dtype,
        return_type: str,
        preprocess_state: dict,
    ):
        failure_policy = self.FAILURE_POLICY
        enforce_same_shape = True
        should_apply_skip_policy = True

        def wrapped(block, block_info=None):
            block_info = block_info or {}
            device = self._resolve_device_hint()
            runtime = {
                **static_runtime,
                "device_hint": device,
                "resources": preprocess_state or {},
            }
            ctx = self._build_context(
                block=block,
                block_info=block_info,
                runtime=runtime,
            )

            try:
                if should_apply_skip_policy and self._should_skip(block, block_info, params, runtime):
                    return np.zeros_like(block, dtype=output_dtype)

                result = self._call_process_block(block, params, ctx)
                self._validate_output_block(result, block, enforce_same_shape=enforce_same_shape)
                self._validate_output_dtype(
                    result=result,
                    block=block,
                    expected_output_dtype=output_dtype,
                    return_type=return_type,
                    node_id=static_runtime.get("node_id"),
                )
                return result
            except Exception as exc:
                if failure_policy != "zeros_like":
                    raise
                logger.error(
                    "BlockMap error in %s[%s]: %s",
                    type(self).__name__,
                    static_runtime.get("node_id", ""),
                    exc,
                )
                return np.zeros_like(block, dtype=output_dtype)
            finally:
                resources = getattr(ctx, "resources", None)
                if resources is not None:
                    resources.release_all()

        wrapped._is_blockmap_wrapped = True
        node_id = static_runtime.get("node_id")
        wrapped.__name__ = (
            f"BlockMap_{node_id}_{type(self).__name__}"
            if node_id else f"BlockMap_{type(self).__name__}"
        )
        return wrapped

    def _build_context(self, block: np.ndarray, block_info: dict, runtime: dict) -> BlockContext:
        location = self._extract_block_location(block_info)
        origin = self._extract_block_origin(block_info)
        ctx = BlockContext(
            node_id=runtime.get("node_id"),
            execution_id=runtime.get("execution_id"),
            device_hint=runtime.get("device_hint", "cpu"),
            block_info=block_info,
            block_location=location,
            block_shape=tuple(block.shape),
            input_dtype=np.dtype(block.dtype),
            chunk_origin=origin,
            resources=None,
        )
        resources = BlockResources(self, ctx, runtime.get("resources") or {})
        object.__setattr__(ctx, "resources", resources)
        return ctx

    def _extract_block_origin(self, block_info: Any) -> tuple:
        def origin_from_entry(entry):
            if not isinstance(entry, dict):
                return None
            array_location = entry.get("array-location")
            if array_location:
                starts = []
                for axis_location in array_location:
                    if isinstance(axis_location, slice):
                        starts.append(int(axis_location.start or 0))
                    elif isinstance(axis_location, (list, tuple)) and axis_location:
                        starts.append(int(axis_location[0]))
                    else:
                        starts.append(0)
                return tuple(starts)
            chunk_origin = entry.get("chunk-origin")
            if chunk_origin:
                return tuple(int(x) for x in chunk_origin)
            return None

        try:
            if isinstance(block_info, (list, tuple)) and block_info:
                first = block_info[0]
                if isinstance(first, dict):
                    origin = origin_from_entry(first)
                    if origin is not None:
                        return origin
            if isinstance(block_info, dict):
                origin = origin_from_entry(block_info)
                if origin is not None:
                    return origin
                for key in (0, None):
                    if key in block_info and isinstance(block_info[key], dict):
                        origin = origin_from_entry(block_info[key])
                        if origin is not None:
                            return origin
        except Exception:
            pass
        return None

    def _extract_block_location(self, block_info: Any):
        try:
            if isinstance(block_info, (list, tuple)) and block_info:
                first = block_info[0]
                if isinstance(first, dict):
                    return first.get("chunk-location")
            if isinstance(block_info, dict):
                if "chunk-location" in block_info:
                    return block_info.get("chunk-location")
                if 0 in block_info and isinstance(block_info[0], dict):
                    return block_info[0].get("chunk-location")
                if None in block_info and isinstance(block_info[None], dict):
                    return block_info[None].get("chunk-location")
        except Exception:
            pass
        return None

    def _call_process_block(self, block: np.ndarray, params: dict, ctx: BlockContext) -> np.ndarray:
        fn = self._get_process_block_callable()
        sig = inspect.signature(fn)
        parameters = list(sig.parameters.values())
        if not parameters:
            raise TypeError(f"{type(self).__name__} PROCESS_BLOCK must accept block as its first parameter.")

        if self._is_legacy_process_signature(parameters):
            runtime = {
                "node_id": ctx.node_id,
                "execution_id": ctx.execution_id,
                "device_hint": ctx.device_hint,
                "device": ctx.device_hint,   # backward compat for legacy nodes reading ctx.device
            }
            return fn(block, ctx.block_info, params, runtime)

        explicit_kwargs: Dict[str, Any] = {}
        accepts_kwargs = False

        for param in parameters[1:]:
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                continue
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                accepts_kwargs = True
                continue
            if param.name == "ctx":
                explicit_kwargs["ctx"] = ctx
                continue
            if param.name in params:
                explicit_kwargs[param.name] = params[param.name]
                continue
            if param.default is inspect.Parameter.empty:
                raise TypeError(
                    f"{type(self).__name__} PROCESS_BLOCK requires parameter "
                    f"'{param.name}', but it was not provided by INPUT_TYPES."
                )

        if accepts_kwargs:
            for key, value in params.items():
                explicit_kwargs.setdefault(key, value)

        return fn(block, **explicit_kwargs)

    def _get_process_block_callable(self):
        raw = inspect.getattr_static(type(self), "PROCESS_BLOCK", None)
        if isinstance(raw, staticmethod):
            raw = raw.__func__
        if raw is not None:
            return raw
        return self.process_block

    def _is_legacy_process_signature(self, parameters) -> bool:
        names = [p.name for p in parameters[:4]]
        return names == ["block", "block_info", "params", "runtime"]

    def _validate_output_block(
        self,
        result: np.ndarray,
        block: np.ndarray,
        enforce_same_shape: bool = True,
    ) -> None:
        validator = getattr(self, "validate_output_block", None)
        if validator is not None:
            validator(result, block)
            return

        if not isinstance(result, np.ndarray):
            raise ValueError(
                f"{type(self).__name__} PROCESS_BLOCK must return np.ndarray, "
                f"got {type(result).__name__}."
            )

        if not enforce_same_shape:
            return

        if result.ndim != block.ndim:
            raise ValueError(
                f"{type(self).__name__} PROCESS_BLOCK returned ndim {result.ndim}, "
                f"but input block ndim is {block.ndim}. BaseMapBlockNode requires "
                "PROCESS_BLOCK to return an np.ndarray with the same ndim and shape "
                "as the input block. Shape-changing outputs are not supported by "
                "default. Use a dedicated node implementation for shape-changing "
                "map_blocks."
            )
        if result.shape != block.shape:
            raise ValueError(
                f"{type(self).__name__} PROCESS_BLOCK returned shape {result.shape}, "
                f"but input block shape is {block.shape}. BaseMapBlockNode requires "
                "PROCESS_BLOCK to return an np.ndarray with the same ndim and shape "
                "as the input block. Shape-changing outputs are not supported by "
                "default. Use a dedicated node implementation for shape-changing "
                "map_blocks."
            )

    def _validate_output_dtype(
        self,
        result: np.ndarray,
        block: np.ndarray,
        expected_output_dtype: np.dtype,
        return_type: str,
        node_id: Optional[str],
    ) -> None:
        parsed = parse_port_type(return_type)
        expected_dtype = None
        requirement = None

        if parsed.dtype == "same":
            expected_dtype = np.dtype(block.dtype)
            requirement = f"dtype {expected_dtype}"
        elif parsed.dtype not in (None, "any"):
            expected_dtype = dtype_name_to_numpy(parsed.dtype)
            requirement = f"dtype {expected_dtype}"
        elif self.OUTPUT_DTYPE is not None:
            expected_dtype = np.dtype(expected_output_dtype)
            requirement = f"legacy OUTPUT_DTYPE {expected_dtype}"

        if expected_dtype is None:
            return

        if np.dtype(result.dtype) != np.dtype(expected_dtype):
            raise TypeError(
                f"{type(self).__name__}[{node_id}] declared RETURN_TYPES {return_type} "
                f"requiring {requirement}, but PROCESS_BLOCK returned {result.dtype}. "
                "BaseMapBlockNode does not auto-cast. Fix PROCESS_BLOCK or insert "
                "an explicit DaskTypeCast node downstream."
            )

    def _should_skip(self, block: np.ndarray, block_info: dict, params: dict, runtime: dict) -> bool:
        if self.SKIP_EMPTY_BLOCKS and block.size == 0:
            return True
        if self.SKIP_ALL_ZERO_BLOCKS and np.all(block == 0):
            return True

        custom_skip = getattr(self, "should_skip_block", None)
        if custom_skip is None:
            return False

        try:
            sig = inspect.signature(custom_skip)
            names = list(sig.parameters)
            if names == ["block"]:
                return bool(custom_skip(block))
            if "ctx" in names:
                ctx = self._build_context(block, block_info, runtime)
                return bool(custom_skip(block, ctx=ctx))
        except Exception:
            pass

        try:
            return bool(custom_skip(block, block_info, params, runtime))
        except TypeError:
            return bool(custom_skip(block))

    def _resolve_device_hint(self) -> str:
        try:
            from distributed import get_worker
            worker = get_worker()
            assigned = getattr(worker, "assigned_gpu", None)
            if assigned:
                return assigned
        except Exception:
            pass

        # Only implicit cuda:0 if WorkFlow_ALLOW_IMPLICIT_CUDA0 is set.
        # This prevents multiple workers from silently competing for cuda:0
        # when GPU binding fails or is unavailable.
        allow_implicit_cuda = os.getenv("WorkFlow_ALLOW_IMPLICIT_CUDA0", "").lower() in ("1", "true", "yes")
        if allow_implicit_cuda:
            try:
                import torch
                if torch.cuda.is_available():
                    return "cuda:0"
            except Exception:
                pass

        return "cpu"

BaseBlockMapNode = BaseMapBlockNode
