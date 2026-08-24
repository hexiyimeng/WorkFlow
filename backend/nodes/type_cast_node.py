import numpy as np

from core.type_system import PORT_DTYPE_TO_NUMPY
from core.registry import register_node
from nodes.base import BaseMapBlocksNode


TYPE_CAST_DTYPES = list(PORT_DTYPE_TO_NUMPY.keys())


def type_cast_block(block: np.ndarray, target_dtype: str, clip: bool) -> np.ndarray:
    if target_dtype not in PORT_DTYPE_TO_NUMPY:
        raise ValueError(f"Unsupported target_dtype {target_dtype!r}.")
    dtype = np.dtype(PORT_DTYPE_TO_NUMPY[target_dtype])
    if block.dtype == dtype:
        return block.astype(dtype, copy=False)

    source = block
    if clip and np.issubdtype(dtype, np.integer):
        info = np.iinfo(dtype)
        source = np.clip(source, info.min, info.max)

    return source.astype(dtype, copy=False)


@register_node("DaskTypeCast")
class DaskTypeCast(BaseMapBlocksNode):
    """Blockwise lazy dtype conversion with optional integer clipping."""
    CATEGORY = "WorkFlow/Utility"
    DISPLAY_NAME = "Type Cast"
    required_worker_profile = "cpu-general"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "target_dtype": (TYPE_CAST_DTYPES, {"default": "float32"}),
                "clip": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY[any]",)
    RETURN_NAMES = ("dask_arr",)

    PROCESS_BLOCK = type_cast_block
    MAP_BLOCKS_OUTPUT_SPEC = {
        "dtype": {"param": "target_dtype"},
        "chunks": "same_as_primary",
        "enforce_ndim": True,
    }

    @classmethod
    def RESOLVE_RETURN_TYPES(cls, node_inputs: dict, input_types: dict | None = None):
        target_dtype = node_inputs.get("target_dtype") or "float32"
        if target_dtype not in PORT_DTYPE_TO_NUMPY:
            return cls.RETURN_TYPES
        return (f"DASK_ARRAY[{target_dtype}]",)
