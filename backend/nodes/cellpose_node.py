import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger("WorkFlow.Cellpose")



def create_cellpose_model(model_path: str, device: str):
    """
    Create a Cellpose model for ctx.model().

    ``model_path`` is expected to be an absolute local path resolved by
    WorkFlow's model_registry. ``device`` is WorkFlow's worker device hint.
    Heavy Cellpose/torch imports stay inside this factory to keep plugin
    loading lightweight.
    """
    from cellpose import models
    import torch

    if not Path(model_path).exists():
        raise RuntimeError(
            f"Cellpose model path {model_path!r} does not exist. "
            "WorkFlow requires models to be installed locally before execution."
        )

    device_obj = torch.device(device if device else "cpu")

    return models.CellposeModel(
        gpu=(device_obj.type == "cuda"),
        pretrained_model=model_path,
        device=device_obj,
    )


def cellpose_block(
    block,
    model_name="cpsam",
    diameter=0.0,
    flow_threshold=0.4,
    cellprob_threshold=0.0,
    gpu_batch_size=8,
    ctx=None,
):
    """
    Block function for Cellpose segmentation.

    Uses ctx.model(...) to get a worker-local cached Cellpose model.
    Node authors do not manage caching or cleanup here.
    """
    if ctx is None:
        raise RuntimeError("Cellpose block requires a BlockContext.")
    if block.ndim not in (2, 3):
        raise ValueError(
            "DaskCellpose supports 2D YX blocks or 3D ZYX blocks only. "
            f"Received block ndim={block.ndim}, shape={block.shape}."
        )

    model_spec = ctx.resources.get("model_spec") if ctx.resources else None
    if not isinstance(model_spec, dict):
        fallback_name = model_name or "cpsam"
        model_spec = {
            "provider": "cellpose",
            "requested_name": fallback_name,
            "load_name": fallback_name,
            "clear_cuda": True,
        }

    provider = model_spec.get("provider")
    model_name_or_path = (
        model_spec.get("load_name")
        or model_spec.get("requested_name")
        or model_name
        or "cpsam"
    )

    model = ctx.model(
        provider=provider,
        name=model_name_or_path,
        factory=create_cellpose_model,
        clear_cuda=bool(model_spec.get("clear_cuda", True)),
    )

    masks = None
    flows = None
    styles = None
    try:
        eval_kwargs: dict = {
            "batch_size": gpu_batch_size,
            "progress": None,
            "bsize": 256,
            "tile_overlap": 0.1,
            "resample": False,
        }

        if diameter and diameter > 0:
            eval_kwargs["diameter"] = diameter

        if block.ndim == 3:
            eval_kwargs["do_3D"] = True
            eval_kwargs["z_axis"] = 0
        else:
            eval_kwargs["do_3D"] = False
            eval_kwargs["flow_threshold"] = flow_threshold
            eval_kwargs["cellprob_threshold"] = cellprob_threshold

        masks, flows, styles = model.eval(block, **eval_kwargs)
        return masks.astype(np.uint16, copy=False)
    finally:
        if masks is not None:
            del masks
        if flows is not None:
            del flows
        if styles is not None:
            del styles

from core.model_registry import list_models, resolve_model_path
from core.registry import register_node
from nodes.base import BaseBlockMapNode


@register_node("DaskCellpose")
class DaskCellpose(BaseBlockMapNode):
    """
    Blockwise Cellpose segmentation.

    Supported block shapes are 2D YX and 3D ZYX. Higher-dimensional arrays must
    be sliced or reshaped upstream because channel/time axis semantics are not
    implemented here. Model loading is worker-local and lazy through ctx.model().
    """
    CATEGORY = "WorkFlow/Segmentation"
    DISPLAY_NAME = "Cellpose Segmentation"
    FAILURE_POLICY = "raise"

    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = False
    OUTPUT_DTYPE = np.uint16

    @classmethod
    def INPUT_TYPES(cls):
        local_models = list_models("cellpose")
        models = sorted(set(local_models))
        default_model = models[0] if models else "cpsam"
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "model_name": ("STRING", {"default": default_model, "multiline": False}),
                "diameter": ("FLOAT", {"default": 0.0, "min": 0.0, "max": 500.0}),
                "flow_threshold": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0}),
                "cellprob_threshold": ("FLOAT", {"default": 0.0, "min": -6.0, "max": 6.0}),
                "gpu_batch_size": ("INT", {"default": 8, "min": 1, "max": 64}),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint16]",)
    RETURN_NAMES = ("mask_dask",)
    FUNCTION = "execute"
    PROCESS_BLOCK = cellpose_block
    

    def preprocess(self, dask_arr, params: dict, runtime: dict) -> dict:
        if int(getattr(dask_arr, "ndim", 0)) not in (2, 3):
            raise ValueError(
                "DaskCellpose supports 2D YX arrays or 3D ZYX arrays only. "
                f"Received ndim={getattr(dask_arr, 'ndim', None)}, shape={getattr(dask_arr, 'shape', None)}."
            )
        requested_name = params.get("model_name") or "cpsam"
        load_name = resolve_model_path("cellpose", requested_name)
        if not load_name:
            raise RuntimeError(
                f"Cellpose model {requested_name!r} is not installed locally. "
                "Install it under backend/models/cellpose/ or set WorkFlow_MODELS_DIR."
            )

        logger.info(
            "[Cellpose] requested_model=%r resolved_load_name=%r",
            requested_name,
            load_name,
        )

        # Serializable only. The CellposeModel object is created lazily inside
        # the Dask worker via ctx.model(), so GraphBuilding never loads the
        # heavy cellpose/torch model.
        model_spec = {
            "provider": "cellpose",
            "requested_name": requested_name,
            "load_name": load_name,
            "clear_cuda": True,
        }
        return {"model_spec": model_spec}
