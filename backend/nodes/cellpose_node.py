from __future__ import annotations

import inspect
import itertools
from pathlib import Path
from typing import Any

import numpy as np

from core.model_registry import list_models
from core.registry import register_node
from nodes.base import BaseMapOverlapNode, ChunkPlanner


def create_cellpose_model(model_ref: str, device: str):
    """Create a worker-local Cellpose model without importing torch at plugin load."""

    from cellpose import models
    import torch

    device_obj = torch.device(device if device else "cpu")
    kwargs = {
        "gpu": device_obj.type == "cuda",
        "device": device_obj,
    }

    ref = str(model_ref)
    if Path(ref).exists():
        kwargs["pretrained_model"] = ref
    else:
        try:
            signature = inspect.signature(models.CellposeModel)
        except (TypeError, ValueError):
            signature = None
        parameters = signature.parameters if signature is not None else {}
        if "model_type" in parameters:
            kwargs["model_type"] = ref
        else:
            kwargs["pretrained_model"] = ref

    return models.CellposeModel(**kwargs)


def validate_cellpose_model(model_ref: str, requested_name: str) -> None:
    if not Path(model_ref).exists():
        raise FileNotFoundError(
            f"Cellpose model {requested_name!r} is not installed under "
            "backend/models/cellpose."
        )


def cellpose_block(
    image: np.ndarray,
    primary_channel: int = 0,
    secondary_channel: int = -1,
    model_name: str = "",
    diameter: float = 0.0,
    flow_threshold: float = 0.4,
    cellprob_threshold: float = 0.0,
    gpu_batch_size: int = 8,
    do_3d: str = "auto",
    normalize: bool = True,
    ctx=None,
) -> np.ndarray:
    if ctx is None:
        raise RuntimeError("Cellpose block requires a BlockContext.")

    model_name = str(model_name or "").strip()
    if not model_name:
        raise ValueError(
            "Cellpose requires a model selected from backend/models/cellpose."
        )

    resources = ctx.resources or {}
    axes_by_name = resources.get("axes_by_name") or {}
    axes = resources.get("axes") or axes_by_name.get(ctx.primary_input_name)
    if not axes:
        raise ValueError(
            "Cellpose requires axes in BlockContext.resources. "
            "Declare ARRAY_AXES_BY_NDIM or provide the explicit axes input."
        )
    axes = tuple(str(axis).upper() for axis in axes)
    if len(axes) != image.ndim:
        raise ValueError(f"Cellpose axes {axes!r} length does not match image ndim={image.ndim}.")
    if len(set(axes)) != len(axes):
        raise ValueError(f"Cellpose axes contains duplicate entries: {axes!r}.")

    output_axes = tuple(axis for axis in axes if axis != "C")
    spatial_axes = tuple(axis for axis in ("Z", "Y", "X") if axis in output_axes)
    if "Y" not in spatial_axes or "X" not in spatial_axes:
        raise ValueError(f"Cellpose requires Y and X axes, got output axes {output_axes!r}.")
    if len(spatial_axes) not in (2, 3):
        raise ValueError(f"Cellpose supports 2D YX or 3D ZYX spatial blocks, got {output_axes!r}.")

    do_3d_mode = str(do_3d or "auto").strip().lower()
    if do_3d_mode not in {"auto", "true", "false"}:
        raise ValueError(f"do_3d must be one of 'auto', 'true', or 'false', got {do_3d!r}.")

    channel_axis = axes.index("C") if "C" in axes else None
    if channel_axis is None:
        work_image = image
        has_channels = False
    else:
        channel_count = int(image.shape[channel_axis])
        primary_channel = int(primary_channel)
        secondary_channel = int(secondary_channel)
        if primary_channel < 0 or primary_channel >= channel_count:
            raise ValueError(
                f"primary_channel={primary_channel} is out of bounds "
                f"for channel count {channel_count}."
            )
        if secondary_channel >= channel_count:
            raise ValueError(
                f"secondary_channel={secondary_channel} is out of bounds "
                f"for channel count {channel_count}."
            )

        primary = np.take(image, primary_channel, axis=channel_axis)
        if secondary_channel >= 0:
            secondary = np.take(image, secondary_channel, axis=channel_axis)
            work_image = np.stack([primary, secondary], axis=-1)
            has_channels = True
        else:
            work_image = primary
            has_channels = False

    model = ctx.model(
        provider="cellpose",
        name=model_name,
        factory=create_cellpose_model,
        clear_cuda=True,
        validate=validate_cellpose_model,
    )

    output = np.zeros(
        work_image.shape[:-1] if has_channels else work_image.shape,
        dtype=np.uint32,
    )
    batch_axes = [
        index
        for index, axis in enumerate(output_axes)
        if axis not in ("Z", "Y", "X")
    ]
    batch_axis_set = set(batch_axes)
    batch_shape = tuple(int(output.shape[index]) for index in batch_axes)
    batch_indices = (
        itertools.product(*(range(size) for size in batch_shape))
        if batch_axes
        else [()]
    )

    for batch_index in batch_indices:
        output_selection: list[Any] = [slice(None)] * output.ndim
        for axis_index, value in zip(batch_axes, batch_index):
            output_selection[axis_index] = value

        segment_index = tuple(output_selection + ([slice(None)] if has_channels else []))
        segment = work_image[segment_index]
        remaining_axes = tuple(
            axis
            for index, axis in enumerate(output_axes)
            if index not in batch_axis_set
        )
        segment_spatial_axes = tuple(
            axis for axis in ("Z", "Y", "X") if axis in remaining_axes
        )
        transpose_order = [
            remaining_axes.index(axis)
            for axis in segment_spatial_axes
        ]
        if has_channels:
            transpose_order.append(segment.ndim - 1)
        segment_for_model = (
            np.transpose(segment, transpose_order)
            if transpose_order != list(range(segment.ndim))
            else segment
        )

        eval_kwargs: dict[str, Any] = {
            "batch_size": int(gpu_batch_size),
            "progress": None,
            "bsize": 256,
            "tile_overlap": 0.1,
            "resample": False,
            "normalize": bool(normalize),
            "flow_threshold": float(flow_threshold),
            "cellprob_threshold": float(cellprob_threshold),
        }
        if float(diameter or 0.0) > 0:
            eval_kwargs["diameter"] = float(diameter)
        if has_channels:
            eval_kwargs["channel_axis"] = -1

        has_z = "Z" in segment_spatial_axes
        use_3d = do_3d_mode == "true" or (
            do_3d_mode == "auto"
            and has_z
            and len(segment_spatial_axes) == 3
        )
        if do_3d_mode == "false":
            use_3d = False

        if not has_z or use_3d:
            eval_kwargs["do_3D"] = use_3d
            if use_3d:
                eval_kwargs["z_axis"] = 0
            result = model.eval(np.asarray(segment_for_model), **eval_kwargs)
            mask = np.asarray(result[0] if isinstance(result, tuple) else result)
        else:
            eval_kwargs["do_3D"] = False
            mask = np.zeros(segment_for_model.shape[:3], dtype=np.uint32)
            for z_index in range(int(segment_for_model.shape[0])):
                result = model.eval(
                    np.asarray(segment_for_model[z_index]),
                    **eval_kwargs,
                )
                plane_mask = result[0] if isinstance(result, tuple) else result
                mask[z_index] = np.asarray(plane_mask, dtype=np.uint32)

        inverse_order = [
            segment_spatial_axes.index(axis)
            for axis in remaining_axes
        ]
        if inverse_order != list(range(mask.ndim)):
            mask = np.transpose(mask, inverse_order)
        output[tuple(output_selection)] = mask.astype(np.uint32, copy=False)

    return output.astype(np.uint32, copy=False)


@register_node("Cellpose")
class Cellpose(BaseMapOverlapNode):
    CATEGORY = "WorkFlow/Segmentation"
    DISPLAY_NAME = "Cellpose1111111"

    MAP_INPUTS = ["image"]
    PRIMARY_INPUT = "image"
    PROCESS_BLOCK = cellpose_block
    ARRAY_AXES_BY_NDIM = {
        "image": {
            2: ("Y", "X"),
            3: ("Z", "Y", "X"),
            4: ("C", "Z", "Y", "X"),
            5: ("T", "C", "Z", "Y", "X"),
        }
    }

    MAP_OVERLAP_SPEC = {
        "depth": {"Z": 8, "Y": 64, "X": 64},
        "boundary": "reflect",
        "trim": True,
        "align_arrays": True,
        "allow_rechunk": True,
    }
    MAP_BLOCKS_OUTPUT_SPEC = {
        "dtype": "uint32",
        "drop_axis": "C",
        "chunks": "drop_axis_from_primary",
        "enforce_ndim": True,
    }

    @classmethod
    def INPUT_TYPES(cls):
        model_names = list_models("cellpose")
        return {
            "required": {
                "image": ("DASK_ARRAY[any]",),
                "primary_channel": ("INT", {"default": 0, "min": 0, "max": 255}),
                "secondary_channel": ("INT", {"default": -1, "min": -1, "max": 255}),
                "model_name": (
                    model_names,
                    {"default": model_names[0] if model_names else ""},
                ),
                "diameter": ("FLOAT", {"default": 0.0, "min": 0.0, "max": 500.0}),
                "flow_threshold": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0}),
                "cellprob_threshold": ("FLOAT", {"default": 0.0, "min": -6.0, "max": 6.0}),
                "gpu_batch_size": ("INT", {"default": 8, "min": 1, "max": 256}),
                "do_3d": (["auto", "true", "false"], {"default": "auto"}),
                "normalize": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "axes": ("STRING", {"default": "", "multiline": False}),
                "image_metadata": ("DICT",),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint32]",)
    RETURN_NAMES = ("mask",)

    def preprocess(
        self,
        dask_arr=None,
        array_inputs: dict | None = None,
        params: dict | None = None,
        runtime: dict | None = None,
    ) -> dict[str, Any] | None:
        image = (array_inputs or {}).get("image", dask_arr)
        params = params or {}
        axes = ChunkPlanner.normalize_axes(params.get("axes"))
        metadata = params.get("image_metadata")
        if axes is None and isinstance(metadata, dict):
            axes = ChunkPlanner.normalize_axes(metadata.get("axes"))
        if axes is None:
            return None
        axes = tuple(str(axis).upper() for axis in axes)
        if image is not None and len(axes) != int(image.ndim):
            raise ValueError(f"Cellpose axes {axes!r} length does not match image ndim={image.ndim}.")
        axes_by_name = {**(getattr(self, "_axes_by_name", {}) or {}), "image": axes}
        self._axes_by_name = axes_by_name
        return {"axes": axes, "axes_by_name": dict(axes_by_name)}
