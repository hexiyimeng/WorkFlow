from __future__ import annotations

import inspect
import itertools
import logging
import math
import os
from pathlib import Path
from typing import Any

import numpy as np

from core.model_registry import list_models
from core.registry import register_node
from nodes.base import BaseMapOverlapNode


logger = logging.getLogger("WorkFlow.Cellpose")

# Cellpose 4.x scales Y/X relative to the model's nominal training diameter.
# Keep this diagnostic constant local to the integration rather than importing
# Cellpose during graph construction (which must remain lightweight and must
# not initialize CUDA on the Driver).
CELLPOSE_TRAINING_DIAMETER = 30.0


def create_cellpose_model(model_ref: str, device: str):
    """Create a worker-local Cellpose model without importing torch at plugin load."""

    if device != "cuda:0":
        raise RuntimeError(
            "Cellpose models may only be created on an isolated GPU Worker "
            "using logical device 'cuda:0'; CPU fallback is not supported."
        )

    physical_gpu_id = os.getenv("WORKFLOW_PHYSICAL_GPU_ID", "unknown")
    visible_device = os.getenv("CUDA_VISIBLE_DEVICES", "")
    try:
        import torch

        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is not available")
        visible_count = int(torch.cuda.device_count())
        if visible_count != 1:
            raise RuntimeError(
                f"expected exactly one visible CUDA device, found {visible_count}"
            )
        torch.cuda.set_device(0)
    except Exception as exc:
        raise RuntimeError(
            "Cellpose GPU Worker CUDA initialization failed for physical GPU "
            f"{physical_gpu_id!r} (CUDA_VISIBLE_DEVICES={visible_device!r}): {exc}"
        ) from exc

    from cellpose import models

    device_obj = torch.device("cuda:0")
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
    if ctx.device != "cuda:0":
        raise RuntimeError(
            "Cellpose requires an isolated GPU Worker using logical "
            "device 'cuda:0'; CPU fallback is not supported."
        )

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

        explicit_diameter = float(diameter or 0.0)
        eval_kwargs: dict[str, Any] = {
            "batch_size": int(gpu_batch_size),
            "progress": None,
            "bsize": 256,
            "tile_overlap": 0.1,
            # An explicit diameter makes Cellpose rescale Y/X before inference.
            # Cellpose 4.x only restores diameter-scaled flows/masks to the
            # caller's shape when resample=True.  Keep the cheaper path when no
            # diameter scaling was requested.
            "resample": explicit_diameter > 0,
            "normalize": bool(normalize),
            "flow_threshold": float(flow_threshold),
            "cellprob_threshold": float(cellprob_threshold),
        }
        if explicit_diameter > 0:
            eval_kwargs["diameter"] = explicit_diameter
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
            # With do_3D disabled, Cellpose treats a 4D array as an NHWC batch
            # of independent 2D images.  Make the channel dimension explicit
            # for grayscale stacks so all Z planes share one network call and
            # ``batch_size`` can batch work across planes.  Cellpose's explicit
            # channel_axis path is for a single image, so let its 4D batch path
            # consume the already channels-last array.
            eval_kwargs.pop("channel_axis", None)
            if bool(normalize):
                eval_kwargs["normalize"] = {
                    "normalize": True,
                    "norm3D": False,
                }
            plane_batch = (
                segment_for_model
                if has_channels
                else segment_for_model[..., np.newaxis]
            )
            result = model.eval(np.asarray(plane_batch), **eval_kwargs)
            mask = np.asarray(
                result[0] if isinstance(result, tuple) else result,
                dtype=np.uint32,
            )

        expected_shape = tuple(
            int(size)
            for size in (
                segment_for_model.shape[:-1]
                if has_channels
                else segment_for_model.shape
            )
        )
        if mask.shape != expected_shape:
            # Cellpose squeezes its result, which may remove any singleton
            # spatial axis.  Restore only singleton dimensions; never resize,
            # crop, or otherwise reinterpret non-singleton model output.
            actual_non_singleton = tuple(size for size in mask.shape if size != 1)
            expected_non_singleton = tuple(size for size in expected_shape if size != 1)
            if actual_non_singleton == expected_non_singleton:
                mask = mask.reshape(expected_shape)
        if mask.shape != expected_shape:
            raise ValueError(
                "Cellpose returned mask shape "
                f"{mask.shape}, expected input-block spatial shape {expected_shape}."
            )

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
    DISPLAY_NAME = "Cellpose"
    EXECUTION_RESOURCE = "gpu"
    EXECUTION_WORKERS = 1

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
        "chunks": "same_as_primary",
        "enforce_ndim": True,
    }

    def preprocess(self, dask_arr=None, params=None, runtime=None):
        """Log one graph-level estimate of the largest model input block."""

        del runtime
        if dask_arr is None:
            return {}

        params = dict(params or {})
        axes = tuple(
            str(axis).upper()
            for axis in ((self._axes_by_name or {}).get(self.PRIMARY_INPUT) or ())
        )
        if len(axes) != int(dask_arr.ndim):
            return {}

        from dask.array.overlap import ensure_minimum_chunksize

        halo_block = []
        for axis, axis_chunks, axis_length in zip(
            axes,
            dask_arr.chunks,
            dask_arr.shape,
        ):
            configured_depth = int(self.MAP_OVERLAP_SPEC["depth"].get(axis, 0))
            depth = min(configured_depth, max(0, int(axis_length)))
            normalized_chunks = tuple(int(chunk) for chunk in axis_chunks)
            if self.MAP_OVERLAP_SPEC.get("allow_rechunk", False) and depth:
                # Match Dask's own overlap rechunk rule.  Merely adding the
                # halo to the original maximum chunk underestimates the model
                # block when chunks smaller than ``depth`` are merged first.
                normalized_chunks = ensure_minimum_chunksize(
                    depth,
                    normalized_chunks,
                )
            halo_block.append(max(normalized_chunks) + 2 * depth)

        diameter = float(params.get("diameter") or 0.0)
        scale = CELLPOSE_TRAINING_DIAMETER / diameter if diameter > 0 else 1.0
        estimated_model_block = tuple(
            int(math.ceil(length * scale)) if axis in {"Y", "X"} else int(length)
            for axis, length in zip(axes, halo_block)
            if axis != "C"
        )
        logger.info(
            "[Cellpose] input_shape=%s input_chunks=%s axes=%s "
            "halo_max_block=%s diameter=%s estimated_model_block=%s do_3d=%s",
            tuple(int(length) for length in dask_arr.shape),
            dask_arr.chunks,
            axes,
            tuple(halo_block),
            diameter,
            estimated_model_block,
            params.get("do_3d", "auto"),
        )
        if scale > 1.0:
            logger.warning(
                "[Cellpose] diameter=%s scales Y/X by about %.2fx "
                "(%.2fx pixels per plane); reduce the source Y/X chunk sizes "
                "if host-memory pressure persists.",
                diameter,
                scale,
                scale * scale,
            )
        return {}

    def infer_output_spec(self, array_inputs, _params, primary_name):
        """Drop a declared channel axis while preserving channel-less inputs."""

        axes = tuple(
            str(axis).upper()
            for axis in ((self._axes_by_name or {}).get(primary_name) or ())
        )
        if "C" not in axes:
            return self.MAP_BLOCKS_OUTPUT_SPEC
        return {
            "dtype": "uint32",
            "drop_axis": axes.index("C"),
            "chunks": "drop_axis_from_primary",
            "enforce_ndim": True,
        }

    def infer_overlap_spec(
        self,
        *,
        array_inputs,
        ordered_names,
        primary_name,
        params,
        runtime,
    ):
        """Apply only halos whose named spatial axes exist on this input."""

        del ordered_names, params, runtime
        primary = array_inputs[primary_name]
        axes = tuple(
            str(axis).upper()
            for axis in ((self._axes_by_name or {}).get(primary_name) or ())
        )
        axis_lookup = {axis: index for index, axis in enumerate(axes)}
        depth_by_axis = {}
        for axis, configured_depth in self.MAP_OVERLAP_SPEC["depth"].items():
            if axis not in axis_lookup:
                continue
            axis_length = int(primary.shape[axis_lookup[axis]])
            # Dask rejects overlap depth greater than a complete axis.  Clamp
            # short axes while retaining as much reflected context as possible.
            depth_by_axis[axis] = min(
                int(configured_depth),
                max(0, axis_length),
            )
        return {
            **self.MAP_OVERLAP_SPEC,
            "depth": depth_by_axis,
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
        }

    RETURN_TYPES = ("DASK_ARRAY[uint32]",)
    RETURN_NAMES = ("mask",)
