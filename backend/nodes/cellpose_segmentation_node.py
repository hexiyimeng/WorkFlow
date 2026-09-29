"""Untrimmed Cellpose segmentation node (halo-preserving).

Mirrors scripts/segmentation.py: every output block keeps its overlap halo
instead of being trimmed, so the downstream process_segmentation node can
compare the two labelings of each duplicated seam region and merge them
locally.  Attach the produced mask to process_segmentation, never directly
to a writer - the untrimmed blocks tile a larger-than-image array.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import numpy as np

from core.model_registry import (
    KNOWN_MODEL_EXTENSIONS,
    list_models,
    register_model_search_root,
)
from core.registry import register_node
from nodes.base import BaseMapOverlapNode
from nodes.cellpose_node import (
    INCOMPATIBLE_CPSAM_MODEL_NAMES,
    cellpose_block,
)


SEG_HALO_METADATA_ATTR = "_workflow_seg_halo"
SEG_SPATIAL_AXES = ("Z", "Y", "X")

# Node-local model directory: every model file (or model directory) placed
# under backend/nodes/cellpose_segmentation/ becomes selectable in this
# node.  The directory is registered as an extra cellpose search root, so
# bare model names found here resolve through the shared model registry.
SEG_MODELS_DIR = Path(__file__).resolve().parent / "cellpose_segmentation"
register_model_search_root("cellpose", SEG_MODELS_DIR)

# Legacy Cellpose 3 model used by scripts/segmentation.py
# (pretrained_model='../models/199082').
SEG_SCRIPT_MODEL_NAME = "199082"
SEG_SCRIPT_MODEL_DIAM_MEAN = 15.0


def list_seg_models() -> list[str]:
    """Model names found in the node-local cellpose_segmentation directory."""

    names: set[str] = set()
    if not SEG_MODELS_DIR.is_dir():
        return []
    for item in SEG_MODELS_DIR.iterdir():
        if item.name.startswith("."):
            continue
        if item.is_dir():
            names.add(item.name)
        elif item.is_file():
            names.add(
                item.stem
                if item.suffix.lower() in KNOWN_MODEL_EXTENSIONS
                else item.name
            )
    return sorted(names)


def attach_seg_halo_metadata(array: Any, metadata: Mapping[str, Any]) -> Any:
    setattr(array, SEG_HALO_METADATA_ATTR, dict(metadata))
    return array


def get_seg_halo_metadata(array: Any) -> dict[str, Any] | None:
    value = getattr(array, SEG_HALO_METADATA_ATTR, None)
    return dict(value) if isinstance(value, Mapping) else None


@register_node("cellpose_segmentation")
class CellposeSegmentation(BaseMapOverlapNode):
    """Cellpose segmentation whose output blocks keep their halo (untrimmed).

    Feed the mask output to process_segmentation, which merges the
    overlapping labelings and trims back to the original chunking.
    """

    CATEGORY = "WorkFlow/Segmentation"
    DISPLAY_NAME = "Cellpose Segmentation (Untrimmed)"
    required_worker_profile = "GPU"

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
        "depth": {},
        "boundary": "reflect",
        "trim": False,
        "align_arrays": True,
        "allow_rechunk": True,
    }
    ALLOW_UNTRIMMED_OVERLAP_OUTPUT = True

    @staticmethod
    def _requested_depths(params: Mapping[str, Any] | None) -> dict[str, int]:
        params = params or {}
        return {
            "Z": max(0, int(params.get("overlap_z", 30))),
            "Y": max(0, int(params.get("overlap_y", 30))),
            "X": max(0, int(params.get("overlap_x", 30))),
        }

    @classmethod
    def _effective_depths(
        cls,
        primary: Any,
        axes: tuple[str, ...],
        params: Mapping[str, Any] | None,
    ) -> dict[int, int]:
        """Requested halo per input axis index, clamped to the axis length."""

        requested = cls._requested_depths(params)
        depth_by_index: dict[int, int] = {}
        for index, axis in enumerate(axes):
            depth = int(requested.get(axis, 0))
            if depth > 0:
                depth = min(depth, int(primary.shape[index]))
            depth_by_index[index] = depth
        return depth_by_index

    def _primary_axes(self) -> tuple[str, ...]:
        return tuple(
            str(axis).upper()
            for axis in ((self._axes_by_name or {}).get(self.PRIMARY_INPUT) or ())
        )

    def infer_overlap_spec(
        self,
        *,
        array_inputs,
        ordered_names,
        primary_name,
        params,
        runtime,
    ):
        del ordered_names, runtime
        depth = self._effective_depths(
            array_inputs[primary_name], self._primary_axes(), params
        )
        return {**self.MAP_OVERLAP_SPEC, "depth": depth}

    def infer_output_spec(self, array_inputs, params, primary_name):
        from dask.array.overlap import ensure_minimum_chunksize

        primary = array_inputs[primary_name]
        axes = self._primary_axes()
        depth_by_index = self._effective_depths(primary, axes, params)
        drop_axis = axes.index("C") if "C" in axes else None
        chunks = []
        for index, axis in enumerate(axes):
            if drop_axis is not None and index == drop_axis:
                continue
            depth = depth_by_index.get(index, 0)
            axis_chunks = tuple(int(chunk) for chunk in primary.chunks[index])
            if depth > 0:
                # Mirror dask's overlap rechunk so the declared chunks match
                # the blocks the map function actually receives.
                axis_chunks = ensure_minimum_chunksize(depth, axis_chunks)
            chunks.append(tuple(chunk + 2 * depth for chunk in axis_chunks))
        spec: dict[str, Any] = {
            "dtype": "uint32",
            "chunks": tuple(chunks),
            "enforce_ndim": True,
        }
        if drop_axis is not None:
            spec["drop_axis"] = drop_axis
        return spec

    def execute(self, **kwargs):
        invocation = self.get_invocation(kwargs)
        (masks,) = super().execute(**kwargs)
        axes = self._primary_axes()
        output_axes = tuple(axis for axis in axes if axis != "C")
        image = invocation.inputs.get(self.PRIMARY_INPUT)
        depth_by_index = (
            self._effective_depths(image, axes, invocation.inputs)
            if image is not None
            else {}
        )
        attach_seg_halo_metadata(
            masks,
            {
                "version": 1,
                "axes": output_axes,
                "depth": {
                    axes[index]: int(depth)
                    for index, depth in depth_by_index.items()
                    if axes[index] in output_axes and axes[index] in SEG_SPATIAL_AXES
                },
                "boundary": "reflect",
            },
        )
        self.assert_lazy_collection(masks)
        return (masks,)

    @classmethod
    def INPUT_TYPES(cls):
        seg_models = list_seg_models()
        installed_models = list_models("cellpose")
        model_names = list(
            dict.fromkeys([
                *seg_models,
                "cpsam",
                "cpdino",
                "cpdino-vitb",
                *(
                    name
                    for name in installed_models
                    if name.lower() not in INCOMPATIBLE_CPSAM_MODEL_NAMES
                ),
            ])
        )
        default_model = (
            SEG_SCRIPT_MODEL_NAME
            if SEG_SCRIPT_MODEL_NAME in model_names
            else (seg_models[0] if seg_models else "cpsam")
        )
        return {
            "required": {
                "image": ("DASK_ARRAY[any]",),
                "primary_channel": ("INT", {"default": 0, "min": 0, "max": 255}),
                "secondary_channel": ("INT", {"default": 1, "min": -1, "max": 255}),
                "model_name": (
                    model_names,
                    {"default": default_model},
                ),
                # scripts/segmentation.py builds this model with diam_mean=15.
                # Ignored by Cellpose v4 runtimes; leave 0 to keep the
                # library default for CPSAM models.
                "diam_mean": ("FLOAT", {"default": SEG_SCRIPT_MODEL_DIAM_MEAN, "min": 0.0, "max": 500.0}),
                "diameter": ("FLOAT", {"default": 15.0, "min": 0.0, "max": 500.0}),
                "flow_threshold": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0}),
                "cellprob_threshold": ("FLOAT", {"default": 0.0, "min": -6.0, "max": 6.0}),
                "gpu_batch_size": ("INT", {"default": 2, "min": 1, "max": 256}),
                "do_3d": (["auto", "true", "false"], {"default": "true"}),
                "normalize": ("BOOLEAN", {"default": True}),
                "overlap_z": ("INT", {"default": 30, "min": 0, "max": 4096}),
                "overlap_y": ("INT", {"default": 30, "min": 0, "max": 16384}),
                "overlap_x": ("INT", {"default": 30, "min": 0, "max": 16384}),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint32]",)
    RETURN_NAMES = ("mask",)
