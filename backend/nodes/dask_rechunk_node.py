from __future__ import annotations

from typing import Any

from core.registry import register_node
from nodes.base import ChunkPlanner


@register_node("DaskRechunk")
class DaskRechunk:
    """Lazy Dask Array rechunk transform. Does not compute input data."""

    PREFLIGHT_SAFE = True
    CATEGORY = "WorkFlow/Dask"
    DISPLAY_NAME = "Dask Rechunk"
    EXECUTION_RESOURCE = "cpu"
    EXECUTION_WORKERS = 1

    RETURN_TYPES = ("DASK_ARRAY[any]",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "rechunk"

    @classmethod
    def RESOLVE_RETURN_TYPES(cls, node_inputs: dict, input_types: dict | None = None):
        input_types = input_types or {}
        input_type = input_types.get("dask_arr")
        if isinstance(input_type, str) and input_type.startswith("DASK_ARRAY["):
            return (input_type,)
        return cls.RETURN_TYPES

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "mode": (["explicit", "axis_index", "axis_name", "match_reference"], {"default": "explicit"}),
            },
            "optional": {
                "chunks": ("STRING", {"default": "", "visible_when": {"mode": "explicit"}}),
                "axis_chunks": (
                    "STRING",
                    {"default": "", "visible_when": {"mode": ["axis_index", "axis_name"]}},
                ),
                "reference_arr": ("DASK_ARRAY[any]", {"visible_when": {"mode": "match_reference"}}),
                "axes": ("STRING", {"default": "", "visible_when": {"mode": "axis_name"}}),
            },
        }

    def rechunk(
        self,
        dask_arr,
        mode: str = "explicit",
        chunks: str = "",
        axis_chunks: str = "",
        reference_arr=None,
        axes: str = "",
        **kwargs: Any,
    ):
        mode = (mode or "explicit").strip().lower()

        if mode == "explicit":
            if not str(chunks or "").strip():
                raise ValueError("DaskRechunk mode 'explicit' requires non-empty chunks.")
            spec = {"explicit": chunks}
            return (ChunkPlanner.rechunk_array(dask_arr, spec, input_name="dask_arr"),)

        if mode == "axis_index":
            if not str(axis_chunks or "").strip():
                raise ValueError("DaskRechunk mode 'axis_index' requires non-empty axis_chunks.")
            spec = {"axis_index": ChunkPlanner.parse_axis_chunks(axis_chunks)}
            return (ChunkPlanner.rechunk_array(dask_arr, spec, input_name="dask_arr"),)

        if mode == "axis_name":
            normalized_axes = ChunkPlanner.normalize_axes(axes)
            if normalized_axes is None:
                raise ValueError(
                    "DaskRechunk mode 'axis_name' requires non-empty axes. "
                    "Provide axes explicitly, for example 'Z,Y,X'."
                )
            if not str(axis_chunks or "").strip():
                raise ValueError("DaskRechunk mode 'axis_name' requires non-empty axis_chunks.")
            spec = {"axis_name": ChunkPlanner.parse_axis_chunks(axis_chunks)}
            return (
                ChunkPlanner.rechunk_array(
                    dask_arr,
                    spec,
                    input_name="dask_arr",
                    axes=normalized_axes,
                ),
            )

        if mode == "match_reference":
            if reference_arr is None:
                raise ValueError("DaskRechunk mode 'match_reference' requires reference_arr.")
            spec = {"match": "reference_arr"}
            return (
                ChunkPlanner.rechunk_array(
                    dask_arr,
                    spec,
                    input_name="dask_arr",
                    all_inputs={"dask_arr": dask_arr, "reference_arr": reference_arr},
                ),
            )

        raise ValueError(f"Unsupported DaskRechunk mode {mode!r}.")
