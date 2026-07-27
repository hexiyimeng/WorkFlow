from __future__ import annotations

from core.logger import logger
from core.registry import register_node


def _resolve_end(end: int, extent: int) -> int:
    return int(extent) if int(end) == 0 else int(end)


def _validate_range(axis_name: str, start: int, end: int, extent: int) -> slice:
    start = int(start)
    resolved_end = _resolve_end(end, extent)
    if start < 0:
        raise ValueError(f"[ROI] {axis_name} start must be >= 0, got {start}.")
    if resolved_end < 0:
        raise ValueError(f"[ROI] {axis_name} end must be >= 0, got {end}.")
    if start >= resolved_end:
        raise ValueError(f"[ROI] Invalid {axis_name} range: start={start}, end={resolved_end}.")
    if start >= extent or resolved_end > extent:
        raise ValueError(
            f"[ROI] {axis_name} range [{start}:{resolved_end}] exceeds axis extent {extent}."
        )
    return slice(start, resolved_end)


@register_node("DaskROI")
class DaskROI:
    """
    Lazy ROI crop transform.

    Axis order assumptions are 2D YX, 3D ZYX, 4D CZYX, and 5D TCZYX. An end
    value of 0 means full extent for that axis. Future metadata-aware versions
    should crop by axis name rather than ndim convention.
    """

    PREFLIGHT_SAFE = True
    CATEGORY = "WorkFlow/DataProcessing"
    DISPLAY_NAME = "ROI Crop"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "start_x": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "end_x": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "start_y": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "end_y": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "start_z": ("INT", {"default": 0, "min": 0, "max": 99999}),
                "end_z": ("INT", {"default": 0, "min": 0, "max": 99999}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY[any]",)
    RETURN_NAMES = ("cropped_dask",)

    @classmethod
    def RESOLVE_RETURN_TYPES(cls, node_inputs: dict, input_types: dict | None = None):
        input_type = (input_types or {}).get("dask_arr")
        if isinstance(input_type, str) and input_type.startswith("DASK_ARRAY["):
            return (input_type,)
        return cls.RETURN_TYPES

    def execute(self, dask_arr, start_x, end_x, start_y, end_y, start_z, end_z, **kwargs):
        ndim = int(dask_arr.ndim)
        if ndim not in (2, 3, 4, 5):
            raise ValueError(f"[ROI] Supports 2D/3D/4D/5D arrays only, got ndim={ndim}.")

        logger.info(
            "[ROI] Input shape=%s ndim=%s crop x=[%s:%s] y=[%s:%s] z=[%s:%s]",
            dask_arr.shape,
            ndim,
            start_x,
            end_x,
            start_y,
            end_y,
            start_z,
            end_z,
        )

        sl_x = _validate_range("X", start_x, end_x, dask_arr.shape[-1])
        sl_y = _validate_range("Y", start_y, end_y, dask_arr.shape[-2])
        sl_z = None
        if ndim >= 3:
            z_extent = dask_arr.shape[-3]
            sl_z = _validate_range("Z", start_z, end_z, z_extent)

        if ndim == 2:
            cropped = dask_arr[sl_y, sl_x]
        elif ndim == 3:
            cropped = dask_arr[sl_z, sl_y, sl_x]
        elif ndim == 4:
            cropped = dask_arr[:, sl_z, sl_y, sl_x]
        else:
            cropped = dask_arr[:, :, sl_z, sl_y, sl_x]

        logger.info("[ROI] Output shape=%s", cropped.shape)
        return (cropped,)
