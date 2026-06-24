from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np


PORT_DTYPE_TO_NUMPY = {
    "uint8": np.uint8,
    "uint16": np.uint16,
    "uint32": np.uint32,
    "uint64": np.uint64,
    "int16": np.int16,
    "int32": np.int32,
    "float32": np.float32,
    "float64": np.float64,
    "bool": np.bool_,
}

SPECIAL_DASK_DTYPES = {"any", "same"}
SUPPORTED_SIMPLE_PORT_TYPES = frozenset({
    "*",
    "BOOLEAN",
    "DATA_STREAM",
    "DICT",
    "FLOAT",
    "IMAGE",
    "INT",
    "LONG",
    "METADATA",
    "STRING",
    "ZARR_HANDLE",
})


@dataclass(frozen=True)
class ParsedPortType:
    raw: str
    container: str
    dtype: Optional[str] = None


def parse_port_type(port_type: str | object) -> ParsedPortType:
    raw = str(port_type or "").strip()
    prefix = "DASK_ARRAY["
    if not raw.startswith(prefix) or not raw.endswith("]"):
        return ParsedPortType(raw=raw, container=raw, dtype=None)

    dtype = raw[len(prefix):-1].strip().lower() or None
    return ParsedPortType(raw=raw, container="DASK_ARRAY", dtype=dtype)


def is_dask_array_type(port_type: str | object) -> bool:
    return parse_port_type(port_type).container == "DASK_ARRAY"


def validate_port_type(port_type: str | object) -> ParsedPortType:
    parsed = parse_port_type(port_type)
    if parsed.container == "DASK_ARRAY":
        if parsed.dtype is None:
            raise ValueError("DASK_ARRAY port type must declare a dtype.")
        if parsed.dtype not in SPECIAL_DASK_DTYPES and parsed.dtype not in PORT_DTYPE_TO_NUMPY:
            raise ValueError(f"Unsupported DASK_ARRAY dtype '{parsed.dtype}'.")
        return parsed
    if "[" in parsed.raw or "]" in parsed.raw:
        raise ValueError(
            f"Unsupported structured port type '{parsed.raw}'. "
            "Only DASK_ARRAY[dtype] ports are supported."
        )
    if parsed.raw not in SUPPORTED_SIMPLE_PORT_TYPES:
        raise ValueError(f"Unsupported port type '{parsed.raw}'.")
    return parsed


def dtype_name_to_numpy(dtype_name: str | None, input_dtype=None):
    if dtype_name is None or dtype_name == "any":
        return None
    if dtype_name == "same":
        return None if input_dtype is None else np.dtype(input_dtype)
    if dtype_name not in PORT_DTYPE_TO_NUMPY:
        raise ValueError(f"Unsupported DASK_ARRAY dtype '{dtype_name}'.")
    return np.dtype(PORT_DTYPE_TO_NUMPY[dtype_name])


def dtype_name_for_numpy(dtype) -> str:
    np_dtype = np.dtype(dtype)
    if np_dtype == np.dtype(np.bool_):
        return "bool"
    return np_dtype.name


def can_connect_types(source_type: str, target_type: str) -> Tuple[bool, Optional[str]]:
    try:
        source = validate_port_type(source_type)
        target = validate_port_type(target_type)
    except ValueError as exc:
        return False, str(exc)

    if source.container != target.container:
        return False, f"source container {source.container} does not match target container {target.container}"

    if source.container != "DASK_ARRAY":
        if source.raw == target.raw or source.raw == "*" or target.raw == "*":
            return True, None
        return False, f"source type {source.raw} does not match target type {target.raw}"

    source_dtype = source.dtype
    target_dtype = target.dtype

    if target_dtype in (None, "any"):
        return True, None

    if target_dtype == "same":
        if source_dtype in (None, "any"):
            return False, "source dtype is unknown; insert DaskTypeCast or use a typed source"
        return True, None

    if source_dtype in (None, "any"):
        return False, "source dtype is unknown; insert DaskTypeCast or use a typed source"

    if source_dtype == "same":
        return False, "source dtype is relative; insert DaskTypeCast or use a concrete typed source"

    if source_dtype == target_dtype:
        return True, None

    return False, f"source dtype {source_dtype} does not match target dtype {target_dtype}"
