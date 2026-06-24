from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import sys
import traceback as traceback_module
from typing import Any


PLUGIN_DIAGNOSTICS: dict[str, list[dict[str, Any]]] = {
    "loaded": [],
    "failed_imports": [],
    "warnings": [],
    "node_info_errors": [],
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path_string(path: str | Path | None) -> str | None:
    if path is None:
        return None
    return str(Path(path).resolve())


def reset_plugin_diagnostics() -> None:
    for key in PLUGIN_DIAGNOSTICS:
        PLUGIN_DIAGNOSTICS[key].clear()


def clear_node_info_errors() -> None:
    PLUGIN_DIAGNOSTICS["node_info_errors"].clear()


def record_loaded_plugin(module: str, file: str | Path) -> dict[str, Any]:
    entry = {
        "module": module,
        "file": _path_string(file),
        "timestamp": _timestamp(),
    }
    PLUGIN_DIAGNOSTICS["loaded"].append(entry)
    return entry


def record_import_failure(module: str, file: str | Path, exc: BaseException) -> dict[str, Any]:
    entry = {
        "stage": "import",
        "module": module,
        "file": _path_string(file),
        "error_type": type(exc).__name__,
        "message": str(exc),
        "traceback": "".join(traceback_module.format_exception(type(exc), exc, exc.__traceback__)),
        "timestamp": _timestamp(),
    }
    PLUGIN_DIAGNOSTICS["failed_imports"].append(entry)
    return entry


def record_node_info_error(node: str, cls: type, exc: BaseException) -> dict[str, Any]:
    module_name = getattr(cls, "__module__", None)
    module = sys.modules.get(module_name) if module_name else None
    entry = {
        "stage": "object_info",
        "node": node,
        "class_name": getattr(cls, "__name__", str(cls)),
        "module": module_name,
        "file": _path_string(getattr(module, "__file__", None)),
        "error_type": type(exc).__name__,
        "message": str(exc),
        "traceback": "".join(traceback_module.format_exception(type(exc), exc, exc.__traceback__)),
        "timestamp": _timestamp(),
    }
    PLUGIN_DIAGNOSTICS["node_info_errors"].append(entry)
    return entry


def record_plugin_warning(stage: str, message: str, *, module: str | None = None, file: str | Path | None = None) -> dict[str, Any]:
    entry = {
        "stage": stage,
        "message": message,
        "module": module,
        "file": _path_string(file),
        "timestamp": _timestamp(),
    }
    PLUGIN_DIAGNOSTICS["warnings"].append(entry)
    return entry


def get_plugin_diagnostics() -> dict[str, Any]:
    loaded = deepcopy(PLUGIN_DIAGNOSTICS["loaded"])
    failed_imports = deepcopy(PLUGIN_DIAGNOSTICS["failed_imports"])
    warnings = deepcopy(PLUGIN_DIAGNOSTICS["warnings"])
    node_info_errors = deepcopy(PLUGIN_DIAGNOSTICS["node_info_errors"])
    return {
        "ok": len(failed_imports) == 0 and len(node_info_errors) == 0,
        "loaded_count": len(loaded),
        "failed_count": len(failed_imports),
        "node_info_error_count": len(node_info_errors),
        "loaded": loaded,
        "failed_imports": failed_imports,
        "warnings": warnings,
        "node_info_errors": node_info_errors,
    }
