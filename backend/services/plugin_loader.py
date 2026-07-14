from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import List, Tuple

from core.plugin_diagnostics import (
    get_plugin_diagnostics,
    record_import_failure,
    record_loaded_plugin,
    record_plugin_warning,
    reset_plugin_diagnostics,
)
from core.platform import BACKEND_ROOT, nodes_dir

import logging

from core.registry import clear_node_registry


logger = logging.getLogger("WorkFlow.Plugins")


# Critical plugins are opt-in only.
#
# The framework is provider-agnostic: Cellpose, OME-Zarr, SAM, StarDist, etc.
# are plugins, not core startup requirements. Set WorkFlow_CRITICAL_PLUGINS
# when a deployment wants selected plugins to fail fast, for example:
#
#   WorkFlow_CRITICAL_PLUGINS=nodes.ome_zarr_reader,nodes.zarr_writer_node
#
CRITICAL_PLUGINS: set[str] = set()


def _get_critical_plugins() -> set[str]:
    critical = set(CRITICAL_PLUGINS)
    env_plugins = os.getenv("WorkFlow_CRITICAL_PLUGINS", "")
    for plugin in env_plugins.split(","):
        plugin = plugin.strip()
        if plugin:
            critical.add(plugin)
    return critical


def _plugin_strict_mode_enabled() -> bool:
    return str(os.getenv("WorkFlow_PLUGIN_STRICT", "")).strip().lower() in {"1", "true", "yes", "on"}


def _import_root_for_nodes(path: Path) -> Path:
    return path.resolve().parent


def _module_name_for_file(file_path: Path, import_root: Path) -> str:
    rel_path = file_path.resolve().relative_to(import_root.resolve())
    return ".".join(rel_path.with_suffix("").parts)


def _iter_plugin_files(path: Path) -> list[Path]:
    if not path.exists() or not path.is_dir():
        return []
    base_dir = (path / "base").resolve()
    return sorted(
        file_path
        for file_path in path.rglob("*.py")
        if (
            file_path.name != "__init__.py"
            and "__pycache__" not in file_path.parts
            and not file_path.resolve().is_relative_to(base_dir)
        )
    )


def _remove_plugin_modules(plugin_dir: Path, import_root: Path) -> list[str]:
    root_package = plugin_dir.resolve().relative_to(import_root.resolve()).parts[0]
    removed: list[str] = []

    for module_name in sorted(list(sys.modules), key=len, reverse=True):
        if not (module_name == root_package or module_name.startswith(f"{root_package}.")):
            continue
        if module_name == root_package:
            continue
        if module_name == f"{root_package}.base" or module_name.startswith(f"{root_package}.base."):
            continue
        sys.modules.pop(module_name, None)
        removed.append(module_name)
    if removed:
        logger.info("[PluginLoader] Removed %s plugin module(s) from sys.modules.", len(removed))
        logger.debug("[PluginLoader] Removed plugin modules: %s", removed)
    return removed


def _handle_missing_nodes(path: Path, critical_plugins: set[str]) -> Tuple[bool, List[str], List[str]]:
    if critical_plugins:
        missing = sorted(critical_plugins)
        raise RuntimeError(
            f"Nodes directory not found at {path}; critical plugins are configured and unavailable: {missing}"
        )
    message = f"Nodes directory not found at {path}; continuing with an empty registry."
    record_plugin_warning("scan", message, file=path)
    logger.warning("[PluginLoader] %s", message)
    return True, [], []


def _format_failed_imports_for_runtime_error(failures: list[dict]) -> str:
    lines = ["Plugin import failures:"]
    for failure in failures:
        lines.extend(
            [
                f"- module: {failure.get('module')}",
                f"  file: {failure.get('file')}",
                f"  error: {failure.get('error_type')}: {failure.get('message')}",
            ]
        )
    return "\n".join(lines)


def load_all_plugins() -> Tuple[bool, List[str], List[str]]:
    """
    Recursively load Python plugins under backend/nodes or WorkFlow_NODES_DIR.

    Missing or empty nodes directories are allowed for framework-only startup.
    Configured critical plugins still fail fast if unavailable or broken.
    """
    reset_plugin_diagnostics()
    plugin_dir = nodes_dir().resolve()
    critical_plugins = _get_critical_plugins()
    strict_mode = _plugin_strict_mode_enabled()

    if not plugin_dir.exists():
        return _handle_missing_nodes(plugin_dir, critical_plugins)
    if not plugin_dir.is_dir():
        raise RuntimeError(f"Nodes path is not a directory: {plugin_dir}")

    import_root = _import_root_for_nodes(plugin_dir)
    for path in (BACKEND_ROOT, import_root):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)

    logger.info("[PluginLoader] Scanning plugins in: %s", plugin_dir)

    success_list: list[str] = []
    failed_list: list[str] = []
    discovered = set()

    for file_path in _iter_plugin_files(plugin_dir):
        try:
            module_name = _module_name_for_file(file_path, import_root)
        except ValueError as exc:
            message = f"Skipping {file_path}: {exc}"
            record_plugin_warning("scan", message, file=file_path)
            logger.warning("[PluginLoader] %s", message)
            continue
        discovered.add(module_name)

        try:
            importlib.import_module(module_name)
            success_list.append(module_name)
            record_loaded_plugin(module_name, file_path)
            logger.debug("[PluginLoader] Loaded: %s", module_name)
        except Exception as exc:
            failed_list.append(module_name)
            record_import_failure(module_name, file_path, exc)
            logger.exception(
                "[PluginLoader] Failed to import node plugin: %s\n"
                "File: %s\n"
                "Error: %s: %s",
                module_name,
                file_path.resolve(),
                type(exc).__name__,
                exc,
            )

    missing_critical = sorted(critical_plugins - discovered)
    if missing_critical:
        raise RuntimeError(
            f"Critical plugins are configured but were not found under {plugin_dir}: {missing_critical}"
        )

    total = len(success_list) + len(failed_list)
    logger.info("[PluginLoader] Loaded %s plugin modules, %s failed.", len(success_list), len(failed_list))

    if failed_list:
        logger.error("[PluginLoader] Some nodes failed to load. Open /plugin_status for details.")
        failed_critical = [module for module in failed_list if module in critical_plugins]
        diagnostics = get_plugin_diagnostics()
        if strict_mode:
            raise RuntimeError(_format_failed_imports_for_runtime_error(diagnostics["failed_imports"]))
        if failed_critical:
            failures = [
                entry
                for entry in diagnostics["failed_imports"]
                if entry.get("module") in failed_critical
            ]
            raise RuntimeError(
                _format_failed_imports_for_runtime_error(failures)
                + "\nCannot start in degraded mode. Fix the critical plugin or remove it from WorkFlow_CRITICAL_PLUGINS."
            )
    if success_list:
        logger.info("[PluginLoader] Successfully loaded: %s", success_list)
    elif total == 0:
        message = f"No plugin modules found in {plugin_dir}; continuing with an empty registry."
        record_plugin_warning("scan", message, file=plugin_dir)
        logger.warning("[PluginLoader] %s", message)

    return len(failed_list) == 0, success_list, failed_list


def reload_all_plugins() -> Tuple[bool, List[str], List[str]]:
    plugin_dir = nodes_dir().resolve()
    import_root = _import_root_for_nodes(plugin_dir)

    for path in (BACKEND_ROOT, import_root):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)

    clear_node_registry()
    importlib.invalidate_caches()
    _remove_plugin_modules(plugin_dir, import_root)
    importlib.invalidate_caches()
    return load_all_plugins()
