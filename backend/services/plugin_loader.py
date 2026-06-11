from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import List, Tuple

from core.platform import BACKEND_ROOT, nodes_dir

import logging


logger = logging.getLogger("WorkFlow.Plugins")


# Critical plugins are opt-in only.
#
# The framework is provider-agnostic: Cellpose, OME-Zarr, SAM, StarDist, etc.
# are plugins, not core startup requirements. Set WorkFlow_CRITICAL_PLUGINS
# when a deployment wants selected plugins to fail fast, for example:
#
#   WorkFlow_CRITICAL_PLUGINS=nodes.ome_zarr_reader,nodes.ome_zarr_writer
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


def _import_root_for_nodes(path: Path) -> Path:
    return path.resolve().parent


def _module_name_for_file(file_path: Path, import_root: Path) -> str:
    rel_path = file_path.resolve().relative_to(import_root.resolve())
    return ".".join(rel_path.with_suffix("").parts)


def _iter_plugin_files(path: Path) -> list[Path]:
    if not path.exists() or not path.is_dir():
        return []
    return sorted(
        file_path
        for file_path in path.rglob("*.py")
        if file_path.name != "__init__.py" and "__pycache__" not in file_path.parts
    )


def _handle_missing_nodes(path: Path, critical_plugins: set[str]) -> Tuple[bool, List[str], List[str]]:
    if critical_plugins:
        missing = sorted(critical_plugins)
        raise RuntimeError(
            f"Nodes directory not found at {path}; critical plugins are configured and unavailable: {missing}"
        )
    logger.warning("[Plugins] Nodes directory not found at %s; continuing with an empty registry.", path)
    return True, [], []


def load_all_plugins() -> Tuple[bool, List[str], List[str]]:
    """
    Recursively load Python plugins under backend/nodes or WorkFlow_NODES_DIR.

    Missing or empty nodes directories are allowed for framework-only startup.
    Configured critical plugins still fail fast if unavailable or broken.
    """
    plugin_dir = nodes_dir().resolve()
    critical_plugins = _get_critical_plugins()

    if not plugin_dir.exists():
        return _handle_missing_nodes(plugin_dir, critical_plugins)
    if not plugin_dir.is_dir():
        raise RuntimeError(f"Nodes path is not a directory: {plugin_dir}")

    import_root = _import_root_for_nodes(plugin_dir)
    for path in (BACKEND_ROOT, import_root):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)

    logger.info("[Plugins] Scanning plugins in: %s", plugin_dir)

    success_list: list[str] = []
    failed_list: list[str] = []
    discovered = set()

    for file_path in _iter_plugin_files(plugin_dir):
        try:
            module_name = _module_name_for_file(file_path, import_root)
        except ValueError as exc:
            logger.warning("[Plugins] Skipping %s: %s", file_path, exc)
            continue
        discovered.add(module_name)

        try:
            importlib.import_module(module_name)
            success_list.append(module_name)
            logger.debug("[Plugins] Loaded: %s", module_name)
        except Exception as exc:
            failed_list.append(module_name)
            is_critical = module_name in critical_plugins

            if is_critical:
                logger.error(
                    "[Plugins] CRITICAL plugin failed: %s\n"
                    "    Error: %s: %s\n"
                    "    This plugin is required for this deployment.",
                    module_name,
                    type(exc).__name__,
                    exc,
                )
                raise RuntimeError(
                    f"Critical plugin '{module_name}' failed to load: {type(exc).__name__}: {exc}\n"
                    "Cannot start in degraded mode. Fix the plugin or remove it from WorkFlow_CRITICAL_PLUGINS."
                ) from exc

            logger.warning(
                "[Plugins] Non-critical plugin failed: %s\n"
                "    Error: %s: %s\n"
                "    System will continue, but some nodes may be unavailable.",
                module_name,
                type(exc).__name__,
                exc,
            )

    missing_critical = sorted(critical_plugins - discovered)
    if missing_critical:
        raise RuntimeError(
            f"Critical plugins are configured but were not found under {plugin_dir}: {missing_critical}"
        )

    total = len(success_list) + len(failed_list)
    logger.info("[Plugins] Load summary: %s/%s succeeded, %s failed", len(success_list), total, len(failed_list))

    if failed_list:
        logger.warning("[Plugins] Failed plugins: %s", failed_list)
    if success_list:
        logger.info("[Plugins] Successfully loaded: %s", success_list)
    elif total == 0:
        logger.warning("[Plugins] No plugin modules found in %s; continuing with an empty registry.", plugin_dir)

    return len(failed_list) == 0, success_list, failed_list
