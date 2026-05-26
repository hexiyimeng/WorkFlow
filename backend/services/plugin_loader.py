import os
import sys
import importlib
import logging
from typing import List, Tuple


logger = logging.getLogger("WorkFlow.Plugins")


# Critical plugins are opt-in only.
#
# The framework is provider-agnostic: Cellpose, OME-Zarr, SAM, StarDist, etc.
# are plugins, not core startup requirements.  Set WorkFlow_CRITICAL_PLUGINS
# when a deployment wants selected plugins to fail-fast, for example:
#
#   WorkFlow_CRITICAL_PLUGINS=nodes.ome_zarr_reader,nodes.ome_zarr_writer
#
CRITICAL_PLUGINS: set[str] = set()


def _get_critical_plugins() -> set[str]:
    """Return environment-configured critical plugins."""
    critical = set(CRITICAL_PLUGINS)
    env_plugins = os.getenv("WorkFlow_CRITICAL_PLUGINS", "")
    for plugin in env_plugins.split(","):
        plugin = plugin.strip()
        if plugin:
            critical.add(plugin)
    return critical


def load_all_plugins() -> Tuple[bool, List[str], List[str]]:
    """
    Recursively load Python plugins under backend/nodes.

    Returns:
        Tuple[bool, List[str], List[str]]: all_success, loaded modules, failed modules.

    Raises:
        RuntimeError: when a critical plugin fails to load.
    """
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(current_file_dir)

    # Compatibility fallback for unusual launch locations.
    if not os.path.exists(os.path.join(root_dir, "nodes")):
        root_dir = current_file_dir

    nodes_path = os.path.join(root_dir, "nodes")
    if not os.path.exists(nodes_path):
        logger.error(f"[Plugins] Nodes directory not found at: {nodes_path}")
        raise RuntimeError(f"Nodes directory not found: {nodes_path}")

    if root_dir not in sys.path:
        sys.path.append(root_dir)

    logger.info(f"[Plugins] Scanning plugins in: {nodes_path}")

    success_list = []
    failed_list = []
    critical_plugins = _get_critical_plugins()

    for root, dirs, files in os.walk(nodes_path):
        if "__pycache__" in dirs:
            dirs.remove("__pycache__")

        for filename in files:
            if not filename.endswith(".py") or filename == "__init__.py":
                continue

            file_path = os.path.join(root, filename)
            rel_path = os.path.relpath(file_path, root_dir)
            module_name = rel_path.replace(os.sep, ".")[:-3]

            try:
                importlib.import_module(module_name)
                success_list.append(module_name)
                logger.debug(f"[Plugins] Loaded: {module_name}")
            except Exception as e:
                failed_list.append(module_name)
                error_msg = str(e)
                is_critical = module_name in critical_plugins

                if is_critical:
                    logger.error(
                        f"[Plugins] CRITICAL plugin failed: {module_name}\n"
                        f"    Error: {type(e).__name__}: {error_msg}\n"
                        f"    This plugin is required for core functionality."
                    )
                    raise RuntimeError(
                        f"Critical plugin '{module_name}' failed to load: {type(e).__name__}: {error_msg}\n"
                        f"Cannot start in degraded mode. Please fix the plugin or remove it from WorkFlow_CRITICAL_PLUGINS."
                    ) from e

                logger.warning(
                    f"[Plugins] Non-critical plugin failed: {module_name}\n"
                    f"    Error: {type(e).__name__}: {error_msg}\n"
                    f"    System will continue, but some nodes may be unavailable."
                )

    total = len(success_list) + len(failed_list)
    logger.info(
        f"[Plugins] Load summary: {len(success_list)}/{total} succeeded, "
        f"{len(failed_list)} failed"
    )

    if failed_list:
        logger.warning(f"[Plugins] Failed plugins: {failed_list}")

    if success_list:
        logger.info(f"[Plugins] Successfully loaded: {success_list}")

    return len(failed_list) == 0, success_list, failed_list
