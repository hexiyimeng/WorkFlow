import asyncio
import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core.config import config
from core.platform import rewrite_dashboard_url
from core.plugin_diagnostics import get_plugin_diagnostics
from core.registry import get_node_info
from core.state_manager import state_manager
from services.dask_service import dask_service
from services.execution_dispatcher import preflight_graph
from services.plugin_loader import reload_all_plugins
from services.recovery_service import (
    RecoveryRecordChangedError,
    delete_recovery_record,
    inspect_recovery_directory,
    list_directories,
    open_recovery_directory,
)


router = APIRouter()
_reload_lock = asyncio.Lock()


def _hot_reload_enabled() -> bool:
    return str(os.getenv("WorkFlow_ENABLE_HOT_RELOAD", "")).strip().lower() in {"1", "true", "yes", "on"}


def _dashboard_url_for_client(client) -> str | None:
    dashboard_link = getattr(client, "dashboard_link", None) if client else None
    return rewrite_dashboard_url(dashboard_link, config.DASHBOARD_HOST)


@router.get("/object_info")
async def get_node_definitions():
    return get_node_info()


@router.get("/plugin_status")
async def get_plugin_status():
    return get_plugin_diagnostics()


@router.get("/dashboard_url")
async def get_dashboard_url():
    client = dask_service.get_client()
    return {"dashboard_url": _dashboard_url_for_client(client)}


@router.post("/execution/preflight")
async def execution_preflight(payload: dict):
    """Inspect lazy execution-root metadata without reserving an execution slot."""
    graph = payload.get("graph") if isinstance(payload, dict) else None
    if not isinstance(graph, dict):
        return JSONResponse(
            status_code=400,
            content={
                "windowable": False,
                "message": "Preflight requires a graph object.",
            },
        )
    try:
        # Plugin reload replaces the registry in place.  Keep one coherent set
        # of node classes for the complete lazy metadata build.
        async with _reload_lock:
            execution_config = payload.get("executionConfig")
            if execution_config is None and payload.get("windowShape") is not None:
                execution_config = {
                    "mode": "window",
                    "windowShape": payload["windowShape"],
                }
            return await preflight_graph(
                graph,
                execution_config,
                worker_profiles=payload.get("workerProfiles"),
                worker_pools=payload.get("workerPools"),
            )
    except Exception as exc:
        return JSONResponse(
            status_code=400,
            content={
                "windowable": False,
                "message": str(exc),
                "error_type": type(exc).__name__,
            },
        )


def _recovery_directory_from_payload(payload: dict) -> str:
    if not isinstance(payload, dict):
        raise ValueError("Request body must be an object.")
    directory = payload.get("recoveryDirectory")
    if not isinstance(directory, str) or not directory.strip():
        raise ValueError("recoveryDirectory must be a non-empty absolute path.")
    return directory


def _recovery_error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, PermissionError):
        status_code = 403
        code = "DIRECTORY_ACCESS_FAILED"
        message = "The requested server directory could not be accessed."
    elif isinstance(exc, (FileNotFoundError, NotADirectoryError)):
        status_code = 404
        code = "RECOVERY_NOT_FOUND"
        message = str(exc)
    elif isinstance(exc, RecoveryRecordChangedError):
        status_code = 409
        code = "RECOVERY_CHANGED"
        message = str(exc)
    elif isinstance(exc, RuntimeError):
        status_code = 409
        code = "RECOVERY_ACTIVE"
        message = str(exc)
    else:
        status_code = 400
        code = "INVALID_RECOVERY_DIRECTORY"
        message = str(exc)
    return JSONResponse(
        status_code=status_code,
        content={
            "found": False,
            "valid": False,
            "compatible": False,
            "code": code,
            "message": message,
            "errorType": type(exc).__name__,
        },
    )


@router.post("/execution/recovery/inspect")
async def execution_recovery_inspect(payload: dict):
    """Validate recovery files and return progress without starting execution."""

    try:
        directory = _recovery_directory_from_payload(payload)
        async with _reload_lock:
            inspection = inspect_recovery_directory(directory)
        return inspection.to_summary()
    except Exception as exc:
        return _recovery_error_response(exc)


@router.post("/execution/recovery/open")
async def execution_recovery_open(payload: dict):
    """Open the immutable saved graph for read-only frontend display."""

    try:
        directory = _recovery_directory_from_payload(payload)
        async with _reload_lock:
            return open_recovery_directory(directory)
    except Exception as exc:
        return _recovery_error_response(exc)


@router.post("/execution/recovery/delete")
async def execution_recovery_delete(payload: dict):
    """Delete one inactive recovery record without deleting its outputs."""

    try:
        directory = _recovery_directory_from_payload(payload)
        expected_execution_id = payload.get("expectedExecutionId")
        async with _reload_lock:
            return delete_recovery_record(
                directory,
                expected_execution_id=expected_execution_id,
            )
    except Exception as exc:
        return _recovery_error_response(exc)


@router.get("/filesystem/directories")
async def filesystem_directories(path: str | None = None):
    """Browse server directories available to the recovery service."""

    try:
        return list_directories(path)
    except Exception as exc:
        return _recovery_error_response(exc)


@router.post("/reload_nodes")
async def reload_nodes():
    if not _hot_reload_enabled():
        return JSONResponse(
            status_code=403,
            content={
                "ok": False,
                "code": "HOT_RELOAD_DISABLED",
                "message": "Node hot reload is disabled. Set WorkFlow_ENABLE_HOT_RELOAD=1 to enable it.",
            },
        )

    if _reload_lock.locked():
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "code": "RELOAD_IN_PROGRESS",
                "message": "Node hot reload is already running.",
            },
        )

    async with _reload_lock:
        if state_manager.is_running:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "code": "EXECUTION_ACTIVE",
                    "message": "Cannot reload nodes while an execution is running.",
                },
            )

        loaded: list[str] = []
        failed: list[str] = []
        object_info = {}
        plugin_status = None

        dask_service.stop_cluster()
        try:
            success, loaded, failed = reload_all_plugins()
            object_info = get_node_info()
            plugin_status = get_plugin_diagnostics()
        except Exception as exc:
            plugin_status = get_plugin_diagnostics()
            try:
                object_info = get_node_info()
                plugin_status = get_plugin_diagnostics()
            except Exception:
                object_info = {}
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "code": "PLUGIN_RELOAD_FAILED",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "loaded": loaded,
                    "failed": failed,
                    "plugin_status": plugin_status,
                    "object_info": object_info,
                    "dashboard_url": None,
                },
            )

        if not success:
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "code": "PLUGIN_RELOAD_FAILED",
                    "error_type": "PluginReloadError",
                    "error_message": "One or more node plugins failed to reload.",
                    "loaded": loaded,
                    "failed": failed,
                    "plugin_status": plugin_status,
                    "object_info": object_info,
                    "dashboard_url": None,
                },
            )

        return {
            "ok": True,
            "loaded": loaded,
            "failed": failed,
            "plugin_status": plugin_status,
            "object_info": object_info,
            "dashboard_url": None,
        }
