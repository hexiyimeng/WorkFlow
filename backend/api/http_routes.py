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
from services.plugin_loader import reload_all_plugins


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
        client = None

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
            try:
                client = dask_service.start_cluster()
            except Exception:
                client = None
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
                    "dashboard_url": _dashboard_url_for_client(client),
                },
            )

        try:
            client = dask_service.start_cluster()
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "code": "DASK_RESTART_FAILED",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "loaded": loaded,
                    "failed": failed,
                    "plugin_status": plugin_status,
                    "object_info": object_info,
                    "dashboard_url": None,
                },
            )

        dashboard_url = _dashboard_url_for_client(client)
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
                    "dashboard_url": dashboard_url,
                },
            )

        if client is None:
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "code": "DASK_RESTART_FAILED",
                    "error_type": "RuntimeError",
                    "error_message": "Dask cluster restart failed.",
                    "loaded": loaded,
                    "failed": failed,
                    "plugin_status": plugin_status,
                    "object_info": object_info,
                    "dashboard_url": dashboard_url,
                },
            )

        return {
            "ok": True,
            "loaded": loaded,
            "failed": failed,
            "plugin_status": plugin_status,
            "object_info": object_info,
            "dashboard_url": dashboard_url,
        }
