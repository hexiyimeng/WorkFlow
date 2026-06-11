from fastapi import APIRouter

from core.config import config
from core.platform import rewrite_dashboard_url
from core.registry import get_node_info
from services.dask_service import dask_service


router = APIRouter()


@router.get("/object_info")
async def get_node_definitions():
    return get_node_info()


@router.get("/dashboard_url")
async def get_dashboard_url():
    client = dask_service.get_client()
    dashboard_link = getattr(client, "dashboard_link", None) if client else None
    return {"dashboard_url": rewrite_dashboard_url(dashboard_link, config.DASHBOARD_HOST)}
