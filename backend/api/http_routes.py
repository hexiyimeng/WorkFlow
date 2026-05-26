from fastapi import APIRouter
from core.registry import get_node_info
from services.dask_service import dask_service
from core.config import config

router = APIRouter()

@router.get("/object_info")
async def get_node_definitions():
    return get_node_info()

@router.get("/dashboard_url")
async def get_dashboard_url():
    """
    获取 Dask Dashboard 的 URL。

    优先级：
    1. 环境变量 WorkFlow_DASHBOARD_HOST（用于远程部署/反向代理场景）
    2. 原始 dashboard_link（适合本地开发）
    """
    client = dask_service.get_client()
    if client and client.dashboard_link:
        # 检查是否配置了自定义 host
        custom_host = config.DASHBOARD_HOST
        if custom_host:
            # 提取端口号并替换 host
            try:
                if ":" in client.dashboard_link:
                    parts = client.dashboard_link.split(":")
                    if len(parts) >= 3:
                        port = parts[2].split("/")[0]
                        return {"dashboard_url": f"{custom_host}:{port}"}
            except Exception:
                pass
            return {"dashboard_url": custom_host}
        # 默认返回原始 dashboard_link（适合本地开发）
        return {"dashboard_url": client.dashboard_link}
    return {"dashboard_url": None}