import asyncio
import os
import mimetypes
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

# 引入项目核心组件
from services.dask_service import dask_service
from services.execution_dispatcher import (
    detach_execution_backend,
    reconcile_execution_backend,
    uses_slurm_execution_backend,
)
from services.plugin_loader import load_all_plugins
from core.state_manager import ExecutionStatus, state_manager

# 引入路由模块
from api.http_routes import router as http_router
from api.websocket import router as ws_router

from core.logger import logger


def _direct_run_bind() -> tuple[str, int]:
    """Return the explicit bind address used by ``python main.py``.

    Direct execution is primarily a development and single-user entry point.
    Defaulting it to loopback prevents an accidental unauthenticated network
    exposure; deployments that intentionally use a reverse proxy can opt in to
    another address with ``WORKFLOW_WEB_HOST``.
    """

    host = os.getenv("WORKFLOW_WEB_HOST", "127.0.0.1").strip()
    if not host:
        raise ValueError("WORKFLOW_WEB_HOST must not be empty.")

    raw_port = os.getenv("WORKFLOW_WEB_PORT", "8000").strip()
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ValueError(
            "WORKFLOW_WEB_PORT must be an integer between 1 and 65535."
        ) from exc
    if port < 1 or port > 65535:
        raise ValueError(
            "WORKFLOW_WEB_PORT must be an integer between 1 and 65535."
        )
    return host, port


# ==========================================
# 1. 核心修复：自定义静态资源托管类
# ==========================================
class TypedStaticFiles(StaticFiles):
    """
    针对 Windows 部署的增强版静态文件服务。
    强制为 .js 和 .css 文件设置正确的 MIME 类型，绕过系统注册表污染。
    """

    def file_response(self, full_path: str, stat_result: os.stat_result, scope, status_code: int = 200) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code)
        # 强制拦截并修正 MIME 类型
        if full_path.endswith(".js"):
            response.headers["Content-Type"] = "application/javascript; charset=utf-8"
        elif full_path.endswith(".css"):
            response.headers["Content-Type"] = "text/css; charset=utf-8"
        return response


# ==========================================
# 2. 生命周期管理 (lifespan)
# ==========================================
async def _shutdown_execution_runtime() -> None:
    """Release process-local runtime resources without killing remote jobs."""
    active_execution_id = state_manager.active_execution_id
    active_task = state_manager.current_task

    if uses_slurm_execution_backend():
        # Cancelling only the local monitor task while the session remains
        # RUNNING is intentionally different from cancel_execution(). The
        # Slurm adapter interprets CANCELLING as an explicit user stop and
        # would otherwise send scancel to a compute job during routine control
        # plane maintenance.
        if active_task is not None and not active_task.done():
            if active_execution_id is not None:
                detach_execution_backend(active_execution_id)
            active_task.cancel()
            await asyncio.gather(active_task, return_exceptions=True)
        if active_execution_id is not None:
            state_manager.clear_active_execution(active_execution_id)
        return

    # The local executor must persist interruption state and release its
    # active.lock before Scheduler and Workers are stopped.
    if active_execution_id is not None:
        session = state_manager.get_execution(active_execution_id)
        if session is not None and session.status in {
            ExecutionStatus.RUNNING,
            ExecutionStatus.CANCELLING,
        }:
            state_manager.cancel_execution(active_execution_id)
    if active_task is not None and not active_task.done():
        await asyncio.gather(active_task, return_exceptions=True)
    await asyncio.to_thread(dask_service.stop_cluster)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时：初始化系统状态
    state_manager.add_log(">>> Backend Starting...", "info")

    # Plugin loading must not force torch/CUDA initialization in the main
    # process; GPU-heavy libraries stay lazy inside worker-side helpers.
    # =========================================================
    # [关键步骤] 第一步：先加载插件代码 (without torch/CUDA initialization)
    # 确保主进程在 Dask Worker 启动前完成库的加载，避免 GPU 锁死
    # =========================================================
    try:
        success, loaded, failed = load_all_plugins()
        if success:
            state_manager.add_log("✅ Plugins Loaded Successfully.", "success")
        else:
            # 非关键插件失败：允许降级启动，但明确记录
            state_manager.add_log(f"⚠️ Plugins loaded with failures: {failed}", "warning")
            logger.warning(f"[Startup] Some non-critical plugins failed to load: {failed}")
    except RuntimeError as e:
        # 关键插件失败：必须中止启动，不允许半残模式
        # RuntimeError 由 plugin_loader 在检测到关键插件失败时抛出
        error_msg = str(e)
        logger.critical(f"[Startup] CRITICAL: {error_msg}")
        state_manager.add_log(f"❌ STARTUP FAILED: {error_msg}", "error")
        # 重新抛出，让 uvicorn/FastAPI 知道启动失败
        raise
    except Exception as e:
        # 其他未知异常：记录后重新抛出，保守处理
        logger.critical(f"[Startup] Unexpected error during plugin loading: {type(e).__name__}: {e}")
        state_manager.add_log(f"❌ STARTUP FAILED: {type(e).__name__}: {e}", "error")
        raise

    # A workflow's resource plan determines its Worker topology. Starting a
    # fixed pool here would happen before any DAG is available.
    if uses_slurm_execution_backend():
        startup_message = (
            "Slurm control plane ready; compute resources will be requested "
            "from each workflow Graph."
        )
    else:
        startup_message = "Dask Cluster will start when a workflow is executed."
    state_manager.add_log(startup_message, "info")
    logger.info(startup_message)

    if uses_slurm_execution_backend():
        reconciled_execution_id = await reconcile_execution_backend()
        if reconciled_execution_id is not None:
            logger.info(
                "Reattached Slurm execution %s after control-plane restart.",
                reconciled_execution_id,
            )

    try:
        yield
    finally:
        state_manager.add_log("<<< Backend Shutting down...", "warning")
        await _shutdown_execution_runtime()


# ==========================================
# 3. 创建应用实例
# ==========================================
app = FastAPI(title="WorkFlow Backend", lifespan=lifespan)

# 配置 CORS 允许前端跨域访问
# 允许多个开发端口：5173, 5174 等
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("WorkFlow_ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:5174").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册业务 API 路由
app.include_router(http_router)
app.include_router(ws_router)

# ==========================================
# 4. 前端静态资源托管 (单页应用 SPA 模式)
# ==========================================
# 定位 dist 文件夹路径（假设 dist 与 main.py 在同级目录）
dist_dir = os.path.join(os.path.dirname(__file__), "dist")

if os.path.exists(dist_dir):
    # 挂载 /assets 目录下的编译资源 (JS/CSS)
    # 使用自定义的 TypedStaticFiles 以确保 MIME 类型正确
    assets_dir = os.path.join(dist_dir, "assets")
    if os.path.exists(assets_dir):
        app.mount("/assets", TypedStaticFiles(directory=assets_dir), name="assets")


    # 处理单页应用路由：任何非 API 请求都回退到 index.html
    @app.get("/{full_path:path}")
    async def serve_react_app(full_path: str):
        file_path = os.path.join(dist_dir, full_path)

        # 如果请求的是物理存在的文件（如图标、json等），直接返回
        if os.path.exists(file_path) and os.path.isfile(file_path):
            media_type = None
            if file_path.endswith(".js"): media_type = "application/javascript"
            if file_path.endswith(".css"): media_type = "text/css"
            return FileResponse(file_path, media_type=media_type)

        # 否则返回 index.html，让 React Router 接管路由
        return FileResponse(os.path.join(dist_dir, "index.html"))
else:
    logger.warning("警告: 未检测到 'dist' 文件夹。请先在前端运行 'npm run build'。")

# ==========================================
# 5. 启动入口
# ==========================================
if __name__ == "__main__":
    import uvicorn

    # 在 Windows 上设置 MALLOC_TRIM_THRESHOLD_ 环境变量（虽然主要针对 Linux 内存回收）
    os.environ["MALLOC_TRIM_THRESHOLD_"] = "0"

    # 运行服务。默认仅监听 loopback；网络暴露必须显式配置。
    web_host, web_port = _direct_run_bind()
    uvicorn.run(app, host=web_host, port=web_port)
