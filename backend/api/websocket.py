import asyncio
import ipaddress
import os
import uuid
from urllib.parse import urlsplit

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from core.logger import logger
from core.state_manager import state_manager, ExecutionStatus
from core.window_execution import (
    parse_execution_config,
    require_window_recovery_location,
)
from services.execution_dispatcher import (
    execute_graph,
    uses_slurm_execution_backend,
)
from services.dask_service import dask_service

router = APIRouter()

_DEFAULT_ALLOWED_ORIGINS = "http://localhost:5173,http://localhost:5174"


def _origin_identity(value: str) -> tuple[str, str, int | None] | None:
    """Normalize an HTTP Origin for exact allow-list comparison."""

    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except (TypeError, ValueError):
        return None
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.hostname:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return None
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None
    return scheme, parsed.hostname.lower(), port


def _host_authority(
    value: str,
    *,
    origin_scheme: str,
) -> tuple[str, int | None] | None:
    """Normalize a Host header without trusting forwarded-host headers."""

    try:
        parsed = urlsplit(f"//{value.strip()}")
        port = parsed.port
    except (TypeError, ValueError):
        return None
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        return None
    if (origin_scheme == "http" and port == 80) or (
        origin_scheme == "https" and port == 443
    ):
        port = None
    return parsed.hostname.lower(), port


def _is_loopback_client(websocket: WebSocket) -> bool:
    client = getattr(websocket, "client", None)
    host = getattr(client, "host", "") if client is not None else ""
    if str(host).lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(str(host)).is_loopback
    except ValueError:
        return False


def _websocket_origin_allowed(websocket: WebSocket) -> bool:
    headers = getattr(websocket, "headers", None)
    origin = headers.get("origin") if headers is not None else None
    if not origin:
        # Browser clients always send Origin. Keep local health checks and CLI
        # clients usable through the loopback-only control-plane endpoint,
        # while denying a remote no-Origin bypass.
        return _is_loopback_client(websocket)

    origin_identity = _origin_identity(origin)
    if origin_identity is None:
        return False

    host = headers.get("host") if headers is not None else None
    host_authority = (
        _host_authority(host, origin_scheme=origin_identity[0])
        if host
        else None
    )
    if host_authority == origin_identity[1:]:
        return True

    configured_origins = os.getenv(
        "WorkFlow_ALLOWED_ORIGINS",
        _DEFAULT_ALLOWED_ORIGINS,
    )
    allowed_identities = {
        identity
        for configured in configured_origins.split(",")
        if (identity := _origin_identity(configured)) is not None
    }
    return origin_identity in allowed_identities


@router.websocket("/ws/run")
async def websocket_endpoint(websocket: WebSocket):
    client_ip = websocket.client.host if websocket.client else "unknown"

    if not _websocket_origin_allowed(websocket):
        headers = getattr(websocket, "headers", None)
        origin = headers.get("origin") if headers is not None else None
        logger.warning(
            "Rejected WebSocket origin %r from %s.",
            origin,
            client_ip,
        )
        await websocket.close(code=1008, reason="WebSocket origin is not allowed.")
        return

    # ========== accept 连接 ==========
    try:
        await asyncio.wait_for(websocket.accept(), timeout=60)
    except asyncio.TimeoutError:
        return

    # 心跳配置
    HEARTBEAT_INTERVAL = 30  # 秒

    # 心跳任务
    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            try:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json({"type": "ping"})
            except Exception:
                break

    heartbeat_task = asyncio.create_task(heartbeat())

    # 1. 连接初始化
    try:
        # A Slurm control plane never owns the per-execution Dask cluster.
        # The compute-node runner reports its cluster state through relayed
        # execution events instead.
        if uses_slurm_execution_backend():
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "type": "log",
                    "message": "[System] Slurm execution control plane ready."
                })
        else:
            client = dask_service.get_client()
            if client and websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "type": "log",
                    "message": f"[System] Dask Cluster Connected: {client.dashboard_link}"
                })
    except Exception as e:
        logger.warning(f"WebSocket initialization failed for {client_ip}: {e}")
        state_manager.unsubscribe_client(websocket)
        heartbeat_task.cancel()
        return

    # 2. 消息监听主循环
    try:
        while True:
            try:
                timeout = 30
                data = await asyncio.wait_for(websocket.receive_json(), timeout=timeout)
                command = data.get("command")

                if command == "execute_graph":
                    try:
                        execution_config = parse_execution_config(data.get("executionConfig"))
                        require_window_recovery_location(execution_config)
                    except ValueError as exc:
                        await websocket.send_json({
                            "type": "error",
                            "message": str(exc),
                        })
                        continue

                    graph = data.get("graph")
                    recovery_location = execution_config.recovery_location
                    is_direct_custom_recovery = (
                        execution_config.mode == "window"
                        and execution_config.resume_action in {"resume", "restart"}
                        and recovery_location is not None
                        and recovery_location.mode == "custom"
                    )
                    if not isinstance(graph, dict):
                        if is_direct_custom_recovery and graph is None:
                            graph = {}
                        else:
                            await websocket.send_json({
                                "type": "error",
                                "message": "Received invalid graph",
                            })
                            continue
                    if not graph and not is_direct_custom_recovery:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Received empty graph",
                        })
                        continue

                    # Generate or use provided execution_id
                    execution_id = data.get("executionId") or uuid.uuid4().hex

                    # --- Idempotent: if this execution_id is already active, subscribe only ---
                    existing_session = state_manager.get_execution(execution_id)
                    if existing_session and not ExecutionStatus.is_finished(existing_session.status):
                        state_manager.subscribe_client(execution_id, websocket)
                        logger.info(f"[WebSocket] execution_id={execution_id} already active, subscribing client")
                        await state_manager.sync_history_to_client(websocket, execution_id)
                        await websocket.send_json({
                            "type": "subscribed",
                            "executionId": execution_id
                        })
                        continue

                    # --- Active execution guard: historical executions are kept, but only
                    # one RUNNING/CANCELLING execution may own the active slot.
                    try:
                        state_manager.start_execution(execution_id)
                    except RuntimeError as exc:
                        logger.warning(f"[WebSocket] execution rejected: active execution already running, "
                                       f"client={client_ip}, requested_execution_id={execution_id}")
                        await websocket.send_json({
                            "type": "execution_rejected",
                            "status": "rejected",
                            "code": "TASK_ALREADY_RUNNING",
                            "message": str(exc) or "Another execution is already running"
                        })
                        continue

                    # Create execution session and subscribe client
                    state_manager.subscribe_client(execution_id, websocket)

                    logger.info(f"Executing graph for {client_ip}, execution_id={execution_id}")

                    # Bind the task before acknowledging the run.  If the socket
                    # drops while the acknowledgement is in flight, the durable
                    # execution id can still be used to subscribe after reconnect.
                    start_gate = asyncio.Event()

                    async def execute_after_ack(
                        execution_graph=graph,
                        attached_execution_id=execution_id,
                        attached_config=execution_config,
                        attached_worker_profiles=data.get("workerProfiles"),
                        attached_worker_pools=data.get("workerPools"),
                        gate=start_gate,
                    ):
                        await gate.wait()
                        return await execute_graph(
                            execution_graph,
                            attached_execution_id,
                            attached_config,
                            worker_profiles=attached_worker_profiles,
                            worker_pools=attached_worker_pools,
                        )

                    execution_task = asyncio.create_task(execute_after_ack())
                    if not state_manager.attach_execution_task(execution_id, execution_task):
                        execution_task.cancel()
                        await asyncio.gather(execution_task, return_exceptions=True)
                        state_manager.set_execution_status(
                            execution_id,
                            ExecutionStatus.FAILED,
                        )
                        await websocket.send_json({
                            "type": "error",
                            "message": "Failed to bind execution task"
                        })
                        continue

                    # Send executionId to frontend only after the active session
                    # owns a cancellable task.
                    try:
                        await websocket.send_json({
                            "type": "execution_started",
                            "executionId": execution_id
                        })
                    finally:
                        # A lost acknowledgement must not discard a durable
                        # execution. The saved execution ID can subscribe after
                        # reconnect, while a connected client always observes
                        # execution_started before any terminal broadcast.
                        start_gate.set()

                elif command == "stop_execution":
                    # 只停止当前客户端的 execution
                    execution_id = state_manager.get_client_execution(websocket)
                    if execution_id:
                        success = state_manager.cancel_execution(execution_id)
                        if success:
                            logger.info(f"Execution {execution_id} cancelled by user {client_ip}")
                            # 统一使用 execution_control_ack 类型，实时和历史一致
                            await state_manager.broadcast(execution_id, {
                                "type": "execution_control_ack",
                                "executionId": execution_id,
                                "action": "stopped",
                                "message": "Execution terminated by user."
                            })
                            state_manager.add_log("Execution terminated by user.", "warning", execution_id=execution_id)
                        else:
                            await websocket.send_json({
                                "type": "error",
                                "message": "Cannot cancel execution (already finished or not found)"
                            })
                    else:
                        await websocket.send_json({
                            "type": "error",
                            "message": "No active execution to stop"
                        })

                elif command == "pong":
                    # 心跳响应
                    continue

                elif command == "ping":
                    if websocket.client_state == WebSocketState.CONNECTED:
                        await websocket.send_json({"type": "pong"})

                elif command == "subscribe":
                    # 支持客户端订阅特定 execution（用于重连或监听已有 execution）
                    execution_id = data.get("executionId")
                    if execution_id:
                        session = state_manager.get_execution(execution_id)
                        if session:
                            state_manager.subscribe_client(execution_id, websocket)
                            # 同步历史状态
                            await state_manager.sync_history_to_client(websocket, execution_id)
                            await websocket.send_json({
                                "type": "subscribed",
                                "executionId": execution_id
                            })
                        else:
                            await websocket.send_json({
                                "type": "execution_not_found",
                                "code": "EXECUTION_NOT_FOUND",
                                "executionId": execution_id,
                                "message": f"Execution {execution_id} not found",
                            })

            except asyncio.TimeoutError:
                # 网络假活或极慢网络：跳过继续等待，不主动断开
                logger.debug(f"Client {client_ip} socket timeout, continuing")
                continue

            except WebSocketDisconnect as e:
                # 1001 = endpoint going away (正常客户端断开)
                # 其他 code 可能是错误情况
                if e.code == 1001:
                    logger.info(f"Client WebSocket disconnected: {client_ip}")
                else:
                    logger.warning(f"Client WebSocket disconnected (code={e.code}): {client_ip}")
                break

            except RuntimeError as e:
                # Starlette may surface a completed disconnect as RuntimeError
                # ("WebSocket is not connected") instead of WebSocketDisconnect
                # when a heartbeat send and receive finish concurrently.
                application_state = getattr(
                    websocket,
                    "application_state",
                    websocket.client_state,
                )
                if (
                    websocket.client_state != WebSocketState.CONNECTED
                    or application_state != WebSocketState.CONNECTED
                ):
                    logger.info(
                        "Client WebSocket was already disconnected: %s (%s)",
                        client_ip,
                        e,
                    )
                    break
                logger.error(
                    "WebSocket loop error for %s: %s",
                    client_ip,
                    e,
                    exc_info=True,
                )
                break

            except Exception as e:
                logger.error(f"WebSocket loop error for {client_ip}: {e}", exc_info=True)
                break

    except WebSocketDisconnect:
        logger.info(f"Client disconnected gracefully: {client_ip}")
    except Exception as e:
        logger.error(f"WebSocket error for {client_ip}: {e}", exc_info=True)
    finally:
        # 清理心跳任务
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

        # 解绑客户端
        state_manager.unsubscribe_client(websocket)
        logger.info(f"Client disconnected: {client_ip}")
