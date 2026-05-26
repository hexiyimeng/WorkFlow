import asyncio
import uuid
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from core.logger import logger
from core.state_manager import state_manager, ExecutionStatus
from services.executor import execute_graph
from services.dask_service import dask_service

router = APIRouter()


@router.websocket("/ws/run")
async def websocket_endpoint(websocket: WebSocket):
    client_ip = websocket.client.host if websocket.client else "unknown"
    current_execution_id = None  # 当前客户端订阅的 execution_id

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
    initialized = False
    try:
        # 发送 Dask 服务状态
        client = dask_service.get_client()
        if client and websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_json({
                "type": "log",
                "message": f"[System] Dask Cluster Connected: {client.dashboard_link}"
            })
        initialized = True
    except Exception as e:
        logger.warning(f"WebSocket initialization failed for {client_ip}: {e}")
        state_manager.unsubscribe_client(websocket)
        heartbeat_task.cancel()
        return

    # 2. 消息监听主循环
    try:
        while initialized:
            try:
                timeout = 30
                data = await asyncio.wait_for(websocket.receive_json(), timeout=timeout)
                command = data.get("command")

                if command == "execute_graph":
                    graph = data.get("graph")
                    if not graph:
                        await websocket.send_json({"type": "error", "message": "Received empty graph"})
                        continue

                    # Generate or use provided execution_id
                    execution_id = data.get("executionId") or uuid.uuid4().hex

                    # --- Idempotent: if this execution_id is already active, subscribe only ---
                    existing_session = state_manager.get_execution(execution_id)
                    if existing_session and not ExecutionStatus.is_finished(existing_session.status):
                        state_manager.subscribe_client(execution_id, websocket)
                        current_execution_id = execution_id
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
                        session = state_manager.start_execution(execution_id)
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
                    current_execution_id = execution_id

                    logger.info(f"Executing graph for {client_ip}, execution_id={execution_id}")

                    # Send executionId to frontend
                    await websocket.send_json({
                        "type": "execution_started",
                        "executionId": execution_id
                    })

                    # Start execution task and bind the real execute_graph task for cancellation.
                    execution_task = asyncio.create_task(execute_graph(graph, execution_id))
                    if not state_manager.attach_execution_task(execution_id, execution_task):
                        execution_task.cancel()
                        await websocket.send_json({
                            "type": "error",
                            "message": "Failed to bind execution task"
                        })

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
                            current_execution_id = execution_id
                            # 同步历史状态
                            await state_manager.sync_history_to_client(websocket, execution_id)
                            await websocket.send_json({
                                "type": "subscribed",
                                "executionId": execution_id
                            })
                        else:
                            await websocket.send_json({
                                "type": "error",
                                "message": f"Execution {execution_id} not found"
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
