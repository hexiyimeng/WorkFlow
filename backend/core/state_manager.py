import asyncio
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from starlette.websockets import WebSocketState

logger = logging.getLogger("WorkFlow.State")


class ExecutionStatus:
    RUNNING = "running"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    FAILED = "failed"
    SUCCEEDED = "succeeded"

    VALID_TRANSITIONS = {
        RUNNING: [CANCELLING, FAILED, SUCCEEDED],
        CANCELLING: [CANCELLED, FAILED],
        CANCELLED: [],
        FAILED: [],
        SUCCEEDED: [],
    }

    @classmethod
    def is_finished(cls, status: str) -> bool:
        return status in (cls.CANCELLED, cls.FAILED, cls.SUCCEEDED)


@dataclass
class ExecutionSession:
    execution_id: str
    task: Optional[asyncio.Task] = None
    status: str = ExecutionStatus.RUNNING
    node_states: Dict[str, Dict] = field(default_factory=dict)
    logs: deque = field(default_factory=lambda: deque(maxlen=100))
    subscribers: Set = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None

    def get_log_summary(self) -> dict:
        return {
            "execution_id": self.execution_id,
            "status": self.status,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "log_count": len(self.logs),
            "has_errors": any(
                entry.get("type") in ("error", "warning")
                for entry in self.logs
            ),
            "node_count": len(self.node_states),
        }


class GlobalStateManager:
    _instance = None

    MAX_FINISHED_EXECUTIONS = 10
    EXECUTION_TTL_SECONDS = 3600
    MAX_FAILED_EXECUTION_LOGS = 3

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalStateManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._init_state()

    def _init_state(self):
        self.executions: Dict[str, ExecutionSession] = {}
        self.client_execution_map: Dict = {}
        self._failed_execution_logs: deque = deque(maxlen=self.MAX_FAILED_EXECUTION_LOGS)
        self._lock = threading.RLock()
        self.active_execution_id: Optional[str] = None
        self.active_task: Optional[asyncio.Task] = None

    def create_execution(self, execution_id: str) -> ExecutionSession:
        with self._lock:
            existing = self.executions.get(execution_id)
            if existing and not ExecutionStatus.is_finished(existing.status):
                logger.debug(f"[StateManager] Execution {execution_id} already running, reusing")
                return existing

            session = ExecutionSession(execution_id=execution_id)
            self.executions[execution_id] = session
            logger.info(f"[StateManager] Created execution session: {execution_id}")
            return session

    def start_execution(self, execution_id: str) -> ExecutionSession:
        """
        Atomically reserve the single active execution slot while keeping
        historical ExecutionSession records intact.
        """
        with self._lock:
            active_id = self.active_execution_id
            if active_id and active_id != execution_id:
                active_session = self.executions.get(active_id)
                if active_session and not ExecutionStatus.is_finished(active_session.status):
                    raise RuntimeError("Another execution is already running")
                self._clear_active_locked(active_id)

            session = self.create_execution(execution_id)
            if (
                self.active_execution_id == execution_id
                and session.status in (ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING)
            ):
                return session

            if not ExecutionStatus.is_finished(session.status):
                self.active_execution_id = execution_id
                self.active_task = session.task
                logger.info(f"[StateManager] Active execution reserved: {execution_id}")
                return session

            # Reusing a finished execution id means starting a new session history entry.
            session = ExecutionSession(execution_id=execution_id)
            self.executions[execution_id] = session
            self.active_execution_id = execution_id
            self.active_task = None
            logger.info(f"[StateManager] Active execution recreated: {execution_id}")
            return session

    def attach_execution_task(self, execution_id: str, task: asyncio.Task) -> bool:
        """Bind the real asyncio.Task running execute_graph to the active session."""
        with self._lock:
            if self.active_execution_id != execution_id:
                logger.warning(
                    f"[StateManager] Refusing to attach task for non-active execution {execution_id}"
                )
                return False
            session = self.executions.get(execution_id)
            if not session or ExecutionStatus.is_finished(session.status):
                return False
            session.task = task
            self.active_task = task
            logger.debug(f"[StateManager] Attached active task for execution {execution_id}")
            return True

    def _clear_active_locked(self, execution_id: str):
        if self.active_execution_id == execution_id:
            logger.info(f"[StateManager] Active execution released: {execution_id}")
            self.active_execution_id = None
            self.active_task = None

    def clear_active_execution(self, execution_id: str):
        with self._lock:
            self._clear_active_locked(execution_id)

    def get_execution(self, execution_id: str) -> Optional[ExecutionSession]:
        return self.executions.get(execution_id)

    def get_client_execution(self, websocket) -> Optional[str]:
        return self.client_execution_map.get(websocket)

    def set_execution_status(self, execution_id: str, status: str) -> bool:
        with self._lock:
            session = self.executions.get(execution_id)
            if not session:
                logger.warning(f"[StateManager] Cannot set status: execution {execution_id} not found")
                return False

            old_status = session.status
            if ExecutionStatus.is_finished(old_status):
                logger.warning(
                    f"[StateManager] Execution {execution_id} already finished ({old_status}), "
                    f"cannot change to {status}"
                )
                return False

            allowed = ExecutionStatus.VALID_TRANSITIONS.get(old_status, [])
            if status not in allowed:
                logger.warning(
                    f"[StateManager] Invalid transition: {execution_id} {old_status} -> {status} "
                    f"(allowed: {allowed})"
                )
                return False

            session.status = status
            if ExecutionStatus.is_finished(status):
                session.finished_at = time.time()
                self._clear_active_locked(execution_id)
            logger.info(f"[StateManager] Execution {execution_id}: {old_status} -> {status}")
            return True

    def cancel_execution(self, execution_id: str) -> bool:
        with self._lock:
            if self.active_execution_id != execution_id:
                logger.warning(
                    f"[StateManager] Refusing to cancel non-active execution {execution_id}"
                )
                return False

            session = self.executions.get(execution_id)
            if not session or ExecutionStatus.is_finished(session.status):
                return False

            if session.status == ExecutionStatus.CANCELLING:
                task = session.task or self.active_task
                if task and not task.done():
                    task.cancel()
                return True

            if session.status != ExecutionStatus.RUNNING:
                return False

            session.status = ExecutionStatus.CANCELLING
            logger.info(f"[StateManager] Execution {execution_id} -> cancelling")
            task = session.task or self.active_task
            if task and not task.done():
                task.cancel()
            return True

    def subscribe_client(self, execution_id: str, websocket):
        session = self.executions.get(execution_id)
        if not session:
            logger.warning(f"[StateManager] Cannot subscribe: execution {execution_id} not found")
            return False

        old_execution_id = self.client_execution_map.get(websocket)
        if old_execution_id and old_execution_id != execution_id:
            self._unsubscribe_from_execution(old_execution_id, websocket)

        self.client_execution_map[websocket] = execution_id
        session.subscribers.add(websocket)
        logger.debug(f"[StateManager] Client subscribed to execution {execution_id}")
        return True

    def unsubscribe_client(self, websocket):
        execution_id = self.client_execution_map.pop(websocket, None)
        if execution_id:
            self._unsubscribe_from_execution(execution_id, websocket)

    def _unsubscribe_from_execution(self, execution_id: str, websocket):
        session = self.executions.get(execution_id)
        if session and websocket in session.subscribers:
            session.subscribers.discard(websocket)
            logger.debug(f"[StateManager] Client unsubscribed from execution {execution_id}")

    def update_node_status(
        self,
        node_id: str,
        message: str,
        execution_id: str = None,
        run_state: str = None,
        device: str = None,
        progress: int | None = None,
        progress_type: str | None = None,
        progress_role: str | None = None,
    ):
        """Store node status fields for execution snapshots and reconnects."""
        if not execution_id:
            return
        session = self.executions.get(execution_id)
        if not session:
            return

        state = {
            "message": message,
            "executionId": execution_id,
        }
        if run_state is not None:
            state["runState"] = run_state
        if device is not None:
            state["device"] = device
        if progress is not None:
            state["progress"] = progress
        if progress_type is not None:
            state["progressType"] = progress_type
        if progress_role is not None:
            state["progressRole"] = progress_role
        session.node_states[node_id] = state

    def add_log(self, message: str, level: str = "info", execution_id: str = None) -> dict:
        entry = {"type": "log", "message": message}
        if level in ["error", "success", "warning"]:
            entry["type"] = level
        if execution_id:
            entry["executionId"] = execution_id

        if execution_id:
            session = self.executions.get(execution_id)
            if session:
                session.logs.append(entry)
        return entry

    async def broadcast(self, execution_id: str, message_dict: dict):
        session = self.executions.get(execution_id)
        if not session or not session.subscribers:
            return

        if "executionId" not in message_dict:
            message_dict["executionId"] = execution_id

        bad_sockets = []
        for ws in list(session.subscribers):
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json(message_dict)
                else:
                    bad_sockets.append(ws)
            except Exception:
                bad_sockets.append(ws)

        for ws in bad_sockets:
            self.unsubscribe_client(ws)

    async def broadcast_to_subscribers(self, websocket, message_dict: dict):
        execution_id = self.client_execution_map.get(websocket)
        if execution_id:
            await self.broadcast(execution_id, message_dict)

    def get_execution_snapshot(self, execution_id: str) -> dict | None:
        session = self.executions.get(execution_id)
        if not session:
            return None

        return {
            "type": "execution_snapshot",
            "executionId": execution_id,
            "status": session.status,
            "createdAt": session.created_at * 1000,
            "finishedAt": session.finished_at * 1000 if session.finished_at else None,
            "nodeCount": len(session.node_states),
            "logCount": len(session.logs),
        }

    async def sync_history_to_client(self, websocket, execution_id: str):
        session = self.executions.get(execution_id)
        if not session:
            return

        try:
            snapshot = self.get_execution_snapshot(execution_id)
            if snapshot and websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json(snapshot)
            elif not snapshot:
                return

            for log_entry in session.logs:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json(log_entry)
                else:
                    return

            for node_id, state in session.node_states.items():
                if websocket.client_state != WebSocketState.CONNECTED:
                    return
                await websocket.send_json({
                    "type": "progress",
                    "taskId": node_id,
                    "executionId": execution_id,
                    "progressType": state.get("progressType"),
                    "progress": state.get("progress"),
                    "message": state["message"],
                    "runState": state.get("runState"),
                    "progressRole": state.get("progressRole"),
                    "device": state.get("device"),
                })
        except Exception as e:
            logger.debug(f"[StateManager] Sync interrupted: {e}")
            self.unsubscribe_client(websocket)

    def cleanup_old_executions(self):
        now = time.time()
        to_remove = []

        finished_executions = [
            (eid, session) for eid, session in self.executions.items()
            if ExecutionStatus.is_finished(session.status)
        ]

        for eid, session in finished_executions:
            if session.status == ExecutionStatus.FAILED:
                existing_ids = [s["execution_id"] for s in self._failed_execution_logs]
                if eid not in existing_ids:
                    self._failed_execution_logs.append(session.get_log_summary())
                    logger.info(
                        f"[StateManager] Preserving failed execution log: {eid} "
                        f"(status={session.status}, logs={len(session.logs)})"
                    )

        for eid, session in finished_executions:
            if session.finished_at:
                age = now - session.finished_at
                if age > self.EXECUTION_TTL_SECONDS:
                    to_remove.append(eid)
                    logger.info(f"[StateManager] Cleaning up execution {eid} (TTL expired, age={age:.0f}s)")

        remaining = [s for e, s in finished_executions if e not in to_remove]
        remaining.sort(
            key=lambda s: (
                0 if s.status == ExecutionStatus.FAILED else 1,
                s.finished_at or 0,
            ),
            reverse=True,
        )

        if len(remaining) > self.MAX_FINISHED_EXECUTIONS:
            for session in remaining[self.MAX_FINISHED_EXECUTIONS:]:
                if session.execution_id not in to_remove:
                    to_remove.append(session.execution_id)
                    logger.info(f"[StateManager] Cleaning up execution {session.execution_id} (exceeds max count)")

        cleaned_count = 0
        for eid in to_remove:
            session = self.executions.pop(eid, None)
            if session:
                for ws in list(session.subscribers):
                    self.client_execution_map.pop(ws, None)
                session.node_states.clear()
                session.logs.clear()
                session.subscribers.clear()
                cleaned_count += 1

        if cleaned_count > 0:
            logger.info(f"[StateManager] Cleaned up {cleaned_count} execution(s), {len(self.executions)} remaining")

    def get_failed_execution_logs(self) -> list:
        return list(self._failed_execution_logs)

    @property
    def is_running(self) -> bool:
        with self._lock:
            if self.active_execution_id:
                session = self.executions.get(self.active_execution_id)
                if session and session.status in (ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING):
                    return True
            return False

    @property
    def current_task(self) -> Optional[asyncio.Task]:
        return self.active_task

    @property
    def active_websockets(self) -> Set:
        return set(self.client_execution_map.keys())

    @property
    def log_buffer(self) -> deque:
        for session in reversed(list(self.executions.values())):
            return session.logs
        return deque(maxlen=100)

    @property
    def node_states(self) -> Dict:
        for session in reversed(list(self.executions.values())):
            return session.node_states
        return {}

    def clear_state(self, execution_id: str = None):
        if execution_id:
            session = self.executions.get(execution_id)
            if session:
                session.node_states.clear()
                session.logs.clear()
            self.clear_active_execution(execution_id)
        else:
            self.executions.clear()
            self.client_execution_map.clear()
            self.active_execution_id = None
            self.active_task = None


state_manager = GlobalStateManager()
