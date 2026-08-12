import asyncio

from fastapi import WebSocketDisconnect
from starlette.websockets import WebSocketState

from api import websocket as websocket_api
from services import execution_dispatcher


class _DisconnectingWebSocket:
    def __init__(self):
        self.client = type("Client", (), {"host": "127.0.0.1"})()
        self.client_state = WebSocketState.CONNECTED
        self.application_state = WebSocketState.CONNECTED
        self.sent = []

    async def accept(self):
        return None

    async def send_json(self, payload):
        self.sent.append(payload)

    async def receive_json(self):
        self.client_state = WebSocketState.DISCONNECTED
        self.application_state = WebSocketState.DISCONNECTED
        raise WebSocketDisconnect(code=1001)


def test_websocket_route_uses_execution_dispatcher():
    assert websocket_api.execute_graph is execution_dispatcher.execute_graph


def test_slurm_websocket_initialization_does_not_touch_local_dask(monkeypatch):
    socket = _DisconnectingWebSocket()
    monkeypatch.setattr(
        websocket_api,
        "uses_slurm_execution_backend",
        lambda: True,
    )
    monkeypatch.setattr(
        websocket_api.dask_service,
        "get_client",
        lambda: (_ for _ in ()).throw(
            AssertionError("Slurm control plane must not query local Dask")
        ),
    )

    asyncio.run(websocket_api.websocket_endpoint(socket))

    assert socket.sent == [{
        "type": "log",
        "message": "[System] Slurm execution control plane ready.",
    }]
