import asyncio
from types import SimpleNamespace

from api import websocket as websocket_api


def _socket(*, origin=None, host="127.0.0.1:8000", client="127.0.0.1"):
    headers = {"host": host}
    if origin is not None:
        headers["origin"] = origin
    return SimpleNamespace(
        headers=headers,
        client=SimpleNamespace(host=client),
    )


def test_websocket_accepts_same_authority_origin(monkeypatch) -> None:
    monkeypatch.setenv("WorkFlow_ALLOWED_ORIGINS", "")

    assert websocket_api._websocket_origin_allowed(
        _socket(origin="http://127.0.0.1:8000")
    )


def test_websocket_accepts_an_explicit_allowed_origin(monkeypatch) -> None:
    monkeypatch.setenv(
        "WorkFlow_ALLOWED_ORIGINS",
        "https://workflow.example.org",
    )

    assert websocket_api._websocket_origin_allowed(
        _socket(
            origin="https://workflow.example.org",
            host="127.0.0.1:8000",
        )
    )


def test_websocket_rejects_an_unlisted_cross_origin(monkeypatch) -> None:
    monkeypatch.setenv("WorkFlow_ALLOWED_ORIGINS", "https://trusted.example.org")

    assert not websocket_api._websocket_origin_allowed(
        _socket(origin="https://attacker.example.org")
    )


def test_websocket_accepts_missing_origin_only_from_loopback(monkeypatch) -> None:
    monkeypatch.setenv("WorkFlow_ALLOWED_ORIGINS", "")

    assert websocket_api._websocket_origin_allowed(_socket(client="127.0.0.1"))
    assert websocket_api._websocket_origin_allowed(_socket(client="::1"))
    assert not websocket_api._websocket_origin_allowed(_socket(client="10.2.3.4"))


def test_websocket_rejects_null_and_malformed_origins(monkeypatch) -> None:
    monkeypatch.setenv("WorkFlow_ALLOWED_ORIGINS", "")

    assert not websocket_api._websocket_origin_allowed(_socket(origin="null"))
    assert not websocket_api._websocket_origin_allowed(
        _socket(origin="http://user:password@127.0.0.1:8000")
    )


def test_websocket_endpoint_closes_an_invalid_origin_before_accept(monkeypatch) -> None:
    class _RejectedSocket:
        def __init__(self) -> None:
            self.headers = {
                "host": "127.0.0.1:8000",
                "origin": "https://attacker.example.org",
            }
            self.client = SimpleNamespace(host="127.0.0.1")
            self.accepted = False
            self.closed = None

        async def accept(self) -> None:
            self.accepted = True

        async def close(self, *, code: int, reason: str) -> None:
            self.closed = (code, reason)

    monkeypatch.setenv("WorkFlow_ALLOWED_ORIGINS", "")
    socket = _RejectedSocket()

    asyncio.run(websocket_api.websocket_endpoint(socket))

    assert not socket.accepted
    assert socket.closed == (1008, "WebSocket origin is not allowed.")
