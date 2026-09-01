from __future__ import annotations

import socket

import pytest

from services.dask_worker_network import resolve_scheduler_route_ipv4


class _RouteSocket:
    def __init__(self, local_host: str) -> None:
        self.local_host = local_host
        self.remote_address: tuple[object, ...] | None = None

    def connect(self, remote_address: tuple[object, ...]) -> None:
        self.remote_address = remote_address

    def getsockname(self) -> tuple[str, int]:
        return self.local_host, 49152

    def close(self) -> None:
        pass


def _scheduler_addresses(*_args: object) -> list[tuple[int, int, int, str, tuple[str, int]]]:
    return [
        (socket.AF_INET, socket.SOCK_DGRAM, 0, "", ("10.1.0.3", 8786)),
    ]


def test_resolves_worker_host_from_scheduler_route() -> None:
    selected = resolve_scheduler_route_ipv4(
        "mn02",
        8786,
        getaddrinfo=_scheduler_addresses,
        socket_factory=lambda *_args: _RouteSocket("10.1.1.6"),
    )

    assert selected == "10.1.1.6"


@pytest.mark.parametrize("selected", ["0.0.0.0", "127.0.0.1", "224.0.0.1"])
def test_rejects_unusable_worker_route(selected: str) -> None:
    with pytest.raises(RuntimeError, match="Cannot select a Worker IPv4 route"):
        resolve_scheduler_route_ipv4(
            "mn02",
            8786,
            getaddrinfo=_scheduler_addresses,
            socket_factory=lambda *_args: _RouteSocket(selected),
        )


def test_requires_an_ipv4_scheduler_result() -> None:
    with pytest.raises(RuntimeError, match="has no IPv4 address"):
        resolve_scheduler_route_ipv4(
            "mn02",
            8786,
            getaddrinfo=lambda *_args: (),
        )
