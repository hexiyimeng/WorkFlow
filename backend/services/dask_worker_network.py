"""Resolve the per-node IPv4 address used by a Slurm Dask Worker.

Compute nodes in one Slurm cluster do not necessarily use the same interface
name.  Resolve the local address selected by the operating-system route to the
Scheduler instead of passing a site-wide ``--interface`` value to every
Worker.
"""

from __future__ import annotations

import argparse
from contextlib import closing
import ipaddress
import socket
from typing import Callable, Iterable


AddressInfo = tuple[int, int, int, str, tuple[object, ...]]


def resolve_scheduler_route_ipv4(
    scheduler_host: str,
    scheduler_port: int,
    *,
    getaddrinfo: Callable[..., Iterable[AddressInfo]] = socket.getaddrinfo,
    socket_factory: Callable[..., socket.socket] = socket.socket,
) -> str:
    """Return the non-loopback IPv4 selected for traffic to the Scheduler.

    A connected UDP socket performs route selection without sending traffic or
    opening a Scheduler connection.  Trying every resolved IPv4 also handles a
    Scheduler hostname with more than one address.
    """

    if not isinstance(scheduler_host, str) or not scheduler_host.strip():
        raise ValueError("scheduler_host must be non-empty.")
    if type(scheduler_port) is not int or not 1 <= scheduler_port <= 65535:
        raise ValueError("scheduler_port must be 1..65535.")

    host = scheduler_host.strip()
    try:
        candidates = tuple(getaddrinfo(
            host,
            scheduler_port,
            socket.AF_INET,
            socket.SOCK_DGRAM,
        ))
    except OSError as exc:
        raise RuntimeError(
            f"Cannot resolve Scheduler host {host!r} to IPv4: {exc}"
        ) from exc
    if not candidates:
        raise RuntimeError(f"Scheduler host {host!r} has no IPv4 address.")

    failures: list[str] = []
    for family, socktype, protocol, _canonical_name, remote_address in candidates:
        try:
            with closing(socket_factory(family, socktype, protocol)) as probe:
                probe.connect(remote_address)
                selected = str(probe.getsockname()[0])
            address = ipaddress.ip_address(selected)
            if not isinstance(address, ipaddress.IPv4Address):
                raise ValueError(f"route selected non-IPv4 address {selected!r}")
            if address.is_loopback or address.is_unspecified or address.is_multicast:
                raise ValueError(f"route selected unusable address {selected!r}")
            return str(address)
        except (OSError, TypeError, ValueError) as exc:
            failures.append(str(exc))

    detail = "; ".join(item for item in failures if item) or "no usable route"
    raise RuntimeError(
        f"Cannot select a Worker IPv4 route to Scheduler {host}:{scheduler_port}: "
        f"{detail}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print this node's IPv4 route to the Dask Scheduler."
    )
    parser.add_argument("--scheduler-host", required=True)
    parser.add_argument("--scheduler-port", required=True, type=int)
    args = parser.parse_args(argv)
    try:
        selected = resolve_scheduler_route_ipv4(
            args.scheduler_host,
            args.scheduler_port,
        )
    except (RuntimeError, ValueError) as exc:
        parser.exit(2, f"Dask Worker route selection failed: {exc}\n")
    print(selected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
