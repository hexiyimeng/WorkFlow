#!/usr/bin/env python3
"""Probe compute-node TCP reachability to an on-demand service-node Scheduler.

This intentionally tests only routing/firewall reachability.  It does not
start Dask and is not evidence that TLS identities or multi-node execution are
configured correctly.
"""

from __future__ import annotations

import argparse
import hmac
import ipaddress
import json
import os
from pathlib import Path
import secrets
import socket
import sys
import tempfile
from typing import Any


_SUCCESS = b"workflow-scheduler-connectivity-ok\n"


def _validate_host(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("host must be a non-empty IPv4 address or DNS name")
    lowered = value.lower().rstrip(".")
    if lowered in {"localhost", "localhost.localdomain", "0.0.0.0", "*"}:
        raise ValueError("host must not be loopback or wildcard")
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        labels = lowered.split(".")
        if any(
            not label
            or len(label) > 63
            or not label[0].isalnum()
            or not label[-1].isalnum()
            or any(not (character.isalnum() or character == "-") for character in label)
            for label in labels
        ):
            raise ValueError("host is not a valid IPv4 address or DNS name") from None
    else:
        if address.version != 4 or address.is_loopback or address.is_unspecified:
            raise ValueError("host must be a routable IPv4 address")
    return value


def _validate_port(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 65535:
        raise ValueError("port must be an integer between 1 and 65535")
    return value


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _receive_line(connection: socket.socket, *, maximum: int) -> bytes:
    received = bytearray()
    while len(received) < maximum:
        piece = connection.recv(min(128, maximum - len(received)))
        if not piece:
            break
        received.extend(piece)
        if b"\n" in piece:
            break
    line, separator, remainder = bytes(received).partition(b"\n")
    if not separator or remainder:
        raise RuntimeError("probe message is missing a single newline terminator")
    return line.rstrip(b"\r")


def create_request(path: Path, *, host: str, port: int) -> None:
    resolved = path.resolve()
    if resolved.exists():
        raise ValueError(f"request path already exists: {resolved}")
    _atomic_json(
        resolved,
        {
            "schemaVersion": 1,
            "host": _validate_host(host),
            "port": _validate_port(port),
            "nonce": secrets.token_urlsafe(32),
            "readyPath": str((resolved.parent / "ready.json").resolve()),
            "resultPath": str((resolved.parent / "result.json").resolve()),
        },
    )


def _load_request(path: Path) -> dict[str, Any]:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError("request must be an absolute regular non-symlink file")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {
        "schemaVersion", "host", "port", "nonce", "readyPath", "resultPath"
    }:
        raise ValueError("invalid probe request schema")
    if payload["schemaVersion"] != 1:
        raise ValueError("unsupported probe request version")
    payload["host"] = _validate_host(payload["host"])
    payload["port"] = _validate_port(payload["port"])
    nonce = payload["nonce"]
    if not isinstance(nonce, str) or not 32 <= len(nonce) <= 128:
        raise ValueError("invalid probe nonce")
    for name in ("readyPath", "resultPath"):
        candidate = Path(payload[name])
        if not candidate.is_absolute() or candidate.parent.resolve() != path.parent.resolve():
            raise ValueError(f"{name} must be next to the request")
        payload[name] = candidate
    return payload


def serve(path: Path, *, timeout: float) -> None:
    request = _load_request(path)
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    addresses = socket.getaddrinfo(
        request["host"], request["port"], socket.AF_INET, socket.SOCK_STREAM
    )
    if not addresses:
        raise RuntimeError("scheduler host did not resolve to IPv4")
    with socket.socket(addresses[0][0], addresses[0][1], addresses[0][2]) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(addresses[0][4])
        listener.listen(1)
        listener.settimeout(timeout)
        _atomic_json(
            request["readyPath"],
            {"schemaVersion": 1, "host": request["host"], "port": request["port"]},
        )
        connection, source = listener.accept()
        with connection:
            connection.settimeout(10)
            received = _receive_line(connection, maximum=512)
            if not hmac.compare_digest(received, request["nonce"].encode("ascii")):
                raise RuntimeError("compute-node probe supplied the wrong nonce")
            connection.sendall(_SUCCESS)
    _atomic_json(
        request["resultPath"],
        {
            "schemaVersion": 1,
            "reachable": True,
            "schedulerHost": request["host"],
            "schedulerPort": request["port"],
            "computeSourceAddress": str(source[0]),
        },
    )


def connect(path: Path, *, timeout: float) -> None:
    request = _load_request(path)
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    with socket.create_connection(
        (request["host"], request["port"]), timeout=timeout
    ) as connection:
        connection.sendall(request["nonce"].encode("ascii") + b"\n")
        response = _receive_line(connection, maximum=len(_SUCCESS) + 1)
    if response + b"\n" != _SUCCESS:
        raise RuntimeError("service-node probe returned an invalid response")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create")
    create.add_argument("request", type=Path)
    create.add_argument("--host", required=True)
    create.add_argument("--port", required=True, type=int)
    server = commands.add_parser("server")
    server.add_argument("request", type=Path)
    server.add_argument("--timeout", type=float, default=1800)
    client = commands.add_parser("client")
    client.add_argument("request", type=Path)
    client.add_argument("--timeout", type=float, default=30)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        if arguments.command == "create":
            create_request(arguments.request, host=arguments.host, port=arguments.port)
        elif arguments.command == "server":
            serve(arguments.request, timeout=arguments.timeout)
        else:
            connect(arguments.request, timeout=arguments.timeout)
    except Exception as exc:
        print(f"scheduler connectivity probe failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
