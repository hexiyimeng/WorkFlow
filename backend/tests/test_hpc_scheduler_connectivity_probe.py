import asyncio
import json
import os
from pathlib import Path
import socket

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROBE_PATH = PROJECT_ROOT / "deploy" / "hpc" / "scheduler_connectivity_probe.py"


def _load_probe_module():
    import importlib.util

    specification = importlib.util.spec_from_file_location(
        "scheduler_connectivity_probe", PROBE_PATH
    )
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "host", ("", "localhost", "127.0.0.1", "0.0.0.0", "::1", "bad/name")
)
def test_probe_rejects_non_routable_scheduler_hosts(host):
    probe = _load_probe_module()

    with pytest.raises(ValueError, match="host"):
        probe._validate_host(host)


def test_probe_request_contains_no_shell_command_and_is_private(tmp_path):
    probe = _load_probe_module()
    request = tmp_path / "request.json"

    probe.create_request(request, host="mn02.cluster", port=8786)

    payload = json.loads(request.read_text(encoding="utf-8"))
    assert set(payload) == {
        "schemaVersion", "host", "port", "nonce", "readyPath", "resultPath"
    }
    assert payload["host"] == "mn02.cluster"
    assert payload["port"] == 8786
    if os.name != "nt":
        assert request.stat().st_mode & 0o077 == 0


def test_compute_client_and_service_listener_complete_authenticated_probe(tmp_path):
    probe = _load_probe_module()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]

    # Loopback is prohibited for generated deployment requests; use a valid
    # request and substitute loopback only in this isolated socket-level test.
    request = tmp_path / "request.json"
    probe.create_request(request, host="mn02.cluster", port=port)
    payload = json.loads(request.read_text(encoding="utf-8"))
    payload["host"] = "127.0.0.1"
    request.write_text(json.dumps(payload), encoding="utf-8")
    original_validate = probe._validate_host
    probe._validate_host = lambda value: value

    async def scenario():
        server = asyncio.create_task(
            asyncio.to_thread(probe.serve, request.resolve(), timeout=5)
        )
        for _ in range(100):
            if (tmp_path / "ready.json").exists():
                break
            await asyncio.sleep(0.01)
        await asyncio.to_thread(probe.connect, request.resolve(), timeout=2)
        await server

    try:
        asyncio.run(scenario())
    finally:
        probe._validate_host = original_validate

    result = json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))
    assert result["reachable"] is True
    assert result["schedulerPort"] == port
