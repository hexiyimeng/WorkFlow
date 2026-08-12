import pytest

import main


def test_direct_run_defaults_to_loopback(monkeypatch) -> None:
    monkeypatch.delenv("WORKFLOW_WEB_HOST", raising=False)
    monkeypatch.delenv("WORKFLOW_WEB_PORT", raising=False)

    assert main._direct_run_bind() == ("127.0.0.1", 8000)


def test_direct_run_accepts_an_explicit_host_and_port(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_WEB_HOST", "::1")
    monkeypatch.setenv("WORKFLOW_WEB_PORT", "18000")

    assert main._direct_run_bind() == ("::1", 18000)


@pytest.mark.parametrize("value", ["", "zero", "0", "65536"])
def test_direct_run_rejects_an_invalid_port(monkeypatch, value: str) -> None:
    monkeypatch.setenv("WORKFLOW_WEB_PORT", value)

    with pytest.raises(ValueError, match="between 1 and 65535"):
        main._direct_run_bind()


def test_direct_run_rejects_an_empty_host(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_WEB_HOST", "   ")

    with pytest.raises(ValueError, match="must not be empty"):
        main._direct_run_bind()
