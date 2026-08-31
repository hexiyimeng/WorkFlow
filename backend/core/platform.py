from __future__ import annotations

import mimetypes
import os
import platform
import tempfile
from pathlib import Path
from urllib.parse import urlparse, urlunparse


BACKEND_ROOT = Path(__file__).resolve().parents[1]


def current_platform() -> str:
    return platform.system()


def is_windows() -> bool:
    return current_platform() == "Windows"


def is_linux() -> bool:
    return current_platform() == "Linux"


def backend_path(*parts: str) -> Path:
    return BACKEND_ROOT.joinpath(*parts)


def dist_dir() -> Path:
    return backend_path("dist")


def nodes_dir() -> Path:
    override = os.getenv("WorkFlow_NODES_DIR")
    if override:
        return normalize_path(override)
    return backend_path("nodes")


def default_models_root() -> Path:
    return backend_path("models")


def dask_spill_dir(configured: str | os.PathLike | None = None) -> Path:
    if configured:
        return normalize_path(configured)
    return Path(tempfile.gettempdir()) / "WorkFlow_dask_spill"


def normalize_path(value: str | os.PathLike) -> Path:
    raw = os.fspath(value)
    if os.sep == "/" and "\\" in raw:
        raw = raw.replace("\\", "/")
    return Path(raw).expanduser()


def configure_deterministic_mimetypes() -> None:
    """
    Set MIME types that must be deterministic across OS registry/database state.

    Windows can inherit incorrect values from the registry and Linux can vary by
    distro MIME database. These explicit mappings make static JS/CSS serving
    stable on both platforms.
    """
    mimetypes.init()
    mimetypes.add_type("application/javascript", ".js", strict=True)
    mimetypes.add_type("application/javascript", ".mjs", strict=True)
    mimetypes.add_type("text/css", ".css", strict=True)


def media_type_for_static_path(path: str | os.PathLike) -> str | None:
    suffix = Path(path).suffix.lower()
    if suffix in {".js", ".mjs"}:
        return "application/javascript"
    if suffix == ".css":
        return "text/css"
    return None


def apply_linux_malloc_trim_env() -> None:
    """Enable glibc malloc trimming only on Linux."""
    if is_linux():
        os.environ.setdefault("MALLOC_TRIM_THRESHOLD_", "0")


def should_schedule_malloc_trim() -> bool:
    return is_linux()


def rewrite_dashboard_url(dashboard_link: str | None, custom_host: str | None) -> str | None:
    if not dashboard_link:
        return None
    if not custom_host:
        return dashboard_link

    parsed_link = urlparse(dashboard_link)
    parsed_custom = urlparse(custom_host if "://" in custom_host else f"http://{custom_host}")

    if parsed_custom.netloc:
        host = parsed_custom.hostname or parsed_custom.netloc
        scheme = parsed_custom.scheme or parsed_link.scheme or "http"
        port = parsed_custom.port or parsed_link.port
        netloc = f"{host}:{port}" if port else host
        # A host-only browser override changes the SSH-forwarded host/port but
        # must retain Dask's application path (normally ``/status``). Dropping
        # it opens Tornado's root route, which is a valid server but returns
        # a misleading 404.
        path = parsed_custom.path.rstrip("/") or parsed_link.path
        return urlunparse((scheme, netloc, path, "", "", ""))

    return custom_host
