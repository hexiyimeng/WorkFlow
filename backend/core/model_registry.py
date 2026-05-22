from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


BACKEND_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODELS_ROOT = BACKEND_ROOT / "models"
KNOWN_MODEL_EXTENSIONS = (
    ".pth",
    ".pt",
    ".ckpt",
    ".onnx",
    ".engine",
    ".safetensors",
    ".bin",
)


class ModelRegistry:
    """
    Generic local model catalog for WorkFlow.

    Storage convention:

        backend/models/{provider}/{model-file-or-directory}

    or, when WorkFlow_MODELS_DIR is set:

        {WorkFlow_MODELS_DIR}/{provider}/{model-file-or-directory}

    This registry is provider-agnostic. It does not set third-party environment
    variables, download weights, know model zoo defaults, or special-case
    Cellpose/SAM/StarDist/etc. Provider nodes/adapters can use provider_dir()
    when a library needs its own cache directory.
    """

    def __init__(self, models_root: Path | None = None):
        env_root = os.getenv("WorkFlow_MODELS_DIR")
        self.models_root = Path(models_root or env_root or DEFAULT_MODELS_ROOT)

    def provider_dir(self, provider: str, *, create: bool = False) -> Path:
        provider = self._normalize_provider(provider)
        if not provider:
            raise ValueError("provider must be a non-empty string")
        directory = self.models_root / provider
        if create:
            directory.mkdir(parents=True, exist_ok=True)
        return directory

    def list_models(self, provider: str) -> list[str]:
        provider = self._normalize_provider(provider)
        names: set[str] = set()
        directory = self.provider_dir(provider)
        if not directory.exists() or not directory.is_dir():
            return []
        for item in directory.iterdir():
            if item.name.startswith("."):
                continue
            if item.is_file() or item.is_dir():
                names.add(item.name)
            if item.is_file() and item.suffix.lower() in KNOWN_MODEL_EXTENSIONS:
                names.add(item.stem)
        return sorted(names)

    def resolve_model_path(self, provider: str, name: str) -> str | None:
        """
        Resolve a model reference to an absolute local path.

        Search order:
          1. Existing absolute/relative path exactly as provided.
          2. backend/models/{provider}/{name}
          3. backend/models/{provider}/{name}{known_extension}

        Missing names return None because they may be valid provider built-ins,
        remote identifiers, or downloadable model names. Provider-specific code
        decides what to do with unresolved names.
        """
        provider = self._normalize_provider(provider)
        name = str(name or "").strip()
        if not provider or not name:
            return None

        candidate = Path(name)
        if candidate.exists():
            return str(candidate.resolve())

        directory = self.provider_dir(provider)
        resolved = self._resolve_in_directory(directory, name)
        if resolved:
            return resolved
        return None

    def _resolve_in_directory(self, directory: Path, name: str) -> str | None:
        path = directory / name
        if path.exists():
            return str(path.resolve())

        for extension in KNOWN_MODEL_EXTENSIONS:
            path = directory / f"{name}{extension}"
            if path.exists():
                return str(path.resolve())
        return None

    @staticmethod
    def _normalize_provider(provider: str) -> str:
        return str(provider or "").strip().lower()


model_registry = ModelRegistry()


def get_models_root() -> str:
    return str(model_registry.models_root)


def get_provider_model_dir(provider: str, *, create: bool = False) -> str:
    return str(model_registry.provider_dir(provider, create=create))


def list_models(provider: str) -> list[str]:
    return model_registry.list_models(provider)


def resolve_model_path(provider: str, name: str) -> str | None:
    return model_registry.resolve_model_path(provider, name)
