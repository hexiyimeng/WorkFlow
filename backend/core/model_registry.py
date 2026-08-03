from __future__ import annotations

import os
from pathlib import Path

from core.platform import default_models_root, normalize_path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODELS_ROOT = default_models_root()
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
    ##1. 确定 provider 模型目录
    ##2. 列出本地已有模型
    ##3. 将模型名称解析为绝对路径

    def __init__(self, models_root: Path | None = None):
        env_root = os.getenv("WorkFlow_MODELS_DIR")
        self.models_root = normalize_path(models_root or env_root or DEFAULT_MODELS_ROOT).resolve()

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
            if item.is_dir():
                names.add(item.name)
            elif item.is_file():
                names.add(
                    item.stem
                    if item.suffix.lower() in KNOWN_MODEL_EXTENSIONS
                    else item.name
                )
        return sorted(names)

    def resolve_model_path(self, provider: str, name: str) -> str | None:
        """
        Resolve a model reference to an absolute local path.

        Search order:
          1. Existing absolute path exactly as provided.
          2. Safe relative path exactly as provided (no ".." traversal).
          3. backend/models/{provider}/{name}
          4. backend/models/{provider}/{name}{known_extension}

        Missing names return None because they may be valid provider built-ins,
        remote identifiers, or downloadable model names. Provider-specific code
        decides what to do with unresolved names.
        """
        provider = self._normalize_provider(provider)
        name = str(name or "").strip()
        if not provider or not name:
            return None

        candidate = normalize_path(name)
        if candidate.is_absolute() and candidate.exists():
            return str(candidate.resolve())

        # Resolve explicitly supplied relative paths against backend root so the
        # result does not depend on the process working directory.
        if self._is_safe_relative_path(candidate):
            relative_candidate = BACKEND_ROOT / candidate
            if relative_candidate.exists():
                return str(relative_candidate.resolve())

        directory = self.provider_dir(provider)
        if not self._is_safe_relative_path(normalize_path(name)):
            return None
        resolved = self._resolve_in_directory(directory, name)
        if resolved:
            return resolved
        return None

    def _resolve_in_directory(self, directory: Path, name: str) -> str | None:
        directory_resolved = directory.resolve()
        path = directory / normalize_path(name)
        if path.exists():
            return self._safe_resolved_child(path, directory_resolved)

        for extension in KNOWN_MODEL_EXTENSIONS:
            path = directory / normalize_path(f"{name}{extension}")
            if path.exists():
                return self._safe_resolved_child(path, directory_resolved)
        return None

    @staticmethod
    def _safe_resolved_child(path: Path, directory_resolved: Path) -> str | None:
        resolved = path.resolve()
        try:
            if not resolved.is_relative_to(directory_resolved):
                return None
        except AttributeError:  # pragma: no cover - Python < 3.9 fallback
            if directory_resolved not in resolved.parents and resolved != directory_resolved:
                return None
        return str(resolved)

    @staticmethod
    def _is_safe_relative_path(path: Path) -> bool:
        if path.is_absolute():
            return False
        parts = path.parts
        unsafe_parts = {"", ".", ".."}
        return bool(parts) and all(part not in unsafe_parts for part in parts)

    @staticmethod
    def _normalize_provider(provider: str) -> str:
        provider = str(provider or "").strip().lower()
        if not provider:
            return ""
        if "/" in provider or "\\" in provider:
            raise ValueError(
                "provider must be a simple name, not a path "
                f"(received {provider!r})"
            )
        if not ModelRegistry._is_safe_relative_path(Path(provider)) or len(Path(provider).parts) != 1:
            raise ValueError(
                "provider must be a simple name, not a path "
                f"(received {provider!r})"
            )
        return provider


model_registry = ModelRegistry()


def get_models_root() -> str:
    return str(model_registry.models_root)


def get_provider_model_dir(provider: str, *, create: bool = False) -> str:
    return str(model_registry.provider_dir(provider, create=create))


def list_models(provider: str) -> list[str]:
    return model_registry.list_models(provider)


def resolve_model_path(provider: str, name: str) -> str | None:
    return model_registry.resolve_model_path(provider, name)
