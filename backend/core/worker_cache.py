"""
Lightweight worker-local cache for block functions.

This module provides a generic process-local cache reused by block executions
within the same workflow run on the same Dask worker.

Node authors should use BlockContext.cached() or BlockContext.model() to
access cached objects from block functions. Framework code may call
clear_worker_cache() and force_clear_worker_cache() after execution.

Usage for node authors::

    def create_my_model(model_path: str, device: str):
        import my_model_lib
        return my_model_lib.load(model_path, device=device)

    def process_block(block, model_name, ctx=None):
        model = ctx.model("my_provider", model_name, create_my_model)
        return model.predict(block)

The framework handles worker-local caching, cache key construction, and cleanup
automatically. Executor cleanup clears this cache after every run, including
successful runs, so model objects do not remain resident in worker processes.
"""

from __future__ import annotations

import gc
import threading
from typing import Any, Callable

logger = __import__("logging").getLogger("WorkFlow.WorkerCache")


# Process-local cache: reused across blocks on the same worker during a run.
# Key: (namespace, key) -> (value, dispose_fn | None, clear_cuda: bool)
_CACHE: dict[tuple, tuple] = {}
_CACHE_LOCK = threading.RLock()


def get_or_create_worker_cached(
    namespace: str,
    key: Any,
    factory: Callable[[], Any],
    dispose: Callable[[Any], None] | None = None,
    clear_cuda: bool = False,
) -> Any:
    """
    Get a cached object from the worker-local cache, or create it via factory.

    Args:
        namespace: Logical namespace (e.g. "model:cellpose").
        key: Unique key within the namespace (e.g. model path + device).
        factory: Callable to create the object on cache miss.
        dispose: Optional callback to release the object on eviction.
        clear_cuda: If True, call torch.cuda.empty_cache() after cleanup.

    Returns:
        The cached (or newly created) object.
    """
    cache_key = (namespace, key)

    with _CACHE_LOCK:
        if cache_key in _CACHE:
            value, _, _ = _CACHE[cache_key]
            return value

    # Create outside the lock to avoid holding the lock during model loading.
    value = factory()

    with _CACHE_LOCK:
        # Double-check after creation: another thread may have stored first.
        if cache_key in _CACHE:
            # Race: another thread stored the same key first.
            # Dispose our newly-created value if dispose is provided, then return cached.
            existing, _, _ = _CACHE[cache_key]
            if dispose is not None:
                try:
                    dispose(value)
                except Exception:
                    pass
            return existing
        _CACHE[cache_key] = (value, dispose, bool(clear_cuda))

    return value


def clear_worker_cache(namespace: str | None = None) -> dict:
    """
    Clear cached objects for a specific namespace, or all if namespace is None.

    Args:
        namespace: If provided, only clear entries whose namespace matches.
                   If None, clear all entries.

    Returns:
        Stats dict with "cleared" count and "errors" list.
    """
    stats = {"cleared": 0, "errors": []}
    need_clear_cuda = False

    with _CACHE_LOCK:
        if namespace is None:
            items = list(_CACHE.items())
            _CACHE.clear()
        else:
            items = [(k, v) for k, v in _CACHE.items() if k[0] == namespace]
            for k in [k for k, _ in items]:
                del _CACHE[k]

    for cache_key, (value, dispose_fn, entry_clear_cuda) in items:
        try:
            if dispose_fn is not None:
                dispose_fn(value)
        except Exception as exc:
            stats["errors"].append(f"{cache_key}: {type(exc).__name__}: {exc}")
        stats["cleared"] += 1
        if entry_clear_cuda:
            need_clear_cuda = True

    gc.collect()
    if need_clear_cuda:
        _clear_cuda_if_available()

    logger.debug("[WorkerCache] clear_worker_cache(%r): %s", namespace, stats)
    return stats


def force_clear_worker_cache() -> dict:
    """
    Force-clear all cached objects across all namespaces.

    Returns:
        Stats dict with "cleared" count and "errors" list.
    """
    return clear_worker_cache(namespace=None)


def _clear_cuda_if_available() -> None:
    """Lazily import torch and clear CUDA cache if available."""
    try:
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        pass
