from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.worker_profiles import DEFAULT_WORKER_PROFILE


@dataclass(frozen=True)
class NodeRuntime:
    node_id: str | None
    execution_id: str | None
    is_preflight: bool = False
    is_resuming: bool = False
    worker_profile: str = DEFAULT_WORKER_PROFILE


@dataclass(frozen=True)
class NodeInvocation:
    runtime: NodeRuntime
    input_defs: dict
    inputs: dict[str, Any]
