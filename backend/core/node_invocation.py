from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.execution_resources import ExecutionResource


@dataclass(frozen=True)
class NodeRuntime:
    node_id: str | None
    execution_id: str | None
    is_preflight: bool = False
    is_resuming: bool = False
    execution_resource: ExecutionResource = "any"


@dataclass(frozen=True)
class NodeInvocation:
    runtime: NodeRuntime
    input_defs: dict
    inputs: dict[str, Any]
