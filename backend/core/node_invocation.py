from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NodeRuntime:
    node_id: str | None
    execution_id: str | None


@dataclass(frozen=True)
class NodeInvocation:
    runtime: NodeRuntime
    input_defs: dict
    inputs: dict[str, Any]
