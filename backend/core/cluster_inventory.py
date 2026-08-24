from __future__ import annotations

from dataclasses import dataclass
import re
import subprocess
from typing import Callable, Mapping, Sequence


_GPU_GRES_RE = re.compile(
    r"gpu(?::[A-Za-z0-9_.-]+)?:(?P<count>[0-9]+)(?:\([^)]*\))?\Z",
    re.IGNORECASE,
)
_UNAVAILABLE_STATE_FLAGS = frozenset({
    "DOWN", "DRAIN", "DRAINING", "FAIL", "FAILING", "FUTURE", "INVAL",
    "MAINT", "NO_RESPOND", "POWER_DOWN", "POWERED_DOWN", "UNKNOWN",
})


def _positive_or_zero(value: object, *, name: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}.") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be non-negative, got {parsed}.")
    return parsed


def parse_gpu_gres(value: object) -> int:
    """Return configured GPU count from Gres without partition inference."""

    if value is None:
        return 0
    text = str(value).strip()
    if not text or text.lower() in {"(null)", "none", "n/a"}:
        return 0
    return sum(
        int(match.group("count"))
        for item in text.split(",")
        if (match := _GPU_GRES_RE.fullmatch(item.strip())) is not None
    )


@dataclass(frozen=True, slots=True)
class ClusterNode:
    name: str
    partitions: tuple[str, ...]
    cpu: int
    memory_mib: int
    gpu: int
    state: str

    def __post_init__(self) -> None:
        if not self.name or self.name != self.name.strip():
            raise ValueError("Cluster node name must be non-empty and canonical.")
        if not self.partitions:
            raise ValueError(f"Cluster node {self.name!r} has no partition.")
        _positive_or_zero(self.cpu, name=f"{self.name}.cpu")
        _positive_or_zero(self.memory_mib, name=f"{self.name}.memory_mib")
        _positive_or_zero(self.gpu, name=f"{self.name}.gpu")
        if not self.state:
            raise ValueError(f"Cluster node {self.name!r} has no state.")

    @property
    def memory_gib(self) -> int:
        return self.memory_mib // 1024

    @property
    def is_available(self) -> bool:
        flags = {part.upper() for part in re.split(r"[+~#*]", self.state) if part}
        return bool(flags) and not flags.intersection(_UNAVAILABLE_STATE_FLAGS)

    def belongs_to(self, partition: str) -> bool:
        return partition in self.partitions

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "partitions": list(self.partitions),
            "cpu": self.cpu,
            "memoryMiB": self.memory_mib,
            "memoryGiB": self.memory_gib,
            "gpu": self.gpu,
            "state": self.state,
            "available": self.is_available,
        }


@dataclass(frozen=True, slots=True)
class ClusterInventory:
    nodes: tuple[ClusterNode, ...]

    def for_partition(self, partition: str) -> tuple[ClusterNode, ...]:
        return tuple(
            node for node in self.nodes
            if node.belongs_to(partition) and node.is_available
        )

    def to_dict(self) -> dict[str, object]:
        return {"nodes": [node.to_dict() for node in self.nodes]}


def _records(output: str) -> tuple[str, ...]:
    normalized = output.replace("\r\n", "\n")
    starts = [match.start() for match in re.finditer(r"(?m)(?<!\S)NodeName=", normalized)]
    if not starts:
        return ()
    starts.append(len(normalized))
    return tuple(normalized[starts[index]:starts[index + 1]].strip() for index in range(len(starts) - 1))


def _fields(record: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for token in re.split(r"\s+", record.strip()):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        result[key] = value
    return result


def parse_scontrol_show_node(output: str) -> ClusterInventory:
    if not isinstance(output, str):
        raise TypeError("scontrol output must be text.")
    nodes: list[ClusterNode] = []
    for record in _records(output):
        fields = _fields(record)
        name = fields.get("NodeName", "").strip()
        partitions_text = fields.get("Partitions", fields.get("Partition", ""))
        partitions = tuple(
            item for item in partitions_text.split(",")
            if item and item.lower() not in {"(null)", "n/a"}
        )
        nodes.append(ClusterNode(
            name=name,
            partitions=partitions,
            cpu=_positive_or_zero(
                fields.get("CPUTot", fields.get("CPUs", 0)),
                name=f"{name}.CPUTot",
            ),
            memory_mib=_positive_or_zero(
                fields.get("RealMemory", 0),
                name=f"{name}.RealMemory",
            ),
            gpu=parse_gpu_gres(fields.get("Gres")),
            state=fields.get("State", "UNKNOWN").split("(", 1)[0],
        ))
    if not nodes:
        raise ValueError("scontrol show node returned no NodeName records.")
    names = [node.name for node in nodes]
    if len(set(names)) != len(names):
        raise ValueError("scontrol show node returned duplicate NodeName records.")
    return ClusterInventory(nodes=tuple(nodes))


class ClusterInventoryService:
    def __init__(
        self,
        *,
        scontrol_executable: str = "scontrol",
        command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.scontrol_executable = scontrol_executable
        self.command_runner = command_runner

    def load(self) -> ClusterInventory:
        completed = self.command_runner(
            (self.scontrol_executable, "show", "node", "--oneliner"),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            shell=False,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "unknown error").strip()
            raise RuntimeError(f"scontrol show node failed: {detail}")
        return parse_scontrol_show_node(completed.stdout)


__all__ = [
    "ClusterInventory",
    "ClusterInventoryService",
    "ClusterNode",
    "parse_gpu_gres",
    "parse_scontrol_show_node",
]
