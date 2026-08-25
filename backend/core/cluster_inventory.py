from __future__ import annotations

from dataclasses import dataclass, replace
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
    partitions: tuple[str, ...] = ()
    default_partition: str | None = None

    @property
    def partition_names(self) -> tuple[str, ...]:
        if self.partitions:
            return self.partitions
        discovered: list[str] = []
        for node in self.nodes:
            for partition in node.partitions:
                if partition not in discovered:
                    discovered.append(partition)
        return tuple(discovered)

    def for_partition(self, partition: str) -> tuple[ClusterNode, ...]:
        return tuple(
            node for node in self.nodes
            if node.belongs_to(partition) and node.is_available
        )

    def for_partitions(
        self,
        partitions: Sequence[str],
        *,
        excluded_nodes: Sequence[str] = (),
    ) -> tuple[ClusterNode, ...]:
        selected = frozenset(partitions)
        excluded = frozenset(excluded_nodes)
        return tuple(
            node for node in self.nodes
            if node.name not in excluded
            and node.is_available
            and selected.intersection(node.partitions)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "partitions": list(self.partition_names),
            "defaultPartition": self.default_partition,
            "nodes": [node.to_dict() for node in self.nodes],
        }


def parse_sinfo_partitions(output: str) -> tuple[tuple[str, ...], str | None]:
    """Parse ``sinfo --noheader --format=%P`` without assuming one partition."""

    if not isinstance(output, str):
        raise TypeError("sinfo output must be text.")
    partitions: list[str] = []
    default_partition: str | None = None
    for raw_line in output.replace("\r\n", "\n").splitlines():
        value = raw_line.strip()
        if not value:
            continue
        is_default = value.endswith("*")
        name = value[:-1] if is_default else value
        if not name:
            continue
        if name not in partitions:
            partitions.append(name)
        if is_default:
            if default_partition is not None and default_partition != name:
                raise ValueError("sinfo reported more than one default partition.")
            default_partition = name
    if not partitions:
        raise ValueError("sinfo returned no Slurm partitions.")
    return tuple(partitions), default_partition


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
        sinfo_executable: str = "sinfo",
        scontrol_executable: str = "scontrol",
        command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.sinfo_executable = sinfo_executable
        self.scontrol_executable = scontrol_executable
        self.command_runner = command_runner

    def load(self) -> ClusterInventory:
        partition_result = self.command_runner(
            (self.sinfo_executable, "--noheader", "--format=%P"),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            shell=False,
        )
        if partition_result.returncode != 0:
            detail = (
                partition_result.stderr
                or partition_result.stdout
                or "unknown error"
            ).strip()
            raise RuntimeError(f"sinfo partition discovery failed: {detail}")
        partitions, default_partition = parse_sinfo_partitions(
            partition_result.stdout
        )
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
        inventory = parse_scontrol_show_node(completed.stdout)
        return replace(
            inventory,
            partitions=partitions,
            default_partition=default_partition,
        )


__all__ = [
    "ClusterInventory",
    "ClusterInventoryService",
    "ClusterNode",
    "parse_gpu_gres",
    "parse_sinfo_partitions",
    "parse_scontrol_show_node",
]
