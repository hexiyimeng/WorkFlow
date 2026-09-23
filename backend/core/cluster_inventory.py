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
    cpu_total: int = 0
    memory_total_mib: int = 0
    gpu_total: int = 0

    def __post_init__(self) -> None:
        if not self.name or self.name != self.name.strip():
            raise ValueError("Cluster node name must be non-empty and canonical.")
        if not self.partitions:
            raise ValueError(f"Cluster node {self.name!r} has no partition.")
        _positive_or_zero(self.cpu, name=f"{self.name}.cpu")
        _positive_or_zero(self.memory_mib, name=f"{self.name}.memory_mib")
        _positive_or_zero(self.gpu, name=f"{self.name}.gpu")
        for field_name, available, configured in (
            ("cpu_total", self.cpu, self.cpu_total),
            ("memory_total_mib", self.memory_mib, self.memory_total_mib),
            ("gpu_total", self.gpu, self.gpu_total),
        ):
            total = _positive_or_zero(configured, name=f"{self.name}.{field_name}")
            if total == 0:
                total = available
            if total < available:
                raise ValueError(f"{self.name}.{field_name} cannot be below available capacity.")
            object.__setattr__(self, field_name, total)
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
            "cpuTotal": self.cpu_total,
            "cpuAllocated": self.cpu_total - self.cpu,
            "memoryMiB": self.memory_mib,
            "memoryGiB": self.memory_gib,
            "memoryTotalMiB": self.memory_total_mib,
            "memoryAllocatedMiB": self.memory_total_mib - self.memory_mib,
            "gpu": self.gpu,
            "gpuTotal": self.gpu_total,
            "gpuAllocated": self.gpu_total - self.gpu,
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


@dataclass(frozen=True, slots=True)
class SinfoNodeResources:
    name: str
    partitions: tuple[str, ...]
    cpu_allocated: int
    cpu_idle: int
    cpu_other: int
    cpu_total: int
    state: str


def parse_sinfo_nodes(
    output: str,
) -> tuple[tuple[SinfoNodeResources, ...], tuple[str, ...], str | None]:
    """Parse one uncompressed ``sinfo --Node`` resource snapshot."""

    if not isinstance(output, str):
        raise TypeError("sinfo output must be text.")
    nodes: dict[str, SinfoNodeResources] = {}
    partition_order: list[str] = []
    default_partition: str | None = None
    for line_number, raw_line in enumerate(output.replace("\r\n", "\n").splitlines(), start=1):
        if not raw_line.strip():
            continue
        parts = [part.strip() for part in raw_line.split("|")]
        if len(parts) != 4:
            raise ValueError(f"Invalid sinfo node row at line {line_number}: {raw_line!r}.")
        name, raw_partition, raw_cpu_state, state = parts
        is_default = raw_partition.endswith("*")
        partition = raw_partition[:-1] if is_default else raw_partition
        if not name or not partition or not state:
            raise ValueError(f"Incomplete sinfo node row at line {line_number}.")
        if partition not in partition_order:
            partition_order.append(partition)
        if is_default:
            if default_partition is not None and default_partition != partition:
                raise ValueError("sinfo reported more than one default partition.")
            default_partition = partition
        cpu_parts = raw_cpu_state.split("/")
        if len(cpu_parts) != 4:
            raise ValueError(
                f"sinfo CPU state for {name!r} must be allocated/idle/other/total."
            )
        allocated, idle, other, total = (
            _positive_or_zero(value, name=f"{name}.cpuState") for value in cpu_parts
        )
        if allocated + idle + other != total:
            raise ValueError(f"sinfo CPU state for {name!r} does not add up to total CPU.")
        existing = nodes.get(name)
        if existing is None:
            nodes[name] = SinfoNodeResources(
                name=name,
                partitions=(partition,),
                cpu_allocated=allocated,
                cpu_idle=idle,
                cpu_other=other,
                cpu_total=total,
                state=state,
            )
        else:
            if (
                existing.cpu_allocated,
                existing.cpu_idle,
                existing.cpu_other,
                existing.cpu_total,
                existing.state.upper(),
            ) != (allocated, idle, other, total, state.upper()):
                raise ValueError(f"sinfo reported conflicting resource rows for {name!r}.")
            if partition not in existing.partitions:
                nodes[name] = replace(
                    existing,
                    partitions=(*existing.partitions, partition),
                )
    if not nodes:
        raise ValueError("sinfo returned no per-node resource rows.")
    return tuple(nodes.values()), tuple(partition_order), default_partition


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


def _tres_fields(value: object) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in str(value or "").split(","):
        if "=" not in item:
            continue
        name, amount = item.split("=", 1)
        result[name.strip().lower()] = amount.strip()
    return result


def _memory_mib(value: object, *, name: str) -> int:
    text = str(value or "0").strip()
    match = re.fullmatch(r"([0-9]+)([KMGTP]?)", text, re.IGNORECASE)
    if match is None:
        raise ValueError(f"{name} has an invalid Slurm memory value: {value!r}.")
    amount = int(match.group(1))
    suffix = match.group(2).upper()
    multipliers = {"": 1, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 ** 2, "P": 1024 ** 3}
    return int(amount * multipliers[suffix])


def _allocated_gpu(fields: Mapping[str, str]) -> int:
    tres = _tres_fields(fields.get("AllocTRES"))
    if "gres/gpu" in tres:
        return _positive_or_zero(tres["gres/gpu"], name="AllocTRES.gres/gpu")
    typed = [
        _positive_or_zero(amount, name=f"AllocTRES.{name}")
        for name, amount in tres.items()
        if name.startswith("gres/gpu:")
    ]
    if typed:
        return sum(typed)
    return parse_gpu_gres(fields.get("GresUsed"))


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
        cpu_total = _positive_or_zero(
            fields.get("CPUTot", fields.get("CPUs", 0)),
            name=f"{name}.CPUTot",
        )
        cpu_allocated = _positive_or_zero(
            fields.get("CPUAlloc", 0),
            name=f"{name}.CPUAlloc",
        )
        memory_total = _positive_or_zero(
            fields.get("RealMemory", 0),
            name=f"{name}.RealMemory",
        )
        alloc_tres = _tres_fields(fields.get("AllocTRES"))
        memory_allocated = (
            _memory_mib(fields["AllocMem"], name=f"{name}.AllocMem")
            if "AllocMem" in fields
            else _memory_mib(alloc_tres.get("mem", 0), name=f"{name}.AllocTRES.mem")
        )
        gpu_total = parse_gpu_gres(fields.get("Gres"))
        gpu_allocated = _allocated_gpu(fields)
        nodes.append(ClusterNode(
            name=name,
            partitions=partitions,
            cpu=max(0, cpu_total - cpu_allocated),
            memory_mib=max(0, memory_total - memory_allocated),
            gpu=max(0, gpu_total - gpu_allocated),
            state=fields.get("State", "UNKNOWN").split("(", 1)[0],
            cpu_total=cpu_total,
            memory_total_mib=memory_total,
            gpu_total=gpu_total,
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
            (
                self.sinfo_executable,
                "--Node",
                "--noheader",
                "--format=%N|%P|%C|%T",
            ),
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
        sinfo_nodes, partitions, default_partition = parse_sinfo_nodes(
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
        sinfo_by_name = {node.name: node for node in sinfo_nodes}
        merged_nodes: list[ClusterNode] = []
        for node in inventory.nodes:
            snapshot = sinfo_by_name.get(node.name)
            if snapshot is None:
                continue
            merged_nodes.append(replace(
                node,
                partitions=snapshot.partitions,
                cpu=min(node.cpu, snapshot.cpu_idle),
                cpu_total=snapshot.cpu_total,
                state=snapshot.state,
            ))
        if not merged_nodes:
            raise RuntimeError("sinfo and scontrol reported no common Slurm nodes.")
        return replace(
            inventory,
            nodes=tuple(merged_nodes),
            partitions=partitions,
            default_partition=default_partition,
        )


__all__ = [
    "ClusterInventory",
    "ClusterInventoryService",
    "ClusterNode",
    "parse_gpu_gres",
    "parse_sinfo_partitions",
    "parse_sinfo_nodes",
    "parse_scontrol_show_node",
]
