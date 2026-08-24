from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any, Mapping, Sequence


CPU_GENERAL_PROFILE = "cpu-general"
GPU_INFERENCE_PROFILE = "gpu-inference"
DEFAULT_WORKER_PROFILE = CPU_GENERAL_PROFILE

_PROFILE_NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}\Z")
_MEMORY_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*(gib|gb)\Z", re.IGNORECASE)
_RESERVED_LOGICAL_RESOURCES = frozenset({"CPU", "GPU"})


def normalize_worker_profile(value: object, *, owner: str = "Node") -> str:
    if not isinstance(value, str):
        raise ValueError(
            f"{owner}.required_worker_profile must be a string, got {value!r}."
        )
    normalized = value.strip().lower()
    if _PROFILE_NAME_RE.fullmatch(normalized) is None:
        raise ValueError(
            f"{owner}.required_worker_profile must be a lowercase profile slug, "
            f"got {value!r}."
        )
    return normalized


def resolve_worker_profile(node_cls: type) -> str:
    return normalize_worker_profile(
        getattr(node_cls, "required_worker_profile", DEFAULT_WORKER_PROFILE),
        owner=getattr(node_cls, "__name__", str(node_cls)),
    )


def _positive_integer(value: object, *, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {value!r}.")
    return value


def _nonnegative_integer(value: object, *, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.")
    return value


def parse_memory_gib(value: object, *, name: str = "memory") -> int:
    if type(value) is int:
        return _positive_integer(value, name=name)
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a positive GiB value such as '32GB'.")
    match = _MEMORY_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"{name} must use GB or GiB syntax, for example '32GB'.")
    amount = float(match.group(1))
    if not math.isfinite(amount) or amount <= 0:
        raise ValueError(f"{name} must be positive.")
    # Slurm's --mem=NG is integral. Round up so a decimal user request is
    # never silently under-allocated.
    return int(math.ceil(amount))


@dataclass(frozen=True, slots=True)
class PhysicalResources:
    cpu: int
    memory_gib: int
    gpu: int = 0

    def __post_init__(self) -> None:
        _positive_integer(self.cpu, name="physical_resources.cpu")
        _positive_integer(self.memory_gib, name="physical_resources.memory")
        gpu = _nonnegative_integer(self.gpu, name="physical_resources.gpu")
        if gpu > 1:
            raise ValueError(
                "physical_resources.gpu must be 0 or 1 because each GPU Profile "
                "Worker is isolated to exactly one CUDA device."
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "cpu": self.cpu,
            "memory": f"{self.memory_gib}GB",
            "gpu": self.gpu,
        }


def _logical_resources(
    value: object,
    *,
    profile_name: str,
    physical: PhysicalResources,
) -> dict[str, float]:
    if value is None:
        result: dict[str, float] = {}
    elif not isinstance(value, Mapping):
        raise ValueError("logical_resources must be an object.")
    else:
        result = {}
        for key, raw_amount in value.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError("logical_resources names must be non-empty strings.")
            if isinstance(raw_amount, bool) or not isinstance(raw_amount, (int, float)):
                raise ValueError(f"logical_resources[{key!r}] must be numeric.")
            amount = float(raw_amount)
            if not math.isfinite(amount) or amount <= 0:
                raise ValueError(f"logical_resources[{key!r}] must be positive.")
            result[key.strip()] = amount

    expected = {
        profile_name: 1.0,
        "CPU": float(physical.cpu),
        **({"GPU": float(physical.gpu)} if physical.gpu else {}),
    }
    for key, amount in expected.items():
        existing = result.get(key)
        if existing is not None and existing != amount:
            raise ValueError(
                f"logical_resources[{key!r}] must equal physical capability {amount:g}."
            )
        result[key] = amount
    if physical.gpu == 0 and "GPU" in result:
        raise ValueError("A CPU-only profile must not advertise logical GPU capability.")
    return dict(sorted(result.items()))


@dataclass(frozen=True, slots=True)
class WorkerProfile:
    name: str
    physical_resources: PhysicalResources
    logical_resources: Mapping[str, float]
    threads: int

    def __post_init__(self) -> None:
        name = normalize_worker_profile(self.name, owner="WorkerProfile")
        if not isinstance(self.physical_resources, PhysicalResources):
            raise ValueError("physical_resources must be PhysicalResources.")
        threads = _positive_integer(self.threads, name="threads")
        if threads > self.physical_resources.cpu:
            raise ValueError("threads must not exceed physical_resources.cpu.")
        logical = _logical_resources(
            self.logical_resources,
            profile_name=name,
            physical=self.physical_resources,
        )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "logical_resources", logical)

    @property
    def capabilities(self) -> tuple[str, ...]:
        return tuple(
            name
            for name in self.logical_resources
            if name not in _RESERVED_LOGICAL_RESOURCES
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "physical_resources": self.physical_resources.to_dict(),
            "logical_resources": dict(self.logical_resources),
            "capabilities": list(self.capabilities),
            "threads": self.threads,
        }

    @classmethod
    def from_dict(cls, value: object) -> "WorkerProfile":
        if not isinstance(value, Mapping):
            raise ValueError("Worker Profile must be an object.")
        name = normalize_worker_profile(value.get("name"), owner="WorkerProfile")
        raw_physical = value.get("physical_resources")
        if raw_physical is None:
            raw_physical = value
        if not isinstance(raw_physical, Mapping):
            raise ValueError(f"Worker Profile {name!r} physical_resources must be an object.")
        physical = PhysicalResources(
            cpu=_positive_integer(raw_physical.get("cpu"), name=f"{name}.cpu"),
            memory_gib=parse_memory_gib(
                raw_physical.get("memory", raw_physical.get("memoryGiB")),
                name=f"{name}.memory",
            ),
            gpu=_nonnegative_integer(raw_physical.get("gpu", 0), name=f"{name}.gpu"),
        )
        raw_logical = value.get("logical_resources")
        if raw_logical is None:
            capabilities = value.get("capabilities", ())
            if isinstance(capabilities, (str, bytes)) or not isinstance(capabilities, Sequence):
                raise ValueError(f"{name}.capabilities must be an array.")
            raw_logical = {
                normalize_worker_profile(item, owner=f"{name}.capabilities"): 1
                for item in capabilities
            }
        return cls(
            name=name,
            physical_resources=physical,
            logical_resources=_logical_resources(
                raw_logical,
                profile_name=name,
                physical=physical,
            ),
            threads=_positive_integer(value.get("threads", 1), name=f"{name}.threads"),
        )


def parse_worker_profiles(value: object) -> tuple[WorkerProfile, ...]:
    if not isinstance(value, list):
        raise ValueError("workerProfiles must be an array.")
    profiles = tuple(WorkerProfile.from_dict(item) for item in value)
    names = [profile.name for profile in profiles]
    if len(set(names)) != len(names):
        raise ValueError("workerProfiles must not contain duplicate names.")
    return profiles


def dask_resources_for_node(node_cls: type) -> dict[str, float]:
    return {resolve_worker_profile(node_cls): 1.0}


def worker_logical_resources(worker: Any) -> dict[str, float]:
    """Read configured resources across supported Distributed Worker APIs.

    Distributed 2026 exposes Worker resources through
    ``worker.state.total_resources`` (and ``worker.total_resources``), while
    older releases also exposed ``worker.resources``. Runtime task validation
    must inspect the same totals that the Scheduler used for placement.
    """

    state = getattr(worker, "state", None)
    resources = getattr(state, "total_resources", None)
    if resources is None:
        resources = getattr(worker, "total_resources", None)
    if resources is None:
        resources = getattr(worker, "resources", None)
    return {
        str(name): float(amount)
        for name, amount in dict(resources or {}).items()
    }


def dask_annotation_kwargs(node_cls: type, node_id: str | None) -> dict[str, object]:
    profile = resolve_worker_profile(node_cls)
    return {
        "brainflow_node_id": node_id,
        "required_worker_profile": profile,
        "resources": {profile: 1.0},
    }


__all__ = [
    "CPU_GENERAL_PROFILE",
    "DEFAULT_WORKER_PROFILE",
    "GPU_INFERENCE_PROFILE",
    "PhysicalResources",
    "WorkerProfile",
    "dask_annotation_kwargs",
    "dask_resources_for_node",
    "normalize_worker_profile",
    "parse_memory_gib",
    "parse_worker_profiles",
    "resolve_worker_profile",
    "worker_logical_resources",
]
