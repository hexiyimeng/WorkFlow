from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from core.worker_profiles import WorkerProfile, normalize_worker_profile


def _positive_integer(value: object, *, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {value!r}.")
    return value


@dataclass(frozen=True, slots=True)
class WorkerPool:
    profile: str
    minimum_jobs: int
    maximum_jobs: int
    processes: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "profile",
            normalize_worker_profile(self.profile, owner="WorkerPool"),
        )
        _positive_integer(self.minimum_jobs, name=f"WorkerPool[{self.profile}].minimum_jobs")
        _positive_integer(self.maximum_jobs, name=f"WorkerPool[{self.profile}].maximum_jobs")
        if self.maximum_jobs < self.minimum_jobs:
            raise ValueError("maximum_jobs must be greater than or equal to minimum_jobs.")
        _positive_integer(
            self.processes,
            name=f"WorkerPool[{self.profile}].processes",
        )

    @property
    def worker_count(self) -> int:
        return self.minimum_jobs * self.processes

    def validate_profile(self, profile: WorkerProfile) -> None:
        if profile.name != self.profile:
            raise ValueError(
                f"WorkerPool profile {self.profile!r} does not match {profile.name!r}."
            )
        if profile.physical_resources.gpu > 0 and self.processes != 1:
            raise ValueError(
                f"GPU Worker Pool {self.profile!r} must use processes=1; "
                "one Slurm job launches exactly one GPU Worker."
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "processes": self.processes,
            "minimum_jobs": self.minimum_jobs,
            "maximum_jobs": self.maximum_jobs,
            "workerCount": self.worker_count,
        }

    @classmethod
    def from_dict(cls, value: object) -> "WorkerPool":
        if not isinstance(value, Mapping):
            raise ValueError("Worker Pool must be an object.")
        return cls(
            profile=value.get("profile"),
            processes=_positive_integer(
                value.get("processes", 1),
                name="WorkerPool.processes",
            ),
            minimum_jobs=_positive_integer(value.get("minimum_jobs"), name="WorkerPool.minimum_jobs"),
            maximum_jobs=_positive_integer(value.get("maximum_jobs"), name="WorkerPool.maximum_jobs"),
        )


def parse_worker_pools(value: object) -> tuple[WorkerPool, ...]:
    if not isinstance(value, list):
        raise ValueError("workerPools must be an array.")
    pools = tuple(WorkerPool.from_dict(item) for item in value)
    names = [pool.profile for pool in pools]
    if len(set(names)) != len(names):
        raise ValueError("workerPools must contain exactly one Pool per Profile.")
    return pools


__all__ = ["WorkerPool", "parse_worker_pools"]
