"""Bounded task-failure classification for compute-node fault tolerance.

Workers report each failed block attempt with a normalized signature and the
host it ran on.  A driver-side tracker folds the reports into one of four
categories:

- ``transient``: first/infrequent occurrences; plain Dask retries handle them.
- ``node``: the same signature keeps failing on a single host, or repeated
  failures with an infrastructure pattern; the host is recorded as a bad node
  and later Slurm submissions exclude it.
- ``deterministic``: one signature fails on two or more hosts, so retrying on
  any node cannot succeed; the execution should stop early with a diagnosis.
- ``systemic``: an infrastructure-pattern failure appears on multiple hosts at
  once (shared storage/network outage); the execution should fail fast with a
  distinct alert instead of being mistaken for a code bug.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import time

FAILURE_EVENT_TOPIC = "workflow-task-failure"

BAD_NODES_FILENAME = "bad-nodes.json"
# Repeatedly marked nodes escalate through longer exclusion windows so that
# chronically broken nodes stop "reviving" between runs.
BAD_NODE_TTL_TIERS = (48 * 3600, 7 * 24 * 3600, 30 * 24 * 3600)

_INFRA_RE = re.compile(
    r"cuda\s*(error|failure|initialization|exception)"
    r"|ecc\s*error|\bxid\b|device-side assert"
    r"|input/output error|errno\s*5|\beio\b"
    r"|transport endpoint|broken pipe|connection (reset|refused|timed out)"
    r"|timed?\s*out|no route to host|network is unreachable"
    r"|lustre|\bnfs\b",
    re.IGNORECASE,
)

_NORMALIZE_RES = (
    re.compile(r"0x[0-9a-fA-F]+"),
    re.compile(r"\d+"),
    re.compile(r"[^\w\s]+"),
)


def failure_signature(exc: BaseException) -> str:
    """Stable short signature of an exception, independent of paths/numbers."""

    text = f"{type(exc).__name__}: {exc}"[:300]
    text = re.sub(r"(?:[A-Za-z]:)?[/\\][\w./\\-]*", " ", text)
    for pattern in _NORMALIZE_RES:
        text = pattern.sub(" ", text)
    text = " ".join(text.split()).lower()
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def is_infrastructure_error(exc: BaseException) -> bool:
    return bool(_INFRA_RE.search(f"{type(exc).__name__}: {exc}"))


def node_failure_threshold() -> int:
    raw = os.getenv("WorkFlow_NODE_FAILURE_THRESHOLD", "2").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 2


@dataclass(frozen=True)
class FailureVerdict:
    category: str
    message: str


@dataclass
class TaskFailureTracker:
    """Fold per-attempt block failure reports into A/B/C/D verdicts."""

    threshold: int = 2
    bad_nodes: set[str] = field(default_factory=set)
    _records: dict[str, dict] = field(default_factory=dict)

    TRANSIENT = "transient"
    NODE = "node"
    DETERMINISTIC = "deterministic"
    SYSTEMIC = "systemic"

    def observe(self, *, host: str, signature: str, infra: bool,
                summary: str) -> FailureVerdict:
        record = self._records.setdefault(
            signature,
            {"hosts": set(), "count": 0, "infra": infra, "summary": summary},
        )
        if summary and not record["summary"]:
            record["summary"] = summary
        record["hosts"].add(host)
        record["count"] += 1

        if len(record["hosts"]) >= 2:
            if infra:
                return FailureVerdict(
                    self.SYSTEMIC,
                    "infrastructure-pattern failure on multiple hosts "
                    f"{sorted(record['hosts'])}: {summary}",
                )
            return FailureVerdict(
                self.DETERMINISTIC,
                "same failure signature on multiple hosts "
                f"{sorted(record['hosts'])}; retrying cannot succeed: {summary}",
            )
        if host in self.bad_nodes:
            return FailureVerdict(
                self.NODE, f"known bad node {host} failed again: {summary}",
            )
        # Infrastructure-pattern failures (CUDA/ECC/IO/timeout) almost always
        # recur on the same host: mark on the first occurrence so isolation
        # lands before the retry budget burns out.  Other signatures keep the
        # threshold as a guard against transient noise.
        if infra or record["count"] >= self.threshold:
            self.bad_nodes.add(host)
            return FailureVerdict(
                self.NODE,
                f"node {host} marked as bad after {record['count']} "
                f"failures: {summary}",
            )
        return FailureVerdict(self.TRANSIENT, "transient failure")

    def summary_for(self, host: str) -> str:
        for record in self._records.values():
            if host in record["hosts"]:
                return str(record["summary"])
        return ""


def report_block_failure(node_name: str, node_id: str | None,
                         exc: BaseException) -> None:
    """Best-effort report of a failed block attempt to the Scheduler event log."""

    try:
        from distributed import get_worker
    except ImportError:
        return
    try:
        worker = get_worker()
    except Exception:
        return
    try:
        worker.log_event(
            FAILURE_EVENT_TOPIC,
            {
                "node": node_name,
                "node_id": node_id,
                "host": socket.gethostname(),
                "signature": failure_signature(exc),
                "infra": is_infrastructure_error(exc),
                "summary": f"{type(exc).__name__}: {str(exc)[:200]}",
            },
        )
    except Exception:
        pass


def load_bad_nodes(path: str | Path, *, now: float | None = None) -> set[str]:
    """Read persisted bad nodes still within their TTL window."""

    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(data, dict):
        return set()
    now = time.time() if now is None else now
    result: set[str] = set()
    for host, record in data.items():
        if not isinstance(record, dict):
            continue
        tier = min(
            int(record.get("tier", 0) or 0), len(BAD_NODE_TTL_TIERS) - 1
        )
        if now - float(record.get("marked_at", 0) or 0) < BAD_NODE_TTL_TIERS[tier]:
            result.add(str(host))
    return result


def record_bad_node(path: str | Path, host: str, summary: str) -> None:
    """Persist one bad-node mark, escalating the TTL tier on repeats."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    previous = data.get(host)
    tier = 0
    if isinstance(previous, dict):
        tier = min(
            int(previous.get("tier", 0) or 0) + 1,
            len(BAD_NODE_TTL_TIERS) - 1,
        )
    data[host] = {
        "marked_at": time.time(),
        "summary": str(summary)[:200],
        "tier": tier,
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=1), encoding="utf-8")
    temporary.replace(path)
