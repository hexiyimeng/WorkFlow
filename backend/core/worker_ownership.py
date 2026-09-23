"""Unforgeable-by-workflow logical resources used to bind Slurm Workers.

The resource names contain only SHA-256 digests.  Raw submission tokens never
enter the Scheduler identity payload, and these resources are injected by the
backend after user-supplied Worker Profile resources have been validated.
"""

from __future__ import annotations

import hashlib


EXECUTION_RESOURCE_PREFIX = "workflow-execution-"
SUBMISSION_RESOURCE_PREFIX = "workflow-submission-"


def _ownership_digest(value: str, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string.")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def execution_ownership_resource(execution_id: str) -> str:
    return EXECUTION_RESOURCE_PREFIX + _ownership_digest(
        execution_id, name="execution_id"
    )


def submission_ownership_resource(submission_token: str) -> str:
    return SUBMISSION_RESOURCE_PREFIX + _ownership_digest(
        submission_token, name="submission_token"
    )


def is_ownership_resource(name: object) -> bool:
    return isinstance(name, str) and name.startswith(
        (EXECUTION_RESOURCE_PREFIX, SUBMISSION_RESOURCE_PREFIX)
    )
