"""Lean bridge_tasks listings: status/assignee filters and a compact row view."""

from __future__ import annotations

from typing import Any

from .taskqueue import Task, TaskStatus, task_to_dict
from .validation import BridgeValidationError

OPEN_STATUSES = (TaskStatus.PENDING.value, TaskStatus.CLAIMED.value)

# The fields a dispatcher needs to steer workers; result and payload stay out.
COMPACT_FIELDS = (
    "task_id",
    "title",
    "status",
    "priority",
    "attempts",
    "max_attempts",
    "assignee",
    "lease_owner",
    "lease_expires_at",
    "progress",
)


def requested_statuses(arguments: dict[str, Any]) -> list[str] | None:
    """The status filter: ``status`` (one or a list), narrowed by ``exclude_terminal``."""
    status = arguments.get("status")
    if status is None:
        statuses = None
    elif isinstance(status, str):
        statuses = [status]
    elif isinstance(status, list) and all(isinstance(s, str) for s in status):
        statuses = status
    else:
        raise BridgeValidationError(
            "status", "invalid_type", "must be a status or a list of statuses"
        )
    if not arguments.get("exclude_terminal"):
        return statuses
    return [s for s in (statuses or OPEN_STATUSES) if s in OPEN_STATUSES]


def task_row(task: Task, *, compact: bool) -> dict[str, Any]:
    row = task_to_dict(task, include_payload=False)
    if not compact:
        return row
    return {field: row[field] for field in COMPACT_FIELDS if field in row}


__all__ = ["COMPACT_FIELDS", "OPEN_STATUSES", "requested_statuses", "task_row"]
