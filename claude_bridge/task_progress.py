"""The progress line of a task: what its worker does now, apart from real errors."""

from __future__ import annotations

from datetime import datetime, timezone

from .taskqueue import Task, TaskNotActiveError, TaskNotFoundError, TaskStatus, TaskStore
from .timestamps import utc_timestamp
from .validation import validate_progress

# SQL fragment and parameters that keep the progress (None) or set it ("" clears).
PROGRESS_UPDATE = "progress = CASE WHEN ? THEN progress ELSE ? END"


def progress_params(progress: str | None) -> tuple[bool, str | None]:
    if progress is None:
        return True, None
    return False, validate_progress(progress)


def set_progress(
    store: TaskStore,
    *,
    channel: str,
    task_id: str,
    progress: str,
    now: datetime | None = None,
) -> Task:
    """Set the progress line of a pending or claimed task."""
    keep, value = progress_params(progress)
    with store.write_scope():
        task = store.find(channel, task_id)
        if task is None:
            raise TaskNotFoundError(channel, task_id)
        if task.status not in (TaskStatus.PENDING, TaskStatus.CLAIMED):
            raise TaskNotActiveError(channel, task_id)
        store.conn.execute(
            f"UPDATE tasks SET {PROGRESS_UPDATE}, updated_at = ? WHERE seq = ?",
            (keep, value, utc_timestamp(now or datetime.now(timezone.utc)), task.seq),
        )
        return store.by_seq(task.seq)


__all__ = ["PROGRESS_UPDATE", "progress_params", "set_progress"]
