"""Lease upkeep for the worker that holds a task: give it back, or keep it longer.

Neither counts as a failure. Only a claim that ends in ``fail`` or in an expired
lease uses up one of the task's attempts.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .task_progress import PROGRESS_UPDATE, progress_params
from .taskqueue import Task, TaskStore
from .timestamps import as_utc, utc_timestamp
from .validation import (
    BridgeValidationError,
    validate_channel,
    validate_non_negative_int,
    validate_reference,
)

_MAX_SECONDS = 10**9  # Same ceiling as the task queue's delays and leases.


def release_task(
    store: TaskStore,
    *,
    channel: str,
    task_id: str,
    lease_token: str,
    delay_seconds: int = 0,
    progress: str | None = None,
    now: datetime | None = None,
) -> Task:
    """Return a held task to the queue untouched; its claim's attempt is refunded."""
    channel, task_id, lease_token = _lease_reference(channel, task_id, lease_token)
    keep_progress, new_progress = progress_params(progress)
    delay_seconds = validate_non_negative_int(
        delay_seconds, "delay_seconds", maximum=_MAX_SECONDS
    )
    instant = _instant(now)
    available_at = utc_timestamp(instant + timedelta(seconds=delay_seconds))
    with store.write_scope():
        held = store.held_task(channel, task_id, lease_token)
        store.conn.execute(
            "UPDATE tasks SET status = 'pending', lease_owner = NULL, "
            "lease_token = NULL, lease_expires_at = NULL, "
            "attempts = MAX(attempts - 1, 0), available_at = ?, updated_at = ?, "
            f"{PROGRESS_UPDATE} WHERE seq = ?",
            (
                available_at,
                utc_timestamp(instant),
                keep_progress,
                new_progress,
                held.seq,
            ),
        )
        return store.by_seq(held.seq)


def extend_lease(
    store: TaskStore,
    *,
    channel: str,
    task_id: str,
    lease_token: str,
    lease_seconds: int,
    progress: str | None = None,
    now: datetime | None = None,
) -> Task:
    """Move a held lease's expiry to ``lease_seconds`` from now, without a new claim."""
    channel, task_id, lease_token = _lease_reference(channel, task_id, lease_token)
    keep_progress, new_progress = progress_params(progress)
    lease_seconds = validate_non_negative_int(
        lease_seconds, "lease_seconds", maximum=_MAX_SECONDS
    )
    if lease_seconds < 1:
        raise BridgeValidationError("lease_seconds", "out_of_range", "must be at least 1")
    instant = _instant(now)
    expires_at = utc_timestamp(instant + timedelta(seconds=lease_seconds))
    with store.write_scope():
        held = store.held_task(channel, task_id, lease_token)
        store.conn.execute(
            "UPDATE tasks SET lease_expires_at = ?, updated_at = ?, "
            f"{PROGRESS_UPDATE} WHERE seq = ?",
            (expires_at, utc_timestamp(instant), keep_progress, new_progress, held.seq),
        )
        return store.by_seq(held.seq)


def _lease_reference(channel: str, task_id: str, lease_token: str) -> tuple[str, str, str]:
    return (
        validate_channel(channel),
        validate_reference(task_id, "task_id"),
        validate_reference(lease_token, "lease_token"),
    )


def _instant(now: datetime | None) -> datetime:
    return as_utc(now or datetime.now(timezone.utc))


__all__ = ["extend_lease", "release_task"]
