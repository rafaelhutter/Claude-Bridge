"""Read cursors for bridge_receive/bridge_wait: full id, unique id prefix or seq."""

from __future__ import annotations

import re
import sqlite3

from .reliability import resolve_channel_message_seq
from .validation import BridgeValidationError, validate_non_negative_int

MIN_PREFIX_CHARS = 8
_ID_PREFIX_RE = re.compile(r"[0-9a-f-]+")


def resolve_message_cursor(
    conn: sqlite3.Connection, channel: str, since_id: str
) -> int | None:
    """The seq of the channel message ``since_id`` names in full or by unique prefix."""
    exact = resolve_channel_message_seq(conn, channel, since_id)
    if exact is not None or not _is_id_prefix(since_id):
        return exact
    # The prefix is plain hex, so it holds no LIKE wildcard.
    rows = conn.execute(
        "SELECT seq FROM messages WHERE channel = ? AND id LIKE ? || '%' LIMIT 2",
        (channel, since_id.lower()),
    ).fetchall()
    if len(rows) > 1:
        raise BridgeValidationError(
            "since_id", "ambiguous", "prefix matches more than one message"
        )
    return int(rows[0][0]) if rows else None


def _is_id_prefix(value: str) -> bool:
    lowered = value.lower()
    return len(lowered) >= MIN_PREFIX_CHARS and bool(_ID_PREFIX_RE.fullmatch(lowered))


def validate_since_seq(value: object) -> int:
    return validate_non_negative_int(value, "since_seq")


def stale_cursor_error() -> BridgeValidationError:
    return BridgeValidationError(
        "since_id",
        "not_found",
        "no message in this channel has this id or id prefix; "
        "re-sync without since_id or pass since_seq",
    )


__all__ = [
    "MIN_PREFIX_CHARS",
    "resolve_message_cursor",
    "stale_cursor_error",
    "validate_since_seq",
]
