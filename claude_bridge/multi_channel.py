"""Cursors for one bridge_receive/bridge_wait call over several channels."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .validation import BridgeValidationError, validate_channel

MAX_CHANNELS = 10
_CURSOR_KEYS = ("since_id", "since_seq", "consumer_id")


@dataclass(frozen=True, slots=True)
class ChannelCursor:
    channel: str
    since_id: str | None = None
    since_seq: int | None = None
    consumer_id: str | None = None

    @property
    def has_cursor(self) -> bool:
        return bool(self.since_id or self.consumer_id) or self.since_seq is not None

    def read_arguments(self, compact: bool) -> dict[str, Any]:
        return {
            "since_id": self.since_id,
            "since_seq": self.since_seq,
            "consumer_id": self.consumer_id,
            "compact": compact,
        }


def parse_channel_cursors(
    arguments: dict[str, Any], *, require_cursor: bool
) -> list[ChannelCursor]:
    """The ``channels`` list of a call, validated; no top-level channel or cursor."""
    for key in ("channel", *_CURSOR_KEYS):
        if key in arguments:
            raise BridgeValidationError(
                key, "conflict", "give cursors inside channels, not next to it"
            )
    entries = arguments["channels"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_CHANNELS:
        raise BridgeValidationError(
            "channels", "invalid", f"must be a list of 1 to {MAX_CHANNELS} entries"
        )
    cursors = [_parse_entry(entry) for entry in entries]
    if len({c.channel for c in cursors}) != len(cursors):
        raise BridgeValidationError("channels", "duplicate", "names a channel twice")
    if require_cursor and not all(c.has_cursor for c in cursors):
        raise BridgeValidationError(
            "channels", "cursor_required",
            "every entry needs since_id, since_seq or consumer_id",
        )
    return cursors


def _parse_entry(entry: Any) -> ChannelCursor:
    if not isinstance(entry, dict) or "channel" not in entry:
        raise BridgeValidationError(
            "channels", "invalid", "each entry must be an object with a channel"
        )
    unknown = set(entry) - {"channel", *_CURSOR_KEYS}
    if unknown:
        raise BridgeValidationError(
            "channels", "invalid", f"unknown entry key(s): {', '.join(sorted(unknown))}"
        )
    return ChannelCursor(
        channel=validate_channel(entry["channel"]),
        since_id=entry.get("since_id"),
        since_seq=entry.get("since_seq"),
        consumer_id=entry.get("consumer_id"),
    )


__all__ = ["MAX_CHANNELS", "ChannelCursor", "parse_channel_cursors"]
