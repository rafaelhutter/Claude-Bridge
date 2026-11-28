"""Compact reads: a structured message comes back once, not twice."""

from __future__ import annotations

import pytest

import claude_bridge.server as bridge

ENVELOPE = {"schema_version": 1, "type": "note", "content": {"text": "hi"}}


async def data(name, args):
    _content, result = await bridge.dispatch_tool(name, args, structured=True)
    return result


async def seed():
    await bridge.dispatch_tool(
        "bridge_send", {"channel": "c", "sender": "a", "message": ENVELOPE}
    )
    await bridge.dispatch_tool(
        "bridge_send", {"channel": "c", "sender": "a", "content": "plain text"}
    )


@pytest.mark.asyncio
async def test_compact_receive_drops_the_raw_copy_of_a_structured_message(fresh_db):
    await seed()
    structured, plain = (await data("bridge_receive", {"channel": "c", "compact": True}))[
        "messages"
    ]
    assert "content" not in structured
    assert structured["message"]["content"] == {"text": "hi"}
    assert plain["content"] == "plain text"


@pytest.mark.asyncio
async def test_compact_wait_drops_the_raw_copy(fresh_db):
    await seed()
    result = await data(
        "bridge_wait",
        {"channel": "c", "consumer_id": "w", "compact": True, "timeout_seconds": 0},
    )
    assert "content" not in result["messages"][0]


@pytest.mark.asyncio
async def test_default_receive_keeps_both_forms(fresh_db):
    await seed()
    structured = (await data("bridge_receive", {"channel": "c"}))["messages"][0]
    assert structured["content"].startswith("{") and "message" in structured
