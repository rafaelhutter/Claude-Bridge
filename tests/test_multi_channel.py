"""Several channels in one bridge_receive/bridge_wait call."""

from __future__ import annotations

import asyncio

import pytest

import claude_bridge.server as bridge
from claude_bridge.validation import BridgeValidationError


async def data(name, args):
    _content, result = await bridge.dispatch_tool(name, args, structured=True)
    return result


async def send(channel, content):
    return await data("bridge_send", {"channel": channel, "sender": "a", "content": content})


def by_channel(result):
    return {b["channel"]: [m["content"] for m in b["messages"]] for b in result["channels"]}


@pytest.mark.asyncio
async def test_receive_reads_each_channel_after_its_own_cursor(fresh_db):
    seen = await send("results", "old result")
    await send("results", "new result")
    await send("agents", "hello")
    result = await data(
        "bridge_receive",
        {"channels": [
            {"channel": "results", "since_id": seen["id"]},
            {"channel": "agents", "since_seq": 0},
        ]},
    )
    assert by_channel(result) == {"results": ["new result"], "agents": ["hello"]}
    assert result["channels"][0]["next_seq"] == result["channels"][0]["messages"][-1]["seq"]


@pytest.mark.asyncio
async def test_wait_returns_when_any_channel_has_news(fresh_db):
    a = await send("a", "seen")
    b = await send("b", "seen")

    async def later():
        await asyncio.sleep(0.2)
        await send("b", "news")

    writer = asyncio.create_task(later())
    result = await data(
        "bridge_wait",
        {"channels": [
            {"channel": "a", "since_id": a["id"]},
            {"channel": "b", "since_seq": b["seq"]},
        ], "timeout_seconds": 5},
    )
    await writer
    assert by_channel(result) == {"a": [], "b": ["news"]}
    assert result["timed_out"] is False


@pytest.mark.asyncio
async def test_wait_times_out_with_every_channel_empty(fresh_db):
    a = await send("a", "seen")
    result = await data(
        "bridge_wait",
        {"channels": [{"channel": "a", "since_id": a["id"]}], "timeout_seconds": 0},
    )
    assert result["timed_out"] is True and by_channel(result) == {"a": []}


@pytest.mark.asyncio
async def test_compact_applies_to_every_channel(fresh_db):
    envelope = {"schema_version": 1, "type": "note", "content": {"x": 1}}
    await bridge.dispatch_tool("bridge_send", {"channel": "a", "sender": "s", "message": envelope})
    result = await data(
        "bridge_receive", {"channels": [{"channel": "a"}], "compact": True}
    )
    assert "content" not in result["channels"][0]["messages"][0]


@pytest.mark.asyncio
async def test_stale_cursor_in_one_channel_fails_the_call(fresh_db):
    await send("a", "x")
    with pytest.raises(BridgeValidationError) as err:
        await data(
            "bridge_receive",
            {"channels": [{"channel": "a"}, {"channel": "b", "since_id": "ffffffffff"}]},
        )
    assert err.value.code == "not_found"


@pytest.mark.parametrize(
    "args, code",
    [
        ({"channels": []}, "invalid"),
        ({"channels": [{"channel": "a"}, {"channel": "a"}]}, "duplicate"),
        ({"channels": [{"channel": "a"}], "channel": "a"}, "conflict"),
        ({"channels": [{"channel": "a", "bogus": 1}]}, "invalid"),
    ],
)
@pytest.mark.asyncio
async def test_bad_channel_lists_are_refused(fresh_db, args, code):
    with pytest.raises(BridgeValidationError) as err:
        await data("bridge_receive", args)
    assert err.value.code == code


@pytest.mark.asyncio
async def test_wait_needs_a_cursor_per_channel(fresh_db):
    with pytest.raises(BridgeValidationError) as err:
        await data("bridge_wait", {"channels": [{"channel": "a"}], "timeout_seconds": 0})
    assert err.value.code == "cursor_required"
