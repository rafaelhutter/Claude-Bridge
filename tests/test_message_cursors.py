"""Read cursors: full id, unique id prefix or since_seq; a stale one is an error."""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

import claude_bridge.server as bridge
from claude_bridge.validation import BridgeValidationError


async def data(name, args):
    _content, result = await bridge.dispatch_tool(name, args, structured=True)
    return result


async def send(channel, content):
    return await data("bridge_send", {"channel": channel, "sender": "a", "content": content})


def contents(result):
    return [m["content"] for m in result["messages"]]


@pytest.mark.asyncio
async def test_since_id_prefix_reads_after_that_message(fresh_db):
    first = await send("c", "one")
    await send("c", "two")
    result = await data("bridge_receive", {"channel": "c", "since_id": first["id"][:8]})
    assert contents(result) == ["two"]


@pytest.mark.asyncio
async def test_prefix_shorter_than_eight_chars_is_not_a_cursor(fresh_db):
    first = await send("c", "one")
    with pytest.raises(BridgeValidationError) as err:
        await data("bridge_receive", {"channel": "c", "since_id": first["id"][:7]})
    assert err.value.code == "not_found"


@pytest.mark.asyncio
async def test_ambiguous_prefix_is_an_error(fresh_db):
    conn = bridge.db()
    for suffix in ("a", "b"):
        conn.execute(
            "INSERT INTO messages (id, channel, sender, content, timestamp) "
            "VALUES (?, 'c', 's', 'x', '2026-01-01T00:00:00Z')",
            ("deadbeef-0000-0000-0000-00000000000" + suffix,),
        )
    conn.commit()
    with pytest.raises(BridgeValidationError) as err:
        await data("bridge_receive", {"channel": "c", "since_id": "deadbeef"})
    assert err.value.code == "ambiguous"


@pytest.mark.asyncio
async def test_since_seq_reads_after_that_seq_and_next_seq_resumes(fresh_db):
    first = await send("c", "one")
    await send("c", "two")
    result = await data("bridge_receive", {"channel": "c", "since_seq": first["seq"]})
    assert contents(result) == ["two"]
    await send("c", "three")
    resumed = await data(
        "bridge_receive", {"channel": "c", "since_seq": result["next_seq"]}
    )
    assert contents(resumed) == ["three"]


@pytest.mark.asyncio
async def test_since_seq_zero_reads_from_the_start(fresh_db):
    await send("c", "one")
    assert contents(await data("bridge_receive", {"channel": "c", "since_seq": 0})) == [
        "one"
    ]


@pytest.mark.asyncio
async def test_since_id_and_since_seq_together_are_refused(fresh_db):
    first = await send("c", "one")
    with pytest.raises(BridgeValidationError) as err:
        await data(
            "bridge_receive", {"channel": "c", "since_id": first["id"], "since_seq": 1}
        )
    assert err.value.code == "conflict"


@pytest.mark.asyncio
async def test_wait_accepts_since_seq(fresh_db):
    first = await send("c", "one")
    await send("c", "two")
    result = await data(
        "bridge_wait", {"channel": "c", "since_seq": first["seq"], "timeout_seconds": 0}
    )
    assert contents(result) == ["two"] and result["timed_out"] is False


@pytest.mark.asyncio
async def test_wait_with_unknown_since_id_is_an_error(fresh_db):
    await send("c", "one")
    with pytest.raises(BridgeValidationError) as err:
        await data(
            "bridge_wait",
            {"channel": "c", "since_id": "ffffffff-dead", "timeout_seconds": 0},
        )
    assert err.value.code == "not_found"


@pytest.mark.asyncio
async def test_full_id_cursor_is_unchanged(fresh_db):
    first = await send("c", "one")
    await send("c", "two")
    result = await data("bridge_receive", {"channel": "c", "since_id": first["id"]})
    assert contents(result) == ["two"] and result["warning"] is None


def test_http_api_keeps_its_stale_cursor_warning(fresh_db):
    with TestClient(bridge.app, base_url="http://localhost") as client:
        client.post("/api/send", json={"channel": "c", "sender": "a", "content": "x"})
        body = client.get("/api/messages?channel=c&since_id=ffffffff-0000").json()
    assert body["warning"] == "since_id_not_found" and body["messages"] == []
