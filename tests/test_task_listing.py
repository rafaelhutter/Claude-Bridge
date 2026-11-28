"""Lean bridge_tasks: filters and compact rows keep a listing small."""

from __future__ import annotations

import pytest

import claude_bridge.server as bridge
from claude_bridge.task_listing import COMPACT_FIELDS
from claude_bridge.validation import BridgeValidationError


async def data(name, args):
    _content, result = await bridge.dispatch_tool(name, args, structured=True)
    return result


async def listing(**args):
    return await data("bridge_tasks", {"channel": "jobs", **args})


def ids(result):
    return [t["task_id"] for t in result["tasks"]]


async def seed_queue():
    """One completed task done by w1, one claimed by w2, one pending for w3."""
    done = await data("bridge_enqueue", {"channel": "jobs", "content": "a"})
    claimed = await data("bridge_claim", {"channel": "jobs", "consumer": "w1"})
    await data(
        "bridge_complete",
        {"channel": "jobs", "task_id": done["task_id"],
         "lease_token": claimed["task"]["lease_token"], "result": {"big": "x" * 500}},
    )
    held = await data("bridge_enqueue", {"channel": "jobs", "content": "b"})
    await data("bridge_claim", {"channel": "jobs", "consumer": "w2"})
    waiting = await data(
        "bridge_enqueue", {"channel": "jobs", "content": "c", "assignee": "w3"}
    )
    return done["task_id"], held["task_id"], waiting["task_id"]


@pytest.mark.asyncio
async def test_exclude_terminal_drops_finished_tasks(fresh_db):
    done, held, waiting = await seed_queue()
    assert ids(await listing(exclude_terminal=True)) == [waiting, held]


@pytest.mark.asyncio
async def test_status_accepts_a_list(fresh_db):
    done, held, _waiting = await seed_queue()
    assert ids(await listing(status=["completed", "claimed"])) == [held, done]


@pytest.mark.asyncio
async def test_single_status_string_still_works(fresh_db):
    done, _held, _waiting = await seed_queue()
    assert ids(await listing(status="completed")) == [done]


@pytest.mark.asyncio
async def test_exclude_terminal_with_only_terminal_statuses_lists_nothing(fresh_db):
    await seed_queue()
    result = await listing(status=["completed"], exclude_terminal=True)
    assert result["tasks"] == [] and result["counts"]["completed"] == 1


@pytest.mark.asyncio
async def test_filter_by_assignee_and_worked_by(fresh_db):
    done, held, waiting = await seed_queue()
    assert ids(await listing(assignee="w3")) == [waiting]
    assert ids(await listing(worked_by="w1")) == [done]


@pytest.mark.asyncio
async def test_compact_rows_carry_only_the_steering_fields(fresh_db):
    await seed_queue()
    rows = (await listing(compact=True))["tasks"]
    core = {"task_id", "title", "status", "attempts", "assignee", "lease_owner"}
    assert all(core <= set(row) <= set(COMPACT_FIELDS) for row in rows)


@pytest.mark.asyncio
async def test_default_listing_keeps_full_rows(fresh_db):
    done, _held, _waiting = await seed_queue()
    row = next(t for t in (await listing())["tasks"] if t["task_id"] == done)
    assert row["result"] == '{"big":"' + "x" * 500 + '"}'


@pytest.mark.asyncio
async def test_unknown_status_in_a_list_is_rejected(fresh_db):
    with pytest.raises(BridgeValidationError) as err:
        await listing(status=["pending", "nope"])
    assert err.value.field == "status"


@pytest.mark.asyncio
async def test_unknown_status_is_rejected_even_with_exclude_terminal(fresh_db):
    with pytest.raises(BridgeValidationError) as err:
        await listing(status="bogus", exclude_terminal=True)
    assert err.value.field == "status"


@pytest.mark.asyncio
async def test_bad_assignee_filter_is_rejected_before_an_empty_status_match(fresh_db):
    with pytest.raises(BridgeValidationError) as err:
        await listing(status=["completed"], exclude_terminal=True, assignee=" w")
    assert err.value.field == "assignee"
