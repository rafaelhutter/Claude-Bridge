"""Worker-addressed tasks: an assignee limits who may claim a task."""

from __future__ import annotations

import pytest

import claude_bridge.server as bridge
from claude_bridge.validation import BridgeValidationError

from .legacy_schema import create_v130_database


async def data(name, args):
    _content, result = await bridge.dispatch_tool(name, args, structured=True)
    return result


async def enqueue(**args):
    return await data(
        "bridge_enqueue",
        {"channel": "jobs", "content": "work", "enqueued_by": "lead", **args},
    )


async def claim(consumer):
    result = await data("bridge_claim", {"channel": "jobs", "consumer": consumer})
    return result["task"]


async def task_row(task_id):
    listing = await data("bridge_tasks", {"channel": "jobs"})
    return next(t for t in listing["tasks"] if t["task_id"] == task_id)


@pytest.mark.asyncio
async def test_other_worker_skips_an_assigned_task(fresh_db):
    enq = await enqueue(assignee="w2")
    assert enq["assignee"] == "w2"
    assert await claim("w1") is None
    claimed = await claim("w2")
    assert claimed["task_id"] == enq["task_id"] and claimed["assignee"] == "w2"


@pytest.mark.asyncio
async def test_a_skipped_assigned_task_keeps_its_attempts(fresh_db):
    enq = await enqueue(assignee="w2")
    await claim("w1")
    assert (await task_row(enq["task_id"]))["attempts"] == 0


@pytest.mark.asyncio
async def test_claim_passes_over_an_assigned_task_to_a_later_open_one(fresh_db):
    await enqueue(assignee="w2", priority=5)
    open_task = await enqueue()
    assert (await claim("w1"))["task_id"] == open_task["task_id"]


@pytest.mark.asyncio
async def test_unassigned_task_stays_claimable_by_any_worker(fresh_db):
    enq = await enqueue()
    assert enq["assignee"] is None
    assert (await claim("anyone"))["task_id"] == enq["task_id"]


@pytest.mark.asyncio
async def test_assign_readdresses_a_pending_task(fresh_db):
    enq = await enqueue(assignee="w2")
    assigned = await data(
        "bridge_assign",
        {"channel": "jobs", "task_id": enq["task_id"], "sender": "lead",
         "assignee": "w1"},
    )
    assert assigned["assignee"] == "w1"
    assert (await claim("w1"))["task_id"] == enq["task_id"]


@pytest.mark.asyncio
async def test_assign_null_opens_the_task_to_every_worker(fresh_db):
    enq = await enqueue(assignee="w2")
    await data(
        "bridge_assign",
        {"channel": "jobs", "task_id": enq["task_id"], "sender": "lead",
         "assignee": None},
    )
    assert (await claim("w1"))["task_id"] == enq["task_id"]


@pytest.mark.asyncio
async def test_only_the_enqueuer_may_assign(fresh_db):
    enq = await enqueue(assignee="w2")
    with pytest.raises(BridgeValidationError) as err:
        await data(
            "bridge_assign",
            {"channel": "jobs", "task_id": enq["task_id"], "sender": "w2",
             "assignee": "w2-other"},
        )
    assert err.value.code == "forbidden"


@pytest.mark.asyncio
async def test_a_finished_task_cannot_be_assigned(fresh_db):
    enq = await enqueue()
    claimed = await claim("w1")
    await data(
        "bridge_complete",
        {"channel": "jobs", "task_id": enq["task_id"],
         "lease_token": claimed["lease_token"]},
    )
    with pytest.raises(BridgeValidationError) as err:
        await data(
            "bridge_assign",
            {"channel": "jobs", "task_id": enq["task_id"], "sender": "lead",
             "assignee": "w2"},
        )
    assert err.value.code == "not_active"


@pytest.mark.asyncio
async def test_replace_keeps_the_assignee_unless_given(fresh_db):
    enq = await enqueue(assignee="w2")
    args = {"channel": "jobs", "task_id": enq["task_id"], "sender": "lead"}
    kept = await data("bridge_replace", {**args, "content": "v2"})
    assert kept["assignee"] == "w2"
    moved = await data("bridge_replace", {**args, "content": "v3", "assignee": "w3"})
    assert moved["assignee"] == "w3"
    cleared = await data("bridge_replace", {**args, "content": "v4", "assignee": None})
    assert cleared["assignee"] is None


@pytest.mark.asyncio
async def test_assignee_must_be_a_valid_consumer_name(fresh_db):
    with pytest.raises(BridgeValidationError) as err:
        await enqueue(assignee=" w2")
    assert err.value.field == "assignee"


@pytest.mark.asyncio
async def test_legacy_database_gains_the_assignee_column(tmp_path, monkeypatch):
    db_path = tmp_path / "old.db"
    create_v130_database(str(db_path), pending_task_id="tsk_legacy")
    monkeypatch.setattr(bridge, "DB_PATH", str(db_path))
    monkeypatch.setattr(bridge, "_conn", None)
    try:
        claimed = await data("bridge_claim", {"channel": "old", "consumer": "w1"})
        assert claimed["task"]["task_id"] == "tsk_legacy"
        assert claimed["task"]["assignee"] is None
    finally:
        bridge._conn.close()
        monkeypatch.setattr(bridge, "_conn", None)
