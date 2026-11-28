"""Progress line: a worker's status apart from last_error."""

from __future__ import annotations

import pytest

import claude_bridge.server as bridge
from claude_bridge.validation import BridgeValidationError

from .legacy_schema import create_v130_database


async def call(name, args):
    return await bridge.dispatch_tool(name, args, structured=True)


async def data(name, args):
    _content, result = await call(name, args)
    return result


async def enqueue_and_claim():
    await data("bridge_enqueue", {"channel": "jobs", "content": "work"})
    claimed = await data("bridge_claim", {"channel": "jobs", "consumer": "w1"})
    return claimed["task"]


def lease_args(task):
    return {
        "channel": "jobs",
        "task_id": task["task_id"],
        "lease_token": task["lease_token"],
    }


@pytest.mark.asyncio
async def test_extend_sets_progress_and_leaves_last_error_alone(fresh_db):
    task = await enqueue_and_claim()
    extended = await data(
        "bridge_extend", {**lease_args(task), "progress": "tests 3/7 green"}
    )
    assert extended["progress"] == "tests 3/7 green"
    assert extended["last_error"] is None


@pytest.mark.asyncio
async def test_progress_shows_in_the_task_listing(fresh_db):
    task = await enqueue_and_claim()
    await data("bridge_extend", {**lease_args(task), "progress": "building"})
    content, listing = await call("bridge_tasks", {"channel": "jobs"})
    assert listing["tasks"][0]["progress"] == "building"
    assert "progress: building" in content[0].text


@pytest.mark.asyncio
async def test_release_leaves_progress_on_the_pending_task(fresh_db):
    task = await enqueue_and_claim()
    released = await data(
        "bridge_release", {**lease_args(task), "progress": "waiting for gate"}
    )
    assert released["progress"] == "waiting for gate"


@pytest.mark.asyncio
async def test_extend_without_progress_keeps_the_last_one(fresh_db):
    task = await enqueue_and_claim()
    await data("bridge_extend", {**lease_args(task), "progress": "step 1"})
    kept = await data("bridge_extend", lease_args(task))
    assert kept["progress"] == "step 1"


@pytest.mark.asyncio
async def test_empty_progress_clears_it(fresh_db):
    task = await enqueue_and_claim()
    await data("bridge_extend", {**lease_args(task), "progress": "step 1"})
    cleared = await data("bridge_extend", {**lease_args(task), "progress": ""})
    assert cleared["progress"] is None


@pytest.mark.asyncio
async def test_note_can_set_the_progress(fresh_db):
    task = await enqueue_and_claim()
    await data(
        "bridge_task_note",
        {"channel": "jobs", "task_id": task["task_id"], "sender": "w1",
         "content": "detail for the lead", "progress": "half done"},
    )
    listing = await data("bridge_tasks", {"channel": "jobs"})
    assert listing["tasks"][0]["progress"] == "half done"


@pytest.mark.asyncio
async def test_progress_is_one_bounded_line(fresh_db):
    task = await enqueue_and_claim()
    for bad in ("two\nlines", "x" * 301):
        with pytest.raises(BridgeValidationError) as err:
            await data("bridge_extend", {**lease_args(task), "progress": bad})
        assert err.value.field == "progress"


@pytest.mark.asyncio
async def test_legacy_database_gains_the_progress_column(tmp_path, monkeypatch):
    db_path = tmp_path / "old.db"
    create_v130_database(str(db_path), pending_task_id="tsk_legacy")
    monkeypatch.setattr(bridge, "DB_PATH", str(db_path))
    monkeypatch.setattr(bridge, "_conn", None)
    try:
        claimed = await data("bridge_claim", {"channel": "old", "consumer": "w1"})
        assert claimed["task"]["progress"] is None
    finally:
        bridge._conn.close()
        monkeypatch.setattr(bridge, "_conn", None)


@pytest.mark.asyncio
async def test_a_new_claim_starts_without_the_old_progress(fresh_db):
    task = await enqueue_and_claim()
    await data("bridge_release", {**lease_args(task), "progress": "gave it back"})
    again = await data("bridge_claim", {"channel": "jobs", "consumer": "w2"})
    assert again["task"]["progress"] is None


@pytest.mark.asyncio
async def test_replace_clears_the_progress(fresh_db):
    enq = await data(
        "bridge_enqueue", {"channel": "jobs", "content": "v1", "enqueued_by": "lead"}
    )
    claimed = await data("bridge_claim", {"channel": "jobs", "consumer": "w1"})
    await data("bridge_release", {**lease_args(claimed["task"]), "progress": "blocked"})
    replaced = await data(
        "bridge_replace",
        {"channel": "jobs", "task_id": enq["task_id"], "sender": "lead", "content": "v2"},
    )
    assert replaced["progress"] is None
