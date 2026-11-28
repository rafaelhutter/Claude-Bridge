"""Release and lease extension: a healthy worker never uses up attempts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import claude_bridge.server as bridge
from claude_bridge.task_leases import extend_lease, release_task
from claude_bridge.taskqueue import TaskLeaseError, TaskStatus
from claude_bridge.validation import BridgeValidationError

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


async def data(name, args):
    _content, result = await bridge.dispatch_tool(name, args, structured=True)
    return result


async def enqueue_and_claim(max_attempts=2):
    enq = await data(
        "bridge_enqueue",
        {"channel": "jobs", "content": "work", "max_attempts": max_attempts},
    )
    claimed = await data("bridge_claim", {"channel": "jobs", "consumer": "w1"})
    assert claimed["task"]["task_id"] == enq["task_id"]
    return claimed["task"]


def lease_args(task):
    return {
        "channel": "jobs",
        "task_id": task["task_id"],
        "lease_token": task["lease_token"],
    }


@pytest.mark.asyncio
async def test_release_refunds_the_attempt_and_keeps_last_error(fresh_db):
    task = await enqueue_and_claim()
    released = await data("bridge_release", lease_args(task))
    assert released["status"] == "pending"
    assert released["attempts"] == 0
    assert released["lease_owner"] is None and released["last_error"] is None


@pytest.mark.asyncio
async def test_repeated_releases_never_dead_letter_a_task(fresh_db):
    task = await enqueue_and_claim(max_attempts=1)
    await data("bridge_release", lease_args(task))
    for _ in range(4):
        claimed = await data("bridge_claim", {"channel": "jobs", "consumer": "w1"})
        await data("bridge_release", lease_args(claimed["task"]))
    listing = await data("bridge_tasks", {"channel": "jobs"})
    assert listing["counts"] == {**listing["counts"], "pending": 1, "dead": 0}


@pytest.mark.asyncio
async def test_release_with_delay_hides_the_task_for_that_time(fresh_db):
    task = await enqueue_and_claim()
    await data("bridge_release", {**lease_args(task), "delay_seconds": 600})
    again = await data("bridge_claim", {"channel": "jobs", "consumer": "w2"})
    assert again["task"] is None


@pytest.mark.asyncio
async def test_released_lease_token_is_fenced(fresh_db):
    task = await enqueue_and_claim()
    await data("bridge_release", lease_args(task))
    with pytest.raises(BridgeValidationError) as err:
        await data("bridge_complete", lease_args(task))
    assert err.value.code == "lost"


@pytest.mark.asyncio
async def test_extend_moves_the_expiry_and_keeps_attempts(fresh_db):
    task = await enqueue_and_claim()
    extended = await data("bridge_extend", {**lease_args(task), "lease_seconds": 3600})
    assert extended["status"] == "claimed" and extended["attempts"] == 1
    assert extended["lease_expires_at"] > task["lease_expires_at"]
    assert extended["unread_notes"] == []


@pytest.mark.asyncio
async def test_extend_returns_unread_notes_without_marking_them_read(fresh_db):
    task = await enqueue_and_claim()
    await data(
        "bridge_task_note",
        {"channel": "jobs", "task_id": task["task_id"], "sender": "lead",
         "content": "use branch b"},
    )
    extended = await data("bridge_extend", lease_args(task))
    assert [n["content"] for n in extended["unread_notes"]] == ["use branch b"]
    notes = await data("bridge_task_notes", lease_args(task))
    assert [n["content"] for n in notes["notes"]] == ["use branch b"]


@pytest.mark.asyncio
async def test_extend_needs_the_live_lease_token(fresh_db):
    task = await enqueue_and_claim()
    with pytest.raises(BridgeValidationError) as err:
        await data("bridge_extend", {**lease_args(task), "lease_token": "stale"})
    assert err.value.code == "lost"


def test_extended_lease_survives_its_original_expiry(fresh_db):
    store = bridge.task_store()
    store.enqueue(channel="jobs", payload="p", max_attempts=1, now=at(0))
    task = store.claim(channel="jobs", consumer="w1", lease_seconds=60, now=at(0))
    extend_lease(
        store, channel="jobs", task_id=task.id, lease_token=task.lease_token,
        lease_seconds=600, now=at(50),
    )
    sweep = store.requeue_expired(now=at(120))
    assert sweep.requeued == 0 and sweep.dead_lettered == 0
    assert store.find("jobs", task.id).status is TaskStatus.CLAIMED


def test_an_expired_lease_still_counts_as_an_attempt(fresh_db):
    store = bridge.task_store()
    store.enqueue(channel="jobs", payload="p", max_attempts=1, now=at(0))
    store.claim(channel="jobs", consumer="w1", lease_seconds=60, now=at(0))
    assert store.requeue_expired(now=at(120)).dead_lettered == 1


def test_release_after_reclaim_is_refused(fresh_db):
    store = bridge.task_store()
    store.enqueue(channel="jobs", payload="p", max_attempts=3, now=at(0))
    first = store.claim(channel="jobs", consumer="w1", lease_seconds=60, now=at(0))
    store.claim(channel="jobs", consumer="w2", lease_seconds=60, now=at(120))
    with pytest.raises(TaskLeaseError):
        release_task(
            store, channel="jobs", task_id=first.id, lease_token=first.lease_token,
            now=at(130),
        )
