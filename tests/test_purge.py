"""purge() tests, run against both backends that implement it (PostgresBackend
and SQLiteBackend) through the `purge_backend` fixture in conftest.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from hookrelay.models import EventStatus, WebhookEvent


def make_event(**overrides) -> WebhookEvent:
    defaults = {"source": "test", "payload": {}, "max_attempts": 1}
    return WebhookEvent(**{**defaults, **overrides})


async def test_purge_removes_only_events_older_than_the_cutoff(purge_backend):
    old_success = make_event()
    await purge_backend.enqueue(old_success)
    await purge_backend.claim_due(limit=10)
    await purge_backend.ack(old_success.id)

    await asyncio.sleep(0.05)
    cutoff = datetime.now(timezone.utc)
    await asyncio.sleep(0.05)

    recent_dead_letter = make_event(max_attempts=1)
    await purge_backend.enqueue(recent_dead_letter)
    await purge_backend.claim_due(limit=10)
    await purge_backend.fail(recent_dead_letter.id, "fatal")

    purged = await purge_backend.purge(older_than=cutoff)

    assert purged == 1
    remaining_dead_letters = await purge_backend.list_dead_letters()
    assert [event.id for event in remaining_dead_letters] == [recent_dead_letter.id]


async def test_purge_defaults_to_success_and_dead_letter_statuses(purge_backend):
    dead_event = make_event(max_attempts=1)
    await purge_backend.enqueue(dead_event)
    await purge_backend.claim_due(limit=10)
    await purge_backend.fail(dead_event.id, "fatal")
    assert len(await purge_backend.list_dead_letters()) == 1

    far_future = datetime.now(timezone.utc) + timedelta(days=365)
    purged = await purge_backend.purge(older_than=far_future)

    assert purged == 1
    assert await purge_backend.list_dead_letters() == []


async def test_purge_can_target_a_narrower_set_of_statuses(purge_backend):
    dead_event = make_event(max_attempts=1)
    await purge_backend.enqueue(dead_event)
    await purge_backend.claim_due(limit=10)
    await purge_backend.fail(dead_event.id, "fatal")

    far_future = datetime.now(timezone.utc) + timedelta(days=365)
    purged = await purge_backend.purge(older_than=far_future, statuses=(EventStatus.SUCCESS,))

    assert purged == 0
    assert len(await purge_backend.list_dead_letters()) == 1


async def test_purge_leaves_pending_events_untouched(purge_backend):
    pending_event = make_event()
    await purge_backend.enqueue(pending_event)

    far_future = datetime.now(timezone.utc) + timedelta(days=365)
    purged = await purge_backend.purge(older_than=far_future)

    assert purged == 0
    claimed = await purge_backend.claim_due(limit=10)
    assert [event.id for event in claimed] == [pending_event.id]
