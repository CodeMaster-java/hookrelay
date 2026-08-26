"""Contract tests run against every backend (memory, postgres, redis) through the
`backend` fixture in conftest.py, so all three are held to identical behavior."""

from __future__ import annotations

from hookrelay.models import EventStatus, WebhookEvent


def make_event(**overrides) -> WebhookEvent:
    defaults = {"source": "test", "payload": {"hello": "world"}, "max_attempts": 3}
    return WebhookEvent(**{**defaults, **overrides})


async def test_enqueue_then_claim_returns_the_event(backend):
    event = make_event()
    accepted = await backend.enqueue(event)
    assert accepted is True

    claimed = await backend.claim_due(limit=10)
    assert [e.id for e in claimed] == [event.id]
    assert claimed[0].status == EventStatus.PROCESSING
    assert claimed[0].payload == {"hello": "world"}


async def test_claim_only_returns_events_that_are_due(backend):
    from datetime import datetime, timedelta, timezone

    future_event = make_event(next_retry_at=datetime.now(timezone.utc) + timedelta(hours=1))
    await backend.enqueue(future_event)

    claimed = await backend.claim_due(limit=10)
    assert claimed == []


async def test_claim_does_not_return_the_same_event_twice(backend):
    event = make_event()
    await backend.enqueue(event)

    first_batch = await backend.claim_due(limit=10)
    second_batch = await backend.claim_due(limit=10)
    assert len(first_batch) == 1
    assert second_batch == []


async def test_ack_marks_event_as_no_longer_claimable(backend):
    event = make_event()
    await backend.enqueue(event)
    await backend.claim_due(limit=10)

    await backend.ack(event.id)

    assert await backend.claim_due(limit=10) == []
    assert await backend.list_dead_letters() == []


async def test_fail_reschedules_until_max_attempts_then_dead_letters(backend):
    event = make_event(max_attempts=2)
    await backend.enqueue(event)

    await backend.claim_due(limit=10)
    await backend.fail(event.id, "boom 1")
    dead_letters = await backend.list_dead_letters()
    assert dead_letters == []  # first failure just reschedules

    import asyncio

    await asyncio.sleep(0.1)  # let the (tiny, test-config) backoff delay elapse
    reclaimed = await backend.claim_due(limit=10)
    assert len(reclaimed) == 1
    assert reclaimed[0].attempts == 1

    await backend.fail(event.id, "boom 2")
    dead_letters = await backend.list_dead_letters()
    assert len(dead_letters) == 1
    assert dead_letters[0].id == event.id
    assert dead_letters[0].status == EventStatus.DEAD_LETTER
    assert dead_letters[0].last_error == "boom 2"


async def test_requeue_dead_letter_makes_it_claimable_again(backend):
    event = make_event(max_attempts=1)
    await backend.enqueue(event)
    await backend.claim_due(limit=10)
    await backend.fail(event.id, "fatal")
    assert len(await backend.list_dead_letters()) == 1

    requeued = await backend.requeue_dead_letter(event.id)
    assert requeued is True

    claimed = await backend.claim_due(limit=10)
    assert [e.id for e in claimed] == [event.id]
    assert claimed[0].attempts == 0


async def test_requeue_dead_letter_returns_false_for_unknown_id(backend):
    assert await backend.requeue_dead_letter("does-not-exist") is False


async def test_idempotency_key_prevents_duplicate_enqueue(backend):
    first = make_event(idempotency_key="order-123")
    second = make_event(idempotency_key="order-123")

    assert await backend.enqueue(first) is True
    assert await backend.enqueue(second) is False

    claimed = await backend.claim_due(limit=10)
    assert len(claimed) == 1
