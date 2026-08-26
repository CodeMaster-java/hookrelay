"""Stale-claim recovery tests, specific to RedisBackend's claim lease."""

from __future__ import annotations

import asyncio

import pytest

from hookrelay.models import EventStatus, WebhookEvent

# Short enough that tests don't have to sleep long for the lease to expire.
SHORT_LEASE_SECONDS = 0.05


def make_event(**overrides) -> WebhookEvent:
    defaults = {"source": "test", "payload": {}, "max_attempts": 3}
    return WebhookEvent(**{**defaults, **overrides})


@pytest.mark.parametrize("redis_backend", [SHORT_LEASE_SECONDS], indirect=True)
async def test_reap_stale_claims_reschedules_an_expired_lease(redis_backend):
    event = make_event()
    await redis_backend.enqueue(event)
    claimed = await redis_backend.claim_due(limit=10)
    assert len(claimed) == 1

    await asyncio.sleep(SHORT_LEASE_SECONDS * 2)
    reaped = await redis_backend.reap_stale_claims()
    assert reaped == 1

    # reap_stale_claims() reuses fail(), which reschedules with the same backoff
    # delay as any other failure; wait for it to elapse before reclaiming.
    await asyncio.sleep(0.1)
    reclaimed = await redis_backend.claim_due(limit=10)
    assert [e.id for e in reclaimed] == [event.id]
    assert reclaimed[0].attempts == 1
    assert reclaimed[0].last_error is not None


@pytest.mark.parametrize("redis_backend", [SHORT_LEASE_SECONDS], indirect=True)
async def test_reap_stale_claims_dead_letters_once_attempts_are_exhausted(redis_backend):
    event = make_event(max_attempts=1)
    await redis_backend.enqueue(event)
    await redis_backend.claim_due(limit=10)

    await asyncio.sleep(SHORT_LEASE_SECONDS * 2)
    reaped = await redis_backend.reap_stale_claims()

    assert reaped == 1
    dead_letters = await redis_backend.list_dead_letters()
    assert [e.id for e in dead_letters] == [event.id]
    assert dead_letters[0].status == EventStatus.DEAD_LETTER


@pytest.mark.parametrize("redis_backend", [SHORT_LEASE_SECONDS], indirect=True)
async def test_reap_stale_claims_ignores_leases_that_have_not_expired(redis_backend):
    event = make_event()
    await redis_backend.enqueue(event)
    await redis_backend.claim_due(limit=10)

    reaped = await redis_backend.reap_stale_claims()

    assert reaped == 0


@pytest.mark.parametrize("redis_backend", [SHORT_LEASE_SECONDS], indirect=True)
async def test_acked_events_are_not_reaped(redis_backend):
    event = make_event()
    await redis_backend.enqueue(event)
    await redis_backend.claim_due(limit=10)
    await redis_backend.ack(event.id)

    await asyncio.sleep(SHORT_LEASE_SECONDS * 2)
    reaped = await redis_backend.reap_stale_claims()

    assert reaped == 0
