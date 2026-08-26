from __future__ import annotations

import asyncio

from hookrelay.backends.memory import MemoryBackend
from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy
from hookrelay.worker import Worker

FAST_POLICY = RetryPolicy(max_attempts=2, base_delay=0.01, max_delay=0.02, jitter=0.0)


async def test_worker_acks_on_successful_handler():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    await backend.enqueue(WebhookEvent(source="test", payload={}, max_attempts=2))

    processed = []

    async def handler(event):
        processed.append(event.id)

    worker = Worker(backend, handler)
    count = await worker.run_once()

    assert count == 1
    assert len(processed) == 1
    assert await backend.claim_due(10) == []


async def test_worker_retries_then_dead_letters_on_repeated_failure():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    event = WebhookEvent(source="test", payload={}, max_attempts=2)
    await backend.enqueue(event)

    async def always_fails(_event):
        raise RuntimeError("downstream is down")

    worker = Worker(backend, always_fails)

    await worker.run_once()
    await asyncio.sleep(0.05)
    await worker.run_once()

    dead_letters = await backend.list_dead_letters()
    assert len(dead_letters) == 1
    assert dead_letters[0].status == EventStatus.DEAD_LETTER
    assert dead_letters[0].last_error == "downstream is down"


async def test_concurrency_runs_handlers_in_the_same_batch_in_parallel():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    for _ in range(3):
        await backend.enqueue(WebhookEvent(source="test", payload={}, max_attempts=1))

    async def slow_handler(_event):
        await asyncio.sleep(0.1)

    worker = Worker(backend, slow_handler, concurrency=3)
    start = asyncio.get_event_loop().time()
    processed = await worker.run_once()
    elapsed = asyncio.get_event_loop().time() - start

    assert processed == 3
    assert elapsed < 0.25  # sequential would take roughly 0.3s


async def test_concurrency_still_acks_and_fails_each_event_individually():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    ok_event = WebhookEvent(source="test", payload={"ok": True}, max_attempts=2)
    bad_event = WebhookEvent(source="test", payload={"ok": False}, max_attempts=2)
    await backend.enqueue(ok_event)
    await backend.enqueue(bad_event)

    async def handler(event):
        if not event.payload["ok"]:
            raise RuntimeError("boom")

    worker = Worker(backend, handler, concurrency=2)
    await worker.run_once()

    assert await backend.claim_due(10) == []  # ok_event acked, bad_event rescheduled later
    await asyncio.sleep(0.05)
    reclaimed = await backend.claim_due(10)
    assert [e.id for e in reclaimed] == [bad_event.id]


async def test_concurrency_defaults_to_one_and_rejects_non_positive_values():
    backend = MemoryBackend(retry_policy=FAST_POLICY)

    async def handler(_event):
        pass

    Worker(backend, handler)  # default concurrency=1 does not raise

    try:
        Worker(backend, handler, concurrency=0)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for concurrency=0")


async def test_run_forever_stops_when_stop_is_called():
    backend = MemoryBackend(retry_policy=FAST_POLICY)

    async def handler(_event):
        pass

    worker = Worker(backend, handler, poll_interval=0.01)
    task = asyncio.create_task(worker.run_forever())
    await asyncio.sleep(0.03)
    worker.stop()
    await asyncio.wait_for(task, timeout=1.0)
    assert task.done()
