from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from hookrelay.backends.base import Backend, apply_failure
from hookrelay.exceptions import EventNotFoundError
from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy


class MemoryBackend(Backend):
    """In-process backend backed by a plain dict.

    Intended for local development, examples, and tests: it has no persistence
    and does not coordinate across processes. Use PostgresBackend or RedisBackend
    in production.
    """

    def __init__(self, retry_policy: RetryPolicy | None = None) -> None:
        super().__init__(retry_policy)
        self._events: dict[str, WebhookEvent] = {}
        self._idempotency_keys: set[str] = set()
        self._lock = asyncio.Lock()

    async def enqueue(self, event: WebhookEvent) -> bool:
        async with self._lock:
            if event.idempotency_key is not None:
                if event.idempotency_key in self._idempotency_keys:
                    return False
                self._idempotency_keys.add(event.idempotency_key)
            self._events[event.id] = event
            return True

    async def claim_due(self, limit: int) -> list[WebhookEvent]:
        now = datetime.now(timezone.utc)
        async with self._lock:
            due = sorted(
                (
                    e
                    for e in self._events.values()
                    if e.status == EventStatus.PENDING and e.next_retry_at <= now
                ),
                key=lambda e: e.next_retry_at,
            )[:limit]
            claimed = [e.model_copy(update={"status": EventStatus.PROCESSING}) for e in due]
            for event in claimed:
                self._events[event.id] = event
            return claimed

    async def ack(self, event_id: str) -> None:
        async with self._lock:
            event = self._events.get(event_id)
            if event is not None:
                self._events[event_id] = event.model_copy(update={"status": EventStatus.SUCCESS})

    async def fail(self, event_id: str, error: str) -> WebhookEvent:
        async with self._lock:
            event = self._events.get(event_id)
            if event is None:
                raise EventNotFoundError(event_id)
            updated = apply_failure(event, error, self.retry_policy)
            self._events[event_id] = updated
            return updated

    async def list_dead_letters(self, limit: int = 100, offset: int = 0) -> list[WebhookEvent]:
        async with self._lock:
            dead = sorted(
                (e for e in self._events.values() if e.status == EventStatus.DEAD_LETTER),
                key=lambda e: e.updated_at,
                reverse=True,
            )
            return dead[offset : offset + limit]

    async def requeue_dead_letter(self, event_id: str) -> bool:
        async with self._lock:
            event = self._events.get(event_id)
            if event is None or event.status != EventStatus.DEAD_LETTER:
                return False
            now = datetime.now(timezone.utc)
            self._events[event_id] = event.model_copy(
                update={
                    "status": EventStatus.PENDING,
                    "attempts": 0,
                    "last_error": None,
                    "next_retry_at": now,
                    "updated_at": now,
                }
            )
            return True
