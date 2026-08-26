from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from redis.asyncio import Redis

from hookrelay.backends.base import Backend, apply_failure
from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy

_IDEMPOTENCY_TTL_SECONDS = 7 * 24 * 60 * 60


class RedisBackend(Backend):
    """Redis-backed implementation.

    Suitable for a small number of workers: claiming relies on `ZREM` being atomic
    per member, which guarantees an event is only ever claimed once, but there is no
    stale-claim recovery if a worker crashes mid-processing after claiming an event.
    For that guarantee, or for very high worker concurrency, prefer PostgresBackend.
    """

    def __init__(
        self,
        redis: Redis,
        retry_policy: RetryPolicy | None = None,
        namespace: str = "hookrelay",
    ) -> None:
        super().__init__(retry_policy)
        self._redis = redis
        self._namespace = namespace

    def _event_key(self, event_id: str) -> str:
        return f"{self._namespace}:event:{event_id}"

    def _idempotency_key(self, key: str) -> str:
        return f"{self._namespace}:idempotency:{key}"

    @property
    def _schedule_key(self) -> str:
        return f"{self._namespace}:schedule"

    @property
    def _dead_letter_key(self) -> str:
        return f"{self._namespace}:dead_letter"

    async def _get_event(self, event_id: str) -> WebhookEvent | None:
        data = await self._redis.get(self._event_key(event_id))
        return WebhookEvent.model_validate_json(data) if data is not None else None

    async def _save_event(self, event: WebhookEvent) -> None:
        await self._redis.set(self._event_key(event.id), event.model_dump_json())

    async def enqueue(self, event: WebhookEvent) -> bool:
        if event.idempotency_key is not None:
            is_new = await self._redis.set(
                self._idempotency_key(event.idempotency_key),
                event.id,
                nx=True,
                ex=_IDEMPOTENCY_TTL_SECONDS,
            )
            if not is_new:
                return False
        await self._save_event(event)
        await self._redis.zadd(self._schedule_key, {event.id: event.next_retry_at.timestamp()})
        return True

    async def claim_due(self, limit: int) -> list[WebhookEvent]:
        now = datetime.now(timezone.utc).timestamp()
        candidate_ids = cast(
            list[bytes | str],
            await self._redis.zrangebyscore(self._schedule_key, min=0, max=now, start=0, num=limit),
        )
        claimed: list[WebhookEvent] = []
        for raw_id in candidate_ids:
            event_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            removed = await self._redis.zrem(self._schedule_key, event_id)
            if not removed:
                continue  # another worker claimed it between the read and this ZREM
            event = await self._get_event(event_id)
            if event is None:
                continue
            processing_event = event.model_copy(update={"status": EventStatus.PROCESSING})
            await self._save_event(processing_event)
            claimed.append(processing_event)
        return claimed

    async def ack(self, event_id: str) -> None:
        await self._redis.delete(self._event_key(event_id))

    async def fail(self, event_id: str, error: str) -> None:
        event = await self._get_event(event_id)
        if event is None:
            return
        updated_event = apply_failure(event, error, self.retry_policy)
        await self._save_event(updated_event)
        if updated_event.status is EventStatus.DEAD_LETTER:
            await self._redis.zadd(
                self._dead_letter_key, {event_id: updated_event.updated_at.timestamp()}
            )
        else:
            await self._redis.zadd(
                self._schedule_key, {event_id: updated_event.next_retry_at.timestamp()}
            )

    async def list_dead_letters(self, limit: int = 100, offset: int = 0) -> list[WebhookEvent]:
        end = offset + limit - 1
        ids = cast(
            list[bytes | str],
            await self._redis.zrevrange(self._dead_letter_key, start=offset, end=end),
        )
        events = []
        for raw_id in ids:
            event_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            event = await self._get_event(event_id)
            if event is not None:
                events.append(event)
        return events

    async def requeue_dead_letter(self, event_id: str) -> bool:
        removed = await self._redis.zrem(self._dead_letter_key, event_id)
        if not removed:
            return False
        event = await self._get_event(event_id)
        if event is None:
            return False
        now = datetime.now(timezone.utc)
        requeued_event = event.model_copy(
            update={
                "status": EventStatus.PENDING,
                "attempts": 0,
                "last_error": None,
                "next_retry_at": now,
                "updated_at": now,
            }
        )
        await self._save_event(requeued_event)
        await self._redis.zadd(self._schedule_key, {event_id: now.timestamp()})
        return True
